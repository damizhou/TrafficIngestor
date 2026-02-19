#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   sudo bash set_nofile_limits.sh [username]
# Optional env vars:
#   SOFT_LIMIT=65535 HARD_LIMIT=65535 SYS_NR_OPEN=1048576 SYS_FILE_MAX=2097152
#   ENABLE_DOCKER=1 RESTART_DOCKER=0

TARGET_USER="${1:-${SUDO_USER:-${USER:-pcz}}}"
SOFT_LIMIT="${SOFT_LIMIT:-65535}"
HARD_LIMIT="${HARD_LIMIT:-65535}"
SYS_NR_OPEN="${SYS_NR_OPEN:-1048576}"
SYS_FILE_MAX="${SYS_FILE_MAX:-2097152}"
ENABLE_DOCKER="${ENABLE_DOCKER:-1}"
RESTART_DOCKER="${RESTART_DOCKER:-0}"

LIMITS_FILE="/etc/security/limits.d/99-${TARGET_USER}-nofile.conf"
SYSCTL_FILE="/etc/sysctl.d/99-nofile.conf"
SYSTEMD_SYSTEM_FILE="/etc/systemd/system.conf.d/99-nofile.conf"
SYSTEMD_USER_FILE="/etc/systemd/user.conf.d/99-nofile.conf"
DOCKER_DAEMON_JSON="/etc/docker/daemon.json"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "ERROR: Please run as root, e.g. sudo bash $0 ${TARGET_USER}"
  exit 1
fi

echo "== Target user: ${TARGET_USER}"
echo "== Desired nofile soft/hard: ${SOFT_LIMIT}/${HARD_LIMIT}"
echo

echo "== Current values (before) =="
echo "shell soft: $(ulimit -Sn)"
echo "shell hard: $(ulimit -Hn)"
echo "fs.nr_open: $(sysctl -n fs.nr_open 2>/dev/null || echo N/A)"
echo "fs.file-max: $(sysctl -n fs.file-max 2>/dev/null || echo N/A)"
echo

install -d /etc/security/limits.d
cat > "${LIMITS_FILE}" <<EOF
${TARGET_USER} soft nofile ${SOFT_LIMIT}
${TARGET_USER} hard nofile ${HARD_LIMIT}
root soft nofile ${SOFT_LIMIT}
root hard nofile ${HARD_LIMIT}
EOF
echo "Wrote ${LIMITS_FILE}"

for pamf in /etc/pam.d/common-session /etc/pam.d/common-session-noninteractive; do
  if [[ -f "${pamf}" ]]; then
    if ! grep -Eq '^[[:space:]]*session[[:space:]]+required[[:space:]]+pam_limits\.so' "${pamf}"; then
      echo 'session required pam_limits.so' >> "${pamf}"
      echo "Patched ${pamf} (added pam_limits.so)"
    fi
  fi
done

install -d /etc/sysctl.d
cat > "${SYSCTL_FILE}" <<EOF
fs.nr_open = ${SYS_NR_OPEN}
fs.file-max = ${SYS_FILE_MAX}
EOF
echo "Wrote ${SYSCTL_FILE}"
sysctl --system >/dev/null
echo "Applied sysctl settings"

install -d /etc/systemd/system.conf.d /etc/systemd/user.conf.d
cat > "${SYSTEMD_SYSTEM_FILE}" <<EOF
[Manager]
DefaultLimitNOFILE=${HARD_LIMIT}
EOF
cat > "${SYSTEMD_USER_FILE}" <<EOF
[Manager]
DefaultLimitNOFILE=${HARD_LIMIT}
EOF
echo "Wrote ${SYSTEMD_SYSTEM_FILE}"
echo "Wrote ${SYSTEMD_USER_FILE}"

if [[ "${ENABLE_DOCKER}" == "1" || "${ENABLE_DOCKER}" == "true" ]]; then
  if command -v docker >/dev/null 2>&1; then
    if command -v python3 >/dev/null 2>&1; then
      python3 - "$DOCKER_DAEMON_JSON" "$SOFT_LIMIT" "$HARD_LIMIT" <<'PY'
import json
import pathlib
import sys

p = pathlib.Path(sys.argv[1])
soft = int(sys.argv[2])
hard = int(sys.argv[3])
data = {}
if p.exists():
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        backup = p.with_suffix(p.suffix + ".bak")
        p.rename(backup)
        data = {}
ul = data.setdefault("default-ulimits", {})
ul["nofile"] = {"Name": "nofile", "Soft": soft, "Hard": hard}
p.parent.mkdir(parents=True, exist_ok=True)
p.write_text(json.dumps(data, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
print(str(p))
PY
      echo "Updated ${DOCKER_DAEMON_JSON}"
      if [[ "${RESTART_DOCKER}" == "1" || "${RESTART_DOCKER}" == "true" ]]; then
        systemctl restart docker
        echo "Docker restarted"
      else
        echo "NOTE: Docker not restarted. If needed, run: sudo systemctl restart docker"
      fi
    else
      echo "WARN: python3 not found, skip docker daemon.json patch"
    fi
  else
    echo "INFO: docker not found, skip docker settings"
  fi
fi

echo
echo "== Validation hints =="
echo "1) Re-login shell, then check:"
echo "   ulimit -Sn && ulimit -Hn"
echo "2) Check process limits:"
echo "   cat /proc/\$\$/limits | grep -i 'open files'"
echo "3) For running worker process:"
echo "   cat /proc/<PID>/limits | grep -i 'open files'"
echo "   ls /proc/<PID>/fd | wc -l"
echo
echo "Done."
