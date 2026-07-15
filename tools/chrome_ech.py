"""Chrome ECH driver facade and ECH evidence validation."""

import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from tools.base_chrome import (
    CHROME_BACKGROUND_BLOCKED_HOSTS,
    BaseChromeDriverFactory,
    add_cookies,
    build_browser_error_diagnostics,
    get_chrome_background_blocked_hosts,
    kill_chrome_processes,
    open_url_and_save_content,
    screenshot_full_page,
)


ECH_DOH_TEMPLATE = os.environ.get(
    "CHROME_ECH_DOH_TEMPLATE",
    "https://cloudflare-dns.com/dns-query",
)
ECH_NETLOG_PATH = os.environ.get("CHROME_ECH_NETLOG_PATH", "/tmp/netlog.json")
ECH_ENABLE_FEATURES = "EncryptedClientHello"
ECH_NETLOG_MARKERS = (
    "encrypted_client_hello",
    "encryptedclienthello",
    "ech_config",
    "echconfig",
    '"ech"',
)
ECH_KEYLOG_LABELS = (
    "ECH_SECRET",
    "ECH_CONFIG",
)
TLS13_KEYLOG_LABELS_FOR_HTTP = (
    "CLIENT_HANDSHAKE_TRAFFIC_SECRET",
    "SERVER_HANDSHAKE_TRAFFIC_SECRET",
    "CLIENT_TRAFFIC_SECRET_0",
    "SERVER_TRAFFIC_SECRET_0",
)
WIRESHARK_ECH_KEYLOG_LABELS = ECH_KEYLOG_LABELS + TLS13_KEYLOG_LABELS_FOR_HTTP


class EchChromeDriverFactory(BaseChromeDriverFactory):
    """Create Chrome with the ECH capture network configuration."""

    def build_managed_policy(self):
        policy = super().build_managed_policy()
        policy.update({
            "DnsOverHttpsMode": "secure",
            "DnsOverHttpsTemplates": ECH_DOH_TEMPLATE,
        })
        return policy

    def build_profile_local_state(self):
        local_state = super().build_profile_local_state()
        local_state["dns_over_https"] = {
            "mode": "secure",
            "templates": ECH_DOH_TEMPLATE,
        }
        return local_state

    def get_disabled_features(self, context):
        return tuple(
            feature
            for feature in super().get_disabled_features(context)
            if feature != "AsyncDns"
        )

    def disable_async_dns(self, context):
        return False

    def get_startup_arguments(self, context):
        return ("--disable-quic",)

    def get_netlog_path(self, context):
        return ECH_NETLOG_PATH

    def get_feature_arguments(self, context):
        if not context.get("force_ech", True):
            return ()
        return (
            f"--enable-features={ECH_ENABLE_FEATURES}",
            "--dns-over-https-mode=secure",
            f"--dns-over-https-templates={ECH_DOH_TEMPLATE}",
        )

    def configure_created_driver(self, browser, task_name, context):
        browser._traffic_ingestor_force_ech = bool(context.get("force_ech", True))
        browser._traffic_ingestor_ech_target = task_name or ""
        browser._traffic_ingestor_netlog_path = ECH_NETLOG_PATH


_FACTORY = EchChromeDriverFactory()


def create_chrome_driver(task_name=None, formatted_time=None, parsers=None,
                         enable_ssl_key_log=True, data_base_dir=None,
                         proxy_server=None, proxy_bypass_list=None, logger=None,
                         blocked_hosts=None, chrome_binary_path=None,
                         chromedriver_path=None, artifact_label=None,
                         force_ech=True):
    return _FACTORY.create_driver(
        task_name=task_name,
        formatted_time=formatted_time,
        parsers=parsers,
        enable_ssl_key_log=enable_ssl_key_log,
        data_base_dir=data_base_dir,
        proxy_server=proxy_server,
        proxy_bypass_list=proxy_bypass_list,
        logger=logger,
        blocked_hosts=blocked_hosts,
        chrome_binary_path=chrome_binary_path,
        chromedriver_path=chromedriver_path,
        artifact_label=artifact_label,
        context={"force_ech": force_ech},
    )


def _extract_hostname(value):
    candidate = (value or "").strip()
    if not candidate:
        return ""
    if "://" not in candidate:
        candidate = f"https://{candidate}"
    parsed = urlparse(candidate)
    return (parsed.hostname or "").strip(".").lower()


def _has_ech_netlog_marker(netlog_text):
    lowered = netlog_text.lower()
    return any(marker in lowered for marker in ECH_NETLOG_MARKERS)


def _read_text_file(path, max_bytes=80_000_000):
    with open(path, "rb") as f:
        data = f.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise RuntimeError(f"netlog too large for ECH validation: {path} > {max_bytes} bytes")
    return data.decode("utf-8", errors="ignore")


def summarize_ssl_key_log(ssl_key_path, max_bytes=20_000_000):
    """Return label-level diagnostics without copying secret values."""
    summary = {
        "path": ssl_key_path or "",
        "exists": bool(ssl_key_path and os.path.exists(ssl_key_path)),
        "labels": {},
        "line_count": 0,
        "missing_wireshark_ech_labels": list(WIRESHARK_ECH_KEYLOG_LABELS),
        "has_ech_decryption_secrets": False,
        "has_tls13_http_secrets": False,
        "wireshark_can_decrypt_true_sni_and_http": False,
    }
    if not summary["exists"]:
        return summary

    try:
        summary["size"] = os.path.getsize(ssl_key_path)
        text = _read_text_file(ssl_key_path, max_bytes=max_bytes)
    except Exception as e:
        summary["read_error"] = f"{type(e).__name__}: {e}"
        return summary

    labels = {}
    line_count = 0
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        line_count += 1
        label = line.split(None, 1)[0]
        labels[label] = labels.get(label, 0) + 1

    missing = [
        label for label in WIRESHARK_ECH_KEYLOG_LABELS
        if labels.get(label, 0) <= 0
    ]
    summary.update({
        "labels": labels,
        "line_count": line_count,
        "missing_wireshark_ech_labels": missing,
        "has_ech_decryption_secrets": all(labels.get(label, 0) > 0 for label in ECH_KEYLOG_LABELS),
        "has_tls13_http_secrets": all(labels.get(label, 0) > 0 for label in TLS13_KEYLOG_LABELS_FOR_HTTP),
        "wireshark_can_decrypt_true_sni_and_http": not missing,
    })
    return summary


def _validate_ssl_key_log_for_wireshark_ech(ssl_key_path):
    summary = summarize_ssl_key_log(ssl_key_path)
    if not summary["exists"]:
        return False, f"ech_keylog_missing={ssl_key_path or ''}", summary
    if summary.get("read_error"):
        return False, f"ech_keylog_read_failed={summary['read_error']}", summary
    missing = summary.get("missing_wireshark_ech_labels") or []
    if missing:
        return False, f"ech_keylog_missing_labels={','.join(missing)}", summary
    return True, "ech_keylog_wireshark_decryptable=true", summary


def _find_tshark():
    explicit = os.environ.get("TSHARK_PATH", "").strip()
    candidates = [explicit] if explicit else []
    candidates.append("tshark")
    for candidate in candidates:
        if not candidate:
            continue
        resolved = shutil.which(candidate) if candidate == "tshark" else candidate
        if resolved and os.path.exists(resolved):
            return resolved
    return ""


def _list_tls_sni_from_pcap(pcap_path):
    tshark = _find_tshark()
    if not tshark:
        return None, "tshark_unavailable"

    cmd = [
        tshark,
        "-r",
        pcap_path,
        "-Y",
        "tls.handshake.type==1 && tls.handshake.extensions_server_name",
        "-T",
        "fields",
        "-e",
        "tls.handshake.extensions_server_name",
    ]
    cp = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
    )
    if cp.returncode != 0:
        detail = (cp.stderr or cp.stdout or "").strip().splitlines()
        message = detail[0] if detail else f"rc={cp.returncode}"
        return None, f"tshark_failed={message}"

    names = []
    seen = set()
    for line in cp.stdout.splitlines():
        for raw_name in line.split(","):
            name = raw_name.strip().strip(".").lower()
            if name and name not in seen:
                seen.add(name)
                names.append(name)
    return names, ""


def _query_https_records(hostname):
    dig = shutil.which("dig")
    if not dig:
        return {"status": "dig_unavailable", "records": []}

    records = []
    errors = []
    for server in ("1.1.1.1", "8.8.8.8"):
        cmd = [dig, "+time=5", "+tries=1", "+short", "HTTPS", hostname, f"@{server}"]
        try:
            cp = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=10,
            )
        except Exception as e:
            errors.append(f"{server}: {type(e).__name__}: {e}")
            continue
        if cp.returncode != 0:
            detail = (cp.stderr or cp.stdout or "").strip()
            errors.append(f"{server}: rc={cp.returncode} {detail[:200]}")
            continue
        for line in cp.stdout.splitlines():
            record = line.strip()
            if record and record not in records:
                records.append(record)

    status = "ok" if records else "empty"
    if errors:
        status = f"{status}_with_errors"
    return {"status": status, "records": records, "errors": errors}


def save_chrome_ech_evidence(domain_or_url, pcap_path, evidence_dir,
                             netlog_path=ECH_NETLOG_PATH, ssl_key_path=None):
    """Persist ECH-related verification evidence for one successful capture."""
    hostname = _extract_hostname(domain_or_url)
    if not hostname:
        raise ValueError(f"invalid ECH target: {domain_or_url!r}")

    evidence_root = Path(evidence_dir)
    evidence_root.mkdir(parents=True, exist_ok=True)
    stem = Path(pcap_path).stem if pcap_path else f"{datetime.now().strftime('%Y%m%d_%H_%M_%S')}_{hostname}"

    saved_netlog_path = ""
    netlog_summary = {
        "path": netlog_path,
        "exists": bool(netlog_path and os.path.exists(netlog_path)),
        "has_ech_marker": False,
    }
    if netlog_summary["exists"]:
        saved_netlog = evidence_root / f"{stem}_netlog.json"
        shutil.copy2(netlog_path, saved_netlog)
        saved_netlog_path = str(saved_netlog)
        try:
            netlog_text = _read_text_file(netlog_path)
            netlog_summary["has_ech_marker"] = _has_ech_netlog_marker(netlog_text)
            netlog_summary["size"] = os.path.getsize(netlog_path)
        except Exception as e:
            netlog_summary["read_error"] = f"{type(e).__name__}: {e}"

    sni_names, sni_error = _list_tls_sni_from_pcap(pcap_path) if pcap_path else (None, "pcap_missing")
    manifest = {
        "target": hostname,
        "captured_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "doh_template": ECH_DOH_TEMPLATE,
        "netlog": netlog_summary,
        "saved_netlog_path": saved_netlog_path,
        "pcap_path": pcap_path or "",
        "https_records": _query_https_records(hostname),
        "tls_clienthello_sni": {
            "status": "ok" if sni_names is not None else sni_error,
            "names": sni_names or [],
            "target_name_visible": hostname in (sni_names or []),
        },
        "ssl_key_log": summarize_ssl_key_log(ssl_key_path),
        "note": (
            "Wireshark needs ECH_SECRET and ECH_CONFIG to decrypt ClientHelloInner "
            "and TLS 1.3 traffic secrets to decrypt HTTP over TLS."
        ),
    }

    manifest_path = evidence_root / f"{stem}_ech_evidence.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return {
        "ech_evidence_manifest": str(manifest_path),
        "ech_netlog": saved_netlog_path,
    }


def validate_chrome_ech_result(domain_or_url, pcap_path=None, netlog_path=ECH_NETLOG_PATH,
                               ssl_key_path=None, require_wireshark_decryptable=False):
    """
    Validate that Chrome left observable ECH evidence and did not expose target SNI.

    This is intentionally strict: if Chrome falls back to normal TLS or the target
    domain has no usable ECHConfig, the capture should fail instead of being mixed
    into the ECH dataset.
    """
    hostname = _extract_hostname(domain_or_url)
    if not hostname:
        return False, f"invalid_ech_target={domain_or_url!r}"

    if not netlog_path or not os.path.exists(netlog_path):
        return False, f"ech_netlog_missing={netlog_path}"

    try:
        netlog_text = _read_text_file(netlog_path)
    except Exception as e:
        return False, f"ech_netlog_read_failed={type(e).__name__}: {e}"

    lowered_netlog = netlog_text.lower()
    if hostname not in lowered_netlog:
        return False, f"ech_netlog_target_missing={hostname}"
    if ECH_DOH_TEMPLATE.lower() not in lowered_netlog:
        return False, f"ech_doh_template_missing={ECH_DOH_TEMPLATE}"
    if not _has_ech_netlog_marker(netlog_text):
        return False, "ech_marker_missing_in_netlog"

    keylog_detail = "keylog_decryptability=not_required"
    if require_wireshark_decryptable:
        keylog_ok, keylog_detail, _ = _validate_ssl_key_log_for_wireshark_ech(ssl_key_path)
        if not keylog_ok:
            return False, keylog_detail

    sni_detail = "sni_check=not_run"
    if pcap_path:
        if not os.path.exists(pcap_path):
            return False, f"ech_pcap_missing={pcap_path}"

        sni_names, sni_error = _list_tls_sni_from_pcap(pcap_path)
        if sni_names is None:
            sni_detail = sni_error
        else:
            sni_detail = f"sni_count={len(sni_names)}"
            if hostname in sni_names:
                return False, f"target_sni_plaintext_in_pcap={hostname}"

    return True, f"ech_validated target={hostname} netlog={netlog_path} {sni_detail} {keylog_detail}"
