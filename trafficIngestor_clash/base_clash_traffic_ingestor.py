#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
base_clash_traffic_ingestor.py

Clash 采集器基类，封装节点分配、配置渲染、文件同步与启动检查。
"""

from __future__ import annotations

import json
import os
import ipaddress
import re
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from trafficIngestor.base_traffic_ingestor import BaseTrafficIngestor, get_real_username


class BaseClashTrafficIngestor(BaseTrafficIngestor):
    CLASH_HOST_PATH: str = os.path.join(_project_root, "clash-for-linux")
    CLASH_TEMPLATE_CONFIG_PATH: str = os.path.join(_project_root, "config", "config.yaml")
    CLASH_RUNTIME_DIR_NAME: str = "clash_runtime"
    CLASH_PORT: int = 7890
    CLASH_TLS_KEYLOG_FILENAME: str = "trojan_outer_sslkey.log"
    CLASH_TLS_KEYLOG_RESULT_SUBDIR: str = "trojan_outer_ssl_key"
    CLASH_TLS_KEYLOG_SNAPSHOT_DIR_NAME: str = "trojan_outer_sslkey_snapshots"
    CLASH_READY_TIMEOUT: int = 60
    CLASH_START_TIMEOUT: int = 180
    CLASH_NODE_PLACEHOLDER_NAME: str = "vpnnodename"
    DELETE_INVALID_FILES_ON_FAIL: bool = True
    AUTO_RUNTIME_NAMESPACE: bool = True
    RUNTIME_NAME_ENV: str = "TRAFFIC_INGESTOR_RUN_NAME"
    AUTO_DOCKER_NETWORK_POOL: str = "172.19.0.0/16"
    AUTO_DOCKER_NETWORK_PREFIX: str = "traffic_ingestor_clash"
    AUTO_DOCKER_NETWORK_SUBNET_PREFIX: int = 22
    AUTO_DOCKER_NETWORK_GATEWAY_OFFSET: int = 1
    AUTO_DOCKER_NETWORK_START_OFFSET: int = 2

    def __init__(self):
        super().__init__()
        self._runtime_namespace = ""
        self._runtime_entry_script = self.get_entry_script_path()
        self._runtime_network_prepared = False
        if self.AUTO_RUNTIME_NAMESPACE:
            self.configure_runtime_namespace()
        self._vpns = self._load_vpns()

    def get_default_action_source(self) -> Path:
        """Fallback to the Clash action when HOST_CODE_PATH/action.py is missing."""
        return Path(_project_root) / "traffic_capture_single_csv_clash" / "action.py"

    def build_runtime_namespace(self) -> str:
        """Derive a safe runtime namespace from env or the entry script name."""
        requested = os.environ.get(self.RUNTIME_NAME_ENV, "").strip()
        if requested:
            raw_name = requested
        elif self._runtime_entry_script is not None:
            raw_name = self._runtime_entry_script.stem
        else:
            raw_name = getattr(type(self), "BASE_NAME", "") or type(self).__name__
        normalized = self.normalize_runtime_name(raw_name, fallback="traffic_capture_single_csv_clash")
        return self.shorten_runtime_name(normalized, max_length=48)

    def build_auto_network_prefix(self, pool: ipaddress.IPv4Network) -> int:
        """Pick a fixed subnet prefix for auto-created Clash networks."""
        prefix = int(self.AUTO_DOCKER_NETWORK_SUBNET_PREFIX)
        start_offset = max(int(self.AUTO_DOCKER_NETWORK_START_OFFSET), 2)
        gateway_offset = max(int(self.AUTO_DOCKER_NETWORK_GATEWAY_OFFSET), 1)
        required_usable = max(int(self.CONTAINER_COUNT), 1) + start_offset + 4
        required_total = required_usable + 2
        if prefix < pool.prefixlen or prefix > 30:
            raise RuntimeError(f"invalid auto docker subnet prefix /{prefix} for pool {pool}")

        subnet_size = 1 << (32 - prefix)
        if required_total > subnet_size:
            raise RuntimeError(
                f"auto docker subnet /{prefix} is too small for {self.CONTAINER_COUNT} containers"
            )
        if gateway_offset >= subnet_size - 1:
            raise RuntimeError(f"gateway offset {gateway_offset} is outside auto subnet /{prefix}")
        if start_offset >= subnet_size - 1:
            raise RuntimeError(f"start offset {start_offset} is outside auto subnet /{prefix}")
        if start_offset == gateway_offset:
            raise RuntimeError("auto docker gateway offset and start offset cannot be the same")
        return prefix

    def select_auto_network_subnet(self, runtime_name: str) -> ipaddress.IPv4Network:
        """Pick the first free subnet inside the managed Clash network pool."""
        pool = ipaddress.ip_network(self.AUTO_DOCKER_NETWORK_POOL, strict=False)
        prefix = self.build_auto_network_prefix(pool)
        if prefix < pool.prefixlen:
            raise RuntimeError(f"invalid auto network prefix {prefix} for pool {pool}")

        candidates = list(pool.subnets(new_prefix=prefix))
        if not candidates:
            raise RuntimeError(f"no candidate subnets available in pool {pool} with prefix /{prefix}")

        existing_networks = self.list_docker_network_ipv4_subnets(ignore_network=self.DOCKER_NETWORK)

        def overlaps(candidate: ipaddress.IPv4Network) -> bool:
            for _, subnets in existing_networks:
                for subnet in subnets:
                    if candidate.overlaps(subnet):
                        return True
            return False

        for candidate in candidates:
            if not overlaps(candidate):
                return candidate

        overlap_desc = ", ".join(
            f"{name}({';'.join(str(subnet) for subnet in subnets)})"
            for name, subnets in existing_networks
            if subnets
        )
        raise RuntimeError(
            f"no free subnet left in auto Clash pool {pool} for {runtime_name}; existing={overlap_desc}"
        )

    def configure_runtime_namespace(self) -> None:
        """Isolate Clash collectors by script-level runtime namespace."""
        runtime_name = self.build_runtime_namespace()
        network_token = self.shorten_runtime_name(runtime_name, max_length=32)

        self._runtime_namespace = runtime_name
        self.BASE_NAME = runtime_name
        self.HOST_CODE_PATH = os.path.join(_project_root, runtime_name)
        self.CONTAINER_PREFIX = f"{get_real_username()}_{runtime_name}"
        self.DOCKER_NETWORK = f"{self.AUTO_DOCKER_NETWORK_PREFIX}_{network_token}_net"
        self.DOCKER_NETWORK_GATEWAY = None
        self.CONTAINER_IP_START = None
        self._runtime_network_prepared = False

    def configure_runtime_network(self) -> None:
        """Resolve the runtime namespace to a concrete Docker subnet."""
        if not self.AUTO_RUNTIME_NAMESPACE:
            return
        if self._runtime_network_prepared:
            return

        network_name = self.DOCKER_NETWORK
        network_info, subnets, _ = self.inspect_docker_network(network_name)
        gateway_offset = max(int(self.AUTO_DOCKER_NETWORK_GATEWAY_OFFSET), 1)
        start_offset = max(int(self.AUTO_DOCKER_NETWORK_START_OFFSET), 2)
        if network_info is not None:
            if not subnets:
                raise RuntimeError(f"docker network {network_name} exists but has no IPv4 subnet")
            subnet = subnets[0]
            gateway = ""
            for item in self.get_network_ipam_configs(network_info):
                gateway = str(item.get("Gateway", "")).strip()
                if gateway:
                    break
            if not gateway:
                gateway = str(ipaddress.IPv4Address(int(subnet.network_address) + gateway_offset))
        else:
            subnet = self.select_auto_network_subnet(self._runtime_namespace or self.build_runtime_namespace())
            gateway = str(ipaddress.IPv4Address(int(subnet.network_address) + gateway_offset))

        start_ip = ipaddress.IPv4Address(int(subnet.network_address) + start_offset)
        if start_ip not in subnet or start_ip == subnet.broadcast_address:
            raise RuntimeError(f"invalid auto start IP {start_ip} for subnet {subnet}")

        self.DOCKER_NETWORK_SUBNET_PREFIX = subnet.prefixlen
        self.DOCKER_NETWORK_GATEWAY = gateway
        self.CONTAINER_IP_START = str(start_ip)
        self._runtime_network_prepared = True

    def get_vpn_items(self) -> List[Dict[str, Any]]:
        from config.sever_info import vpns_info

        return vpns_info

    def _load_vpns(self) -> List[Dict[str, Any]]:
        raw_vpns = self.get_vpn_items()
        if not isinstance(raw_vpns, list) or not raw_vpns:
            raise RuntimeError("config/sever_info.py 中的 vpns_info 不能为空")

        normalized: List[Dict[str, Any]] = []
        for index, item in enumerate(raw_vpns):
            if not isinstance(item, dict):
                raise RuntimeError(f"vpns_info[{index}] 不是 dict")

            node = dict(item)
            name = str(node.get("name", "")).strip()
            node_type = str(node.get("type", "")).strip()
            server = str(node.get("server", "")).strip()
            port = node.get("port")

            if not name:
                raise RuntimeError(f"vpns_info[{index}] 缺少 name")
            if not node_type:
                raise RuntimeError(f"vpns_info[{index}] 缺少 type")
            if not server:
                raise RuntimeError(f"vpns_info[{index}] 缺少 server")

            if isinstance(port, str) and port.isdigit():
                node["port"] = int(port)
            elif isinstance(port, int):
                node["port"] = port
            else:
                raise RuntimeError(f"vpns_info[{index}] 的 port 非法: {port!r}")

            node["name"] = name
            node["type"] = node_type
            node["server"] = server
            normalized.append(node)

        return normalized

    def _get_container_index(self, name: str) -> int:
        if not name.startswith(self.CONTAINER_PREFIX):
            raise RuntimeError(f"容器名不匹配当前前缀: {name}")

        suffix = name[len(self.CONTAINER_PREFIX):]
        try:
            return int(suffix)
        except ValueError as e:
            raise RuntimeError(f"无法从容器名解析序号: {name}") from e

    def get_vpn_for_container(self, name: str) -> Dict[str, Any]:
        container_index = self._get_container_index(name)
        return self._vpns[container_index % len(self._vpns)]

    def get_clash_container_path(self) -> str:
        return f"{self.CONTAINER_CODE_PATH}/clash-for-linux"

    def get_clash_runtime_host_dir(self, name: str) -> Path:
        return Path(self.HOST_CODE_PATH) / self.CLASH_RUNTIME_DIR_NAME / name

    def get_clash_runtime_container_dir(self, name: str) -> str:
        return f"{self.CONTAINER_CODE_PATH}/{self.CLASH_RUNTIME_DIR_NAME}/{name}"

    def get_clash_outer_tls_keylog_host_path(self, name: str) -> Path:
        return self.get_clash_runtime_host_dir(name) / self.CLASH_TLS_KEYLOG_FILENAME

    def get_clash_outer_tls_keylog_container_path(self, name: str) -> str:
        return f"{self.get_clash_runtime_container_dir(name)}/{self.CLASH_TLS_KEYLOG_FILENAME}"

    def build_clash_proxy_config(self, name: str) -> Dict[str, Any]:
        vpn = dict(self.get_vpn_for_container(name))
        vpn["name"] = self.CLASH_NODE_PLACEHOLDER_NAME
        return vpn

    def render_clash_config(self, name: str) -> str:
        template_path = Path(self.CLASH_TEMPLATE_CONFIG_PATH)
        if not template_path.exists():
            raise FileNotFoundError(f"Clash 模板不存在: {template_path}")

        template_text = template_path.read_text(encoding="utf-8")
        vpn_info_str = "- " + json.dumps(self.build_clash_proxy_config(name), ensure_ascii=False)
        placeholder_name = re.escape(self.CLASH_NODE_PLACEHOLDER_NAME)
        pattern = rf"- \{{ name: '{placeholder_name}'.*?\}}"
        config_text, replace_count = re.subn(
            pattern,
            vpn_info_str,
            template_text,
            count=1,
        )
        if replace_count != 1:
            raise RuntimeError(f"Clash 模板中未找到节点占位配置: {template_path}")
        return config_text

    def write_clash_runtime_config(self, name: str) -> None:
        runtime_dir = self.get_clash_runtime_host_dir(name)
        runtime_dir.mkdir(parents=True, exist_ok=True)

        country_mmdb = Path(self.CLASH_HOST_PATH) / "conf" / "Country.mmdb"
        if not country_mmdb.exists():
            raise FileNotFoundError(f"Country.mmdb 不存在: {country_mmdb}")

        runtime_mmdb = runtime_dir / "Country.mmdb"
        if runtime_mmdb.exists():
            runtime_mmdb.unlink()
        try:
            os.link(country_mmdb, runtime_mmdb)
        except OSError:
            shutil.copy2(country_mmdb, runtime_mmdb)

        config_text = self.render_clash_config(name)
        if not config_text.endswith("\n"):
            config_text += "\n"
        (runtime_dir / "config.yaml").write_text(config_text, encoding="utf-8")

    def snapshot_clash_outer_tls_keylog(self, name: str, result: Dict[str, Any]) -> Optional[str]:
        source_path = self.get_clash_outer_tls_keylog_host_path(name)
        if not source_path.is_file():
            self.log(
                f"WARNING: {name} 缺少 Trojan 外层 TLS keylog: {source_path}; "
                "请确认已替换支持 SSLKEYLOGFILE 的 Clash/Trojan core"
            )
            return None

        ssl_key_name = os.path.basename(str(result.get("ssl_key_file_path", "")).strip())
        if ssl_key_name.endswith("_ssl_key.log"):
            snapshot_name = ssl_key_name[:-len("_ssl_key.log")] + "_trojan_outer_sslkey.log"
        elif ssl_key_name:
            snapshot_name = f"{Path(ssl_key_name).stem}_trojan_outer_sslkey.log"
        else:
            snapshot_name = f"{name}_trojan_outer_sslkey.log"

        snapshot_dir = self.get_clash_runtime_host_dir(name) / self.CLASH_TLS_KEYLOG_SNAPSHOT_DIR_NAME
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        snapshot_path = snapshot_dir / snapshot_name
        shutil.copy2(source_path, snapshot_path)
        return str(snapshot_path)

    def build_additional_result_moves(
        self,
        task: Dict[str, str],
        result: Dict[str, Any],
        dst: str,
    ) -> Dict[str, tuple[str, str]]:
        moves = super().build_additional_result_moves(task, result, dst)
        container = str(task.get("container", "")).strip()
        if not container:
            return moves

        snapshot_path = self.snapshot_clash_outer_tls_keylog(container, result)
        if snapshot_path:
            moves["trojan_outer_ssl_key"] = (snapshot_path, self.CLASH_TLS_KEYLOG_RESULT_SUBDIR)
        return moves

    def prepare_pool_once(self) -> List[str]:
        if not os.path.isdir(self.CLASH_HOST_PATH):
            self.log(f"FATAL: clash-for-linux 目录不存在: {self.CLASH_HOST_PATH}")
            sys.exit(2)

        self.ensure_docker_available()
        if self.AUTO_RUNTIME_NAMESPACE:
            self.configure_runtime_network()
        entry_script = str(self._runtime_entry_script) if self._runtime_entry_script else "unknown"
        self.log(
            "runtime isolation:",
            f"entry={entry_script}",
            f"base_name={self.BASE_NAME}",
            f"host_code={self.HOST_CODE_PATH}",
            f"network={self.DOCKER_NETWORK}",
            f"start_ip={self.CONTAINER_IP_START}",
        )
        self.ensure_host_code_path_ready()
        names = self.build_container_names()
        for name in names:
            self.write_clash_runtime_config(name)

        first_vpn = self.get_vpn_for_container(names[0])["name"]
        last_vpn = self.get_vpn_for_container(names[-1])["name"]
        self.log(
            f"已准备 {len(names)} 个容器的 Clash 配置，"
            f"共使用 {len(self._vpns)} 个节点，"
            f"按容器序号循环分配: {names[0]}->{first_vpn}, {names[-1]}->{last_vpn}"
        )

        names = super().prepare_pool_once()
        for name in names:
            self.sync_clash_files_to_container(name)
            self.ensure_clash_ready(name)
        return names

    def create_container(
        self,
        name: str,
        host_code_path: str,
        image: str,
        container_ip: Optional[str] = None,
    ) -> None:
        uid, gid = str(os.getuid()), str(os.getgid())
        tools_path = os.path.join(_project_root, "tools")

        self.log(f"creating container: {name}")
        cmd = [
            "docker", "run",
            "--init",
            "--dns", "172.17.0.1",
            "--volume", f"{host_code_path}:{self.CONTAINER_CODE_PATH}",
            "--volume", f"{tools_path}:{self.CONTAINER_CODE_PATH}/tools",
            "-e", f"HOST_UID={uid}",
            "-e", f"HOST_GID={gid}",
            "-e", f"DELETE_INVALID_FILES_ON_FAIL={1 if self.DELETE_INVALID_FILES_ON_FAIL else 0}",
            "--privileged",
        ]
        dns_args = self.get_docker_dns_args()
        if dns_args:
            cmd[3:5] = dns_args
        else:
            del cmd[3:5]
        target_network = self.get_target_docker_network()
        if target_network != "bridge":
            cmd += ["--network", target_network]
        if container_ip:
            cmd += ["--ip", container_ip]
        if self.CREATE_WITH_TTY:
            cmd += ["-itd"]
        else:
            cmd += ["-d"]
        cmd += ["--name", name, image, "/bin/bash"]

        cp = self.run_cmd(cmd)
        if cp.returncode != 0:
            self.log(f"FATAL: 创建容器失败: {name} -> {cp.stderr.strip()}")
            sys.exit(2)
        if container_ip:
            self.log(f"created container: {name} ip={container_ip}")
        else:
            self.log(f"created container: {name}")

    def sync_clash_files_to_container(self, name: str) -> None:
        clash_path = Path(self.CLASH_HOST_PATH)
        clash_container_path = self.get_clash_container_path()
        runtime_dir = self.get_clash_runtime_host_dir(name)
        config_path = runtime_dir / "config.yaml"
        country_mmdb = runtime_dir / "Country.mmdb"

        if not clash_path.is_dir():
            self.log(f"FATAL: clash-for-linux 目录不存在: {clash_path}")
            sys.exit(2)
        if not config_path.is_file():
            self.log(f"FATAL: clash config 不存在: {config_path}")
            sys.exit(2)
        if not country_mmdb.is_file():
            self.log(f"FATAL: Country.mmdb 不存在: {country_mmdb}")
            sys.exit(2)

        prepare_shell = f"""
set -e
mkdir -p "{self.CONTAINER_CODE_PATH}"
rm -rf "{clash_container_path}"
"""
        cp = self.run_cmd(["docker", "exec", name, "bash", "-lc", prepare_shell])
        if cp.returncode != 0:
            detail = (cp.stderr or cp.stdout).strip() or f"rc={cp.returncode}"
            self.log(f"FATAL: {name} 准备 clash 目录失败: {detail}")
            sys.exit(2)

        copy_steps = [
            (["docker", "cp", str(clash_path), f"{name}:{self.CONTAINER_CODE_PATH}"], "copy clash-for-linux"),
            (["docker", "cp", str(config_path), f"{name}:{clash_container_path}/conf/config.yaml"], "copy config.yaml"),
            (["docker", "cp", str(country_mmdb), f"{name}:{clash_container_path}/conf/Country.mmdb"], "copy Country.mmdb"),
        ]
        for cmd, desc in copy_steps:
            cp = self.run_cmd(cmd)
            if cp.returncode != 0:
                detail = (cp.stderr or cp.stdout).strip() or f"rc={cp.returncode}"
                self.log(f"FATAL: {name} {desc} 失败: {detail}")
                sys.exit(2)

        normalize_shell = f"""
set -e
python - <<'PY'
from pathlib import Path

root = Path({clash_container_path!r})
for path in root.rglob("*"):
    if not path.is_file():
        continue
    if path.suffix == ".sh" or path.name == ".env":
        data = path.read_bytes().replace(b"\\r\\n", b"\\n").replace(b"\\r", b"\\n")
        path.write_bytes(data)

(root / ".env").write_text(
    "export CLASH_URL='http://127.0.0.1:1/unused'\\n"
    "export CLASH_SECRET=''\\n",
    encoding="utf-8",
)
PY
"""
        cp = self.run_cmd(["docker", "exec", name, "bash", "-lc", normalize_shell])
        if cp.returncode != 0:
            detail = (cp.stderr or cp.stdout).strip() or f"rc={cp.returncode}"
            self.log(f"FATAL: {name} normalize clash files failed: {detail}")
            sys.exit(2)

    def ensure_clash_ready(self, name: str) -> None:
        clash_container_path = self.get_clash_container_path()
        runtime_dir = self.get_clash_runtime_container_dir(name)
        keylog_file = self.get_clash_outer_tls_keylog_container_path(name)
        shell = f"""
set -e
CLASH_DIR="{clash_container_path}"
RUNTIME_DIR="{runtime_dir}"
CONFIG_FILE="$CLASH_DIR/conf/config.yaml"
MMDB_FILE="$CLASH_DIR/conf/Country.mmdb"
START_LOG="$RUNTIME_DIR/clash.start.log"
LOG_FILE="$CLASH_DIR/logs/clash.log"
KEYLOG_FILE="{keylog_file}"

mkdir -p "$RUNTIME_DIR"

if [ ! -d "$CLASH_DIR" ]; then
    echo "clash directory not found: $CLASH_DIR" >&2
    exit 1
fi
if [ ! -f "$CONFIG_FILE" ]; then
    echo "clash config not found: $CONFIG_FILE" >&2
    exit 1
fi
if [ ! -f "$MMDB_FILE" ]; then
    echo "Country.mmdb not found: $MMDB_FILE" >&2
    exit 1
fi

if python -c "import socket; s = socket.create_connection(('127.0.0.1', {self.CLASH_PORT}), timeout=1); s.close()" >/dev/null 2>&1; then
    exit 0
fi

rm -f "$START_LOG" "$LOG_FILE" "$KEYLOG_FILE"
export SSLKEYLOGFILE="$KEYLOG_FILE"
if ! bash "$CLASH_DIR/start.sh" >"$START_LOG" 2>&1; then
    cat "$START_LOG" >&2 || true
    cat "$LOG_FILE" >&2 || true
    exit 1
fi

for _ in $(seq 1 {self.CLASH_READY_TIMEOUT}); do
    python -c "import socket; s = socket.create_connection(('127.0.0.1', {self.CLASH_PORT}), timeout=1); s.close()" >/dev/null 2>&1 && exit 0
    sleep 1
done

echo "clash startup timeout" >&2
cat "$START_LOG" >&2 || true
cat "$LOG_FILE" >&2 || true
exit 1
"""
        cp = self.run_cmd(
            ["docker", "exec", name, "bash", "-lc", shell],
            timeout=self.CLASH_START_TIMEOUT,
        )
        if cp.returncode != 0:
            detail = (cp.stderr or cp.stdout).strip() or f"rc={cp.returncode}"
            self.log(f"FATAL: {name} 启动 clash 失败: {detail}")
            sys.exit(2)

        vpn_name = self.get_vpn_for_container(name)["name"]
        self.log(f"{name}: clash is ready with vpn={vpn_name}")
