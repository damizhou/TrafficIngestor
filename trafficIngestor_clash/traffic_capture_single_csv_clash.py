#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
traffic_capture_single_csv_clash.py

从 CSV 读取 URL，使用容器池并发采集流量数据。
每个 Docker 容器绑定一个 VPN 节点，节点分配完一轮后继续循环分配。
"""

import json
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, List

# 添加项目根目录到路径
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from config.sever_info import vpns_info
from trafficIngestor.base_traffic_ingestor import BaseTrafficIngestor, get_real_username


class TrafficIngestor(BaseTrafficIngestor):
    """流量采集器。"""

    # ============== 配置 ==============
    BASE_NAME = 'traffic_capture_single_csv_clash'
    CONTAINER_PREFIX = f"{get_real_username()}_{BASE_NAME}"
    CONTAINER_COUNT = 15
    HOST_CODE_PATH = os.path.join(_project_root, BASE_NAME)
    BASE_DST = '/netdisk/test'
    DOCKER_IMAGE = "chuanzhoupan/trace_spider:250912"
    RETRY = 5
    DOCKER_NETWORK = f"{CONTAINER_PREFIX}_net"
    CONTAINER_IP_START = "172.18.150.10"
    CLASH_HOST_PATH = os.path.join(_project_root, 'clash-for-linux')
    CLASH_CONTAINER_PATH = f"{BaseTrafficIngestor.CONTAINER_CODE_PATH}/clash-for-linux"
    CLASH_PORT = 7890
    CLASH_READY_TIMEOUT = 60
    CLASH_START_TIMEOUT = 180
    CLASH_RUNTIME_DIR_NAME = "clash_runtime"
    CLASH_TEMPLATE_CONFIG_PATH = os.path.join(_project_root, "config", "config.yaml")

    # CSV 必须包含表头，字段名（大小写不敏感）：
    # - id: 唯一标识，用于任务完成/失败后从 CSV 删除对应行
    # - url: 访问地址（建议完整 URL，包含 http:// 或 https://）
    # - domain: 域名（用于日志与流量采集标识）
    # 示例：
    # id,url,domain
    # 1,https://vox-cdn.com,vox-cdn.com
    CSV_PATH = os.path.join(_project_root, 'small_tools', 'result', 'test.csv')

    def __init__(self):
        super().__init__()
        self._has_jobs = True
        self._vpns = self._load_vpns()

    def _load_vpns(self) -> List[Dict[str, Any]]:
        """从 config/sever_info.py 读取并校验节点信息。"""
        if not isinstance(vpns_info, list) or not vpns_info:
            raise RuntimeError("config/sever_info.py 中的 vpns_info 不能为空")

        normalized: List[Dict[str, Any]] = []
        for index, item in enumerate(vpns_info):
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
        suffix = name[len(self.CONTAINER_PREFIX):]
        try:
            return int(suffix)
        except ValueError as e:
            raise RuntimeError(f"无法从容器名解析序号: {name}") from e

    def get_vpn_for_container(self, name: str) -> Dict[str, Any]:
        """为容器按序号循环分配节点。"""
        container_index = self._get_container_index(name)
        return self._vpns[container_index % len(self._vpns)]

    def get_clash_runtime_host_dir(self, name: str) -> Path:
        return Path(self.HOST_CODE_PATH) / self.CLASH_RUNTIME_DIR_NAME / name

    def get_clash_runtime_container_dir(self, name: str) -> str:
        return f"{self.CONTAINER_CODE_PATH}/{self.CLASH_RUNTIME_DIR_NAME}/{name}"

    def render_clash_config(self, name: str) -> str:
        """基于 config/config.yaml 模板替换节点占位内容。"""
        vpn = dict(self.get_vpn_for_container(name))
        vpn["name"] = "vpnnodename"
        template_path = Path(self.CLASH_TEMPLATE_CONFIG_PATH)
        if not template_path.exists():
            raise FileNotFoundError(f"Clash 模板不存在: {template_path}")

        template_text = template_path.read_text(encoding="utf-8")
        vpn_info_str = "- " + json.dumps(vpn, ensure_ascii=False)
        config_text, replace_count = re.subn(
            r"- \{ name: 'vpnnodename'.*?\}",
            vpn_info_str,
            template_text,
            count=1,
        )
        if replace_count != 1:
            raise RuntimeError(f"Clash 模板中未找到节点占位配置: {template_path}")
        return config_text

    def write_clash_runtime_config(self, name: str) -> None:
        """为容器写入独立的 Clash 运行目录和配置。"""
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

    def prepare_pool_once(self) -> List[str]:
        """在创建容器前先准备每个容器自己的 Clash 配置。"""
        if not os.path.isdir(self.CLASH_HOST_PATH):
            self.log(f"FATAL: clash-for-linux 目录不存在: {self.CLASH_HOST_PATH}")
            sys.exit(2)

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
            self.ensure_clash_ready(name)
        return names

    def create_container(
        self,
        name: str,
        host_code_path: str,
        image: str,
        container_ip: str | None = None
    ) -> None:
        """创建容器，同时挂载采集代码、tools 和 clash-for-linux。"""
        uid, gid = str(os.getuid()), str(os.getgid())
        tools_path = os.path.join(_project_root, 'tools')
        clash_path = self.CLASH_HOST_PATH
        if not os.path.isdir(clash_path):
            self.log(f"FATAL: clash-for-linux 目录不存在: {clash_path}")
            sys.exit(2)

        self.log(f"creating container: {name}")
        cmd = [
            "docker", "run",
            "--init",
            "--dns", "172.17.0.1",
            "--volume", f"{host_code_path}:{self.CONTAINER_CODE_PATH}",
            "--volume", f"{tools_path}:{self.CONTAINER_CODE_PATH}/tools",
            "--volume", f"{clash_path}:{self.CLASH_CONTAINER_PATH}",
            "-e", f"HOST_UID={uid}",
            "-e", f"HOST_GID={gid}",
            "--privileged",
        ]
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

    def fetch_jobs(self) -> List[Dict[str, str]]:
        """从 CSV 读取任务。"""
        if not self._has_jobs:
            return []

        jobs, _ = self.read_jobs_from_csv(self.CSV_PATH)
        if not jobs:
            self._has_jobs = False
        return jobs

    def should_continue(self) -> bool:
        """只运行一次。"""
        return False

    def ensure_clash_ready(self, name: str) -> None:
        """在容器内按专属配置启动 Clash。"""
        runtime_dir = self.get_clash_runtime_container_dir(name)
        shell = f"""
set -e
CLASH_DIR="{self.CLASH_CONTAINER_PATH}"
RUNTIME_DIR="{runtime_dir}"
CONFIG_FILE="$RUNTIME_DIR/config.yaml"
LOG_FILE="$RUNTIME_DIR/clash.stdout.log"

if [ ! -d "$CLASH_DIR" ]; then
    echo "clash directory not found: $CLASH_DIR" >&2
    exit 1
fi
if [ ! -f "$CONFIG_FILE" ]; then
    echo "clash config not found: $CONFIG_FILE" >&2
    exit 1
fi
if [ ! -f "$RUNTIME_DIR/Country.mmdb" ]; then
    echo "Country.mmdb not found: $RUNTIME_DIR/Country.mmdb" >&2
    exit 1
fi

if python -c "import socket; s = socket.create_connection(('127.0.0.1', {self.CLASH_PORT}), timeout=1); s.close()" >/dev/null 2>&1; then
    exit 0
fi

if command -v pkill >/dev/null 2>&1; then
    pkill -f 'clash-linux-' >/dev/null 2>&1 || true
fi

case "$(uname -m)" in
    x86_64|amd64)
        CLASH_BIN="$CLASH_DIR/bin/clash-linux-amd64"
        ;;
    aarch64|arm64)
        CLASH_BIN="$CLASH_DIR/bin/clash-linux-arm64"
        ;;
    armv7|armv7l)
        CLASH_BIN="$CLASH_DIR/bin/clash-linux-armv7"
        ;;
    *)
        echo "unsupported cpu architecture: $(uname -m)" >&2
        exit 1
        ;;
esac

chmod +x "$CLASH_BIN"
rm -f "$LOG_FILE"
nohup "$CLASH_BIN" -d "$RUNTIME_DIR" >"$LOG_FILE" 2>&1 &

for _ in $(seq 1 {self.CLASH_READY_TIMEOUT}); do
    python -c "import socket; s = socket.create_connection(('127.0.0.1', {self.CLASH_PORT}), timeout=1); s.close()" >/dev/null 2>&1 && exit 0
    sleep 1
done

echo "clash startup timeout" >&2
cat "$LOG_FILE" >&2 || true
exit 1
"""
        cp = self.run_cmd(["docker", "exec", name, "bash", "-lc", shell], timeout=self.CLASH_START_TIMEOUT)
        if cp.returncode != 0:
            detail = (cp.stderr or cp.stdout).strip() or "unknown error"
            self.log(f"FATAL: {name} 启动 clash 失败: {detail}")
            sys.exit(2)

        vpn_name = self.get_vpn_for_container(name)["name"]
        self.log(f"{name}: clash is ready with vpn={vpn_name}")

    def cleanup(self) -> None:
        """清理容器。"""
        import time
        time.sleep(60)
        self.remove_containers()


if __name__ == "__main__":
    TrafficIngestor.main()
    # TrafficIngestor.main()
    # TrafficIngestor.main()
    # TrafficIngestor.main()
    # TrafficIngestor.main()
