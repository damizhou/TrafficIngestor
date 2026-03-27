#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
x_traffic.py

从 CSV 读取 X(Twitter) 用户 URL，使用容器池并发采集流量数据。
"""

import json
import os
import sys
from typing import Dict, List, Tuple

# 添加项目根目录到路径
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from trafficIngestor.base_traffic_ingestor import BaseTrafficIngestor, get_real_username


class TrafficIngestor(BaseTrafficIngestor):
    """流量采集器"""

    # ============== 配置 ==============
    BASE_NAME = 'traffic_capture_single_csv_clash'
    CONTAINER_PREFIX = f"{get_real_username()}_{BASE_NAME}"
    CONTAINER_COUNT = 15 * 10
    HOST_CODE_PATH = os.path.join(_project_root, BASE_NAME)
    BASE_DST = '/netdisk2/ww/wiki/0325/chrome'
    DOCKER_IMAGE = "chuanzhoupan/trace_spider:250912"
    RETRY = 5
    CLASH_HOST_PATH = os.path.join(_project_root, 'clash-for-linux')
    CLASH_CONTAINER_PATH = f"{BaseTrafficIngestor.CONTAINER_CODE_PATH}/clash-for-linux"
    CLASH_PROXY = "http://127.0.0.1:7890"
    CLASH_READY_TIMEOUT = 60
    CLASH_START_TIMEOUT = 180

    # CSV 必须包含表头，字段名（大小写不敏感）：
    # - id: 唯一标识，用于任务完成/失败后从 CSV 删除对应行
    # - url: 访问地址（建议完整 URL，包含 http:// 或 https://）
    # - domain: 域名（用于日志与流量采集标识）
    # 示例：
    # id,url,domain
    # 1,https://vox-cdn.com,vox-cdn.com
    CSV_PATH = os.path.join(_project_root, 'small_tools', 'wiki_chrome.csv')

    def __init__(self):
        super().__init__()
        self._has_jobs = True

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
        if self.DOCKER_NETWORK:
            cmd += ["--network", self.DOCKER_NETWORK]
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
        """从 CSV 读取任务"""
        if not self._has_jobs:
            return []

        jobs, _ = self.read_jobs_from_csv(self.CSV_PATH)
        if not jobs:
            self._has_jobs = False
        return jobs

    def on_task_success(self, task: Dict[str, str], paths: Dict[str, str]) -> None:
        """任务成功后从 CSV 删除记录"""
        row_id = task.get("row_id", "")
        if row_id:
            try:
                self.remove_from_csv(self.CSV_PATH, row_id)
            except Exception as e:
                self.log(f"ERROR: 删除 CSV 记录失败: {e}")

    def on_task_failed(self, task: Dict[str, str], error: str) -> None:
        """任务失败后也从 CSV 删除记录（避免重复处理）"""
        # row_id = task.get("row_id", "")
        # if row_id:
        #     try:
        #         self.remove_from_csv(self.CSV_PATH, row_id)
        #     except Exception as e:
        #         self.log(f"ERROR: 删除 CSV 记录失败: {e}")

    def should_continue(self) -> bool:
        """只运行一次"""
        return False

    def ensure_clash_ready(self, name: str) -> None:
        """参考旧 VPN 入口，在容器内任务执行前确保 Clash 已启动。"""
        shell = f"""
set -e
CLASH_DIR="{self.CLASH_CONTAINER_PATH}"
if [ ! -d "$CLASH_DIR" ]; then
    echo "clash directory not found: $CLASH_DIR" >&2
    exit 1
fi
if python -c "import socket; s = socket.create_connection(('127.0.0.1', 7890), timeout=1); s.close()" >/dev/null 2>&1; then
    exit 0
fi
if ps -ef | grep '[c]lash-linux-' >/dev/null 2>&1; then
    bash "$CLASH_DIR/shutdown.sh" >/dev/null 2>&1 || true
    sleep 1
fi
bash "$CLASH_DIR/start.sh" >/tmp/clash_start.log 2>&1
for _ in $(seq 1 {self.CLASH_READY_TIMEOUT}); do
    python -c "import socket; s = socket.create_connection(('127.0.0.1', 7890), timeout=1); s.close()" >/dev/null 2>&1 && exit 0
    sleep 1
done
echo "clash startup timeout" >&2
cat /tmp/clash_start.log >&2 || true
exit 1
"""
        cp = self.run_cmd(["docker", "exec", name, "bash", "-lc", shell], timeout=self.CLASH_START_TIMEOUT)
        if cp.returncode != 0:
            detail = (cp.stderr or cp.stdout).strip() or "unknown error"
            self.log(f"FATAL: {name} 启动 clash 失败: {detail}")
            sys.exit(2)
        self.log(f"{name}: clash is ready")

    def exec_once(self, task: Dict[str, str]) -> Tuple[bool, str]:
        """执行单个任务，并让 action 进程带着 Clash 代理环境启动。"""
        container = task["container"]
        self._wait_before_first_exec(container)
        self.ensure_clash_ready(container)
        payload = json.dumps(task, ensure_ascii=False)
        cmd = [
            "docker", "exec",
            "-e", f"http_proxy={self.CLASH_PROXY}",
            "-e", f"https_proxy={self.CLASH_PROXY}",
            "-e", f"all_proxy={self.CLASH_PROXY}",
            "-e", f"HTTP_PROXY={self.CLASH_PROXY}",
            "-e", f"HTTPS_PROXY={self.CLASH_PROXY}",
            "-e", f"ALL_PROXY={self.CLASH_PROXY}",
            "-e", "no_proxy=127.0.0.1,localhost,::1",
            "-e", "NO_PROXY=127.0.0.1,localhost,::1",
            container,
            "python", "-u", f"{self.CONTAINER_CODE_PATH}/action.py",
            payload,
        ]
        self.log("执行命令", cmd)
        cp = self.run_cmd(cmd, timeout=self.DOCKER_EXEC_TIMEOUT)

        if cp.returncode == 0:
            try:
                return self.process_result(task, container)
            except (json.JSONDecodeError, FileNotFoundError, OSError) as e:
                return False, f"post-processing error: {e}"

        stderr = (cp.stderr or "").strip()
        stdout = (cp.stdout or "").strip()
        detail = stderr or stdout or "stdout/stderr empty"
        return False, f"docker exec rc={cp.returncode}: {detail}"

    def cleanup(self) -> None:
        """清理容器"""
        import time
        time.sleep(60)
        self.remove_containers()


if __name__ == "__main__":
    TrafficIngestor.main()
    TrafficIngestor.main()
    TrafficIngestor.main()
    TrafficIngestor.main()
    TrafficIngestor.main()
