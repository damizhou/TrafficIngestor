#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
base_traffic_ingestor.py

流量采集器基类，封装 Docker 容器池管理、任务调度、文件处理等通用逻辑。
各具体采集脚本继承此基类，只需配置参数和实现差异部分。
"""

from __future__ import annotations
import csv
import hashlib
import ipaddress
import os
import posixpath
import re
import stat
import sys
import time
import json
import signal
import queue
import subprocess
import shutil
import threading
import tempfile
from abc import ABC, abstractmethod
from collections import Counter
from pathlib import Path
from typing import Optional, List, Dict, Tuple, Any
from concurrent.futures import ThreadPoolExecutor

try:
    from tqdm import tqdm
except ModuleNotFoundError:
    tqdm = None

# 添加项目根目录到路径
_current_dir: str = os.path.dirname(os.path.abspath(__file__))
_project_root: str = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def get_real_username() -> str:
    """获取当前登录用户名"""
    return os.environ.get('USER') or os.getlogin()


def get_default_uid() -> int:
    """Return the host uid, with a Windows-safe fallback for import-time defaults."""
    getuid = getattr(os, "getuid", None)
    return int(getuid()) if getuid is not None else 1000


def get_default_gid() -> int:
    """Return the host gid, with a Windows-safe fallback for import-time defaults."""
    getgid = getattr(os, "getgid", None)
    return int(getgid()) if getgid is not None else 1000


class BaseTrafficIngestor(ABC):
    """
    流量采集器基类

    子类需要：
    1. 设置类属性（配置参数）
    2. 实现 fetch_jobs() 方法获取任务
    3. 可选：覆盖 process_result() 方法自定义结果处理
    4. 可选：覆盖 on_task_success() / on_task_failed() 方法
    """

    # ============== 子类必须设置的配置 ==============
    BASE_NAME: str = ""
    CONTAINER_PREFIX: str = ""
    HOST_CODE_PATH: str = ""
    BASE_DST: str = ""

    # ============== 可选配置（有默认值）==============
    CONTAINER_COUNT: Optional[int] = None
    MAX_DYNAMIC_CONTAINER_COUNT: int = 600
    DYNAMIC_CONTAINER_TASKS_PER_CONTAINER: int = 10
    DYNAMIC_ONE_CONTAINER_PER_TASK_LIMIT: int = 50
    DOCKER_IMAGE: str = "chuanzhoupan/trace_spider_chrome_149:260611"
    BROWSER_NAME: str = "chrome"
    BROWSER_VERSION_COMMANDS: Tuple[Tuple[str, ...], ...] = (
        ("google-chrome", "--version"),
        ("google-chrome-stable", "--version"),
        ("chromium", "--version"),
        ("chromium-browser", "--version"),
    )
    CONTAINER_CODE_PATH: str = "/app"
    CREATE_WITH_TTY: bool = True
    DOCKER_EXEC_TIMEOUT: int = 6000
    RETRY: int = 5
    FIRST_EXEC_INTERVAL: float = 1.0
    SAME_ID_EXEC_INTERVAL: float = 2.0
    DOCKER_NETWORK: Optional[str] = None
    CONTAINER_IP_START: Optional[str] = None
    DOCKER_DNS: Optional[str] = None
    DOCKER_NETWORK_SUBNET_PREFIX: int = 24
    DOCKER_NETWORK_GATEWAY: Optional[str] = None
    DOCKER_NETWORK_ATTACHMENT_WARN_THRESHOLD: Optional[int] = 900
    DEFAULT_UID: int = get_default_uid()
    DEFAULT_GID: int = get_default_gid()
    CLEAR_HOST_CODE_SUBDIRS_AFTER_BATCH: bool = False
    VERIFY_BASE_DST_WRITABLE_ON_START: bool = True
    NORMALIZE_SUCCESS_OUTPUT_MODES: bool = True
    SUCCESS_OUTPUT_DIR_MODE: int = 0o775
    SUCCESS_OUTPUT_FILE_MODE: int = 0o664

    def __init__(self):
        self._runtime_entry_script = self.get_entry_script_path()
        self.configure_runtime_identity()
        self._stats_lock = threading.Lock()
        self._csv_lock = threading.Lock()
        self._task_success_context = threading.local()
        self._pbar = None
        self._first_exec_lock = threading.Lock()
        self._first_exec_next_ts = 0.0
        self._first_exec_done_containers = set()
        self._same_id_exec_lock = threading.Lock()
        self._same_id_next_ts: Dict[Tuple[str, str], float] = {}
        self._runtime_prepared = False
        self._browser_label = ""
        self._runtime_lock_handle = None
        self._runtime_lock_path = ""
        self._copied_task_csv_path = ""
        self._execution_task_log_lock = threading.Lock()
        self._execution_task_log_path = ""
        self._execution_task_sequence = 0

        # 全局统计
        self._global_start_time = 0.0
        self._global_ok = 0
        self._global_fail = 0
        self._global_total_jobs = 0
        self._global_container_count = 1

    # ============== 日志 ==============
    def log(self, *args) -> None:
        """打印带时间戳的日志，适配进度条"""
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        msg = f"[{ts}] " + " ".join(str(x) for x in args)
        if self._pbar is not None and tqdm is not None:
            tqdm.write(msg)
        else:
            print(msg, flush=True)

    def initialize_execution_task_log(self) -> Optional[str]:
        """Create one concise task lifecycle log for the current execution."""
        csv_path = str(getattr(self, "CSV_PATH", "") or "").strip()
        if not csv_path:
            return None

        logs_dir = os.path.join(os.path.abspath(str(self.BASE_DST)), "logs")
        os.makedirs(logs_dir, exist_ok=True)
        self.chown_path(logs_dir)
        self.normalize_success_output_dirs(str(self.BASE_DST), logs_dir)

        timestamp = time.strftime("%Y%m%d_%H_%M_%S")
        csv_name = os.path.basename(csv_path)
        base_name = f"{timestamp}_{csv_name}"
        candidate = os.path.join(logs_dir, f"{base_name}.log")
        sequence = 1
        while True:
            try:
                with open(candidate, "x", encoding="utf-8"):
                    pass
                break
            except FileExistsError:
                sequence += 1
                candidate = os.path.join(logs_dir, f"{base_name}_{sequence}.log")

        self.chown_path(candidate)
        self.normalize_success_output_path(candidate)
        self._execution_task_log_path = candidate
        self._write_execution_task_log(
            "run_start",
            base_dst=os.path.abspath(str(self.BASE_DST)),
            csv_path=os.path.abspath(csv_path),
        )
        self.log(f"任务执行日志: {candidate}")
        return candidate

    def _write_execution_task_log(self, event: str, **fields: Any) -> None:
        """Append one JSON event to the current execution task log."""
        if not self._execution_task_log_path:
            return

        record = {
            "time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "event": event,
        }
        record.update(fields)
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        with self._execution_task_log_lock:
            with open(self._execution_task_log_path, "a", encoding="utf-8") as log_file:
                log_file.write(line)
                log_file.write("\n")
                log_file.flush()

    def _write_task_start_log(self, task: Dict[str, Any]) -> None:
        self._write_execution_task_log(
            "task_start",
            task_no=task.get("_task_log_no", 0),
            id=task.get("row_id", ""),
            url=task.get("url", ""),
            domain=task.get("domain", ""),
            container=task.get("container", ""),
        )

    def _write_task_end_log(
        self,
        task: Dict[str, Any],
        *,
        success: bool,
        elapsed: float,
        error: str = "",
    ) -> None:
        record = {
            "task_no": task.get("_task_log_no", 0),
            "id": task.get("row_id", ""),
            "url": task.get("url", ""),
            "domain": task.get("domain", ""),
            "container": task.get("container", ""),
            "status": "success" if success else "failed",
            "attempts": int(task.get("_retry_count", 0)) + 1,
            "elapsed_seconds": round(elapsed, 3),
        }
        if error:
            record["error"] = self.compact_error(error, limit=1000)
        self._write_execution_task_log("task_end", **record)

    @staticmethod
    def compact_error(error: Any, limit: int = 1600) -> str:
        """Keep failure logs readable without hiding the useful part."""
        text = str(error or "")
        if len(text) <= limit:
            return text
        return f"{text[:limit]}...<truncated {len(text) - limit} chars>"

    # ============== 命令执行 ==============
    def run_cmd(self, cmd: List[str], timeout: Optional[int] = None) -> subprocess.CompletedProcess:
        """执行命令并返回结果"""
        return subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, timeout=timeout
        )

    def get_entry_script_path(self) -> Optional[Path]:
        """Return the current collector entry script path when available."""
        module = sys.modules.get(type(self).__module__)
        raw_path = getattr(module, "__file__", None) if module is not None else None
        if not raw_path:
            return None
        try:
            return Path(raw_path).resolve()
        except OSError:
            return Path(raw_path)

    @staticmethod
    def normalize_runtime_name(name: str, fallback: str = "collector") -> str:
        """Normalize arbitrary names into safe filesystem / Docker identifiers."""
        cleaned = re.sub(r"[^0-9A-Za-z_.-]+", "_", str(name or "").strip())
        cleaned = cleaned.strip("._-")
        return cleaned or fallback

    @staticmethod
    def shorten_runtime_name(name: str, max_length: int = 48) -> str:
        """Keep identifiers readable while staying within Docker naming limits."""
        if max_length <= 8 or len(name) <= max_length:
            return name
        digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
        head = max_length - len(digest) - 1
        return f"{name[:head]}-{digest}"

    def build_runtime_name(self) -> str:
        """Derive a runtime name for container and workspace identity."""
        raw_name = str(getattr(type(self), "BASE_NAME", "") or "").strip()
        if not raw_name and self._runtime_entry_script is not None:
            raw_name = self._runtime_entry_script.stem
        if not raw_name and self.HOST_CODE_PATH:
            raw_name = Path(self.HOST_CODE_PATH).name
        if not raw_name:
            raw_name = type(self).__name__
        normalized = self.normalize_runtime_name(raw_name, fallback="traffic_ingestor")
        return self.shorten_runtime_name(normalized, max_length=48)

    def parse_browser_version_major(self, version_output: str) -> str:
        """Extract a browser major version from version command output."""
        matched = re.search(r"\d+", str(version_output or ""))
        return matched.group(0) if matched else ""

    def build_browser_artifact_label(self, version_output: str) -> str:
        """Build the stable artifact label used by all tasks in this run."""
        browser = self.normalize_runtime_name(self.BROWSER_NAME, fallback="browser").lower()
        major = self.parse_browser_version_major(version_output)
        if not browser or not major:
            raise RuntimeError(
                f"invalid browser label: browser={self.BROWSER_NAME!r}, version={version_output!r}"
            )
        return f"{browser}{major}"

    def detect_browser_label(self, container: str) -> str:
        """Probe browser version once inside a started container."""
        attempts: List[str] = []
        for version_cmd in self.BROWSER_VERSION_COMMANDS:
            if not version_cmd:
                continue
            cmd = ["docker", "exec", container, *version_cmd]
            cp = self.run_cmd(cmd, timeout=30)
            output = " ".join(
                part.strip()
                for part in (cp.stdout, cp.stderr)
                if part and part.strip()
            )
            if cp.returncode == 0:
                try:
                    label = self.build_browser_artifact_label(output)
                except RuntimeError as e:
                    attempts.append(f"{' '.join(version_cmd)} -> {e}")
                    continue
                self.log(
                    f"browser version detected: container={container}, "
                    f"browser={self.BROWSER_NAME}, version={output}, label={label}"
                )
                return label
            attempts.append(f"{' '.join(version_cmd)} -> rc={cp.returncode}, output={output or '<empty>'}")

        detail = " | ".join(attempts) if attempts else "no browser version command configured"
        self.log(f"FATAL: 浏览器版本探测失败: container={container}, {detail}")
        sys.exit(2)

    def configure_runtime_identity(self) -> None:
        """Populate BASE_NAME / CONTAINER_PREFIX / HOST_CODE_PATH when subclasses omit them."""
        runtime_name = self.build_runtime_name()
        self.BASE_NAME = runtime_name
        self.CONTAINER_PREFIX = f"{get_real_username()}_{runtime_name}"
        if not self.HOST_CODE_PATH:
            self.HOST_CODE_PATH = os.path.join(_project_root, runtime_name)

    def acquire_runtime_lock(self) -> None:
        """Reject a second collector process using the same runtime namespace."""
        if os.name != "posix":
            return

        import fcntl

        lock_path = Path("/tmp") / f"traffic_ingestor_{self.BASE_NAME}.lock"
        lock_handle = lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as e:
            lock_handle.seek(0)
            owner = lock_handle.read().strip() or "owner metadata unavailable"
            lock_handle.close()
            raise RuntimeError(
                f"runtime already active: namespace={self.BASE_NAME}, "
                f"lock={lock_path}, owner={owner}"
            ) from e

        lock_handle.seek(0)
        lock_handle.truncate()
        lock_handle.write(
            f"pid={os.getpid()} started={time.strftime('%Y-%m-%d %H:%M:%S')} "
            f"argv={' '.join(sys.argv)}\n"
        )
        lock_handle.flush()
        self._runtime_lock_handle = lock_handle
        self._runtime_lock_path = str(lock_path)
        self.log(f"runtime lock acquired: {lock_path}")

    def release_runtime_lock(self) -> None:
        """Release the current runtime namespace lock."""
        lock_handle = self._runtime_lock_handle
        if lock_handle is None:
            return

        if os.name == "posix":
            import fcntl

            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        lock_handle.close()
        self._runtime_lock_handle = None

    # ============== Docker 容器管理 ==============
    def ensure_docker_available(self) -> None:
        """确保 Docker 可用"""
        try:
            self.run_cmd(["docker", "version"]).check_returncode()
        except Exception as e:
            self.log("FATAL: docker 不可用。", e)
            sys.exit(2)

    def container_exists(self, name: str) -> Optional[bool]:
        """检查容器是否存在，返回 None 表示不存在"""
        cp = self.run_cmd(["docker", "inspect", "-f", "{{.State.Running}}", name])
        if cp.returncode != 0:
            return None
        out = cp.stdout.strip().lower()
        return (out == "true") or (out == "false")

    def container_running(self, name: str) -> bool:
        """检查容器是否正在运行"""
        cp = self.run_cmd(["docker", "inspect", "-f", "{{.State.Running}}", name])
        return (cp.returncode == 0) and (cp.stdout.strip().lower() == "true")

    def build_container_ip(self, index: int) -> Optional[str]:
        """按容器序号生成固定 IP；未配置时返回 None。"""
        if not self.CONTAINER_IP_START:
            return None
        try:
            return str(ipaddress.IPv4Address(self.CONTAINER_IP_START) + index)
        except ValueError as e:
            self.log(f"FATAL: 非法的容器起始 IP：{self.CONTAINER_IP_START} -> {e}")
            sys.exit(2)

    def get_target_docker_network(self) -> str:
        """返回固定 IP 所在的目标 Docker 网络名。"""
        return self.DOCKER_NETWORK or "bridge"

    def get_docker_dns_args(self) -> List[str]:
        """Return optional explicit Docker DNS args when overriding daemon defaults."""
        dns_server = (self.DOCKER_DNS or "").strip()
        return ["--dns", dns_server] if dns_server else []

    def get_network_ipam_configs(self, network_info: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize docker network IPAM Config to a safe list."""
        if not isinstance(network_info, dict):
            return []

        ipam = network_info.get("IPAM") or {}
        if not isinstance(ipam, dict):
            return []

        configs = ipam.get("Config") or []
        if not isinstance(configs, list):
            return []

        return [item for item in configs if isinstance(item, dict)]

    def inspect_target_network(self) -> Tuple[List[ipaddress.IPv4Network], Dict[str, str]]:
        """读取目标 Docker 网络上的 IPv4 子网与已分配地址。"""
        network_name = self.get_target_docker_network()
        cp = self.run_cmd(["docker", "network", "inspect", network_name])
        if cp.returncode != 0:
            detail = (cp.stderr or cp.stdout).strip() or "unknown error"
            self.log(f"WARN: 无法读取 Docker 网络 {network_name}：{detail}")
            return [], {}

        try:
            payload = json.loads(cp.stdout)
            network_info = payload[0] if payload else {}
        except (json.JSONDecodeError, IndexError, TypeError) as e:
            self.log(f"WARN: 解析 Docker 网络 {network_name} 失败：{e}")
            return [], {}

        subnets: List[ipaddress.IPv4Network] = []
        for item in self.get_network_ipam_configs(network_info):
            subnet = str(item.get("Subnet", "")).strip()
            if not subnet:
                continue
            try:
                subnets.append(ipaddress.ip_network(subnet, strict=False))
            except ValueError:
                self.log(f"WARN: 忽略非法 Docker 子网配置：{subnet}")

        allocated: Dict[str, str] = {}
        for container in (network_info.get("Containers") or {}).values():
            name = str(container.get("Name", "")).strip()
            ipv4 = str(container.get("IPv4Address", "")).split("/", 1)[0].strip()
            if not name or not ipv4:
                continue
            try:
                ipaddress.IPv4Address(ipv4)
            except ValueError:
                continue
            allocated[name] = ipv4

        return subnets, allocated

    def inspect_docker_network(
        self,
        network_name: str,
    ) -> Tuple[Optional[Dict[str, Any]], List[ipaddress.IPv4Network], Dict[str, str]]:
        """Read a Docker network and collect IPv4 subnet/allocation info."""
        cp = self.run_cmd(["docker", "network", "inspect", network_name])
        if cp.returncode != 0:
            return None, [], {}

        try:
            payload = json.loads(cp.stdout)
            network_info = payload[0] if payload else {}
        except (json.JSONDecodeError, IndexError, TypeError) as e:
            self.log(f"WARN: failed to parse docker network {network_name}: {e}")
            return None, [], {}

        subnets: List[ipaddress.IPv4Network] = []
        for item in self.get_network_ipam_configs(network_info):
            subnet = str(item.get("Subnet", "")).strip()
            if not subnet:
                continue
            try:
                subnets.append(ipaddress.ip_network(subnet, strict=False))
            except ValueError:
                self.log(f"WARN: ignore invalid docker subnet config: {subnet}")

        allocated: Dict[str, str] = {}
        for container in (network_info.get("Containers") or {}).values():
            name = str(container.get("Name", "")).strip()
            ipv4 = str(container.get("IPv4Address", "")).split("/", 1)[0].strip()
            if not name or not ipv4:
                continue
            try:
                ipaddress.IPv4Address(ipv4)
            except ValueError:
                continue
            allocated[name] = ipv4

        return network_info, subnets, allocated

    def get_docker_network_diagnostic(
        self,
        network_name: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Collect a concise diagnostic snapshot for a Docker network."""
        target_name = network_name or self.get_target_docker_network()
        network_info, subnets, allocated = self.inspect_docker_network(target_name)
        if network_info is None:
            return None

        options = network_info.get("Options", {}) or {}
        bridge_name = str(options.get("com.docker.network.bridge.name", "")).strip()
        return {
            "name": target_name,
            "subnets": [str(subnet) for subnet in subnets],
            "attached": len(allocated),
            "bridge_name": bridge_name,
        }

    def format_docker_network_diagnostic(
        self,
        diagnostic: Optional[Dict[str, Any]],
        planned_new: int = 0,
    ) -> str:
        """Format Docker network diagnostics for logs."""
        if not diagnostic:
            return ""

        subnet_desc = ", ".join(diagnostic.get("subnets") or []) or "unknown"
        parts = [
            f"network={diagnostic.get('name', 'unknown')}",
            f"subnets={subnet_desc}",
            f"attached_containers={diagnostic.get('attached', 'unknown')}",
        ]
        bridge_name = str(diagnostic.get("bridge_name", "")).strip()
        if bridge_name:
            parts.append(f"bridge={bridge_name}")
        if planned_new > 0:
            attached = diagnostic.get("attached", 0)
            parts.append(f"planned_pool={planned_new}")
            parts.append(f"projected_attached={attached + planned_new}")
        return "; ".join(parts)

    def log_target_network_usage(self, planned_new: int = 0) -> None:
        """Log current Docker network usage and warn on high attachment counts."""
        if not self.DOCKER_NETWORK:
            return

        diagnostic = self.get_docker_network_diagnostic()
        detail = self.format_docker_network_diagnostic(diagnostic, planned_new=planned_new)
        if detail:
            self.log(f"docker network status: {detail}")

        if not diagnostic:
            return

        threshold = self.DOCKER_NETWORK_ATTACHMENT_WARN_THRESHOLD
        if threshold is None:
            return

        projected_attached = int(diagnostic.get("attached", 0)) + max(int(planned_new), 0)
        if projected_attached >= threshold:
            self.log(
                "WARN: projected docker network attachment count is high: "
                f"{projected_attached} >= {threshold}. "
                "Linux bridge may fail with 'exchange full'; consider splitting collectors "
                "across separate Docker networks."
            )

    def find_overlapping_docker_networks(
        self,
        target_subnet: ipaddress.IPv4Network,
        ignore_network: Optional[str] = None,
    ) -> List[Tuple[str, List[ipaddress.IPv4Network]]]:
        """List existing docker networks whose IPv4 subnets overlap target_subnet."""
        overlaps: List[Tuple[str, List[ipaddress.IPv4Network]]] = []
        for network_name, subnets in self.list_docker_network_ipv4_subnets(ignore_network=ignore_network):
            matched = [subnet for subnet in subnets if subnet.overlaps(target_subnet)]
            if matched:
                overlaps.append((network_name, matched))
        return overlaps

    def list_docker_network_ipv4_subnets(
        self,
        ignore_network: Optional[str] = None,
    ) -> List[Tuple[str, List[ipaddress.IPv4Network]]]:
        """List all Docker networks with their IPv4 subnets."""
        cp = self.run_cmd(["docker", "network", "ls", "--format", "{{.Name}}"])
        if cp.returncode != 0:
            return []

        networks: List[Tuple[str, List[ipaddress.IPv4Network]]] = []
        for raw_name in cp.stdout.splitlines():
            network_name = raw_name.strip()
            if not network_name or network_name == ignore_network:
                continue
            _, subnets, _ = self.inspect_docker_network(network_name)
            networks.append((network_name, subnets))
        return networks

    def build_target_network_subnet(self) -> ipaddress.IPv4Network:
        """Build the user-defined bridge subnet from CONTAINER_IP_START."""
        if not self.CONTAINER_IP_START:
            raise RuntimeError("CONTAINER_IP_START is required for fixed-IP network setup")
        try:
            prefix = int(self.DOCKER_NETWORK_SUBNET_PREFIX)
            if prefix < 16 or prefix > 30:
                raise ValueError(f"invalid prefix: {prefix}")
            return ipaddress.ip_network(f"{self.CONTAINER_IP_START}/{prefix}", strict=False)
        except ValueError as e:
            self.log(
                "FATAL: invalid docker network subnet config: "
                f"CONTAINER_IP_START={self.CONTAINER_IP_START}, "
                f"prefix={self.DOCKER_NETWORK_SUBNET_PREFIX} -> {e}"
            )
            sys.exit(2)

    def build_target_network_gateway(self, subnet: ipaddress.IPv4Network) -> str:
        """Return the configured gateway or derive one from the target subnet."""
        if self.DOCKER_NETWORK_GATEWAY:
            try:
                gateway = ipaddress.IPv4Address(self.DOCKER_NETWORK_GATEWAY)
            except ValueError as e:
                self.log(
                    "FATAL: invalid docker network gateway config: "
                    f"{self.DOCKER_NETWORK_GATEWAY} -> {e}"
                )
                sys.exit(2)
            if gateway not in subnet:
                self.log(
                    "FATAL: docker network gateway is outside subnet: "
                    f"gateway={gateway} subnet={subnet}"
                )
                sys.exit(2)
            if gateway == subnet.network_address or gateway == subnet.broadcast_address:
                self.log(
                    "FATAL: docker network gateway cannot be the network/broadcast address: "
                    f"gateway={gateway} subnet={subnet}"
                )
                sys.exit(2)
            return str(gateway)

        if subnet.num_addresses < 4:
            self.log(f"FATAL: docker subnet is too small: {subnet}")
            sys.exit(2)
        gateway = ipaddress.IPv4Address(int(subnet.broadcast_address) - 1)
        if gateway == subnet.network_address:
            gateway = ipaddress.IPv4Address(int(subnet.network_address) + 1)
        return str(gateway)

    def ensure_target_network_ready(self) -> None:
        """Create the user-defined Docker network for fixed-IP containers if needed."""
        if not self.CONTAINER_IP_START:
            return

        network_name = self.get_target_docker_network()
        if network_name == "bridge":
            self.log(
                "FATAL: CONTAINER_IP_START is set but DOCKER_NETWORK is missing. "
                "Docker default bridge does not support explicit --ip."
            )
            sys.exit(2)

        network_info, subnets, _ = self.inspect_docker_network(network_name)
        if network_info is not None:
            if not subnets:
                self.log(f"FATAL: docker network {network_name} has no IPv4 subnet config")
                sys.exit(2)

            start_ip = ipaddress.IPv4Address(self.CONTAINER_IP_START)
            if not any(start_ip in subnet for subnet in subnets):
                subnet_desc = ", ".join(str(subnet) for subnet in subnets)
                self.log(
                    f"FATAL: fixed IP start {self.CONTAINER_IP_START} is outside docker network "
                    f"{network_name} IPv4 subnets: {subnet_desc}"
                )
                sys.exit(2)

            expected_gateway = self.build_target_network_gateway(subnets[0])
            existing_gateways = []
            for item in self.get_network_ipam_configs(network_info):
                gateway = str(item.get("Gateway", "")).strip()
                if gateway:
                    existing_gateways.append(gateway)
            if existing_gateways and expected_gateway not in existing_gateways:
                self.log(
                    f"FATAL: docker network {network_name} gateway mismatch: "
                    f"expected={expected_gateway}, actual={', '.join(existing_gateways)}"
                )
                sys.exit(2)
            return

        subnet = self.build_target_network_subnet()
        gateway = self.build_target_network_gateway(subnet)
        self.log(f"creating docker network: {network_name} subnet={subnet} gateway={gateway}")
        cp = self.run_cmd([
            "docker", "network", "create",
            "--driver", "bridge",
            "--subnet", str(subnet),
            "--gateway", gateway,
            network_name,
        ])
        if cp.returncode != 0:
            detail = (cp.stderr or cp.stdout).strip() or "unknown error"
            overlaps = self.find_overlapping_docker_networks(subnet, ignore_network=network_name)
            if overlaps:
                overlap_desc = ", ".join(
                    f"{name}({';'.join(str(item) for item in subnets)})"
                    for name, subnets in overlaps
                )
                detail = f"{detail}; overlapping networks: {overlap_desc}"
            self.log(f"FATAL: failed to create docker network {network_name}: {detail}")
            sys.exit(2)
        self.log(f"created docker network: {network_name} subnet={subnet}")

    def is_usable_container_ip(
        self,
        candidate: ipaddress.IPv4Address,
        subnets: List[ipaddress.IPv4Network]
    ) -> bool:
        """判断候选 IP 是否落在目标子网内且不是网络/广播地址。"""
        if not subnets:
            return True
        for subnet in subnets:
            if candidate not in subnet:
                continue
            if candidate == subnet.network_address:
                return False
            if subnet.num_addresses > 1 and candidate == subnet.broadcast_address:
                return False
            return True
        return False

    def build_container_specs(self, names: List[str]) -> List[Tuple[int, str, Optional[str]]]:
        """为容器池分配固定 IP，自动跳过已占用或不可用地址。"""
        specs = [(index, name, self.build_container_ip(index)) for index, name in enumerate(names)]
        if not specs or specs[0][2] is None:
            return specs

        try:
            start_ip = ipaddress.IPv4Address(self.CONTAINER_IP_START)
        except ValueError as e:
            self.log(f"FATAL: 非法的容器起始 IP：{self.CONTAINER_IP_START} -> {e}")
            sys.exit(2)

        subnets, allocated = self.inspect_target_network()
        used_ips = {
            ipaddress.IPv4Address(ip)
            for ip in allocated.values()
        }
        if self.DOCKER_NETWORK:
            gateway_ip = ipaddress.IPv4Address(
                self.build_target_network_gateway(self.build_target_network_subnet())
            )
            used_ips.add(gateway_ip)
        assigned: Dict[str, str] = {}
        reserved_ips: List[ipaddress.IPv4Address] = []

        for name in names:
            current_ip = allocated.get(name)
            if not current_ip:
                continue
            current_addr = ipaddress.IPv4Address(current_ip)
            if current_addr < start_ip:
                continue
            if not self.is_usable_container_ip(current_addr, subnets):
                continue
            assigned[name] = current_ip
            reserved_ips.append(current_addr)

        next_ip = start_ip
        if reserved_ips:
            next_ip = ipaddress.IPv4Address(max(int(ip) for ip in reserved_ips) + 1)

        for _, name, _ in specs:
            if name in assigned:
                continue
            while next_ip in used_ips or not self.is_usable_container_ip(next_ip, subnets):
                next_ip += 1
            assigned[name] = str(next_ip)
            used_ips.add(next_ip)
            next_ip += 1

        resolved_specs = [(index, name, assigned[name]) for index, name, _ in specs]
        if resolved_specs and resolved_specs[0][2] != specs[0][2]:
            self.log(
                f"WARN: 固定 IP 自动顺延，起始配置={specs[0][2]}，"
                f"实际首个可用地址={resolved_specs[0][2]}"
            )
        return resolved_specs

    def get_container_ipv4(self, name: str) -> Optional[str]:
        """获取容器当前 IPv4 地址。"""
        target_network = self.get_target_docker_network()
        if target_network != "bridge":
            inspect_expr = f'{{{{with index .NetworkSettings.Networks "{target_network}"}}}}{{{{.IPAddress}}}}{{{{end}}}}'
        else:
            inspect_expr = "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}"
        cp = self.run_cmd(["docker", "inspect", "-f", inspect_expr, name])
        if cp.returncode != 0:
            return None
        values = cp.stdout.strip().split()
        return values[0] if values else None

    def remove_container(self, name: str) -> None:
        """删除单个容器。"""
        cp = self.run_cmd(["docker", "rm", "-f", name])
        if cp.returncode != 0:
            self.log(f"FATAL: 删除容器失败: {name} -> {cp.stderr.strip()}")
            sys.exit(2)
        self.log(f"removed container: {name}")

    def create_container(
        self,
        name: str,
        host_code_path: str,
        image: str,
        container_ip: Optional[str] = None
    ) -> None:
        """创建容器，同时挂载代码目录和 tools 目录"""
        uid, gid = str(os.getuid()), str(os.getgid())
        tools_path = os.path.join(_project_root, 'tools')
        self.log(f"creating container: {name}")
        cmd = [
            "docker", "run",
            "--init",
            "--dns", "172.17.0.1",  # 使用宿主机 dnsmasq 作为 DNS 缓存
            "--volume", f"{host_code_path}:{self.CONTAINER_CODE_PATH}",
            "--volume", f"{tools_path}:{self.CONTAINER_CODE_PATH}/tools",
            "-e", f"HOST_UID={uid}",
            "-e", f"HOST_GID={gid}",
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
            detail = (cp.stderr or cp.stdout).strip() or f"rc={cp.returncode}"
            network_detail = self.format_docker_network_diagnostic(
                self.get_docker_network_diagnostic()
            )
            if network_detail:
                detail = f"{detail}; {network_detail}"
            self.log(f"FATAL: 创建容器失败: {name} -> {detail}")
            sys.exit(2)
        if container_ip:
            self.log(f"created container: {name} ip={container_ip}")
        else:
            self.log(f"created container: {name}")

    def start_container(self, name: str) -> None:
        """启动容器"""
        cp = self.run_cmd(["docker", "start", name])
        if cp.returncode != 0:
            detail = (cp.stderr or cp.stdout).strip() or f"rc={cp.returncode}"
            network_detail = self.format_docker_network_diagnostic(
                self.get_docker_network_diagnostic()
            )
            if network_detail:
                detail = f"{detail}; {network_detail}"
            self.log(f"FATAL: 启动容器失败: {name} -> {detail}")
            sys.exit(2)
        self.log(f"started container: {name}")

    def disable_offload_once(self, name: str) -> None:
        """关闭容器的包合并（TSO/GSO/GRO），每次启动后都强制执行"""
        shell = r'''
            ethtool -K eth0 tso off gso off gro off
            exit $?
        '''
        cp = self.run_cmd(["docker", "exec", name, "sh", "-lc", shell])
        if cp.returncode == 0:
            self.log(f"{name}: offload disabled (TSO/GSO/GRO off)")
        else:
            msg = (cp.stderr or cp.stdout).strip()
            self.log(f"WARN: {name}: 关闭包合并失败：{msg if msg else 'unknown error'}")

    def get_target_bridge_interface(self) -> Optional[str]:
        """Return the host bridge interface name for the target Docker network."""
        network_name = self.get_target_docker_network()
        if network_name == "bridge":
            return "docker0"

        network_info, _, _ = self.inspect_docker_network(network_name)
        if network_info is None:
            return None

        network_id = str(network_info.get("Id", "")).strip()
        if len(network_id) < 12:
            return None
        return f"br-{network_id[:12]}"

    def disable_host_interface_offload(self, interface_name: str, *, fatal: bool, label: str) -> bool:
        """Disable TSO/GSO/GRO on a host interface."""
        shell = f'''
            set -e
            if ! command -v ethtool >/dev/null 2>&1; then
                echo "ethtool not found" >&2
                exit 1
            fi
            if ! ip link show "{interface_name}" >/dev/null 2>&1; then
                echo "interface not found: {interface_name}" >&2
                exit 1
            fi
            ethtool -K "{interface_name}" tso off gso off gro off
        '''
        cp = self.run_cmd(["sh", "-lc", shell])
        if cp.returncode != 0:
            detail = (cp.stderr or cp.stdout).strip() or "unknown error"
            if fatal:
                self.log(f"FATAL: failed to disable {label} offload: {detail}")
                sys.exit(2)
            self.log(f"WARN: failed to disable {label} offload: {detail}")
            return False
        self.log(f"{label}: offload disabled (TSO/GSO/GRO off)")
        return True

    def disable_target_bridge_offload(self) -> None:
        """Disable offload on the host bridge backing the current Docker network."""
        network_name = self.get_target_docker_network()
        if network_name == "bridge":
            self.log("skip docker0 offload disable; managed by systemd service")
            return

        interface_name = self.get_target_bridge_interface()
        if not interface_name:
            self.log(f"FATAL: failed to resolve bridge interface for docker network {network_name}")
            sys.exit(2)
        self.disable_host_interface_offload(
            interface_name,
            fatal=True,
            label=f"{network_name}/{interface_name}",
        )

    def find_host_interface_by_ifindex(self, ifindex: int) -> Optional[str]:
        """Resolve a host interface name from ifindex via /sys/class/net."""
        if ifindex <= 0:
            return None

        net_root = Path("/sys/class/net")
        if not net_root.is_dir():
            return None

        for entry in net_root.iterdir():
            try:
                value = int((entry / "ifindex").read_text(encoding="utf-8").strip())
            except (OSError, ValueError):
                continue
            if value == ifindex:
                return entry.name
        return None

    def disable_host_veth_offload_once(self, name: str) -> None:
        """Disable offload on the host-side veth peer of a container eth0."""
        cp = self.run_cmd(["docker", "exec", name, "cat", "/sys/class/net/eth0/iflink"])
        if cp.returncode != 0:
            detail = (cp.stderr or cp.stdout).strip() or "unknown error"
            self.log(f"WARN: {name}: failed to read eth0 iflink: {detail}")
            return

        try:
            ifindex = int(cp.stdout.strip())
        except ValueError:
            self.log(f"WARN: {name}: invalid eth0 iflink: {cp.stdout.strip()!r}")
            return

        interface_name = self.find_host_interface_by_ifindex(ifindex)
        if not interface_name:
            self.log(f"WARN: {name}: host veth peer not found for ifindex={ifindex}")
            return

        self.disable_host_interface_offload(
            interface_name,
            fatal=False,
            label=f"{name}/{interface_name}",
        )

    def list_runtime_container_names(self) -> List[str]:
        """List all Docker containers that belong to the current numeric namespace."""
        prefix = str(self.CONTAINER_PREFIX or "")
        if not prefix:
            return []

        cp = self.run_cmd(["docker", "ps", "-a", "--format", "{{.Names}}"], timeout=60)
        if cp.returncode != 0:
            detail = (cp.stderr or cp.stdout or "").strip()
            self.log(f"WARN: 列出现有容器失败，改用当前容器池名称清理: {detail}")
            return []

        pattern = re.compile(rf"^{re.escape(prefix)}\d+$")
        names = [
            line.strip()
            for line in (cp.stdout or "").splitlines()
            if pattern.fullmatch(line.strip())
        ]

        def sort_key(name: str) -> Tuple[int, str]:
            suffix = name[len(prefix):]
            return (int(suffix), name) if suffix.isdigit() else (10**12, name)

        return sorted(names, key=sort_key)

    def remove_containers(self) -> None:
        """删除当前运行命名空间下的数字后缀容器，包含旧动态池残留。"""
        removed = 0
        names = set(self.build_container_names())
        names.update(self.list_runtime_container_names())
        for name in sorted(names):
            if self.container_exists(name) is None:
                continue
            self.remove_container(name)
            removed += 1
        if removed:
            self.log(f"removed {removed} containers for current runtime namespace")

    def build_container_names(self) -> List[str]:
        """构建容器名列表"""
        return [f"{self.CONTAINER_PREFIX}{i}" for i in range(self.get_container_count())]

    def get_container_count(self) -> int:
        """Return the resolved positive container count."""
        configured = getattr(self, "CONTAINER_COUNT", None)
        if configured is None:
            return 1
        return max(int(configured), 1)

    def resolve_container_count(self, task_count: int) -> int:
        """Resolve explicit or task-count-based container count before pool creation."""
        configured = getattr(self, "CONTAINER_COUNT", None)
        if configured is not None:
            return max(int(configured), 1)

        tasks_per_container = max(int(self.DYNAMIC_CONTAINER_TASKS_PER_CONTAINER), 1)
        max_count = max(int(self.MAX_DYNAMIC_CONTAINER_COUNT), 1)
        task_count = max(int(task_count), 0)
        one_per_task_limit = max(int(self.DYNAMIC_ONE_CONTAINER_PER_TASK_LIMIT), 0)
        base_count = min(task_count, one_per_task_limit)
        overflow_tasks = max(task_count - one_per_task_limit, 0)
        overflow_count = (overflow_tasks + tasks_per_container - 1) // tasks_per_container
        dynamic_count = base_count + overflow_count
        return min(dynamic_count, max_count)

    def configure_container_count_for_jobs(self, jobs: List[Dict[str, str]]) -> int:
        """Set CONTAINER_COUNT once the first task batch is known."""
        was_dynamic = getattr(self, "CONTAINER_COUNT", None) is None
        count = self.resolve_container_count(len(jobs))
        self.CONTAINER_COUNT = count
        if was_dynamic:
            self.log(
                f"动态容器数={count}（任务数={len(jobs)}，"
                f"前 {self.DYNAMIC_ONE_CONTAINER_PER_TASK_LIMIT} 个任务按一任务一容器，"
                f"之后每 {self.DYNAMIC_CONTAINER_TASKS_PER_CONTAINER} 个任务增加一个容器，"
                f"上限={self.MAX_DYNAMIC_CONTAINER_COUNT}）"
            )
        else:
            self.log(f"容器数={count}（显式配置）")
        return count

    def get_default_action_source(self) -> Path:
        """Return the fallback action.py path."""
        return Path(_project_root) / "traffic_capture_single_csv" / "action.py"

    def ensure_host_code_path_ready(self, ensure_action: bool = True) -> Path:
        """Ensure HOST_CODE_PATH exists and optionally backfill action.py."""
        host_code = Path(self.HOST_CODE_PATH)
        if host_code.exists() and not host_code.is_dir():
            raise NotADirectoryError(f"HOST_CODE_PATH exists but is not a directory: {host_code}")

        if not host_code.exists():
            host_code.mkdir(parents=True, exist_ok=True)
            self.log(f"created HOST_CODE_PATH: {host_code}")

        if ensure_action:
            action_src = self.get_default_action_source()
            if not action_src.exists() or not action_src.is_file():
                raise FileNotFoundError(f"default action.py not found: {action_src}")

            action_dst = host_code / "action.py"
            if action_dst.exists() and not action_dst.is_file():
                raise IsADirectoryError(f"target action.py exists but is not a file: {action_dst}")
            if not action_dst.exists():
                shutil.copy2(action_src, action_dst)
                self.chown_recursive(str(action_dst))
                self.log(f"copied default action.py to: {action_dst}")

        return host_code

    def prepare_pool_once(self) -> List[str]:
        """准备容器池，返回容器名列表"""
        self.ensure_docker_available()

        host_code = self.ensure_host_code_path_ready()
        if not host_code.exists():
            self.log(f"WARN: 宿主机代码目录不存在：{host_code}，仍会尝试挂载。")
        if not host_code.is_absolute():
            self.log(f"WARN: 建议使用绝对路径挂载，当前={host_code}")

        names = self.build_container_names()
        self.ensure_target_network_ready()
        self.disable_target_bridge_offload()
        self.log_target_network_usage(planned_new=len(names))
        self.log("checking and creating containers...")
        self.log(f"容器池规模={len(names)}: {names[0]} … {names[-1]}")
        container_specs = self.build_container_specs(names)
        if container_specs and container_specs[0][2] is not None:
            self.log(f"固定 IP 范围={container_specs[0][2]} -> {container_specs[-1][2]}")

        created: List[str] = []
        created_lock = threading.Lock()

        def check_and_create(spec: Tuple[int, str, Optional[str]]) -> None:
            _, name, expected_ip = spec
            exists = self.container_exists(name)
            if exists is None:
                self.create_container(name, str(host_code), self.DOCKER_IMAGE, container_ip=expected_ip)
                with created_lock:
                    created.append(name)
                return
            if expected_ip:
                current_ip = self.get_container_ipv4(name)
                if current_ip != expected_ip:
                    self.log(
                        f"WARN: {name} 当前 IP={current_ip or 'unknown'}，"
                        f"期望 IP={expected_ip}，将重建容器"
                    )
                    self.remove_container(name)
                    self.create_container(name, str(host_code), self.DOCKER_IMAGE, container_ip=expected_ip)
                    with created_lock:
                        created.append(name)

        # Pass 1：并发创建不存在的容器
        with ThreadPoolExecutor(max_workers=min(len(names), 20)) as pool:
            pool.map(check_and_create, container_specs)

        # Pass 2：启动未运行的容器
        for n in names:
            if not self.container_running(n):
                self.start_container(n)

        time.sleep(5)

        # Pass 3：对容器内 eth0 和宿主机 veth peer 关闭 offload
        for n in names:
            self.disable_offload_once(n)
            self.disable_host_veth_offload_once(n)

        self._browser_label = self.detect_browser_label(names[0])
        return names

    # ============== 文件操作 ==============
    def normalize_host_code_cleanup_permissions(self, base_path: Path) -> bool:
        """Return temporary HOST_CODE_PATH subdirs to the host user before deletion."""
        script = r"""
import os
import sys

root = sys.argv[1]
uid = int(sys.argv[2])
gid = int(sys.argv[3])
dir_mode = int(sys.argv[4], 8)
file_mode = int(sys.argv[5], 8)
skip_names = {"tools"}
errors = []

def apply(path, mode):
    try:
        os.lchown(path, uid, gid)
    except OSError as exc:
        errors.append(f"chown {path}: {exc}")
    if os.path.islink(path):
        return
    try:
        os.chmod(path, mode)
    except OSError as exc:
        errors.append(f"chmod {path}: {exc}")

for name in os.listdir(root):
    if name in skip_names:
        continue
    path = os.path.join(root, name)
    if os.path.islink(path) or not os.path.isdir(path):
        continue
    for current_root, dirs, files in os.walk(path, topdown=False, followlinks=False):
        for file_name in files:
            apply(os.path.join(current_root, file_name), file_mode)
        for dir_name in dirs:
            apply(os.path.join(current_root, dir_name), dir_mode)
        apply(current_root, dir_mode)

if errors:
    print("; ".join(errors[:5]), file=sys.stderr)
    if len(errors) > 5:
        print(f"... {len(errors) - 5} more permission errors", file=sys.stderr)
    sys.exit(1)
"""

        errors: List[str] = []
        for container in self.build_container_names():
            if not self.container_running(container):
                continue
            cp = self.run_cmd(
                [
                    "docker", "exec", "-u", "0:0", container,
                    "python", "-c", script,
                    self.CONTAINER_CODE_PATH.rstrip("/") or "/app",
                    str(self.DEFAULT_UID),
                    str(self.DEFAULT_GID),
                    oct(self.SUCCESS_OUTPUT_DIR_MODE),
                    oct(self.SUCCESS_OUTPUT_FILE_MODE),
                ],
                timeout=120,
            )
            if cp.returncode == 0:
                return True
            detail = (cp.stderr or cp.stdout).strip() or f"rc={cp.returncode}"
            errors.append(f"{container}: {detail}")

        if errors:
            self.log(f"WARN: HOST_CODE_PATH cleanup permission normalization failed: {errors[0]}")
        return False

    def clear_host_code_subdirs(self) -> None:
        """清理 HOST_CODE_PATH 下的临时子目录，保留 tools"""
        base_path = self.ensure_host_code_path_ready()
        if not base_path.exists() or not base_path.is_dir():
            self.log(f"WARN: HOST_CODE_PATH 不存在或不是目录：{base_path}")
            return

        entries = [
            entry for entry in base_path.iterdir()
            if entry.is_dir() and not entry.is_symlink() and entry.name != 'tools'
        ]
        if not entries:
            return

        self.normalize_host_code_cleanup_permissions(base_path)

        for entry in entries:
            if entry.is_dir() and entry.name != 'tools':
                try:
                    shutil.rmtree(entry)
                    self.log(f"删除子目录: {entry}")
                except Exception as e:
                    self.log(f"WARN: 删除子目录失败: {entry} -> {e}")


    def chown_path(self, path: str, uid: int = None, gid: int = None) -> None:
        """Set one path owner when the current process is allowed to do so."""
        uid = self.DEFAULT_UID if uid is None else uid
        gid = self.DEFAULT_GID if gid is None else gid
        try:
            os.chown(path, uid, gid, follow_symlinks=False)
        except Exception:
            pass

    def chown_recursive(self, path: str, uid: int = None, gid: int = None) -> None:
        """递归设置文件/目录的所有者"""
        uid = self.DEFAULT_UID if uid is None else uid
        gid = self.DEFAULT_GID if gid is None else gid
        self.chown_path(path, uid, gid)
        if os.path.isdir(path):
            for root, dirs, files in os.walk(path, followlinks=False):
                for name in dirs:
                    p = os.path.join(root, name)
                    self.chown_path(p, uid, gid)
                for name in files:
                    p = os.path.join(root, name)
                    self.chown_path(p, uid, gid)

    @staticmethod
    def _chmod_no_follow(path: str, mode: int) -> None:
        if os.path.islink(path):
            return
        try:
            os.chmod(path, mode, follow_symlinks=False)
        except (NotImplementedError, TypeError):
            os.chmod(path, mode)

    def _success_output_dir_chain(self, root: str, leaf_dir: str) -> List[str]:
        root_abs = os.path.abspath(root)
        leaf_abs = os.path.abspath(leaf_dir)
        if leaf_abs != root_abs and not leaf_abs.startswith(root_abs + os.sep):
            return [leaf_abs]

        dirs = []
        current = leaf_abs
        while True:
            dirs.append(current)
            if current == root_abs:
                break
            parent = os.path.dirname(current)
            if parent == current:
                break
            current = parent
        return list(reversed(dirs))

    def normalize_success_output_dirs(self, root: str, leaf_dir: str) -> None:
        """Normalize only directories touched by the current result move."""
        if not self.NORMALIZE_SUCCESS_OUTPUT_MODES:
            return

        for path in self._success_output_dir_chain(root, leaf_dir):
            if not os.path.isdir(path) or os.path.islink(path):
                continue
            self.chown_path(path)
            self._chmod_no_follow(path, self.SUCCESS_OUTPUT_DIR_MODE)

    def normalize_success_output_path(self, path: str, raise_on_error: bool = True) -> None:
        """Normalize only the file or directory produced by the current move."""
        if not self.NORMALIZE_SUCCESS_OUTPUT_MODES or not path or not os.path.exists(path):
            return

        errors: List[str] = []

        def chmod_one(target: str, mode: int) -> None:
            try:
                self._chmod_no_follow(target, mode)
            except OSError as e:
                errors.append(f"{target}: {e}")

        if os.path.isdir(path) and not os.path.islink(path):
            for current_root, dirs, files in os.walk(path, topdown=False, followlinks=False):
                for name in files:
                    chmod_one(os.path.join(current_root, name), self.SUCCESS_OUTPUT_FILE_MODE)
                for name in dirs:
                    chmod_one(os.path.join(current_root, name), self.SUCCESS_OUTPUT_DIR_MODE)
                chmod_one(current_root, self.SUCCESS_OUTPUT_DIR_MODE)
        else:
            chmod_one(path, self.SUCCESS_OUTPUT_FILE_MODE)

        if errors:
            detail = "; ".join(errors[:3])
            if len(errors) > 3:
                detail += f"; ... {len(errors) - 3} more"
            message = f"failed to normalize success output path permissions: {detail}"
            if raise_on_error:
                raise PermissionError(message)
            self.log(f"WARN: {message}")

    def prepare_success_output_tree(self, dst: str) -> None:
        """Prepare one domain result directory before moving task outputs into it."""
        os.makedirs(dst, exist_ok=True)
        self.chown_path(dst)
        self.normalize_success_output_dirs(dst, dst)

    def ensure_base_dst_writable(self) -> None:
        """Fail fast when BASE_DST cannot accept result files."""
        if not self.VERIFY_BASE_DST_WRITABLE_ON_START:
            return

        base_dst = str(self.BASE_DST or "").strip()
        if not base_dst:
            self.log("FATAL: BASE_DST is empty; cannot save capture results.")
            sys.exit(2)

        try:
            os.makedirs(base_dst, exist_ok=True)
            self.chown_path(base_dst)
        except OSError as e:
            self.log(f"FATAL: BASE_DST is not writable or cannot be created: {base_dst} -> {e}")
            sys.exit(2)

        probe_path = os.path.join(base_dst, f".write_test_{os.getpid()}_{threading.get_ident()}")
        try:
            with open(probe_path, "w", encoding="utf-8") as probe:
                probe.write("ok\n")
            os.unlink(probe_path)
        except OSError as e:
            try:
                if os.path.exists(probe_path):
                    os.unlink(probe_path)
            except OSError:
                pass
            self.log(f"FATAL: BASE_DST is not writable: {base_dst} -> {e}")
            sys.exit(2)

        try:
            self._chmod_no_follow(base_dst, self.SUCCESS_OUTPUT_DIR_MODE)
        except OSError as e:
            self.log(f"WARN: cannot chmod BASE_DST to {oct(self.SUCCESS_OUTPUT_DIR_MODE)}: {base_dst} -> {e}")

    def copy_task_csv_to_base_dst(self) -> Optional[str]:
        """Copy CSV_PATH to the BASE_DST root once without overwriting it."""
        csv_path = str(getattr(self, "CSV_PATH", "") or "").strip()
        if not csv_path:
            return None

        source_path = os.path.abspath(csv_path)
        if not os.path.isfile(source_path):
            raise FileNotFoundError(f"task CSV does not exist: {source_path}")

        base_dst = os.path.abspath(str(self.BASE_DST))
        target_path = os.path.join(base_dst, os.path.basename(source_path))
        if os.path.exists(target_path):
            self.log(f"任务 CSV 已存在，跳过复制: {target_path}")
            return target_path

        temp_fd, temp_path = tempfile.mkstemp(
            dir=base_dst,
            prefix=f".{os.path.basename(source_path)}.",
            suffix=".tmp",
        )
        os.close(temp_fd)
        try:
            shutil.copy2(source_path, temp_path)
            try:
                os.link(temp_path, target_path)
            except FileExistsError:
                self.log(f"任务 CSV 已存在，跳过复制: {target_path}")
                return target_path
            except OSError:
                target_created = False
                try:
                    with open(temp_path, "rb") as source, open(target_path, "xb") as target:
                        target_created = True
                        shutil.copyfileobj(source, target)
                        target.flush()
                        os.fsync(target.fileno())
                    shutil.copystat(temp_path, target_path)
                except FileExistsError:
                    self.log(f"任务 CSV 已存在，跳过复制: {target_path}")
                    return target_path
                except Exception:
                    if target_created:
                        try:
                            os.unlink(target_path)
                        except FileNotFoundError:
                            pass
                    raise

            self.chown_path(target_path)
            self.normalize_success_output_path(target_path)
            self.log(f"已复制任务 CSV: {source_path} -> {target_path}")
            return target_path
        finally:
            try:
                os.unlink(temp_path)
            except FileNotFoundError:
                pass

    @staticmethod
    def _artifact_stems(directory: Path, suffix: str) -> set:
        """Return artifact filename stems after removing one known suffix."""
        if not directory.is_dir():
            return set()
        return {
            entry.name[:-len(suffix)]
            for entry in directory.iterdir()
            if entry.is_file() and entry.name.endswith(suffix)
        }

    def verify_task_completeness(self, csv_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Compare the copied task CSV with complete five-artifact result groups."""
        manifest_path = str(csv_path or self._copied_task_csv_path or "").strip()
        if not manifest_path:
            self.log("未配置任务 CSV，跳过任务完整度校验")
            return None
        if not os.path.isfile(manifest_path):
            raise FileNotFoundError(f"copied task CSV does not exist: {manifest_path}")

        _, rows = self._read_csv_records(Path(manifest_path))
        expected_counts: Counter = Counter()
        for row in rows:
            normalized = {
                str(key).strip().lower(): (value or "").strip()
                for key, value in row.items()
                if isinstance(key, str)
            }
            task_id = normalized.get("id", "")
            domain = normalized.get("domain", "")
            if not task_id or not domain:
                continue
            expected_counts[(task_id, domain)] += 1

        base_dst = Path(self.BASE_DST).resolve()
        actual_counts: Counter = Counter()
        required_artifacts = {
            "pcap": ".pcap",
            "ssl_key": "_ssl_key.log",
            "content": ".text",
            "html": ".html",
            "screenshot": ".png",
        }
        artifact_counts = {
            artifact_name: Counter()
            for artifact_name in required_artifacts
        }

        for domain in sorted({domain for _, domain in expected_counts}):
            domain_root = base_dst / domain
            stem_sets = {
                artifact_name: self._artifact_stems(domain_root / artifact_name, suffix)
                for artifact_name, suffix in required_artifacts.items()
            }
            for artifact_name, stems in stem_sets.items():
                for stem in stems:
                    task_id, separator, _ = stem.partition("_")
                    if separator and task_id:
                        artifact_counts[artifact_name][(task_id, domain)] += 1

            complete_stems = set.intersection(*stem_sets.values()) if stem_sets else set()
            for stem in complete_stems:
                task_id, separator, _ = stem.partition("_")
                if separator and task_id:
                    actual_counts[(task_id, domain)] += 1

        matched_tasks = sum(
            min(expected_count, actual_counts.get(key, 0))
            for key, expected_count in expected_counts.items()
        )
        missing_items = []
        extra_items = []
        for key in sorted(set(expected_counts) | set(actual_counts)):
            task_id, domain = key
            expected_count = expected_counts.get(key, 0)
            actual_count = actual_counts.get(key, 0)
            if actual_count < expected_count:
                missing_items.append(
                    {
                        "id": task_id,
                        "domain": domain,
                        "expected": expected_count,
                        "complete": actual_count,
                        "missing": expected_count - actual_count,
                        "artifact_counts": {
                            name: counts.get(key, 0)
                            for name, counts in artifact_counts.items()
                        },
                    }
                )
            elif actual_count > expected_count:
                extra_items.append(
                    {
                        "id": task_id,
                        "domain": domain,
                        "expected": expected_count,
                        "complete": actual_count,
                        "extra": actual_count - expected_count,
                        "artifact_counts": {
                            name: counts.get(key, 0)
                            for name, counts in artifact_counts.items()
                        },
                    }
                )

        expected_tasks = sum(expected_counts.values())
        complete_groups = sum(actual_counts.values())
        missing_tasks = max(expected_tasks - matched_tasks, 0)
        extra_groups = sum(item["extra"] for item in extra_items)
        completeness_percent = (
            round(matched_tasks * 100.0 / expected_tasks, 4)
            if expected_tasks
            else 100.0
        )
        report = {
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "csv_path": str(Path(manifest_path).resolve()),
            "base_dst": str(base_dst),
            "required_artifacts": list(required_artifacts),
            "artifact_file_counts": {
                name: sum(counts.values())
                for name, counts in artifact_counts.items()
            },
            "expected_tasks": expected_tasks,
            "complete_artifact_groups": complete_groups,
            "matched_tasks": matched_tasks,
            "missing_tasks": missing_tasks,
            "extra_complete_groups": extra_groups,
            "completeness_percent": completeness_percent,
            "missing": missing_items,
            "extra": extra_items,
        }

        self._write_execution_task_log("completeness", **report)
        self.log(
            "任务完整度校验: "
            f"expected={expected_tasks}, matched={matched_tasks}, "
            f"missing={missing_tasks}, extra={extra_groups}, "
            f"completeness={completeness_percent:.4f}%"
        )
        return report

    def chown_container_result_paths(self, container: str, paths: List[str]) -> None:
        """Let the host user move files created by root inside the container."""
        targets: List[str] = []
        seen = set()
        container_prefix = f"{self.CONTAINER_CODE_PATH.rstrip('/')}/"

        for raw_path in paths:
            path = str(raw_path or "").strip()
            if not path.startswith(container_prefix):
                continue
            parent = posixpath.dirname(path)
            for target in (parent, path):
                if target and target not in seen:
                    targets.append(target)
                    seen.add(target)

        if not targets:
            return

        owner = f"{self.DEFAULT_UID}:{self.DEFAULT_GID}"
        cp = self.run_cmd(["docker", "exec", container, "chown", owner, *targets], timeout=60)
        if cp.returncode != 0:
            detail = (cp.stderr or cp.stdout).strip() or f"rc={cp.returncode}"
            raise PermissionError(
                f"failed to chown container result paths for host move: "
                f"container={container} owner={owner} detail={detail}"
            )

    def move_and_chown(self, src: str, dst_dir: str, result_root: Optional[str] = None) -> str:
        """移动文件并设置所有者"""
        os.makedirs(dst_dir, exist_ok=True)
        self.chown_path(dst_dir)
        self.normalize_success_output_dirs(result_root or dst_dir, dst_dir)
        target_path = os.path.join(dst_dir, os.path.basename(src))
        if os.path.exists(target_path):
            raise FileExistsError(f"result target already exists: {target_path}")
        new_path = shutil.move(src, target_path)
        self.chown_recursive(new_path)
        self.normalize_success_output_path(new_path)
        return new_path

    @staticmethod
    def preflight_result_moves(move_plan: List[Tuple[str, str]]) -> None:
        """Validate all required sources and targets before moving any result."""
        targets = set()
        for src_path, dst_dir in move_plan:
            if not src_path or not os.path.isfile(src_path):
                raise FileNotFoundError(f"result source missing: {src_path or '<empty>'}")

            target_path = os.path.join(dst_dir, os.path.basename(src_path))
            if target_path in targets:
                raise RuntimeError(f"duplicate result target in move plan: {target_path}")
            targets.add(target_path)
            if os.path.exists(target_path):
                raise FileExistsError(f"result target already exists: {target_path}")

    # ============== CSV 操作 ==============
    @staticmethod
    def _get_csv_field_index(header_fields: List[str], field_name: str) -> Optional[int]:
        target = field_name.strip().lower()
        for index, header in enumerate(header_fields):
            if str(header).strip().lower() == target:
                return index
        return None

    def _repair_csv_row_values(self, header_fields: List[str], row_values: List[str]) -> List[str]:
        """Repair common unquoted commas inside the URL field.

        Some input files contain rows like:
        id,https://example/wiki/3,3-title,example.com
        for a 3-column id,url,domain CSV. The stdlib csv reader must split this
        into four values because the URL field is not quoted. Treat surplus
        values as part of the URL and keep the last logical fields aligned.
        """
        header_count = len(header_fields)
        values = ["" if value is None else str(value) for value in row_values]
        if len(values) <= header_count:
            return values + [""] * max(header_count - len(values), 0)

        url_index = self._get_csv_field_index(header_fields, "url")
        if url_index is None:
            return values[:header_count - 1] + [",".join(values[header_count - 1:])]

        extra_count = len(values) - header_count
        repaired: List[str] = []
        source_index = 0
        for header_index in range(header_count):
            if header_index == url_index:
                url_end = source_index + extra_count + 1
                repaired.append(",".join(values[source_index:url_end]))
                source_index = url_end
                continue
            repaired.append(values[source_index] if source_index < len(values) else "")
            source_index += 1
        return repaired

    def _csv_row_to_dict(self, header_fields: List[str], row_values: List[str]) -> Dict[str, str]:
        repaired_values = self._repair_csv_row_values(header_fields, row_values)
        return {
            header_fields[index]: (repaired_values[index] if index < len(repaired_values) else "")
            for index in range(len(header_fields))
        }

    def _read_csv_records(self, p: Path) -> Tuple[List[str], List[Dict[str, str]]]:
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            try:
                raw_header = next(reader)
            except StopIteration:
                return [], []

            header_fields = [str(h).strip() for h in raw_header]
            rows = [
                self._csv_row_to_dict(header_fields, row)
                for row in reader
                if row
            ]
        return header_fields, rows

    def read_jobs_from_csv(self, csv_path: str) -> Tuple[List[Dict[str, str]], List[str]]:
        """从 CSV 读取任务"""
        p = Path(csv_path)
        if not p.exists():
            return [], ["id", "url", "domain"]

        header_fields, rows = self._read_csv_records(p)
        if not header_fields:
            return [], ["id", "url", "domain"]

        def get_case_insensitive(row: Dict[str, str], key: str) -> str:
            for k, v in row.items():
                if isinstance(k, str) and k.lower() == key:
                    return (v or "").strip()
            return ""

        jobs: List[Dict[str, str]] = []
        for r in rows:
            rid = get_case_insensitive(r, "id")
            url = get_case_insensitive(r, "url")
            dom = get_case_insensitive(r, "domain")
            if not url:
                continue
            jobs.append({"row_id": rid, "url": url, "domain": dom})

        return jobs, header_fields

    def _legacy_remove_from_csv_unused(self, csv_path: str, row_id: str) -> None:
        """从 CSV 中删除指定记录（原子操作，一次只删除一条）"""
        target_id = str(row_id).strip()
        if not target_id:
            return

        with self._csv_lock:
            p = Path(csv_path)
            if not p.exists():
                return
            original_stat = p.stat()

            header_fields, rows = self._read_csv_records(p)
            if not header_fields:
                return

            def get_id(row: Dict[str, str]) -> str:
                for k, v in row.items():
                    if isinstance(k, str) and k.lower() == "id":
                        return (v or "").strip()
                return ""

            # 只删除第一条匹配的记录
            remaining_rows = []
            removed = False
            for r in rows:
                if not removed and get_id(r) == target_id:
                    removed = True  # 跳过第一条匹配的记录
                else:
                    remaining_rows.append(r)

            if not removed:
                return

            tmp_fd, tmp_path = tempfile.mkstemp(dir=p.parent, suffix=".tmp", prefix=".csv_")
            try:
                with os.fdopen(tmp_fd, "w", encoding="utf-8-sig", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=header_fields, extrasaction="ignore")
                    writer.writeheader()
                    writer.writerows(remaining_rows)
                os.chmod(tmp_path, stat.S_IMODE(original_stat.st_mode))
                if hasattr(os, "chown"):
                    os.chown(tmp_path, original_stat.st_uid, original_stat.st_gid)
                os.replace(tmp_path, csv_path)
                self.log(f"已从 CSV 删除记录 row_id={row_id}，剩余 {len(remaining_rows)} 条")
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise

    def remove_first_matching_row_from_csv(
        self,
        csv_path: str,
        match_fields: Dict[str, str],
    ) -> None:
        """Remove the first CSV row whose selected fields all match."""
        normalized_fields = [
            (str(key).strip().lower(), (value or "").strip())
            for key, value in match_fields.items()
            if str(key).strip()
        ]
        if not normalized_fields:
            return

        with self._csv_lock:
            p = Path(csv_path)
            if not p.exists():
                return
            original_stat = p.stat()

            header_fields, rows = self._read_csv_records(p)
            if not header_fields:
                return

            def row_matches(row: Dict[str, str]) -> bool:
                for expected_key, expected_value in normalized_fields:
                    actual_value = ""
                    for actual_key, actual_raw_value in row.items():
                        if isinstance(actual_key, str) and actual_key.lower() == expected_key:
                            actual_value = (actual_raw_value or "").strip()
                            break
                    if actual_value != expected_value:
                        return False
                return True

            remaining_rows = []
            removed = False
            for row in rows:
                if not removed and row_matches(row):
                    removed = True
                else:
                    remaining_rows.append(row)

            if not removed:
                return

            tmp_fd, tmp_path = tempfile.mkstemp(dir=p.parent, suffix=".tmp", prefix=".csv_")
            try:
                with os.fdopen(tmp_fd, "w", encoding="utf-8-sig", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=header_fields, extrasaction="ignore")
                    writer.writeheader()
                    writer.writerows(remaining_rows)
                os.chmod(tmp_path, stat.S_IMODE(original_stat.st_mode))
                if hasattr(os, "chown"):
                    os.chown(tmp_path, original_stat.st_uid, original_stat.st_gid)
                os.replace(tmp_path, csv_path)
                match_desc = ", ".join(f"{key}={value}" for key, value in normalized_fields)
                self.log(
                    f"已从 CSV 删除记录 row={match_desc}，剩余 {len(remaining_rows)} 条"
                    f"{self.build_success_csv_remove_log_suffix()}"
                )
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise

    def get_current_success_paths(self) -> Dict[str, str]:
        """Return paths for the task currently inside on_task_success()."""
        context = getattr(self, "_task_success_context", None)
        if context is None:
            return {}
        paths = getattr(context, "paths", None)
        return paths if isinstance(paths, dict) else {}

    def build_success_csv_remove_log_suffix(self) -> str:
        """Append moved pcap path to CSV deletion logs after successful captures."""
        pcap_path = str(self.get_current_success_paths().get("pcap", "")).strip()
        return f"，pcap保存到{pcap_path}" if pcap_path else ""

    def _wait_before_first_exec(self, container: str) -> None:
        """仅在每个容器第一次执行 docker exec 前做全局节流。"""
        interval = max(float(self.FIRST_EXEC_INTERVAL), 0.0)
        with self._first_exec_lock:
            if container in self._first_exec_done_containers:
                return
            now = time.monotonic()
            scheduled = max(now, self._first_exec_next_ts)
            self._first_exec_next_ts = scheduled + interval
            self._first_exec_done_containers.add(container)

        wait = scheduled - now
        if wait > 0:
            time.sleep(wait)

    def _wait_before_same_id_exec(self, task: Dict[str, str]) -> None:
        """确保同一 ID + domain 的相邻 docker exec 启动时间满足最小间隔。"""
        task_id = str(task.get("row_id", "") or "").strip()
        task_domain = str(task.get("domain", "") or "").strip().lower()
        task_key = (task_id, task_domain)
        interval = max(float(self.SAME_ID_EXEC_INTERVAL), 0.0)
        if not task_id or interval <= 0:
            return

        with self._same_id_exec_lock:
            now = time.monotonic()
            scheduled = max(now, self._same_id_next_ts.get(task_key, now))
            self._same_id_next_ts[task_key] = scheduled + interval

        wait = scheduled - now
        if wait > 0:
            self.log(f"同 ID/domain 任务启动节流: id={task_id}, domain={task_domain or '<empty>'}，等待 {wait:.2f} 秒")
            time.sleep(wait)

    # ============== 进度条 ==============
    def _update_progress(self, ok: bool, task_elapsed: float = 0.0) -> None:
        """更新全局进度条"""
        with self._stats_lock:
            if ok:
                self._global_ok += 1
            else:
                self._global_fail += 1

            total_done = self._global_ok + self._global_fail
            elapsed = time.time() - self._global_start_time
            elapsed_min = elapsed / 60.0

            # 计算统计数据
            per_min = total_done / elapsed_min if elapsed_min > 0 else 0
            # 平均耗时按并发容器折算：运行时长 * 容器总数 / 完成任务数
            avg_time = (elapsed * self._global_container_count / total_done) if total_done > 0 else 0
            total_jobs = self._global_total_jobs or total_done
            remaining = max(total_jobs - total_done, 0)

            if self._pbar is not None:
                self._pbar.set_description(
                    f"任务进度: {total_done}/{total_jobs}个 [剩余: {remaining} | 运行: {elapsed_min:.1f}分钟 | "
                    f"成功: {self._global_ok} | 失败: {self._global_fail} | "
                    f"每分钟: {per_min:.2f} | 平均耗时: {avg_time:.1f}秒]"
                )
                self._pbar.update(1)

    # ============== 任务执行 ==============
    def exec_once(self, task: Dict[str, str]) -> Tuple[bool, str]:
        """执行单个任务"""
        container = task["container"]
        self._wait_before_first_exec(container)
        self._wait_before_same_id_exec(task)
        if self._browser_label:
            task["browser_label"] = self._browser_label
        payload = json.dumps(task, ensure_ascii=False)
        cmd = [
            "docker", "exec", container,
            "python", "-u", f"{self.CONTAINER_CODE_PATH}/action.py",
            payload
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
        if stderr or stdout:
            detail = stderr or stdout
        else:
            detail = "stdout/stderr empty"
        return False, f"docker exec rc={cp.returncode}: {detail}"

    def container_path_to_host_path(self, path: str) -> str:
        """Convert a container /app path to the mounted host path for diagnostics."""
        if not path:
            return ""
        return str(path).replace("/app", self.HOST_CODE_PATH, 1)

    def build_missing_result_paths_error(
        self,
        result: Dict[str, Any],
        meta_path: str,
        required_fields: List[str],
    ) -> str:
        """Build a detailed host-side error when action result paths are incomplete."""
        missing_fields = [field for field in required_fields if not result.get(field)]
        parts = [
            f"result JSON missing required paths: missing={','.join(missing_fields) or 'none'}",
            f"meta={meta_path}",
        ]

        failure_reason = str(result.get("failure_reason", "") or "").strip()
        if failure_reason:
            parts.append(f"reason={failure_reason}")

        failure_details = result.get("failure_details", [])
        if isinstance(failure_details, list):
            detail_text = " | ".join(str(item) for item in failure_details[:8] if item)
        else:
            detail_text = str(failure_details or "")
        if detail_text:
            parts.append(f"details={detail_text[:800]}")

        log_path = str(result.get("log_path", "") or "").strip()
        if log_path:
            parts.append(f"log={self.container_path_to_host_path(log_path)}")

        current_url = str(result.get("current_url", "") or "").strip()
        if current_url:
            parts.append(f"current_url={current_url[:300]}")

        attempted_paths = result.get("attempted_paths", {})
        if not isinstance(attempted_paths, dict):
            attempted_paths = {}
        path_diagnostics = result.get("path_diagnostics", {})
        if not isinstance(path_diagnostics, dict):
            path_diagnostics = {}

        path_parts = []
        for field in required_fields:
            diagnostic = path_diagnostics.get(field, {})
            if not isinstance(diagnostic, dict):
                diagnostic = {}
            raw_path = (
                diagnostic.get("path")
                or attempted_paths.get(field)
                or result.get(field)
                or ""
            )
            path = self.container_path_to_host_path(str(raw_path))
            exists = diagnostic.get("exists", "unknown")
            size = diagnostic.get("size", "unknown")
            path_parts.append(f"{field}=path:{path or '<empty>'},exists:{exists},size:{size}")
        if path_parts:
            parts.append("paths=[" + "; ".join(path_parts) + "]")

        return "; ".join(parts)

    def process_result(self, task: Dict[str, str], container: str) -> Tuple[bool, str]:
        """处理任务执行结果，子类可覆盖"""
        meta_path = os.path.join(self.HOST_CODE_PATH, "meta", f"{container}_last.json")
        with open(meta_path, "r", encoding="utf-8") as f:
            result = json.load(f)

        self.log('result', result)

        pcap_path = result.get("pcap_path")
        ssl_key_file_path = result.get("ssl_key_file_path")
        content_path = result.get("content_path")
        html_path = result.get("html_path")
        screenshot_path = result.get("screenshot_path")
        current_url = result.get("current_url", "")

        required_fields = [
            "pcap_path",
            "ssl_key_file_path",
            "content_path",
            "html_path",
            "screenshot_path",
        ]
        if not all([pcap_path, ssl_key_file_path, content_path, html_path, screenshot_path]):
            return False, self.build_missing_result_paths_error(result, meta_path, required_fields)

        container_result_paths = [
            pcap_path,
            ssl_key_file_path,
            content_path,
            html_path,
            screenshot_path,
        ]
        self.chown_container_result_paths(container, container_result_paths)

        # 转换容器路径为宿主机路径
        pcap_path = pcap_path.replace("/app", self.HOST_CODE_PATH)
        ssl_key_file_path = ssl_key_file_path.replace("/app", self.HOST_CODE_PATH)
        content_path = content_path.replace("/app", self.HOST_CODE_PATH)
        html_path = html_path.replace("/app", self.HOST_CODE_PATH)
        screenshot_path = screenshot_path.replace("/app", self.HOST_CODE_PATH)

        # 构建目标目录
        domain = task.get('domain', 'unknown')
        dst = os.path.join(self.BASE_DST, domain)

        move_plan = [
            (pcap_path, os.path.join(dst, 'pcap')),
            (ssl_key_file_path, os.path.join(dst, 'ssl_key')),
            (content_path, os.path.join(dst, 'content')),
            (html_path, os.path.join(dst, 'html')),
            (screenshot_path, os.path.join(dst, 'screenshot')),
        ]
        self.preflight_result_moves(move_plan)
        self.prepare_success_output_tree(dst)

        # 移动文件
        new_pcap = self.move_and_chown(pcap_path, os.path.join(dst, 'pcap'), result_root=dst)
        new_ssl = self.move_and_chown(ssl_key_file_path, os.path.join(dst, 'ssl_key'), result_root=dst)
        new_content = self.move_and_chown(content_path, os.path.join(dst, 'content'), result_root=dst)
        new_html = self.move_and_chown(html_path, os.path.join(dst, 'html'), result_root=dst)
        new_screenshot = self.move_and_chown(screenshot_path, os.path.join(dst, 'screenshot'), result_root=dst)

        extra_saved_paths: Dict[str, str] = {}
        for key, (src_path, dst_subdir) in self.build_additional_result_moves(task, result, dst).items():
            if not src_path:
                continue
            if not os.path.exists(src_path):
                self.log(f"WARNING: extra result file missing: {key} -> {src_path}")
                continue
            extra_saved_paths[key] = self.move_and_chown(
                src_path,
                os.path.join(dst, dst_subdir),
                result_root=dst,
            )

        # 调用成功回调
        success_paths = {
            'pcap': new_pcap,
            'ssl_key': new_ssl,
            'content': new_content,
            'html': new_html,
            'screenshot': new_screenshot,
            'current_url': current_url
        }
        success_paths.update(extra_saved_paths)
        self._task_success_context.paths = success_paths
        try:
            self.on_task_success(task, success_paths)
        finally:
            self._task_success_context.paths = {}

        return True, ""

    def build_additional_result_moves(
        self,
        task: Dict[str, str],
        result: Dict[str, Any],
        dst: str,
    ) -> Dict[str, Tuple[str, str]]:
        """Return extra host files to persist as key -> (src_path, dst_subdir)."""
        return {}

    def on_task_success(self, task: Dict[str, str], paths: Dict[str, str]) -> None:
        """任务成功回调，子类可覆盖（如写数据库）"""
        pass

    def on_task_failed(self, task: Dict[str, str], error: str) -> None:
        """任务失败回调，子类可覆盖（如标记数据库）"""
        pass

    def _handle_final_failure(self, task: Dict[str, str], err: str,
                               stats: Dict[str, Any], task_start_time: float) -> None:
        """处理最终失败的任务"""
        row_id = task.get("row_id", "")
        url = task.get("url", "")
        container = task.get("container", "unknown")
        task_elapsed = time.time() - task_start_time

        self.log(f"{container} -> give up [{row_id}] {url} after {self.RETRY + 1} attempts")
        self.on_task_failed(task, err)
        with self._stats_lock:
            stats["fail"] += 1
            stats["errors"].append((task, err))
        self._write_task_end_log(
            task,
            success=False,
            elapsed=task_elapsed,
            error=err,
        )
        self._update_progress(ok=False, task_elapsed=task_elapsed)

    def worker_loop(self, container: str, q: "queue.Queue[Dict[str, str]]",
                    stats: Dict[str, Any], retry: int) -> None:
        """Worker 循环，失败任务放回队列由其他容器重试"""
        while True:
            try:
                task = q.get_nowait()
            except queue.Empty:
                return

            try:
                row_id = task.get("row_id", "")
                url = task.get("url", "")
                task["container"] = container

                # 记录重试次数和首次开始时间
                attempt = task.get("_retry_count", 0)
                if "_start_time" not in task:
                    task["_start_time"] = time.time()
                task_start_time = task["_start_time"]

                if attempt == 0:
                    self.log(f"{container} -> start [{row_id}] {url}")
                    self._write_task_start_log(task)
                else:
                    self.log(f"{container} -> retry {attempt}/{retry} [{row_id}] {url}")

                try:
                    ok, err = self.exec_once(task)
                except subprocess.TimeoutExpired:
                    err = f"timeout>{self.DOCKER_EXEC_TIMEOUT}s"
                    self.log(f"{container} -> timeout [{row_id}] {url}")
                    if attempt < retry:
                        task["_retry_count"] = attempt + 1
                        time.sleep(2)
                        q.put(task)
                    else:
                        self._handle_final_failure(task, err, stats, task_start_time)
                    continue
                except Exception as e:
                    err = repr(e)
                    self.log(f"{container} -> error [{row_id}] {err}")
                    if attempt < retry:
                        task["_retry_count"] = attempt + 1
                        time.sleep(2)
                        q.put(task)
                    else:
                        self._handle_final_failure(task, err, stats, task_start_time)
                    continue

                if ok:
                    task_elapsed = time.time() - task_start_time
                    self.log(f"{container} -> done  [{row_id}] {url} ({task_elapsed:.1f}s)")
                    with self._stats_lock:
                        stats["ok"] += 1
                    self._write_task_end_log(
                        task,
                        success=True,
                        elapsed=task_elapsed,
                    )
                    self._update_progress(ok=True, task_elapsed=task_elapsed)
                else:
                    self.log(f"{container} -> fail  [{row_id}] {self.compact_error(err)}")
                    # 失败后放回队列，让其他容器重试
                    if attempt < retry:
                        task["_retry_count"] = attempt + 1
                        time.sleep(2)
                        q.put(task)
                    else:
                        self._handle_final_failure(task, err, stats, task_start_time)

            finally:
                q.task_done()

    @staticmethod
    def get_unfinished_task_count(q: "queue.Queue[Dict[str, str]]") -> int:
        """Return Queue unfinished task count without blocking indefinitely."""
        with q.all_tasks_done:
            return int(q.unfinished_tasks)

    # ============== 信号处理 ==============
    def _signal_handler(self, signum, _frame) -> None:
        """信号处理函数"""
        self.log(f"收到中断信号({signum})，立即退出。")
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        finally:
            os._exit(128 + signum)

    # ============== 抽象方法 ==============
    @abstractmethod
    def fetch_jobs(self) -> List[Dict[str, str]]:
        """获取任务列表，子类必须实现"""
        pass

    # ============== 主运行逻辑 ==============
    def run_once(self, names: List[str], jobs: List[Dict[str, str]]) -> Dict[str, Any]:
        """执行一批任务"""
        q: "queue.Queue[Dict[str, str]]" = queue.Queue()
        for t in jobs:
            self._execution_task_sequence += 1
            t["_task_log_no"] = self._execution_task_sequence
            q.put(t)

        stats: Dict[str, Any] = {"ok": 0, "fail": 0, "errors": []}
        self.log(f"开始执行：jobs={len(jobs)}，并发容器={len(names)}，镜像={self.DOCKER_IMAGE}")

        with ThreadPoolExecutor(max_workers=len(names)) as pool:
            futures = [
                pool.submit(self.worker_loop, n, q, stats, self.RETRY)
                for n in names
            ]
            while True:
                unfinished = self.get_unfinished_task_count(q)
                for name, future in zip(names, futures):
                    if future.done():
                        exc = future.exception()
                        if exc is not None:
                            raise RuntimeError(
                                f"worker {name} 异常退出，队列未完成任务数={unfinished}: {exc!r}"
                            ) from exc

                if unfinished == 0:
                    break

                if all(future.done() for future in futures):
                    raise RuntimeError(
                        f"所有 worker 已退出，但队列仍有 {unfinished} 个未完成任务"
                    )

                time.sleep(1.0)

            for future in futures:
                future.result()

        return stats

    def run(self) -> bool:
        """主运行入口"""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # 初始化（子类可覆盖 setup 方法）
        self.setup()
        self.ensure_base_dst_writable()
        self._copied_task_csv_path = self.copy_task_csv_to_base_dst() or ""
        self.initialize_execution_task_log()

        # 准备容器池
        names: List[str] = []

        self._global_container_count = max(len(names), 1)
        self._global_start_time = time.time()
        self._global_ok = 0
        self._global_fail = 0
        self._global_total_jobs = 0
        self._runtime_prepared = False
        batch_num = 0
        run_error = ""
        completeness_report = None

        # 创建常驻进度条（total=None 表示未知总数）
        if tqdm is None:
            self.log("FATAL: 缺少 Python 依赖 tqdm，请先安装后再运行采集器。")
            sys.exit(2)
        self._pbar = tqdm(total=None, unit="个", position=0, leave=True,
                         bar_format='{desc}')
        self._pbar.set_description("任务进度: 0个 [初始化中...]")

        try:
            while True:
                jobs = self.fetch_jobs()
                self.log(f"fetched jobs: {len(jobs)}")
                if not jobs:
                    self.log("没有可处理的任务，退出。")
                    break

                with self._stats_lock:
                    total_done = self._global_ok + self._global_fail
                    self._global_total_jobs = total_done + len(jobs)
                    remaining = max(self._global_total_jobs - total_done, 0)
                    if self._pbar is not None:
                        self._pbar.set_description(
                            f"任务进度: {total_done}/{self._global_total_jobs}个 "
                            f"[剩余: {remaining} | 初始化中...]"
                        )

                if not self._runtime_prepared:
                    self.configure_container_count_for_jobs(jobs)
                    self.remove_containers()
                    names = self.prepare_pool_once()
                    self.clear_host_code_subdirs()
                    self._global_container_count = max(len(names), 1)
                    self._runtime_prepared = True

                batch_num += 1
                self.log(f"===== 批次 {batch_num}: 共 {len(jobs)} 条任务 =====")

                stats = self.run_once(names, jobs)

                self.log(f"[批次 {batch_num} 汇总] success={stats['ok']} fail={stats['fail']} total={len(jobs)}")
                if stats["errors"]:
                    self.log("失败样例：")
                    for task, err in stats["errors"][:10]:
                        self.log(
                            f" - id={task.get('row_id','')} url={task.get('url','')} "
                            f"err={self.compact_error(err, limit=1000)}"
                        )

                # 默认只在启动容器池后清理上一次残留；批次结束后保留本次现场用于排查失败。
                if self.CLEAR_HOST_CODE_SUBDIRS_AFTER_BATCH:
                    self.clear_host_code_subdirs()

                # 如果只需要运行一次，跳出循环
                if not self.should_continue():
                    break

        except Exception as e:
            run_error = repr(e)
            self.log(f"WARN: 执行异常：{e}")
        finally:
            # 关闭进度条
            if self._pbar is not None:
                self._pbar.close()
                self._pbar = None
            try:
                completeness_report = self.verify_task_completeness()
            except Exception as e:
                self.log(f"WARN: 任务完整度校验失败：{e}")
            if self._runtime_prepared or self.should_cleanup_when_idle():
                self.cleanup()

        # 最终汇总
        elapsed = time.time() - self._global_start_time
        elapsed_min = elapsed / 60.0
        total_done = self._global_ok + self._global_fail
        total_jobs = self._global_total_jobs or total_done
        remaining = max(total_jobs - total_done, 0)
        per_min = total_done / elapsed_min if elapsed_min > 0 else 0
        avg_time = (elapsed * self._global_container_count / total_done) if total_done > 0 else 0
        self.log(f"[最终汇总] 批次={batch_num} | 运行时间={elapsed_min:.1f}分钟 | 总数={total_jobs} | "
                 f"完成={total_done} | 剩余={remaining} | "
                 f"成功={self._global_ok} | 失败={self._global_fail} | 每分钟={per_min:.2f} | 平均耗时={avg_time:.1f}秒")
        run_end_record = {
            "status": (
                "failed"
                if run_error
                else "completed_with_failures"
                if self._global_fail
                else "completed"
            ),
            "total": total_jobs,
            "success": self._global_ok,
            "failed": self._global_fail,
            "elapsed_seconds": round(elapsed, 3),
        }
        if run_error:
            run_end_record["error"] = self.compact_error(run_error, limit=1000)
        if completeness_report is not None:
            run_end_record.update(
                {
                    "expected_tasks": completeness_report["expected_tasks"],
                    "matched_tasks": completeness_report["matched_tasks"],
                    "missing_tasks": completeness_report["missing_tasks"],
                    "completeness_percent": completeness_report["completeness_percent"],
                }
            )
        self._write_execution_task_log("run_end", **run_end_record)
        return batch_num > 0

    def setup(self) -> None:
        """初始化设置，子类可覆盖"""
        pass

    def cleanup(self) -> None:
        """清理工作，子类可覆盖"""
        pass

    def should_continue(self) -> bool:
        """是否继续下一批次，子类可覆盖"""
        return True

    def should_cleanup_when_idle(self) -> bool:
        """空任务直接退出时，是否仍执行 cleanup。"""
        return type(self).setup is not BaseTrafficIngestor.setup

    @classmethod
    def main(cls) -> bool:
        """入口方法"""
        ingestor = cls()
        try:
            ingestor.acquire_runtime_lock()
        except RuntimeError as e:
            ingestor.log(f"FATAL: {e}")
            raise SystemExit(2) from e

        try:
            processed_any = ingestor.run()
        finally:
            ingestor.release_runtime_lock()
        if processed_any:
            time.sleep(300)
        return processed_any

    @classmethod
    def has_pending_jobs(cls) -> bool:
        """Quickly check whether another outer retry round has work to do."""
        ingestor = cls()
        try:
            return bool(ingestor.fetch_jobs())
        except Exception as e:
            ingestor.log(f"WARN: failed to check pending jobs after run: {e}")
            return True
