#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""容器池：一次性创建容器，读取实际 IP，按 IP 最后 8 位 % EGRESS_IP_COUNT 分出口组。

容器创建、固定 IP 网络、运行锁等全部复用 BaseTrafficIngestor；运行中不再创建或重建容器。
"""

from __future__ import annotations

import ipaddress
import json
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from typing import Dict, List, Optional

from host_scheduler.base_traffic_ingestor import BaseTrafficIngestor
from urlFind import config
from urlFind.protocol import CONTAINER_WORKER_SCRIPT, RESULT_MARKER


@dataclass
class ContainerInfo:
    name: str
    ip: str
    group: int


def egress_group_of(ip: str, egress_count: int) -> int:
    return (int(ipaddress.IPv4Address(ip)) & 0xFF) % egress_count


class UrlFindPool(BaseTrafficIngestor):
    """只使用基类的容器池、网络和日志能力，不使用其 CSV 任务调度。"""

    BASE_NAME = "url_find"
    # 源码根目录 trafficIngestor/ 挂载为 /app，容器内可直接复用 url_list_collector/utils 与 urlFind。
    HOST_CODE_PATH = config.SOURCE_ROOT
    MOUNT_CODE_IN_CONTAINERS = True
    DISABLE_OFFLOAD_DURING_POOL_PREPARE = False  # URL 发现不抓包，不需要关闭网卡 offload

    def __init__(self) -> None:
        super().__init__()
        self.CONTAINER_COUNT = config.CONTAINER_COUNT
        self.DOCKER_IMAGE = config.DOCKER_IMAGE
        self.DOCKER_NETWORK = config.DOCKER_NETWORK
        self.DOCKER_NETWORK_SUBNET_PREFIX = config.DOCKER_NETWORK_SUBNET_PREFIX
        self.DOCKER_NETWORK_GATEWAY = config.DOCKER_NETWORK_GATEWAY
        self.CONTAINER_IP_START = config.CONTAINER_IP_START
        self.DOCKER_DNS = config.DOCKER_DNS
        self._resolved_container_count = config.CONTAINER_COUNT

    def fetch_jobs(self) -> List[Dict[str, str]]:  # 基类抽象方法，本流程不使用
        return []

    def ensure_host_code_path_ready(self, ensure_action: bool = True):
        # 源码根目录本身就是挂载目录，不需要回填 action.py
        return super().ensure_host_code_path_ready(ensure_action=False)

    def attach_log_file(self, path: str) -> None:
        """把基类 log() 的输出同时写入本次运行的 run.log（含此前缓存的启动日志）。"""
        with self._execution_task_log_lock:
            self._execution_task_log_path = path
            with open(path, "a", encoding="utf-8") as f:
                for line in self._pending_console_log_lines:
                    f.write(line + "\n")
            self._pending_console_log_lines.clear()

    # ---------- 启动 ----------
    def prepare(self) -> List[ContainerInfo]:
        self.ensure_docker_available()
        self.acquire_runtime_lock()
        if config.CLEAN_AND_RECREATE_CONTAINERS:
            self.remove_containers()
        names = self.prepare_pool_once()
        self.selftest(names[0])
        return self.inventory(names)

    def selftest(self, name: str) -> None:
        cmd = ["docker", "exec", "-w", "/tmp", name, config.CONTAINER_PYTHON, "-u",
               CONTAINER_WORKER_SCRIPT, "--selftest"]
        cp = self.run_cmd(cmd, timeout=120)
        if cp.returncode != 0 or RESULT_MARKER not in (cp.stdout or ""):
            detail = (cp.stderr or cp.stdout or "").strip()[-800:]
            self.log(f"FATAL: 容器自检失败 {name}: rc={cp.returncode} {detail}")
            sys.exit(2)
        self.log(f"容器自检通过: {name}")

    def inventory(self, names: List[str]) -> List[ContainerInfo]:
        """读取每个容器的实际 IP，检查是否连续，并按出口组分组。"""
        infos: List[ContainerInfo] = []
        for name in names:
            ip = self.get_container_ipv4(name)
            if not ip:
                self.log(f"FATAL: 读取容器 IP 失败: {name}")
                sys.exit(2)
            infos.append(ContainerInfo(name, ip, egress_group_of(ip, config.EGRESS_IP_COUNT)))

        addrs = sorted(int(ipaddress.IPv4Address(i.ip)) for i in infos)
        first, last = ipaddress.IPv4Address(addrs[0]), ipaddress.IPv4Address(addrs[-1])
        if len(set(addrs)) == len(addrs) and addrs[-1] - addrs[0] + 1 == len(addrs):
            self.log(f"容器 IP 连续: {first} - {last}，共 {len(addrs)} 个")
        else:
            present = set(addrs)
            gaps = [str(ipaddress.IPv4Address(a)) for a in range(addrs[0], addrs[-1] + 1) if a not in present]
            preview = ", ".join(gaps[:10]) + (f" ... (+{len(gaps) - 10})" if len(gaps) > 10 else "")
            self.log(f"WARN: 容器 IP 不连续: {first} - {last}，缺口 {len(gaps)} 个: {preview}；"
                     f"出口分组按每个容器的实际 IP 计算")

        counts = Counter(i.group for i in infos)
        dist = ", ".join(f"g{g}={counts.get(g, 0)}" for g in range(config.EGRESS_IP_COUNT))
        self.log(f"出口分组（IP 最后8位 % {config.EGRESS_IP_COUNT}）: {dist}")
        empty = [g for g in range(config.EGRESS_IP_COUNT) if counts.get(g, 0) == 0]
        if empty:
            self.log(f"WARN: 以下出口组没有容器，对应出口 IP 不会被使用: {empty}")
        return infos

    # ---------- 运行中 ----------
    def kill_browsers(self, name: str) -> None:
        """宿主机侧超时后清理容器内残留进程（尽力而为）。"""
        # [e] 让正则不匹配 sh 自身的命令行
        script = ("pkill -9 -f 'urlFind/worker/visit_pag[e].py'; "
                  "pkill -9 -x chromedriver; pkill -9 -x chrome; pkill -9 -x google-chrome; true")
        try:
            subprocess.run(["docker", "exec", name, "sh", "-c", script], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=30, check=False)
        except (subprocess.TimeoutExpired, OSError):
            self.log(f"WARN: 清理容器残留进程失败: {name}")

    def shutdown(self) -> None:
        if config.REMOVE_CONTAINERS_ON_EXIT:
            self.remove_containers()
        else:
            self.log(f"保留容器（REMOVE_CONTAINERS_ON_EXIT=False），前缀 {self.CONTAINER_PREFIX}")
        self.release_runtime_lock()


def parse_marker_line(stdout: str) -> Optional[dict]:
    for line in reversed((stdout or "").splitlines()):
        if line.startswith(RESULT_MARKER):
            try:
                return json.loads(line[len(RESULT_MARKER):])
            except json.JSONDecodeError:
                return None
    return None
