#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""出口节点 × 网站 冷却调度。

保证同一出口节点（出口组）访问同一网站的两次任务启动间隔不小于 SITE_COOLDOWN_SECONDS：
- 出口组：容器 IP 最后 8 位 % 出口节点数。默认 bridge 为 15 个节点；
  自建网络（DOCKER_NETWORK）必须在配置中给出 DOCKER_NETWORK_OUTPUT_NODES。
- 网站：任务的 domain 列（小写），缺失时取 URL 主机名。
- 冷却从任务被容器取走时开始计算，真正 docker exec 前再按实际启动时间顺延；失败也计入。
- 重试优先派给该任务尚未使用过的出口组，全部用过后放开。

CooldownTaskQueue 继承 queue.Queue：put / task_done / unfinished_tasks / join 语义不变，
只新增 get_for_container() 按 (出口组, 网站) 冷却挑选任务。
"""

from __future__ import annotations

import ipaddress
import itertools
import queue
import threading
import time
from collections import Counter, OrderedDict, deque
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlsplit

GROUP_HISTORY_KEY = "_group_history"


def egress_group_of(ip: str, node_count: int) -> int:
    return (int(ipaddress.IPv4Address(ip)) & 0xFF) % node_count


def task_site_key(task: Dict[str, Any]) -> str:
    domain = str(task.get("domain", "") or "").strip().lower().strip(".")
    if domain:
        return domain
    try:
        return (urlsplit(str(task.get("url", "") or "")).hostname or "").lower()
    except ValueError:
        return ""


def describe_container_ips(ips: Dict[str, str], node_count: int) -> Tuple[List[str], Dict[str, int]]:
    """返回 (日志行, 容器→出口组)。检查 IP 是否连续并统计各出口组容器数。"""
    lines: List[str] = []
    addrs = sorted(int(ipaddress.IPv4Address(ip)) for ip in ips.values())
    first, last = ipaddress.IPv4Address(addrs[0]), ipaddress.IPv4Address(addrs[-1])
    if len(set(addrs)) == len(addrs) and addrs[-1] - addrs[0] + 1 == len(addrs):
        lines.append(f"容器 IP 连续: {first} - {last}，共 {len(addrs)} 个")
    else:
        present = set(addrs)
        gaps = [str(ipaddress.IPv4Address(a)) for a in range(addrs[0], addrs[-1] + 1) if a not in present]
        preview = ", ".join(gaps[:10]) + (f" ... (+{len(gaps) - 10})" if len(gaps) > 10 else "")
        lines.append(f"WARN: 容器 IP 不连续: {first} - {last}，缺口 {len(gaps)} 个: {preview}；"
                     f"出口分组按每个容器的实际 IP 计算")
    groups = {name: egress_group_of(ip, node_count) for name, ip in ips.items()}
    counts = Counter(groups.values())
    lines.append(f"出口分组（IP 最后8位 % {node_count}）: "
                 + ", ".join(f"g{g}={counts.get(g, 0)}" for g in range(node_count)))
    empty = [g for g in range(node_count) if counts.get(g, 0) == 0]
    if empty:
        lines.append(f"WARN: 以下出口组没有容器，对应出口节点不会被使用: {empty}")
    return lines, groups


class EgressCooldownGate:
    """(出口组, 网站) → 冷却到期时间（monotonic）。"""

    def __init__(self, cooldown_seconds: float, container_groups: Dict[str, int],
                 log: Callable[..., None]):
        self.cooldown = float(cooldown_seconds)
        self.container_groups = dict(container_groups)
        self.available_groups = set(self.container_groups.values())
        self.log = log
        self._lock = threading.Lock()
        self._expiry: Dict[Tuple[int, str], float] = {}

    def group_of(self, container: str) -> int:
        return self.container_groups[container]

    def expiry(self, group: int, site: str) -> float:
        with self._lock:
            return self._expiry.get((group, site), 0.0)

    def group_allowed(self, task: Dict[str, Any], group: int) -> bool:
        history = task.get(GROUP_HISTORY_KEY) or []
        if group not in history:
            return True
        return self.available_groups.issubset(history)

    def reserve(self, task: Dict[str, Any], group: int, now: float) -> None:
        site = task_site_key(task)
        with self._lock:
            self._expiry[(group, site)] = now + self.cooldown
        task.setdefault(GROUP_HISTORY_KEY, []).append(group)

    def mark_started(self, task: Dict[str, Any]) -> None:
        """docker exec 实际启动前调用：把冷却顺延到"实际启动时间 + 冷却"。"""
        container = str(task.get("container", "") or "")
        if container not in self.container_groups:
            return
        group = self.container_groups[container]
        site = task_site_key(task)
        now = time.monotonic()
        with self._lock:
            reserved = self._expiry.get((group, site), 0.0)
            self._expiry[(group, site)] = max(reserved, now + self.cooldown)


class CooldownTaskQueue(queue.Queue):
    """按网站分桶的任务队列。普通 get() 仍按全局 FIFO；get_for_container() 遵守冷却。"""

    def __init__(self, gate: EgressCooldownGate):
        self.gate = gate
        super().__init__()

    # ---- queue.Queue 内部钩子（均在 self.mutex 内调用）----
    def _init(self, maxsize: int) -> None:
        self.sites: "OrderedDict[str, Deque[Tuple[int, Dict[str, Any]]]]" = OrderedDict()
        self._count = 0
        self._seq = itertools.count()

    def _qsize(self) -> int:
        return self._count

    def _put(self, item: Dict[str, Any]) -> None:
        self.sites.setdefault(task_site_key(item), deque()).append((next(self._seq), item))
        self._count += 1

    def _get(self) -> Dict[str, Any]:
        site = min(self.sites, key=lambda key: self.sites[key][0][0])
        return self._pop(site, 0)

    def _pop(self, site: str, index: int) -> Dict[str, Any]:
        bucket = self.sites[site]
        _, item = bucket[index]
        del bucket[index]
        if not bucket:
            del self.sites[site]
        self._count -= 1
        return item

    def put(self, item: Dict[str, Any], block: bool = True, timeout: Optional[float] = None) -> None:
        super().put(item, block, timeout)
        with self.not_empty:
            # 新任务可能只适合某个出口组，唤醒所有等待者重新挑选
            self.not_empty.notify_all()

    def get_for_container(self, container: str, timeout: float) -> Dict[str, Any]:
        """取一个该容器出口组可以执行的任务：网站冷却已到期，且优先未用过的出口组。

        按任务入队顺序挑最早的一个；没有可执行任务时等待到最早冷却到期或有新任务，
        超过 timeout 抛出 queue.Empty。
        """
        group = self.gate.group_of(container)
        deadline = time.monotonic() + max(timeout, 0.0)
        with self.not_empty:
            while True:
                now = time.monotonic()
                best: Optional[Tuple[int, str, int]] = None
                wake: Optional[float] = None
                for site, bucket in self.sites.items():
                    expiry = self.gate.expiry(group, site)
                    if expiry > now:
                        wake = expiry if wake is None else min(wake, expiry)
                        continue
                    for index, (seq, item) in enumerate(bucket):
                        if self.gate.group_allowed(item, group):
                            if best is None or seq < best[0]:
                                best = (seq, site, index)
                            break
                if best is not None:
                    item = self._pop(best[1], best[2])
                    self.gate.reserve(item, group, now)
                    self.not_full.notify()
                    return item
                remaining = deadline - now
                if remaining <= 0:
                    raise queue.Empty
                wait = remaining if wake is None else min(remaining, max(wake - now, 0.01))
                self.not_empty.wait(wait)


def max_start_interval_violations(starts: Iterable[Tuple[float, int, str]], cooldown: float) -> List[Tuple[int, str, float]]:
    """测试辅助：给定 (启动时间, 出口组, 网站) 序列，返回间隔小于 cooldown 的记录。"""
    last: Dict[Tuple[int, str], float] = {}
    bad: List[Tuple[int, str, float]] = []
    for ts, group, site in sorted(starts):
        key = (group, site)
        if key in last and ts - last[key] < cooldown - 1e-6:
            bad.append((group, site, ts - last[key]))
        last[key] = ts
    return bad
