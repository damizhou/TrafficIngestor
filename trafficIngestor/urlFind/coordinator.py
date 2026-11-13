#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""总调度：管理所有站点的状态、容器池和 (出口组, 站点) 冷却表。

派发规则：
- 对每个有空闲容器的出口组 g，从轮询指针开始找第一个满足条件的站点 s：
  s 可派发（站点调度器决定）且 冷却表[(g, s)] <= 现在。
- 派发时立刻设置 冷却表[(g, s)] = 现在 + SITE_COOLDOWN_SECONDS（从访问开始计，失败也计入）。
- 没有可派发的组合时，等待到最早的冷却到期或有访问结果返回。

只有一把锁（Condition）。站点调度器的所有方法都在持锁状态下调用；
真正耗时的 docker exec 在工作线程里执行，不持锁。
"""

from __future__ import annotations

import threading
import time
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Deque, Dict, List, Optional, Tuple

from urlFind import config
from urlFind.pool import ContainerInfo
from urlFind.protocol import OUTCOME_ERROR
from urlFind.site import FINISHED_STATUSES, SiteCrawler
from urlFind.storage import RunRecorder, SiteOutput, StateStore
from urlFind.visit import VisitReport

# 主循环单次等待上限（秒），保证能及时响应中断和输出进度
MAX_WAIT_SECONDS = 2.0

VisitFn = Callable[[str, str, str], VisitReport]


class Coordinator:
    def __init__(
        self,
        sites: List[SiteCrawler],
        containers: List[ContainerInfo],
        visit_fn: VisitFn,
        *,
        outputs: Dict[str, SiteOutput],
        store: StateStore,
        recorder: RunRecorder,
        log: Callable[..., None],
    ):
        self.cond = threading.Condition()
        self.sites = sites
        self.visit_fn = visit_fn
        self.outputs = outputs
        self.store = store
        self.recorder = recorder
        self.log = log

        self.container_group: Dict[str, int] = {c.name: c.group for c in containers}
        self.idle: Dict[int, Deque[str]] = {}
        for c in containers:
            self.idle.setdefault(c.group, deque()).append(c.name)
        self.groups: List[int] = sorted(self.idle)
        self.sick: set = set()
        self.fail_streak: Counter = Counter()

        self.cooldowns: Dict[Tuple[int, str], float] = store.load_cooldowns()
        self.in_flight = 0
        self.visit_seq = 0
        self.site_rr = 0
        self.group_rr = 0
        self.stop_requested = False
        self.dirty_sites: set = set()
        self.last_state_save = time.time()
        self.last_status_log = 0.0
        self.started_at = time.time()
        self.total_visits = 0
        self.executor = ThreadPoolExecutor(max_workers=max(len(containers), 1),
                                           thread_name_prefix="urlfind")

    # ============== 对外接口 ==============
    def request_stop(self) -> None:
        """可在信号处理函数里调用：只置标志，不拿锁。"""
        self.stop_requested = True

    def run(self) -> List[Dict]:
        self.log(f"开始调度: 站点={len(self.sites)}，容器={len(self.container_group)}，"
                 f"出口组={len(self.groups)}/{config.EGRESS_IP_COUNT}，冷却={config.SITE_COOLDOWN_SECONDS}s，"
                 f"目标={config.TARGET_URLS_PER_SITE}/站")
        with self.cond:
            for site in self.sites:
                if site.check_finished():
                    self._on_site_finished(site)
            while True:
                now = time.time()
                if self.in_flight == 0 and (self.stop_requested or self._all_finished()):
                    break
                if not self.stop_requested:
                    if not self._usable_containers():
                        self.log("FATAL: 所有容器都已下线，停止调度")
                        self.stop_requested = True
                        continue
                    self._dispatch_round(now)
                self._periodic(now)
                self.cond.wait(timeout=self._wait_seconds(time.time()))
            self._save_all_states()
        self.executor.shutdown(wait=True)
        rows = [s.summary_row() for s in self.sites]
        self.recorder.write_summary(rows)
        self._log_status(final=True)
        return rows

    # ============== 派发 ==============
    def _usable_containers(self) -> bool:
        return len(self.sick) < len(self.container_group)

    def _all_finished(self) -> bool:
        return all(s.status in FINISHED_STATUSES for s in self.sites)

    def _dispatch_round(self, now: float) -> None:
        progress = True
        while progress:
            progress = False
            n = len(self.groups)
            for i in range(n):
                group = self.groups[(self.group_rr + i) % n]
                if not self.idle[group]:
                    continue
                site = self._pick_site(group, now)
                if site is None:
                    continue
                self._launch(site, group, self.idle[group].popleft(), now)
                self.group_rr = (self.group_rr + i + 1) % n
                progress = True
                break

    def _pick_site(self, group: int, now: float) -> Optional[SiteCrawler]:
        n = len(self.sites)
        for i in range(n):
            site = self.sites[(self.site_rr + i) % n]
            if self.cooldowns.get((group, site.key), 0.0) > now:
                continue
            if site.can_dispatch(group):
                self.site_rr = (self.site_rr + i + 1) % n
                return site
        return None

    def _launch(self, site: SiteCrawler, group: int, container: str, now: float) -> None:
        self.visit_seq += 1
        visit_id = f"{self.visit_seq}"
        entry = site.take(group, visit_id)
        if entry is None:  # 理论上不会发生：调用前已 can_dispatch
            self.idle[group].appendleft(container)
            return
        self.cooldowns[(group, site.key)] = now + config.SITE_COOLDOWN_SECONDS
        self.in_flight += 1
        self.total_visits += 1
        self.executor.submit(self._worker, site, group, container, visit_id, entry.url, now)

    def _wait_seconds(self, now: float) -> float:
        """等待到最早一个"有空闲容器 + 站点可派发"组合的冷却到期。"""
        wait = MAX_WAIT_SECONDS
        for group in self.groups:
            if not self.idle[group]:
                continue
            for site in self.sites:
                expiry = self.cooldowns.get((group, site.key), 0.0)
                if now < expiry < now + wait and site.can_dispatch(group):
                    wait = expiry - now
        return max(wait, 0.01)

    # ============== 工作线程 ==============
    def _worker(self, site: SiteCrawler, group: int, container: str, visit_id: str,
                url: str, started: float) -> None:
        try:
            report = self.visit_fn(container, visit_id, url)
        except Exception as exc:  # 调度器自身不能因为单次访问崩溃
            report = VisitReport({
                "visit_id": visit_id, "request_url": url, "final_url": "", "outcome": OUTCOME_ERROR,
                "http_status": None, "reason": f"host_error:{type(exc).__name__}:{exc}", "links": [],
                "elapsed": 0.0,
            }, True)
        with self.cond:
            try:
                self._handle_result(site, group, container, visit_id, report, started)
            except Exception as exc:
                self.log(f"ERROR: 处理访问结果失败 site={site.key} visit={visit_id}: {exc!r}")
                self.idle[group].append(container)
            finally:
                self.in_flight -= 1
                self.cond.notify_all()

    def _handle_result(self, site: SiteCrawler, group: int, container: str, visit_id: str,
                       report: VisitReport, started: float) -> None:
        result = report.result
        outcome = site.on_result(visit_id, group, result)
        if outcome.collected_url:
            self.outputs[site.key].append(len(site.collected), outcome.collected_url)
        self.dirty_sites.add(site.key)
        self.recorder.record_visit({
            "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(started)),
            "site": site.key, "group": group, "container": container, "visit_id": visit_id,
            "request_url": result.get("request_url", ""), "final_url": result.get("final_url", ""),
            "outcome": result.get("outcome", ""), "reason": result.get("reason", ""),
            "http_status": result.get("http_status", ""), "links": len(result.get("links") or []),
            "note": outcome.note, "elapsed": result.get("elapsed", ""),
        })

        if report.infra_error:
            self.fail_streak[container] += 1
            if self.fail_streak[container] >= config.CONTAINER_MAX_CONSECUTIVE_FAILURES:
                self.sick.add(container)
                self.log(f"WARN: 容器 {container}（g{group}）连续 {self.fail_streak[container]} 次基础设施错误，"
                         f"下线。最后错误: {result.get('reason', '')}")
        else:
            self.fail_streak[container] = 0
        if container not in self.sick:
            self.idle[group].append(container)

        if outcome.finished_now:
            self._on_site_finished(site)

    # ============== 状态与日志 ==============
    def _on_site_finished(self, site: SiteCrawler) -> None:
        self.store.save_site(site.key, site.to_state())
        self.dirty_sites.discard(site.key)
        done = sum(1 for s in self.sites if s.status in FINISHED_STATUSES)
        self.log(f"站点结束 [{done}/{len(self.sites)}] {site.key}: {site.status}（{site.finish_reason}），"
                 f"采集 {len(site.collected)}/{site.target}，访问 {site.visits}")

    def _save_all_states(self) -> None:
        for site in self.sites:
            if site.key in self.dirty_sites:
                self.store.save_site(site.key, site.to_state())
        self.dirty_sites.clear()
        self.store.save_cooldowns(self.cooldowns)
        self.last_state_save = time.time()

    def _periodic(self, now: float) -> None:
        if now - self.last_state_save >= config.STATE_SAVE_INTERVAL_SECONDS:
            self._save_all_states()
        if now - self.last_status_log >= config.STATUS_LOG_INTERVAL_SECONDS:
            self._log_status()
            self.last_status_log = now

    def _log_status(self, final: bool = False) -> None:
        statuses = Counter(s.status for s in self.sites)
        collected = sum(len(s.collected) for s in self.sites)
        target = sum(s.target for s in self.sites)
        idle = sum(len(q) for q in self.idle.values())
        elapsed = (time.time() - self.started_at) / 60.0
        rate = self.total_visits / max(elapsed, 1.0)  # 运行不足 1 分钟时按 1 分钟算
        status_text = ", ".join(f"{k}={v}" for k, v in sorted(statuses.items()))
        self.log(f"{'[最终] ' if final else ''}进度: 站点 {status_text} | 采集 {collected}/{target} | "
                 f"访问 {self.total_visits}（{rate:.1f}/分钟）| 进行中 {self.in_flight} | "
                 f"空闲容器 {idle} | 下线容器 {len(self.sick)} | 运行 {elapsed:.1f} 分钟")
