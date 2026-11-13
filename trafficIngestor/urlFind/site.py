#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""站点调度器：每个站点持有自己的待访问队列、已采集列表和状态。

站点调度器不碰容器和冷却，只回答"下一个访问哪个 URL"和"这次访问结果怎么处理"，
由总调度（coordinator）在持锁状态下调用。
"""

from __future__ import annotations

import time
from collections import Counter, OrderedDict, deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit

from urlFind.protocol import OUTCOME_ERROR, OUTCOME_OK, OUTCOME_REJECTED, OUTCOME_TIMEOUT

# 站点状态
STATUS_SEEDING = "seeding"    # 种子页还没有成功打开
STATUS_RUNNING = "running"    # 正在扩展
STATUS_COMPLETE = "complete"  # 采满目标
STATUS_PARTIAL = "partial"    # 队列耗尽或访问次数用完，采到一部分
STATUS_FAILED = "failed"      # 一条也没采到
FINISHED_STATUSES = {STATUS_COMPLETE, STATUS_PARTIAL, STATUS_FAILED}

_DEFAULT_PORTS = {"http": 80, "https": 443}


# ============== URL 工具 ==============
def url_host(url: str) -> str:
    try:
        return (urlsplit(url).hostname or "").lower().strip(".")
    except ValueError:
        return ""


def normalize_url_key(url: str, tracking_params: Iterable[str] = ()) -> str:
    """生成去重用的 key：忽略协议、片段、默认端口、末尾 /、跟踪参数和参数顺序。

    只用于判重，请求和输出仍使用原始 URL。
    """
    try:
        parts = urlsplit((url or "").strip())
        port = parts.port
    except ValueError:
        return ""
    scheme = parts.scheme.lower()
    host = (parts.hostname or "").lower().strip(".")
    if scheme not in _DEFAULT_PORTS or not host:
        return ""
    netloc = host if port in (None, _DEFAULT_PORTS[scheme]) else f"{host}:{port}"
    path = parts.path or "/"
    if len(path) > 1:
        path = path.rstrip("/") or "/"
    drop = {p.lower() for p in tracking_params}
    query_items = [
        (k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if not k.lower().startswith("utm_") and k.lower() not in drop
    ]
    query = urlencode(sorted(query_items))
    return f"{netloc}{path}" + (f"?{query}" if query else "")


def bucket_of(url: str) -> str:
    """分桶：子域名 + 第一级路径，用于在不同栏目之间轮换。"""
    try:
        parts = urlsplit(url)
    except ValueError:
        return ""
    host = (parts.hostname or "").lower()
    first = (parts.path or "/").lstrip("/").split("/", 1)[0]
    return f"{host}/{first}"


class SiteResolver:
    """按输入的 domain 判断 URL 属于哪个站点：主机名等于 key 或以 .key 结尾，取最长匹配。"""

    def __init__(self, keys: Iterable[str]):
        self.keys = sorted({k for k in keys if k}, key=len, reverse=True)

    def site_of(self, url: str) -> Optional[str]:
        host = url_host(url)
        if not host:
            return None
        for key in self.keys:
            if host == key or host.endswith("." + key):
                return key
        return None


@dataclass
class FrontierEntry:
    url: str
    key: str
    retries: int = 0
    tried_groups: Set[int] = field(default_factory=set)
    is_seed: bool = False
    is_reseed: bool = False


@dataclass
class VisitOutcome:
    """on_result 的返回值：本次新采集的 URL（需写入 CSV）和站点是否刚刚结束。"""
    collected_url: Optional[str] = None
    finished_now: bool = False
    note: str = ""


class SiteCrawler:
    def __init__(
        self,
        key: str,
        seed_url: str,
        resolver: SiteResolver,
        *,
        target: int,
        max_visits: int,
        seed_max_attempts: int,
        url_max_retries: int,
        max_urls_per_bucket: int,
        max_frontier: int,
        group_count: int,
        exclude_keywords: List[str],
        tracking_params: List[str],
        non_retryable_rejections: Iterable[str] = (),
    ):
        self.key = key
        self.seed_url = seed_url
        self.resolver = resolver
        self.target = target
        self.max_visits = max_visits
        self.seed_max_attempts = seed_max_attempts
        self.url_max_retries = url_max_retries
        self.max_urls_per_bucket = max_urls_per_bucket
        self.max_frontier = max_frontier
        self.group_count = group_count
        self.exclude_keywords = list(exclude_keywords)
        self.tracking_params = list(tracking_params)
        self.non_retryable_rejections = tuple(non_retryable_rejections)

        self.status = STATUS_SEEDING
        self.finish_reason = ""
        self.collected: List[str] = []
        self.collected_keys: Set[str] = set()
        self.seen: Set[str] = set()
        self.buckets: "OrderedDict[str, Deque[FrontierEntry]]" = OrderedDict()
        self.frontier_size = 0
        # 当前在待访问队列或重试队列中的 key：保证列表内不重复
        self.queued: Set[str] = set()
        self.retry_queue: Deque[FrontierEntry] = deque()
        self.in_flight: Dict[str, FrontierEntry] = {}
        self.bucket_collected: Counter = Counter()
        self.visits = 0
        self.seed_attempts = 0
        # 已提取过链接的页面 key；旧 CSV 恢复的页面不在其中，会被重新访问以带回链接
        self.expanded: Set[str] = set()
        # 运行阶段队列耗尽后重访首页的次数（换出口组，最多 seed_max_attempts 次）
        self.reseeds = 0
        # 访问过首页的出口组：首页的每次尝试都换一个没用过的组（全部用过后放开）
        self.seed_groups: Set[int] = set()
        self.stats: Counter = Counter()
        self.started_at = time.time()
        self.finished_at = 0.0
        self.seed_candidates = self._build_seed_candidates(seed_url)

    # ---------- 基础判断 ----------
    def _build_seed_candidates(self, seed_url: str) -> List[str]:
        candidates = [seed_url]
        host = url_host(seed_url)
        if host and not host.startswith("www."):
            candidates.append(f"https://www.{host}/")
        if host:
            candidates.append(f"http://{host}/")
        unique: List[str] = []
        for item in candidates:
            if item not in unique:
                unique.append(item)
        return unique

    def key_of(self, url: str) -> str:
        return normalize_url_key(url, self.tracking_params)

    def is_excluded(self, url: str) -> bool:
        return any(keyword in url for keyword in self.exclude_keywords)

    @property
    def finished(self) -> bool:
        return self.status in FINISHED_STATUSES

    def has_pending(self) -> bool:
        return self.frontier_size > 0 or bool(self.retry_queue)

    def budget_left(self) -> bool:
        return self.visits < self.max_visits

    # ---------- 派发 ----------
    def can_dispatch(self, group: int) -> bool:
        if self.finished or not self.budget_left():
            return False
        if self.status == STATUS_SEEDING:
            # 种子阶段一次只放一个访问，避免 15 个出口同时打首页
            return (not self.in_flight and self.seed_attempts < self.seed_max_attempts
                    and self._seed_group_allowed(group))
        if len(self.collected) >= self.target:
            return False
        if any(self._retry_allowed(e, group) for e in self.retry_queue):
            return True
        self._prune_full_buckets()
        return self.frontier_size > 0

    def take(self, group: int, visit_id: str) -> Optional[FrontierEntry]:
        """取出下一个要访问的 URL，并登记为 in-flight。调用前应先 can_dispatch。"""
        if not self.can_dispatch(group):
            return None
        entry: Optional[FrontierEntry] = None
        if self.status == STATUS_SEEDING:
            url = self.seed_candidates[self.seed_attempts % len(self.seed_candidates)]
            self.seed_attempts += 1
            entry = FrontierEntry(url=url, key=self.key_of(url), is_seed=True)
            self.seed_groups.add(group)
        else:
            for idx, candidate in enumerate(self.retry_queue):
                if self._retry_allowed(candidate, group):
                    entry = candidate
                    del self.retry_queue[idx]
                    self.queued.discard(entry.key)
                    break
            if entry is None:
                entry = self._pop_frontier()
        if entry is None:
            return None
        if entry.is_reseed:
            self.seed_groups.add(group)
        entry.tried_groups.add(group)
        self.in_flight[visit_id] = entry
        self.visits += 1
        return entry

    def _seed_group_allowed(self, group: int) -> bool:
        return group not in self.seed_groups or len(self.seed_groups) >= self.group_count

    def _retry_allowed(self, entry: FrontierEntry, group: int) -> bool:
        # 重试尽量换一个出口组；所有组都试过时不再限制
        return group not in entry.tried_groups or len(entry.tried_groups) >= self.group_count

    def _prune_full_buckets(self) -> None:
        if self.max_urls_per_bucket <= 0:
            return
        for bucket in [b for b in self.buckets if self.bucket_collected[b] >= self.max_urls_per_bucket]:
            removed = self.buckets.pop(bucket)
            self.frontier_size -= len(removed)
            self.queued.difference_update(e.key for e in removed)
            self.stats["dropped:bucket_full"] += 1

    def _pop_frontier(self) -> Optional[FrontierEntry]:
        """取已采集最少的桶；并列时按轮换顺序，取出后把桶移到末尾。"""
        best = None
        best_count = None
        for bucket, queue_ in self.buckets.items():
            if not queue_:
                continue
            count = self.bucket_collected[bucket]
            if best_count is None or count < best_count:
                best, best_count = bucket, count
        if best is None:
            return None
        queue_ = self.buckets[best]
        entry = queue_.popleft()
        self.frontier_size -= 1
        self.queued.discard(entry.key)
        if queue_:
            self.buckets.move_to_end(best)
        else:
            del self.buckets[best]
        return entry

    def _push_frontier(self, entry: FrontierEntry) -> bool:
        if entry.key in self.queued:
            return False
        self.queued.add(entry.key)
        bucket = bucket_of(entry.url)
        self.buckets.setdefault(bucket, deque()).append(entry)
        self.frontier_size += 1
        return True

    def _push_retry(self, entry: FrontierEntry) -> bool:
        if entry.key in self.queued:
            return False
        self.queued.add(entry.key)
        self.retry_queue.append(entry)
        return True

    # ---------- 结果处理 ----------
    def on_result(self, visit_id: str, group: int, result: Dict[str, Any]) -> VisitOutcome:
        entry = self.in_flight.pop(visit_id, None)
        outcome = VisitOutcome()
        if entry is None:
            return outcome
        kind = str(result.get("outcome", OUTCOME_ERROR))
        reason = str(result.get("reason", "") or "")
        self.stats[f"outcome:{kind}"] += 1
        if kind == OUTCOME_REJECTED:
            self.stats[f"rejected:{reason or 'unknown'}"] += 1

        if kind == OUTCOME_OK:
            outcome.collected_url, outcome.note = self._handle_ok(entry, result)
        elif self._retryable(kind, reason) and not entry.is_seed:
            if entry.retries < self.url_max_retries:
                entry.retries += 1
                self._push_retry(entry)
                self.stats["retried"] += 1
            else:
                self.stats[f"dropped:{kind}"] += 1
        if entry.is_seed and self.status == STATUS_SEEDING:
            self.stats["seed_failures"] += 1

        was_finished = self.finished
        self._update_status()
        outcome.finished_now = self.finished and not was_finished
        return outcome

    def _retryable(self, kind: str, reason: str) -> bool:
        if kind in (OUTCOME_ERROR, OUTCOME_TIMEOUT):
            return True
        return kind == OUTCOME_REJECTED and not reason.startswith(self.non_retryable_rejections)

    def _handle_ok(self, entry: FrontierEntry, result: Dict[str, Any]) -> Tuple[Optional[str], str]:
        final_url = str(result.get("final_url") or entry.url)
        if self.resolver.site_of(final_url) != self.key:
            self.stats["rejected:offsite_redirect"] += 1
            return None, "offsite_redirect"

        if entry.is_seed and self.status == STATUS_SEEDING:
            self.status = STATUS_RUNNING
        # 先登记请求 URL 和最终 URL，再加入页面链接，避免页面的自链接重新进入队列
        final_key = self.key_of(final_url)
        self.seen.update(k for k in (entry.key, final_key) if k)
        self.expanded.update(k for k in (entry.key, final_key) if k)
        self._add_links(result.get("links") or [])

        if self.is_excluded(final_url):
            self.stats["rejected:excluded_final"] += 1
            return None, "excluded_final"
        if final_key in self.collected_keys:
            self.stats["duplicate_final"] += 1
            return None, "duplicate_final"
        bucket = bucket_of(final_url)
        if self.max_urls_per_bucket > 0 and self.bucket_collected[bucket] >= self.max_urls_per_bucket:
            self.stats["dropped:bucket_full"] += 1
            return None, "bucket_full"
        if len(self.collected) >= self.target:
            return None, "target_reached"
        self.collected.append(final_url)
        self.collected_keys.add(final_key)
        self.bucket_collected[bucket] += 1
        return final_url, "collected"

    def _add_links(self, links: Iterable[Any]) -> None:
        for link in links:
            if self.frontier_size >= self.max_frontier:
                self.stats["dropped:frontier_full"] += 1
                return
            if not isinstance(link, str):
                continue
            key = self.key_of(link)
            if not key or key in self.seen:
                continue
            if self.resolver.site_of(link) != self.key:
                continue  # 站外或属于其他输入站点：直接丢弃
            if self.is_excluded(link):
                continue
            self.seen.add(key)
            self._push_frontier(FrontierEntry(url=link, key=key))

    def _update_status(self) -> None:
        if self.finished:
            return
        if len(self.collected) >= self.target:
            self._finish(STATUS_COMPLETE, "target_reached")
            return
        if self.in_flight:
            return
        if self.status == STATUS_SEEDING:
            if self.seed_attempts >= self.seed_max_attempts:
                self._finish(STATUS_PARTIAL if self.collected else STATUS_FAILED, "seed_unreachable")
            return
        if not self.budget_left():
            self._finish(STATUS_PARTIAL if self.collected else STATUS_FAILED, "visit_budget_exhausted")
            return
        self._prune_full_buckets()
        if not self.has_pending():
            if self.seed_attempts + self.reseeds < self.seed_max_attempts:
                # 首页可能在某个出口拿到的是精简页/拦截页：换出口组再访问一次首页
                # 走重试队列：tried_groups 预置为访问过首页的组，保证换出口；
                # retries 置满，失败后不再按普通 URL 重试，由 reseeds 计数控制次数
                url = self.seed_candidates[self.reseeds % len(self.seed_candidates)]
                self.reseeds += 1
                self.stats["reseeded"] += 1
                if self._push_retry(FrontierEntry(
                        url=url, key=self.key_of(url), retries=self.url_max_retries,
                        tried_groups=set(self.seed_groups), is_reseed=True)):
                    return
            self._finish(STATUS_PARTIAL if self.collected else STATUS_FAILED, "frontier_exhausted")

    def _finish(self, status: str, reason: str) -> None:
        self.status = status
        self.finish_reason = reason
        self.finished_at = time.time()

    def check_finished(self) -> bool:
        """在没有新结果时也可以推进状态（例如恢复后队列已空）。返回是否刚刚结束。"""
        was = self.finished
        self._update_status()
        return self.finished and not was

    # ---------- 状态持久化 ----------
    def to_state(self) -> Dict[str, Any]:
        def pack(e: FrontierEntry) -> list:
            return [e.url, e.retries, sorted(e.tried_groups)]
        frontier = [pack(e) for q in self.buckets.values() for e in q]
        # in-flight 的 URL 放回队列，下次启动会重新访问
        frontier += [pack(e) for e in self.in_flight.values() if not e.is_seed]
        retry = [pack(e) for e in self.retry_queue]
        return {
            "key": self.key,
            "seed_url": self.seed_url,
            "status": self.status,
            "finish_reason": self.finish_reason,
            "visits": self.visits,
            "seed_attempts": self.seed_attempts,
            "reseeds": self.reseeds,
            "seed_groups": sorted(self.seed_groups),
            "expanded": sorted(self.expanded),
            "seen": sorted(self.seen),
            "frontier": frontier,
            "retry": retry,
            "stats": dict(self.stats),
            "saved_at": time.time(),
        }

    def _unpack(self, item: list) -> FrontierEntry:
        url, retries = item[0], int(item[1])
        groups = set(int(g) for g in item[2]) if len(item) > 2 else set()
        return FrontierEntry(url=url, key=self.key_of(url), retries=retries, tried_groups=groups)

    def restore(self, state: Optional[Dict[str, Any]], collected_urls: List[str]) -> None:
        """从状态快照和已有 CSV 恢复。CSV 是已采集结果的唯一依据。"""
        for url in collected_urls:
            key = self.key_of(url)
            if not key or key in self.collected_keys:
                continue
            self.collected.append(url)
            self.collected_keys.add(key)
            self.seen.add(key)
            self.bucket_collected[bucket_of(url)] += 1

        if len(self.collected) >= self.target:
            self._finish(STATUS_COMPLETE, "target_reached")
            return

        if state:
            self.stats.update(state.get("stats", {}))
            self.seen.update(state.get("seen", []))
            self.expanded.update(state.get("expanded", []))
            for item in state.get("frontier", []):
                entry = self._unpack(item)
                if entry.key:
                    self.seen.add(entry.key)
                    self._push_frontier(entry)
            for item in state.get("retry", []):
                entry = self._unpack(item)
                if entry.key:
                    self.seen.add(entry.key)
                    self._push_retry(entry)
            # 上次已结束（partial/failed）的站点重新给一份访问预算；未结束的接着用
            if state.get("status") not in FINISHED_STATUSES:
                self.visits = int(state.get("visits", 0))
                self.reseeds = int(state.get("reseeds", 0))
                self.seed_attempts = int(state.get("seed_attempts", 0))
                self.seed_groups.update(int(g) for g in state.get("seed_groups", []))

        if self.collected:
            self.status = STATUS_RUNNING
            # 已采集但没提取过链接的页面（旧 CSV）放回队列：重访不会重复计数，只为带回链接
            for url in self.collected:
                key = self.key_of(url)
                if key not in self.expanded:
                    self._push_frontier(FrontierEntry(url=url, key=key))
            if not self.has_pending():
                # 队列为空（只有旧 CSV 或上次队列耗尽）：重访种子带回链接，种子本身大概率是重复页
                seed_key = self.key_of(self.seed_url)
                self.seen.add(seed_key)
                self._push_frontier(FrontierEntry(url=self.seed_url, key=seed_key))
        # 没有任何已采集结果：从种子阶段重新开始（队列保留，种子成功后继续使用）

    # ---------- 汇总 ----------
    def summary_row(self) -> Dict[str, Any]:
        rejected = Counter({k.split(":", 1)[1]: v for k, v in self.stats.items() if k.startswith("rejected:")})
        top = "; ".join(f"{k}={v}" for k, v in rejected.most_common(3))
        end = self.finished_at or time.time()
        return {
            "site": self.key,
            "status": self.status,
            "finish_reason": self.finish_reason,
            "collected": len(self.collected),
            "target": self.target,
            "visits": self.visits,
            "ok": self.stats.get("outcome:ok", 0),
            "rejected": self.stats.get("outcome:rejected", 0),
            "error": self.stats.get("outcome:error", 0),
            "timeout": self.stats.get("outcome:timeout", 0),
            "frontier": self.frontier_size + len(self.retry_queue),
            "top_rejections": top,
            "elapsed_min": round((end - self.started_at) / 60.0, 1),
        }
