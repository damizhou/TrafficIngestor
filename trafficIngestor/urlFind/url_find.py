#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""urlFind 入口：读取站点列表 → 恢复状态 → 一次性创建容器池 → 总调度运行。

用法（参数全部在 urlFind/config.py 中修改）：
    python trafficIngestor/urlFind/url_find.py
"""

from __future__ import annotations

import csv
import os
import signal
import sys
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import urlsplit

_SOURCE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PROJECT_ROOT = os.path.dirname(_SOURCE_ROOT)
for _root in (_PROJECT_ROOT, _SOURCE_ROOT):
    if _root not in sys.path:
        sys.path.insert(0, _root)

from urlFind import config  # noqa: E402
from urlFind.coordinator import Coordinator  # noqa: E402
from urlFind.pool import UrlFindPool  # noqa: E402
from urlFind.site import SiteCrawler, SiteResolver  # noqa: E402
from urlFind.storage import RunRecorder, SiteOutput, StateStore  # noqa: E402
from urlFind.visit import run_visit  # noqa: E402

EXCLUDE_KEYWORDS_FILE = os.path.join(config.SOURCE_ROOT, "url_list_collector", "exclude_keywords")


def parse_site(raw: str) -> Tuple[str, str]:
    """输入的 domain（或 URL）→ (站点 key, 种子 URL)。key 为小写主机名。"""
    text = (raw or "").strip()
    if not text:
        return "", ""
    has_scheme = "://" in text
    parts = urlsplit(text if has_scheme else "https://" + text)
    host = (parts.hostname or "").lower().strip(".")
    if not host:
        return "", ""
    if has_scheme and parts.scheme.lower() in ("http", "https"):
        return host, text
    return host, f"https://{host}/"


def read_sites(path: str) -> List[Tuple[str, str]]:
    """读取 id,domain 两列或 domain 单列 CSV，按站点 key 去重（保留第一次出现的种子）。"""
    sites: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        for line_no, row in enumerate(csv.reader(f), start=1):
            if not row:
                continue
            first = (row[0] or "").strip()
            if line_no == 1 and first.lower() in ("id", "rank", "index", "domain"):
                continue
            raw = (row[1] if len(row) > 1 and (row[1] or "").strip() else first)
            key, seed = parse_site(raw)
            if key and key not in sites:
                sites[key] = seed
    return list(sites.items())


def load_exclude_keywords() -> List[str]:
    if config.EXCLUDE_KEYWORDS is not None:
        return [k for k in config.EXCLUDE_KEYWORDS if k]
    if not os.path.isfile(EXCLUDE_KEYWORDS_FILE):
        return []
    with open(EXCLUDE_KEYWORDS_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def build_sites(store: StateStore, log) -> Tuple[List[SiteCrawler], Dict[str, SiteOutput]]:
    entries = read_sites(config.SOURCE_CSV)
    resolver = SiteResolver(key for key, _ in entries)
    exclude = load_exclude_keywords()
    sites: List[SiteCrawler] = []
    outputs: Dict[str, SiteOutput] = {}
    for key, seed in entries:
        site = SiteCrawler(
            key, seed, resolver,
            target=config.TARGET_URLS_PER_SITE,
            max_visits=config.TARGET_URLS_PER_SITE * config.MAX_VISIT_FACTOR,
            seed_max_attempts=config.SEED_MAX_ATTEMPTS,
            url_max_retries=config.URL_MAX_RETRIES,
            max_urls_per_bucket=config.MAX_URLS_PER_BUCKET,
            max_frontier=config.MAX_FRONTIER_PER_SITE,
            group_count=config.EGRESS_IP_COUNT,
            exclude_keywords=exclude,
            tracking_params=config.TRACKING_QUERY_PARAMS,
            non_retryable_rejections=config.NON_RETRYABLE_REJECTIONS,
        )
        output = SiteOutput(config.OUTPUT_BASE_DIR, key)
        existing = output.load_urls()
        site.restore(store.load_site(key), existing)
        if existing != site.collected:
            # 已有 CSV 含重复 URL（如旧流程写入的 /docs 与 /docs/）：按去重结果重写，保证 id 连续不冲突
            backup = output.rewrite(site.collected)
            log(f"WARN: {key} 已有 CSV {len(existing)} 行，去重后 {len(site.collected)} 行，已重写；原文件备份 {backup}")
        sites.append(site)
        outputs[key] = output
    return sites, outputs


def main() -> int:
    if not os.path.isfile(config.SOURCE_CSV):
        print(f"FATAL: 输入 CSV 不存在: {config.SOURCE_CSV}", file=sys.stderr)
        return 2
    Path(config.OUTPUT_BASE_DIR).mkdir(parents=True, exist_ok=True)

    recorder = RunRecorder(config.RUNTIME_DIR)
    pool = UrlFindPool()
    pool.attach_log_file(str(recorder.log_path))
    log = pool.log
    log(f"运行目录: {recorder.run_dir}")
    log(f"输入: {config.SOURCE_CSV}；输出: {config.OUTPUT_BASE_DIR}")

    store = StateStore(config.RUNTIME_DIR)
    sites, outputs = build_sites(store, log)
    pending = [s for s in sites if not s.finished]
    log(f"站点 {len(sites)} 个，待处理 {len(pending)} 个，"
        f"已有采集 {sum(len(s.collected) for s in sites)} 条")
    if not pending:
        log("没有需要处理的站点，退出。")
        recorder.write_summary([s.summary_row() for s in sites])
        recorder.close()
        return 0

    containers = []
    try:
        containers = pool.prepare()
        coordinator = Coordinator(
            sites, containers,
            lambda container, visit_id, url: run_visit(pool, container, visit_id, url),
            outputs=outputs, store=store, recorder=recorder, log=log,
        )

        def on_signal(signum, _frame):
            if coordinator.stop_requested:
                os._exit(128 + signum)
            coordinator.request_stop()
            log(f"收到信号 {signum}：停止派发，等待进行中的访问结束（再次中断立即退出）")

        signal.signal(signal.SIGINT, on_signal)
        signal.signal(signal.SIGTERM, on_signal)
        rows = coordinator.run()
        log(f"汇总: {recorder.summary_path}")
        for row in rows:
            log(f"  {row['site']}: {row['status']} {row['collected']}/{row['target']} "
                f"visits={row['visits']} {row['top_rejections']}")
    finally:
        if containers:
            pool.shutdown()
        else:
            pool.release_runtime_lock()
        recorder.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
