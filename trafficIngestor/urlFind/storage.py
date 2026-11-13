#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""输出与状态文件。

- {OUTPUT_BASE_DIR}/{site}/url_list.csv：采集结果，列 id,url,domain（与旧流程一致）
- {RUNTIME_DIR}/state/{site}.json：站点状态快照，用于断点续传
- {RUNTIME_DIR}/state/cooldowns.json：(出口组, 站点) 冷却表，重启后继续遵守
- {RUNTIME_DIR}/runs/{时间戳}/：run.log、visits.csv（每次访问一行）、summary.csv
"""

from __future__ import annotations

import csv
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

OUTPUT_HEADER = ["id", "url", "domain"]
VISIT_HEADER = [
    "time", "site", "group", "container", "visit_id", "request_url", "final_url",
    "outcome", "reason", "http_status", "links", "note", "elapsed",
]
SUMMARY_HEADER = [
    "site", "status", "finish_reason", "collected", "target", "visits", "ok", "rejected",
    "error", "timeout", "frontier", "top_rejections", "elapsed_min",
]


def safe_name(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z_.-]+", "_", value).strip("._") or "site"


def write_json_atomic(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


def read_json(path: Path) -> Optional[Any]:
    if not path.is_file():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


class SiteOutput:
    """单个站点的 url_list.csv，追加写入。"""

    def __init__(self, base_dir: str, site_key: str):
        self.site_key = site_key
        self.path = Path(base_dir) / site_key / "url_list.csv"

    def load_urls(self) -> List[str]:
        if not self.path.is_file():
            return []
        urls: List[str] = []
        with self.path.open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                url = (row.get("url") or "").strip()
                if url:
                    urls.append(url)
        return urls

    def rewrite(self, urls: List[str]) -> Optional[Path]:
        """用去重后的列表重写 CSV（id 从 1 连续编号），原文件备份为 .bak_<时间>，返回备份路径。"""
        backup = None
        if self.path.exists():
            backup = self.path.with_name(f"{self.path.name}.bak_{time.strftime('%Y%m%d_%H%M%S')}")
            os.replace(self.path, backup)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + ".tmp")
        with tmp.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(OUTPUT_HEADER)
            for idx, url in enumerate(urls, start=1):
                writer.writerow([idx, url, self.site_key])
        os.replace(tmp, self.path)
        return backup

    def append(self, row_id: int, url: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        need_header = not self.path.exists() or self.path.stat().st_size == 0
        with self.path.open("a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            if need_header:
                writer.writerow(OUTPUT_HEADER)
            writer.writerow([row_id, url, self.site_key])


class StateStore:
    def __init__(self, runtime_dir: str):
        self.state_dir = Path(runtime_dir) / "state"

    def site_path(self, site_key: str) -> Path:
        return self.state_dir / f"{safe_name(site_key)}.json"

    def load_site(self, site_key: str) -> Optional[Dict[str, Any]]:
        return read_json(self.site_path(site_key))

    def save_site(self, site_key: str, state: Dict[str, Any]) -> None:
        write_json_atomic(self.site_path(site_key), state)

    def load_cooldowns(self) -> Dict[Tuple[int, str], float]:
        data = read_json(self.state_dir / "cooldowns.json") or []
        now = time.time()
        return {(int(g), str(s)): float(t) for g, s, t in data if float(t) > now}

    def save_cooldowns(self, cooldowns: Dict[Tuple[int, str], float]) -> None:
        now = time.time()
        data = [[g, s, t] for (g, s), t in cooldowns.items() if t > now]
        write_json_atomic(self.state_dir / "cooldowns.json", data)


class RunRecorder:
    """单次运行的日志目录。"""

    def __init__(self, runtime_dir: str):
        stamp = time.strftime("%Y%m%d_%H%M%S")
        self.run_dir = Path(runtime_dir) / "runs" / stamp
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = self.run_dir / "run.log"
        self.visits_path = self.run_dir / "visits.csv"
        self.summary_path = self.run_dir / "summary.csv"
        self._visits_file = self.visits_path.open("a", encoding="utf-8", newline="")
        self._visits_writer = csv.DictWriter(self._visits_file, fieldnames=VISIT_HEADER)
        self._visits_writer.writeheader()

    def record_visit(self, row: Dict[str, Any]) -> None:
        self._visits_writer.writerow({k: row.get(k, "") for k in VISIT_HEADER})
        self._visits_file.flush()

    def write_summary(self, rows: List[Dict[str, Any]]) -> None:
        with self.summary_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=SUMMARY_HEADER)
            writer.writeheader()
            for row in rows:
                writer.writerow({k: row.get(k, "") for k in SUMMARY_HEADER})

    def close(self) -> None:
        self._visits_file.close()
