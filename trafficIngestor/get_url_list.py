#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
get_url_list.py

从 small_tools/top2000.csv 读取站点列表，使用 Docker 容器中的浏览器抓取每个站点
可访问页面的 URL（保存重定向后的最终 URL）。
"""

import csv
import json
import os
import sys
import threading
from pathlib import Path
from typing import Dict, List, Set
from urllib.parse import urlparse

# 添加项目根目录到路径
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from trafficIngestor.base_traffic_ingestor import BaseTrafficIngestor, get_real_username


class URLListIngestor(BaseTrafficIngestor):
    """URL 列表抓取器"""

    # ============== 配置 ==============
    CONTAINER_PREFIX = f"{get_real_username()}_get_url_list"
    CONTAINER_COUNT = int(os.environ.get("URL_LIST_CONTAINER_COUNT", "40"))
    HOST_CODE_PATH = os.path.join(_project_root, "url_list_collector")
    DOCKER_IMAGE = "chuanzhoupan/trace_spider:250912"
    RETRY = int(os.environ.get("URL_LIST_RETRY", "2"))
    DOCKER_EXEC_TIMEOUT = int(os.environ.get("URL_LIST_TIMEOUT", "2400"))
    FIRST_EXEC_INTERVAL = 0.3

    SOURCE_CSV = os.path.join(_project_root, "small_tools", "top2000.csv")
    OUTPUT_BASE_DIR = os.environ.get("URL_LIST_OUTPUT_BASE_DIR", "/netdisk/ww/top2000/subpages")
    FAILED_CSV = os.path.join(_current_dir, "get_url_list_failed.csv")
    TARGET_URLS_PER_DOMAIN = int(os.environ.get("URLS_PER_DOMAIN", "40"))

    OUTPUT_HEADER = ["id", "url", "domain"]
    FAILED_HEADER = ["seed_id", "seed_url", "domain", "collected_count", "error"]

    def __init__(self):
        super().__init__()
        self._has_jobs = True
        self._output_lock = threading.Lock()
        self._failed_lock = threading.Lock()
        self._domain_url_set: Dict[str, Set[str]] = {}
        self._load_existing_output()

    @staticmethod
    def _normalize_domain(value: str) -> str:
        text = (value or "").strip()
        if not text:
            return ""
        if "://" not in text:
            text = "https://" + text
        parsed = urlparse(text)
        host = (parsed.hostname or "").strip().lower().strip(".")
        return host

    def _normalize_seed_url(self, value: str) -> str:
        text = (value or "").strip()
        if not text:
            return ""
        if "://" in text:
            parsed = urlparse(text)
            if parsed.scheme.lower() in ("http", "https") and parsed.netloc:
                return text
        domain = self._normalize_domain(text)
        if not domain:
            return ""
        return f"https://{domain}"

    def _domain_output_csv(self, domain: str) -> str:
        return os.path.join(self.OUTPUT_BASE_DIR, domain, "url_list.csv")

    def _read_source_jobs(self) -> List[Dict[str, str]]:
        p = Path(self.SOURCE_CSV)
        if not p.exists():
            self.log(f"WARN: 源 CSV 不存在: {self.SOURCE_CSV}")
            return []

        jobs: List[Dict[str, str]] = []
        seen_domains: Set[str] = set()

        with p.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            for line_no, row in enumerate(reader, start=1):
                if not row:
                    continue

                first = (row[0] or "").strip()
                second = (row[1] or "").strip() if len(row) > 1 else ""

                if line_no == 1 and first.lower() in ("id", "rank", "index"):
                    continue

                raw_site = second if second else first
                domain = self._normalize_domain(raw_site)
                if not domain or domain in seen_domains:
                    continue
                seen_domains.add(domain)

                row_id = first if first and first.lower() not in ("id", "rank", "index") else str(len(jobs) + 1)
                seed_url = self._normalize_seed_url(raw_site)
                if not seed_url:
                    continue

                jobs.append(
                    {
                        "row_id": row_id,
                        "url": seed_url,
                        "domain": domain,
                        "target_count": str(self.TARGET_URLS_PER_DOMAIN),
                    }
                )

        return jobs

    def _load_existing_output(self) -> None:
        jobs = self._read_source_jobs()
        if not jobs:
            return

        for job in jobs:
            domain = self._normalize_domain(job.get("domain", ""))
            if not domain:
                continue

            p = Path(self._domain_output_csv(domain))
            if not p.exists():
                continue

            try:
                with p.open("r", encoding="utf-8-sig", newline="") as f:
                    reader = csv.DictReader(f)
                    if reader.fieldnames is None:
                        continue

                    for row in reader:
                        url = (row.get("url", "") or "").strip()
                        if not url:
                            continue
                        row_domain = self._normalize_domain(row.get("domain", "")) or domain
                        if row_domain != domain:
                            continue
                        self._domain_url_set.setdefault(domain, set()).add(url)
            except Exception as e:
                self.log(f"WARN: 读取历史输出失败 {p}: {e}")

        self.log(
            f"已加载历史输出: domains={len(self._domain_url_set)}, "
            f"urls={sum(len(v) for v in self._domain_url_set.values())}"
        )

    def _append_rows(self, csv_path: str, header: List[str], rows: List[Dict[str, str]]) -> None:
        if not rows:
            return
        p = Path(csv_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        need_header = (not p.exists()) or p.stat().st_size == 0

        with p.open("a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            if need_header:
                writer.writeheader()
            writer.writerows(rows)

    def fetch_jobs(self) -> List[Dict[str, str]]:
        """读取待抓取站点，仅返回未达到目标 URL 数的 domain。"""
        if not self._has_jobs:
            return []

        source_jobs = self._read_source_jobs()
        pending = []
        for job in source_jobs:
            domain = job.get("domain", "")
            exist_count = len(self._domain_url_set.get(domain, set()))
            if exist_count >= self.TARGET_URLS_PER_DOMAIN:
                continue
            pending.append(job)

        self._has_jobs = False
        self.log(
            f"源站点={len(source_jobs)}, 待处理={len(pending)}, "
            f"目标每站点={self.TARGET_URLS_PER_DOMAIN} URL"
        )
        return pending

    def process_result(self, task: Dict[str, str], container: str):
        """处理 action 输出，增量写入每个 domain 独立的 url_list.csv。"""
        meta_path = os.path.join(self.HOST_CODE_PATH, "meta", f"{container}_last.json")
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                result = json.load(f)
        except Exception as e:
            return False, f"meta read error: {e}"

        domain = self._normalize_domain(task.get("domain", "")) or self._normalize_domain(result.get("domain", ""))
        if not domain:
            return False, "invalid domain in task/result"

        raw_urls = result.get("collected_urls", [])
        if not isinstance(raw_urls, list):
            raw_urls = []

        unique_urls = []
        seen = set()
        for item in raw_urls:
            if not isinstance(item, str):
                continue
            u = item.strip()
            if not u or u in seen:
                continue
            seen.add(u)
            unique_urls.append(u)

        with self._output_lock:
            existing_set = self._domain_url_set.setdefault(domain, set())
            new_rows = []

            for u in unique_urls:
                if len(existing_set) >= self.TARGET_URLS_PER_DOMAIN:
                    break
                if u in existing_set:
                    continue
                existing_set.add(u)
                next_id = len(existing_set)
                new_rows.append({"id": str(next_id), "url": u, "domain": domain})
            if new_rows:
                self._append_rows(self._domain_output_csv(domain), self.OUTPUT_HEADER, new_rows)

            total = len(existing_set)

        self.log(f"{domain}: 新增 {len(new_rows)} 条，累计 {total}/{self.TARGET_URLS_PER_DOMAIN}")

        if total >= self.TARGET_URLS_PER_DOMAIN:
            return True, ""

        err = result.get("error", "")
        if err:
            return False, f"insufficient urls: {total}/{self.TARGET_URLS_PER_DOMAIN}; error={err}"
        return False, f"insufficient urls: {total}/{self.TARGET_URLS_PER_DOMAIN}"

    def on_task_failed(self, task: Dict[str, str], error: str) -> None:
        """记录最终失败的 domain（保留已抓取的部分结果）。"""
        domain = self._normalize_domain(task.get("domain", ""))
        with self._failed_lock:
            rows = [
                {
                    "seed_id": task.get("row_id", ""),
                    "seed_url": task.get("url", ""),
                    "domain": domain,
                    "collected_count": str(len(self._domain_url_set.get(domain, set()))),
                    "error": (error or "")[:500],
                }
            ]
            self._append_rows(self.FAILED_CSV, self.FAILED_HEADER, rows)

    def should_continue(self) -> bool:
        return False

    def cleanup(self) -> None:
        self.remove_containers()


if __name__ == "__main__":
    URLListIngestor.main()
