#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""滚动处理目录内多个 CSV，使每个 CSV 最终获得指定数量的成功 PCAP。"""

import csv
import os
import queue
import shutil
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from host_scheduler.base_traffic_ingestor import BaseTrafficIngestor


# ============== 运行配置 ==============
CSV_DIRECTORY = Path(BaseTrafficIngestor.PROJECT_ROOT) / "scripts" / "csvs"
BASE_DST = Path("/netdisk/cl/260722")
DOCKER_IMAGE = "chuanzhoupan/trace_spider:250912"
DATA_SUBDIR = "data"
LOGS_SUBDIR = "logs"
CSV_GLOB_PATTERN = "*.csv"
TARGET_SUCCESS_PCAPS_PER_CSV = 500
FAILED_URL_RETRY_ROUNDS = 5
MAX_CONTAINER_COUNT = 600
ONE_CONTAINER_PER_TASK_LIMIT = 50
TASKS_PER_CONTAINER_AFTER_LIMIT = 10
PROGRESS_LOG_EVERY_COMPLETIONS = 25


@dataclass
class CsvSiteState:
    csv_path: Path
    jobs: List[Dict[str, str]]
    next_job_index: int = 0
    success_count: int = 0
    inflight_count: int = 0
    retry_jobs: Deque[Dict[str, str]] = field(default_factory=deque)
    discarded_retry_count: int = 0


class MultiCsvTrafficIngestor(BaseTrafficIngestor):
    """按 CSV 优先级滚动填充任务窗口的采集器。"""

    DOCKER_IMAGE = DOCKER_IMAGE
    RETRY = 0
    WORKER_QUEUE_WAIT_TIMEOUT = 0.5
    HOST_CODE_PATH = str(
        Path(BaseTrafficIngestor.PROJECT_ROOT)
        / "runtime"
        / "workspaces"
        / "multi_csv_traffic_ingestor"
    )
    RESULT_DOMAIN_ROOT_DIR = DATA_SUBDIR
    TASK_CSV_DATA_ROOT_LAYOUT = True
    MAX_DYNAMIC_CONTAINER_COUNT = MAX_CONTAINER_COUNT
    DYNAMIC_ONE_CONTAINER_PER_TASK_LIMIT = ONE_CONTAINER_PER_TASK_LIMIT
    DYNAMIC_CONTAINER_TASKS_PER_CONTAINER = TASKS_PER_CONTAINER_AFTER_LIMIT

    def __init__(
        self,
        csv_paths: List[Path],
        base_dst: Path,
        target_successes: int,
        retry_rounds: int,
    ):
        self.BASE_DST = str(base_dst)
        self.CSV_PATH = str(csv_paths[0]) if csv_paths else ""
        self.target_successes = target_successes
        self.retry_rounds = retry_rounds
        self._run_id = time.strftime("%Y%m%d_%H%M%S")
        self.scheduler_capacity = 0
        self._scheduler_lock = threading.RLock()
        self._scheduler_done = threading.Event()
        self._task_queue: Optional["queue.Queue[Dict[str, str]]"] = None
        self._initial_jobs: List[Dict[str, str]] = []
        self._successful_jobs: List[Dict[str, str]] = []
        self._source_csv_paths: List[str] = []
        self._source_manifest_path = ""
        self._success_manifest_path = ""
        self._site_summary_path = ""
        self._sites = self._load_sites(csv_paths)
        super().__init__()

    def _load_sites(self, csv_paths: List[Path]) -> List[CsvSiteState]:
        sites: List[CsvSiteState] = []
        for site_index, csv_path in enumerate(csv_paths):
            jobs, _ = self.read_jobs_from_csv(str(csv_path))
            prepared_jobs = []
            for job in jobs:
                prepared = dict(job)
                prepared["_site_index"] = site_index
                prepared["_csv_path"] = str(csv_path)
                prepared["_capture_attempt"] = 0
                prepared_jobs.append(prepared)
            sites.append(CsvSiteState(csv_path=csv_path, jobs=prepared_jobs))
        return sites

    def copy_task_csv_to_base_dst(self) -> Optional[str]:
        """按 CSV 域名数量将源文件复制到 data，并在 logs 中生成来源清单。"""
        data_root = Path(self.get_result_domain_base_dir())
        logs_root = Path(self.BASE_DST) / LOGS_SUBDIR
        data_root.mkdir(parents=True, exist_ok=True)
        logs_root.mkdir(parents=True, exist_ok=True)

        manifest_path = logs_root / f"source_csv_manifest_{self._run_id}.csv"
        sequence = 1
        while manifest_path.exists():
            sequence += 1
            manifest_path = logs_root / f"source_csv_manifest_{self._run_id}_{sequence}.csv"

        with manifest_path.open("x", encoding="utf-8-sig", newline="") as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=[
                    "source_csv",
                    "source_path",
                    "copied_path",
                    "domain_count",
                    "domains",
                    "url_count",
                    "target_successes",
                ],
            )
            writer.writeheader()
            for site in self._sites:
                domains = sorted(
                    {
                        str(job.get("domain", "")).strip()
                        for job in site.jobs
                        if str(job.get("domain", "")).strip()
                    }
                )
                copied_path = Path(
                    self.build_task_csv_copy_target(str(site.csv_path))
                )
                copied_path.parent.mkdir(parents=True, exist_ok=True)

                if copied_path.exists():
                    self.log(f"目标 CSV 已存在，跳过覆盖: {copied_path}")
                else:
                    shutil.copy2(site.csv_path, copied_path)

                writer.writerow(
                    {
                        "source_csv": site.csv_path.name,
                        "source_path": str(site.csv_path.resolve()),
                        "copied_path": str(copied_path.resolve()),
                        "domain_count": len(domains),
                        "domains": ",".join(domains),
                        "url_count": len(site.jobs),
                        "target_successes": self.target_successes,
                    }
                )
                self.chown_path(str(copied_path))
                self.normalize_success_output_dirs(str(data_root), str(copied_path.parent))
                self.normalize_success_output_path(str(copied_path))
                self._source_csv_paths.append(str(copied_path))

        self.chown_path(str(manifest_path))
        self.normalize_success_output_dirs(str(self.BASE_DST), str(logs_root))
        self.normalize_success_output_path(str(manifest_path))
        self._source_manifest_path = str(manifest_path)
        self.log(f"源 CSV 清单: {manifest_path}")
        return str(manifest_path)

    def initialize_execution_task_log(self) -> Optional[str]:
        """以多 CSV 来源清单初始化日志，并记录完整运行配置。"""
        original_csv_path = self.CSV_PATH
        if self._source_manifest_path:
            self.CSV_PATH = self._source_manifest_path
        try:
            log_path = super().initialize_execution_task_log()
        finally:
            self.CSV_PATH = original_csv_path

        if log_path:
            self._write_execution_task_log(
                "multi_csv_config",
                source_csv_files=self._source_csv_paths,
                target_successes_per_csv=self.target_successes,
                failed_url_retry_rounds=self.retry_rounds,
                csv_count=len(self._sites),
                sources=[
                    {
                        "csv": site.csv_path.name,
                        "path": str(site.csv_path.resolve()),
                        "url_count": len(site.jobs),
                    }
                    for site in self._sites
                ],
            )
        return log_path

    def fetch_jobs(self) -> List[Dict[str, str]]:
        """生成初始滚动窗口；后续任务由成功/失败回调动态补入。"""
        with self._scheduler_lock:
            if self._initial_jobs:
                return []
            planned_task_count = sum(
                min(len(site.jobs), self.target_successes) for site in self._sites
            )
            self.scheduler_capacity = self.resolve_container_count(planned_task_count)
            self.CONTAINER_COUNT = self.scheduler_capacity
            self._fill_available_slots_locked()
            return list(self._initial_jobs)

    def _next_site_job_locked(self, site: CsvSiteState) -> Optional[Dict[str, str]]:
        if site.next_job_index < len(site.jobs):
            task = dict(site.jobs[site.next_job_index])
            site.next_job_index += 1
            return task
        if site.retry_jobs:
            return site.retry_jobs.popleft()
        return None

    def _enqueue_locked(self, site: CsvSiteState, task: Dict[str, str]) -> None:
        site.inflight_count += 1
        if self._task_queue is None:
            self._initial_jobs.append(task)
            return

        self._execution_task_sequence += 1
        task["_task_log_no"] = self._execution_task_sequence
        self._task_queue.put(task)
        with self._stats_lock:
            self._global_total_jobs += 1

    def _fill_available_slots_locked(self) -> None:
        total_inflight = sum(site.inflight_count for site in self._sites)
        capacity = self.scheduler_capacity - total_inflight

        while capacity > 0:
            scheduled = False
            for site in self._sites:
                needed = self.target_successes - site.success_count - site.inflight_count
                while needed > 0 and capacity > 0:
                    task = self._next_site_job_locked(site)
                    if task is None:
                        break
                    self._enqueue_locked(site, task)
                    needed -= 1
                    capacity -= 1
                    scheduled = True
                if capacity <= 0:
                    break
            if not scheduled:
                break

        if sum(site.inflight_count for site in self._sites) == 0:
            self._scheduler_done.set()

    def _build_retry_task(self, task: Dict[str, str]) -> Optional[Dict[str, str]]:
        attempt = int(task.get("_capture_attempt", 0))
        if attempt >= self.retry_rounds:
            return None
        return {
            "row_id": task.get("row_id", ""),
            "url": task.get("url", ""),
            "domain": task.get("domain", ""),
            "_site_index": task.get("_site_index", -1),
            "_csv_path": task.get("_csv_path", ""),
            "_capture_attempt": attempt + 1,
        }

    def on_task_success(self, task: Dict[str, str], paths: Dict[str, str]) -> None:
        site_index = int(task["_site_index"])
        with self._scheduler_lock:
            site = self._sites[site_index]
            site.inflight_count -= 1
            site.success_count += 1
            self._successful_jobs.append(dict(task))
            if site.success_count >= self.target_successes and site.retry_jobs:
                site.discarded_retry_count += len(site.retry_jobs)
                site.retry_jobs.clear()
            self._fill_available_slots_locked()

        try:
            self.remove_first_matching_row_from_csv(
                str(site.csv_path),
                {
                    "id": task.get("row_id", ""),
                    "url": task.get("url", ""),
                    "domain": task.get("domain", ""),
                },
            )
        except Exception as exc:
            self.log(f"ERROR: 删除 CSV 成功记录失败: csv={site.csv_path}, error={exc}")

    def on_task_failed(self, task: Dict[str, str], error: str) -> None:
        site_index = int(task["_site_index"])
        with self._scheduler_lock:
            site = self._sites[site_index]
            site.inflight_count -= 1
            retry_task = self._build_retry_task(task)
            if retry_task is not None:
                site.retry_jobs.append(retry_task)
            self._fill_available_slots_locked()

    def should_wait_for_more_tasks(self) -> bool:
        return not self._scheduler_done.is_set()

    def _build_progress_text(self) -> str:
        with self._scheduler_lock:
            total_success = sum(site.success_count for site in self._sites)
            total_target = len(self._sites) * self.target_successes
            total_inflight = sum(site.inflight_count for site in self._sites)
            reached_sites = sum(
                site.success_count >= self.target_successes for site in self._sites
            )
            active_sites = [
                site
                for site in self._sites
                if site.success_count < self.target_successes
                and (
                    site.inflight_count > 0
                    or site.next_job_index < len(site.jobs)
                    or site.retry_jobs
                )
            ]
            site_parts = [
                f"{site.csv_path.stem}:{site.success_count}/{self.target_successes}"
                f"(在途{site.inflight_count},未请求{len(site.jobs) - site.next_job_index},"
                f"待重试{len(site.retry_jobs)})"
                for site in active_sites[:3]
            ]

        detail = " | ".join(site_parts) if site_parts else "无活动站点"
        return (
            f"站点进度: {reached_sites}/{len(self._sites)}达标 | "
            f"成功PCAP: {total_success}/{total_target} | 在途: {total_inflight} | {detail}"
        )

    def _update_progress(self, ok: bool, task_elapsed: float = 0.0) -> None:
        """保留基类吞吐统计，并改为显示多 CSV 成功目标进度。"""
        super()._update_progress(ok=ok, task_elapsed=task_elapsed)
        progress_text = self._build_progress_text()
        if self._pbar is not None:
            self._pbar.set_description(progress_text)

        total_done = self._global_ok + self._global_fail
        if (
            PROGRESS_LOG_EVERY_COMPLETIONS > 0
            and total_done % PROGRESS_LOG_EVERY_COMPLETIONS == 0
        ):
            self.log(progress_text)
            self._write_execution_task_log(
                "multi_csv_progress",
                completed_attempts=total_done,
                successful_attempts=self._global_ok,
                failed_attempts=self._global_fail,
                sites=self._site_progress_records(),
            )

    def _site_progress_records(self) -> List[Dict[str, Any]]:
        with self._scheduler_lock:
            return [
                {
                    "csv": site.csv_path.name,
                    "success": site.success_count,
                    "target": self.target_successes,
                    "inflight": site.inflight_count,
                    "unrequested": len(site.jobs) - site.next_job_index,
                    "retry_pending": len(site.retry_jobs),
                    "retry_discarded_after_target": site.discarded_retry_count,
                }
                for site in self._sites
            ]

    def run_once(
        self,
        names: List[str],
        jobs: List[Dict[str, str]],
    ) -> Dict[str, Any]:
        task_queue: "queue.Queue[Dict[str, str]]" = queue.Queue()
        self._task_queue = task_queue
        self._initial_jobs = []
        self._global_total_jobs = len(jobs)
        for task in jobs:
            self._execution_task_sequence += 1
            task["_task_log_no"] = self._execution_task_sequence
            task_queue.put(task)

        stats: Dict[str, Any] = {"ok": 0, "fail": 0, "errors": []}
        self.log(
            f"开始滚动执行：初始任务={len(jobs)}，滚动窗口={self.scheduler_capacity}，"
            f"并发容器={len(names)}"
        )

        with ThreadPoolExecutor(max_workers=len(names)) as pool:
            futures = [
                pool.submit(self.worker_loop, name, task_queue, stats, self.RETRY)
                for name in names
            ]
            while not self._scheduler_done.wait(timeout=1.0):
                for name, future in zip(names, futures):
                    if future.done() and future.exception() is not None:
                        raise RuntimeError(f"worker {name} 异常退出") from future.exception()
            for future in futures:
                future.result()

        self._log_site_summary()
        return stats

    def _log_site_summary(self) -> None:
        for site in self._sites:
            status = "达标" if site.success_count >= self.target_successes else "URL及重试已耗尽"
            self.log(
                f"CSV汇总: {site.csv_path.name} | {status} | "
                f"成功={site.success_count}/{self.target_successes} | URL总数={len(site.jobs)}"
            )

    def _write_success_manifest(self) -> Optional[str]:
        if self._success_manifest_path:
            return self._success_manifest_path
        if not self._successful_jobs:
            return None
        logs_root = Path(self.BASE_DST) / LOGS_SUBDIR
        logs_root.mkdir(parents=True, exist_ok=True)
        manifest_path = logs_root / f"multi_csv_success_{self._run_id}.csv"
        sequence = 1
        while manifest_path.exists():
            sequence += 1
            manifest_path = logs_root / f"multi_csv_success_{self._run_id}_{sequence}.csv"

        with manifest_path.open("x", encoding="utf-8-sig", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=["id", "url", "domain", "source_csv"])
            writer.writeheader()
            for task in self._successful_jobs:
                writer.writerow(
                    {
                        "id": task.get("row_id", ""),
                        "url": task.get("url", ""),
                        "domain": task.get("domain", ""),
                        "source_csv": Path(task.get("_csv_path", "")).name,
                    }
                )
        self.chown_path(str(manifest_path))
        self.normalize_success_output_path(str(manifest_path))
        self._success_manifest_path = str(manifest_path)
        return self._success_manifest_path

    def _write_site_summary(self) -> str:
        if self._site_summary_path:
            return self._site_summary_path

        logs_root = Path(self.BASE_DST) / LOGS_SUBDIR
        logs_root.mkdir(parents=True, exist_ok=True)
        summary_path = logs_root / f"multi_csv_site_summary_{self._run_id}.csv"
        sequence = 1
        while summary_path.exists():
            sequence += 1
            summary_path = logs_root / (
                f"multi_csv_site_summary_{self._run_id}_{sequence}.csv"
            )

        with summary_path.open("x", encoding="utf-8-sig", newline="") as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=[
                    "source_csv",
                    "url_count",
                    "success_count",
                    "target_successes",
                    "unrequested_count",
                    "retry_pending_count",
                    "retry_discarded_after_target_count",
                    "status",
                ],
            )
            writer.writeheader()
            for site in self._sites:
                writer.writerow(
                    {
                        "source_csv": site.csv_path.name,
                        "url_count": len(site.jobs),
                        "success_count": site.success_count,
                        "target_successes": self.target_successes,
                        "unrequested_count": len(site.jobs) - site.next_job_index,
                        "retry_pending_count": len(site.retry_jobs),
                        "retry_discarded_after_target_count": site.discarded_retry_count,
                        "status": (
                            "target_reached"
                            if site.success_count >= self.target_successes
                            else "exhausted_before_target"
                        ),
                    }
                )
        self.chown_path(str(summary_path))
        self.normalize_success_output_path(str(summary_path))
        self._site_summary_path = str(summary_path)
        return self._site_summary_path

    def verify_task_completeness(self, csv_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
        summary_path = self._write_site_summary()
        manifest_path = self._write_success_manifest()
        if not manifest_path:
            self.log(f"每站结果汇总: {summary_path}")
            return None
        self._copied_task_csv_path = manifest_path
        report = super().verify_task_completeness(manifest_path)
        self.log(f"成功任务清单: {manifest_path}")
        self.log(f"每站结果汇总: {summary_path}")
        self._write_execution_task_log(
            "multi_csv_result_files",
            success_manifest=manifest_path,
            site_summary=summary_path,
            source_csv_files=self._source_csv_paths,
        )
        return report

    def should_continue(self) -> bool:
        return False

    def get_cleanup_wait_seconds(self) -> float:
        if self._sites and all(
            site.success_count >= self.target_successes for site in self._sites
        ):
            return 0.0
        return super().get_cleanup_wait_seconds()


def main() -> int:
    csv_dir = CSV_DIRECTORY.resolve()
    base_dst = BASE_DST.resolve()
    if not csv_dir.is_dir():
        raise NotADirectoryError(
            f"CSV 目录不存在: {csv_dir}；请先修改文件顶部的 CSV_DIRECTORY"
        )
    if TARGET_SUCCESS_PCAPS_PER_CSV <= 0 or FAILED_URL_RETRY_ROUNDS < 0:
        raise ValueError(
            "TARGET_SUCCESS_PCAPS_PER_CSV 必须大于 0，"
            "FAILED_URL_RETRY_ROUNDS 不能小于 0"
        )

    csv_paths = sorted(
        path for path in csv_dir.glob(CSV_GLOB_PATTERN) if path.is_file()
    )
    if not csv_paths:
        print(
            f"没有找到 CSV: directory={csv_dir}, pattern={CSV_GLOB_PATTERN}",
            flush=True,
        )
        return 0

    ingestor = MultiCsvTrafficIngestor(
        csv_paths=csv_paths,
        base_dst=base_dst,
        target_successes=TARGET_SUCCESS_PCAPS_PER_CSV,
        retry_rounds=FAILED_URL_RETRY_ROUNDS,
    )
    ingestor.acquire_runtime_lock()
    try:
        ingestor.run()
    finally:
        ingestor.release_runtime_lock()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
