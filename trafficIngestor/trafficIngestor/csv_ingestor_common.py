#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""单 CSV 宿主入口的公共任务源和运行策略。"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Type

from trafficIngestor.base_traffic_ingestor import BaseTrafficIngestor


@dataclass(frozen=True)
class RunPolicy:
    """控制一个原单 CSV 入口重复执行采集器的方式。"""

    max_runs: int = 1
    delay_seconds: float = 0.0
    stop_on_false: bool = False
    require_pending_jobs: bool = False
    sleep_after_last_run: bool = False

    def __post_init__(self) -> None:
        if self.max_runs <= 0:
            raise ValueError("max_runs must be positive")
        if self.delay_seconds < 0:
            raise ValueError("delay_seconds must not be negative")


@dataclass(frozen=True)
class CsvIngestorProfile:
    """一个入口的类配置和外层运行策略。"""

    class_attributes: Mapping[str, Any]
    run_policy: RunPolicy = RunPolicy()


class CsvTaskSourceMixin:
    """复用单 CSV 任务读取、成功删行和单轮退出逻辑。"""

    CSV_PATH: str
    _has_jobs: bool

    def __init__(self) -> None:
        super().__init__()
        self._has_jobs = True

    def fetch_jobs(self) -> list[Dict[str, str]]:
        if not self._has_jobs:
            return []

        jobs, _ = self.read_jobs_from_csv(self.CSV_PATH)
        if not jobs:
            self._has_jobs = False
        return jobs

    def on_task_success(
        self,
        task: Dict[str, str],
        paths: Dict[str, str],
    ) -> None:
        del paths
        guard_value = task.get(self.SUCCESS_DELETE_GUARD_FIELD, "")
        if not guard_value:
            return

        try:
            self.remove_first_matching_row_from_csv(
                self.CSV_PATH,
                {
                    "id": task.get("row_id", ""),
                    "url": task.get("url", ""),
                    "domain": task.get("domain", ""),
                },
            )
        except Exception as exc:
            self.log(f"ERROR: 删除 CSV 记录失败: {exc}")

    def should_continue(self) -> bool:
        return False


def build_profile_ingestor(
    base_class: Type[BaseTrafficIngestor],
    class_name: str,
    module_name: str,
    profile_name: str,
    runtime_name: str,
    profile: CsvIngestorProfile,
) -> Type[BaseTrafficIngestor]:
    """按 profile 动态创建类，同时保留原入口名称作为运行身份。"""

    attributes = dict(profile.class_attributes)
    attributes.setdefault("BASE_NAME", runtime_name)
    attributes.setdefault("ACTION_PROFILE", "tools/browsers/chrome.py")
    attributes.setdefault("SYNC_DEFAULT_ACTION", True)
    attributes.update(
        {
            "__module__": module_name,
            "__doc__": f"配置驱动的单 CSV 采集入口：{profile_name}",
            "PROFILE_NAME": profile_name,
        }
    )
    return type(class_name, (CsvTaskSourceMixin, base_class), attributes)


def run_with_policy(
    ingestor_class: Type[BaseTrafficIngestor],
    policy: RunPolicy,
) -> None:
    """按旧入口的循环、等待和停止规则执行采集器。"""

    for run_index in range(policy.max_runs):
        processed_any = ingestor_class.main()
        if policy.stop_on_false and not processed_any:
            break

        has_next_run = run_index + 1 < policy.max_runs
        if not has_next_run and not policy.sleep_after_last_run:
            break
        if policy.require_pending_jobs and not ingestor_class.has_pending_jobs():
            break
        if policy.delay_seconds > 0:
            time.sleep(policy.delay_seconds)
