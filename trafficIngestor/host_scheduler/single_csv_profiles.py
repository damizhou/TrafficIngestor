#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""加载指定的非 Clash 单 CSV 配置文件并运行采集任务。

使用方法：
    python trafficIngestor/host_scheduler/single_csv_profiles.py <配置文件路径>

示例：
    python trafficIngestor/host_scheduler/single_csv_profiles.py trafficIngestor/single_csv/base.py

配置文件必须定义 CONFIG 和 ACTION_PROFILE；RUNTIME_NAME 可选。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Type

_source_root = str(Path(__file__).resolve().parent.parent)
_project_root = str(Path(_source_root).parent)
for _import_root in (_project_root, _source_root):
    if _import_root not in sys.path:
        sys.path.insert(0, _import_root)

from host_scheduler.base_traffic_ingestor import BaseTrafficIngestor
from host_scheduler.batch_csv_ingestor import BatchCsvTaskSourceMixin
from host_scheduler.csv_ingestor_common import (
    CsvIngestorProfile,
    CsvTaskSourceMixin,
    ProfileDefinition,
    RunPolicy,
    build_profile_ingestor,
    load_profile_definition as load_shared_profile_definition,
    run_with_policy,
)


RUN_POLICY = RunPolicy(
    max_runs=5,
    delay_seconds=1200,
    stop_on_false=True,
    require_pending_jobs=True,
)
BATCH_RUN_POLICY = RunPolicy(max_runs=1)


def load_profile_definition(config_path: str | Path) -> ProfileDefinition:
    """加载并校验命令行指定的单个配置文件。"""
    return load_shared_profile_definition(config_path, RUN_POLICY)


def build_ingestor(
    definition: ProfileDefinition,
    module_name: str,
    class_name: str = "TrafficIngestor",
) -> Type[BaseTrafficIngestor]:
    profile_attributes = dict(definition.profile.class_attributes)
    batch_by_domain = profile_attributes.pop("BATCH_BY_DOMAIN", False)
    if not isinstance(batch_by_domain, bool):
        raise TypeError("BATCH_BY_DOMAIN 必须是布尔值")
    if batch_by_domain and definition.action_profile != "tools/browsers/chrome.py":
        raise ValueError("按域名批量采集目前只支持 tools/browsers/chrome.py")
    if batch_by_domain and not profile_attributes.get("MOUNT_CODE_IN_CONTAINERS", True):
        raise ValueError("按域名批量采集需要 MOUNT_CODE_IN_CONTAINERS=True")
    group_size = profile_attributes.get("BATCH_GROUP_SIZE", 10)
    if batch_by_domain and (type(group_size) is not int or group_size <= 0):
        raise ValueError("BATCH_GROUP_SIZE 必须是正整数")
    for timing_name in ("BATCH_PAGE_WAIT_SECONDS", "BATCH_PAGE_GAP_SECONDS"):
        timing_value = profile_attributes.get(timing_name)
        if timing_value is None:
            continue
        if (isinstance(timing_value, bool)
                or not isinstance(timing_value, (int, float))
                or timing_value < 0):
            raise ValueError(f"{timing_name} 必须是非负数")
    profile_attributes["ACTION_PROFILE"] = definition.action_profile
    configured_profile = CsvIngestorProfile(
        profile_attributes,
        BATCH_RUN_POLICY if batch_by_domain else RUN_POLICY,
    )
    return build_profile_ingestor(
        BaseTrafficIngestor,
        class_name,
        module_name,
        definition.profile_name,
        definition.runtime_name,
        configured_profile,
        task_source_class=BatchCsvTaskSourceMixin if batch_by_domain else CsvTaskSourceMixin,
    )


def run_profile(
    definition: ProfileDefinition,
    ingestor_class: Type[BaseTrafficIngestor],
) -> None:
    batch_by_domain = definition.profile.class_attributes.get("BATCH_BY_DOMAIN", False)
    run_with_policy(ingestor_class, BATCH_RUN_POLICY if batch_by_domain else RUN_POLICY)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a non-Clash single-CSV capture from one config file."
    )
    parser.add_argument(
        "config_path",
        type=Path,
        help="Python config file, for example trafficIngestor/single_csv/base.py.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    definition = load_profile_definition(args.config_path)
    ingestor_class = build_ingestor(definition, __name__)
    run_profile(definition, ingestor_class)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
