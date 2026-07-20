#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""加载指定配置文件并运行 Clash 单 CSV 采集任务。

使用方法：
    python trafficIngestor/host_scheduler_clash/single_csv_profiles.py <配置文件路径>

示例：
    python trafficIngestor/host_scheduler_clash/single_csv_profiles.py trafficIngestor/single_csv/github_trojan.py

配置文件必须定义 CONFIG、RUNTIME_NAME 和 ACTION_PROFILE。
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
from host_scheduler.csv_ingestor_common import (
    CsvIngestorProfile,
    ProfileDefinition,
    RunPolicy,
    build_profile_ingestor,
    load_profile_definition,
    run_with_policy,
)
from host_scheduler_clash.base_clash_traffic_ingestor import (
    BaseClashTrafficIngestor,
)


RUN_POLICY = RunPolicy(
    max_runs=5,
    delay_seconds=3600,
    sleep_after_last_run=True,
)


def build_ingestor(
    definition: ProfileDefinition,
    module_name: str,
    class_name: str = "TrafficIngestor",
) -> Type[BaseTrafficIngestor]:
    profile_attributes = dict(definition.profile.class_attributes)
    profile_attributes.update(
        {
            "ACTION_PROFILE": definition.action_profile,
            "RUNTIME_NAMESPACE": definition.runtime_name,
        }
    )
    configured_profile = CsvIngestorProfile(
        profile_attributes,
        RUN_POLICY,
    )
    return build_profile_ingestor(
        BaseClashTrafficIngestor,
        class_name,
        module_name,
        definition.profile_name,
        definition.runtime_name,
        configured_profile,
    )


def run_profile(ingestor_class: Type[BaseTrafficIngestor]) -> None:
    run_with_policy(ingestor_class, RUN_POLICY)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a Clash single-CSV capture from one config file."
    )
    parser.add_argument(
        "config_path",
        type=Path,
        help="Python config file, for example trafficIngestor/single_csv/github_trojan.py.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    definition = load_profile_definition(args.config_path, RUN_POLICY)
    ingestor_class = build_ingestor(definition, __name__)
    run_profile(ingestor_class)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
