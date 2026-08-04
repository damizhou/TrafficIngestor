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
from host_scheduler.csv_ingestor_common import (
    CsvIngestorProfile,
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


def load_profile_definition(config_path: str | Path) -> ProfileDefinition:
    """加载并校验命令行指定的单个配置文件。"""
    return load_shared_profile_definition(config_path, RUN_POLICY)


def build_ingestor(
    definition: ProfileDefinition,
    module_name: str,
    class_name: str = "TrafficIngestor",
) -> Type[BaseTrafficIngestor]:
    profile_attributes = dict(definition.profile.class_attributes)
    profile_attributes["ACTION_PROFILE"] = definition.action_profile
    configured_profile = CsvIngestorProfile(
        profile_attributes,
        RUN_POLICY,
    )
    return build_profile_ingestor(
        BaseTrafficIngestor,
        class_name,
        module_name,
        definition.profile_name,
        definition.runtime_name,
        configured_profile,
    )


def run_profile(
    definition: ProfileDefinition,
    ingestor_class: Type[BaseTrafficIngestor],
) -> None:
    run_with_policy(ingestor_class, RUN_POLICY)


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
