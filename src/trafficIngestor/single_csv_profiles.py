#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""加载指定的非 Clash 单 CSV 配置文件并运行采集任务。"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Mapping, Type

_source_root = str(Path(__file__).resolve().parent.parent)
_project_root = str(Path(_source_root).parent)
for _import_root in (_source_root, _project_root):
    if _import_root not in sys.path:
        sys.path.insert(0, _import_root)

from trafficIngestor.base_traffic_ingestor import BaseTrafficIngestor
from trafficIngestor.csv_ingestor_common import (
    CsvIngestorProfile,
    RunPolicy,
    build_profile_ingestor,
    run_with_policy,
)


@dataclass(frozen=True)
class ProfileDefinition:
    """从一个指定配置文件加载出的完整采集配置。"""

    profile_name: str
    profile: CsvIngestorProfile
    runtime_name: str
    action_profile: str
    source_path: Path


def _required_module_value(
    module: ModuleType,
    field_name: str,
    source_path: Path,
) -> object:
    try:
        return getattr(module, field_name)
    except AttributeError as exc:
        raise ValueError(
            f"配置文件 {source_path} 缺少必填字段 {field_name}"
        ) from exc


def _load_config_module(config_path: Path) -> ModuleType:
    module_name = f"_traffic_ingestor_config_{config_path.stem}"
    module_spec = importlib.util.spec_from_file_location(module_name, config_path)
    if module_spec is None or module_spec.loader is None:
        raise ImportError(f"无法加载配置文件: {config_path}")

    module = importlib.util.module_from_spec(module_spec)
    sys.modules[module_name] = module
    try:
        module_spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


def load_profile_definition(config_path: str | Path) -> ProfileDefinition:
    """加载并校验命令行指定的单个配置文件。"""

    source_path = Path(config_path).expanduser()
    if not source_path.is_absolute():
        source_path = Path.cwd() / source_path
    source_path = source_path.resolve()

    if source_path.suffix.lower() != ".py":
        raise ValueError(f"配置文件必须使用 .py 扩展名: {source_path}")
    if not source_path.is_file():
        raise FileNotFoundError(f"配置文件不存在: {source_path}")

    module = _load_config_module(source_path)

    config = _required_module_value(module, "CONFIG", source_path)
    if not isinstance(config, Mapping):
        raise TypeError(f"配置文件 {source_path} 的 CONFIG 必须是映射类型")

    run_policy = _required_module_value(module, "RUN_POLICY", source_path)
    if not isinstance(run_policy, RunPolicy):
        raise TypeError(f"配置文件 {source_path} 的 RUN_POLICY 必须是 RunPolicy")

    runtime_name = _required_module_value(module, "RUNTIME_NAME", source_path)
    if not isinstance(runtime_name, str) or not runtime_name.strip():
        raise TypeError(f"配置文件 {source_path} 的 RUNTIME_NAME 必须是非空字符串")

    action_profile = _required_module_value(module, "ACTION_PROFILE", source_path)
    if not isinstance(action_profile, str) or not action_profile.strip():
        raise TypeError(f"配置文件 {source_path} 的 ACTION_PROFILE 必须是非空字符串")

    return ProfileDefinition(
        profile_name=source_path.stem,
        profile=CsvIngestorProfile(dict(config), run_policy),
        runtime_name=runtime_name,
        action_profile=action_profile,
        source_path=source_path,
    )


def build_ingestor(
    definition: ProfileDefinition,
    module_name: str,
    class_name: str = "TrafficIngestor",
) -> Type[BaseTrafficIngestor]:
    profile_attributes = dict(definition.profile.class_attributes)
    profile_attributes["ACTION_PROFILE"] = definition.action_profile
    configured_profile = CsvIngestorProfile(
        profile_attributes,
        definition.profile.run_policy,
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
    run_with_policy(ingestor_class, definition.profile.run_policy)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a non-Clash single-CSV capture from one config file."
    )
    parser.add_argument(
        "config_path",
        type=Path,
        help="Python config file, for example configs/single_csv/base.py.",
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
