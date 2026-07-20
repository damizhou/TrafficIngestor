#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""单 CSV 宿主入口的公共任务源和运行策略。"""

from __future__ import annotations

import importlib.util
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, Mapping, Type

from host_scheduler.base_traffic_ingestor import BaseTrafficIngestor


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


@dataclass(frozen=True)
class ProfileDefinition:
    """从指定 Python 文件加载出的完整单 CSV 配置。"""

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


def load_profile_definition(
    config_path: str | Path,
    run_policy: RunPolicy,
) -> ProfileDefinition:
    """加载并校验 HTTPS 与 Clash 入口共用的单 CSV 配置文件。"""

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
    delete_csv_record = config.get("DELETE_CSV_RECORD_ON_SUCCESS", True)
    if not isinstance(delete_csv_record, bool):
        raise TypeError(
            f"配置文件 {source_path} 的 DELETE_CSV_RECORD_ON_SUCCESS 必须是布尔值"
        )

    runtime_name = _required_module_value(module, "RUNTIME_NAME", source_path)
    if not isinstance(runtime_name, str) or not runtime_name.strip():
        raise TypeError(f"配置文件 {source_path} 的 RUNTIME_NAME 必须是非空字符串")

    action_profile = _required_module_value(module, "ACTION_PROFILE", source_path)
    if not isinstance(action_profile, str) or not action_profile.strip():
        raise TypeError(f"配置文件 {source_path} 的 ACTION_PROFILE 必须是非空字符串")
    action_profile = action_profile.strip().replace("\\", "/")
    relative_action_path = Path(action_profile)
    browser_root = Path(BaseTrafficIngestor.SOURCE_ROOT, "tools", "browsers").resolve()
    action_path = Path(BaseTrafficIngestor.SOURCE_ROOT, relative_action_path).resolve()
    if (
        relative_action_path.is_absolute()
        or not action_path.is_relative_to(browser_root)
        or action_path.suffix.lower() != ".py"
        or not action_path.is_file()
    ):
        raise ValueError(
            f"配置文件 {source_path} 的 ACTION_PROFILE 必须指向源码根目录下存在的 Python 文件: "
            f"{action_profile}"
        )

    return ProfileDefinition(
        profile_name=source_path.stem,
        profile=CsvIngestorProfile(dict(config), run_policy),
        runtime_name=runtime_name.strip(),
        action_profile=action_profile,
        source_path=source_path,
    )


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
        if not self.DELETE_CSV_RECORD_ON_SUCCESS:
            return

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
