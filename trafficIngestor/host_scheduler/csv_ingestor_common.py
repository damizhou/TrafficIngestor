#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""单 CSV 宿主入口的公共任务源和运行策略。"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import os
import re
import stat
import sys
import tempfile
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, Mapping, Type

from host_scheduler.base_traffic_ingestor import (
    BaseTrafficIngestor,
    FatalIngestorError,
)


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
    delete_csv_from_journal = config.get(
        "DELETE_CSV_FROM_JOURNAL_ON_START",
        True,
    )
    if not isinstance(delete_csv_from_journal, bool):
        raise TypeError(
            f"配置文件 {source_path} 的 DELETE_CSV_FROM_JOURNAL_ON_START 必须是布尔值"
        )
    clean_and_recreate = config.get("CLEAN_AND_RECREATE_CONTAINERS", True)
    if not isinstance(clean_and_recreate, bool):
        raise TypeError(
            f"配置文件 {source_path} 的 CLEAN_AND_RECREATE_CONTAINERS 必须是布尔值"
        )

    runtime_name = getattr(
        module,
        "RUNTIME_NAME",
        f"traffic_capture_single_csv_{source_path.stem}",
    )
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
    CSV_SUCCESS_FLUSH_SIZE = 1000
    DELETE_CSV_FROM_JOURNAL_ON_START = True
    CSV_NON_RETRYABLE_FAILURE_MARKERS = ("ERR_NAME_NOT_RESOLVED",)
    CSV_NON_RETRYABLE_HTTP_STATUS_CODES = (404, 410)
    CSV_PRESERVE_FAILURE_MARKERS = (
        "PCAP_TOO_SMALL",
        "SSL_KEY_TOO_SMALL",
        "CONTENT_MISSING",
        "SCREENSHOT_MISSING",
    )
    CSV_REMOVAL_HTTP_STATUS_MIN = 400
    CSV_OVERRIDE_PRESERVE_FAILURE_MARKERS = (
        "REASON=HTTP_ERROR",
        "REASON=HUMAN_VERIFICATION",
        "PAGE_NOT_FOUND",
        "OPEN_URL_ERROR=",
        "HUMAN_VERIFICATION=",
        "PCAP_INVALID=",
        "HTML_MISSING",
    )
    CSV_HTTP_STATUS_PATTERN = re.compile(r"HTTP_STATUS=(\d+)")
    CSV_JOURNAL_VERSION = 3
    CSV_JOURNAL_KIND = "csv-removal-journal"
    CSV_PENDING_KIND = "csv-removal-pending"
    CSV_STATE_KINDS = frozenset(
        {
            CSV_JOURNAL_KIND,
            CSV_PENDING_KIND,
        }
    )
    _CSV_ROW_INDEX_FIELD = "_csv_row_index"
    _has_jobs: bool

    def __init__(self) -> None:
        super().__init__()
        self._has_jobs = True
        self._csv_loaded = False
        self._csv_header_fields: list[str] = []
        self._csv_rows: list[Dict[str, str]] = []
        self._csv_applied_indices: set[int] = set()
        self._csv_open_journal_indices: set[int] = set()
        self._csv_commit_indices: set[int] = set()
        self._csv_loaded_sha256 = ""
        self._csv_commit_lock = threading.Lock()

    def fetch_jobs(self) -> list[Dict[str, str]]:
        if not self._has_jobs:
            return []

        if not self._csv_loaded:
            csv_path = self._get_csv_path()
            if not csv_path.exists():
                self._csv_loaded = True
                self._has_jobs = False
                return []
            try:
                self._recover_csv_pending_before_load(csv_path)
            except FatalIngestorError:
                raise
            except Exception as exc:
                raise FatalIngestorError(
                    f"恢复 CSV pending 失败: {exc}"
                ) from exc
            self._csv_header_fields, self._csv_rows = self._read_csv_records(
                csv_path
            )
            self._csv_loaded_sha256 = self._sha256_file(csv_path)
            self._csv_loaded = True

        if not self._csv_header_fields:
            self._has_jobs = False
            return []

        try:
            self._recover_csv_journal_after_load()
        except FatalIngestorError:
            raise
        except Exception as exc:
            raise FatalIngestorError(
                f"恢复 CSV journal 失败: {exc}"
            ) from exc

        jobs: list[Dict[str, str]] = []
        for row_index, row in enumerate(self._csv_rows):
            if self._is_csv_row_reserved(row_index):
                continue
            url = self._get_csv_row_value(row, "url")
            if not url:
                continue
            row.setdefault("row_id", self._get_csv_row_value(row, "id"))
            row.setdefault("url", url)
            row.setdefault("domain", self._get_csv_row_value(row, "domain"))
            row[self._CSV_ROW_INDEX_FIELD] = row_index
            jobs.append(row)

        # 单个实例只发放一次全量内存任务，避免重复入队。
        self._has_jobs = False
        return jobs

    @staticmethod
    def _get_csv_row_value(row: Dict[str, str], key: str) -> str:
        expected_key = key.strip().lower()
        for actual_key, actual_value in row.items():
            if isinstance(actual_key, str) and actual_key.lower() == expected_key:
                return (actual_value or "").strip()
        return ""

    @classmethod
    def _is_non_retryable_csv_failure(cls, error: str) -> bool:
        normalized_error = str(error or "").upper()
        return any(
            marker in normalized_error
            for marker in cls.CSV_NON_RETRYABLE_FAILURE_MARKERS
        ) or any(
            f"HTTP_STATUS={status}" in normalized_error
            for status in cls.CSV_NON_RETRYABLE_HTTP_STATUS_CODES
        ) or "PAGE_NOT_FOUND" in normalized_error

    def is_non_retryable_error(self, error: str) -> bool:
        if self._is_non_retryable_csv_failure(error):
            return True
        return super().is_non_retryable_error(error)

    @classmethod
    def _should_remove_csv_on_failure(cls, error: str) -> bool:
        normalized_error = str(error or "").upper()
        # Preservable capture failures often appear alongside the primary page
        # failure. Preserve the row only when no stronger failure is present.
        has_preserved_failure = any(
            marker in normalized_error
            for marker in cls.CSV_PRESERVE_FAILURE_MARKERS
        )
        if not has_preserved_failure:
            return True
        if cls._has_csv_removal_http_status(normalized_error):
            return True
        return any(
            marker in normalized_error
            for marker in cls.CSV_OVERRIDE_PRESERVE_FAILURE_MARKERS
        )

    @classmethod
    def _has_csv_removal_http_status(cls, normalized_error: str) -> bool:
        for match in cls.CSV_HTTP_STATUS_PATTERN.finditer(normalized_error):
            try:
                status_code = int(match.group(1))
            except ValueError:
                continue
            if status_code >= cls.CSV_REMOVAL_HTTP_STATUS_MIN:
                return True
        return False

    def _mark_csv_row_for_removal(self, task: Dict[str, str]) -> bool:
        if not self.DELETE_CSV_RECORD_ON_SUCCESS:
            return False

        guard_value = task.get(self.SUCCESS_DELETE_GUARD_FIELD, "")
        if not guard_value:
            return False

        stop_event = getattr(self, "_stop_tasks_event", None)
        if stop_event is not None and stop_event.is_set():
            existing_error = getattr(self, "_fatal_error", None)
            if isinstance(existing_error, FatalIngestorError):
                raise existing_error
            raise FatalIngestorError("CSV 事务已停止，拒绝继续写入")

        try:
            return self._mark_csv_row_for_removal_unchecked(task)
        except FatalIngestorError as exc:
            recorder = getattr(self, "record_fatal_error", None)
            if callable(recorder):
                recorder(exc)
            raise
        except Exception as exc:
            fatal_error = FatalIngestorError(
                f"CSV 删除事务失败: {exc}"
            )
            recorder = getattr(self, "record_fatal_error", None)
            if callable(recorder):
                recorder(fatal_error)
            raise fatal_error from exc

    def _mark_csv_row_for_removal_unchecked(
        self,
        task: Dict[str, str],
    ) -> bool:
        should_commit = False
        rotation_error: Exception | None = None
        with self._csv_lock:
            stop_event = getattr(self, "_stop_tasks_event", None)
            if stop_event is not None and stop_event.is_set():
                existing_error = getattr(self, "_fatal_error", None)
                if isinstance(existing_error, FatalIngestorError):
                    raise existing_error
                raise FatalIngestorError("CSV 事务已停止，拒绝继续写入")
            row_index = task.get(self._CSV_ROW_INDEX_FIELD)
            if not isinstance(row_index, int):
                row_index = self._find_csv_row_index(task)
            if row_index is None or self._is_csv_row_reserved(row_index):
                return False

            self._append_csv_journal_entry(task)
            self._csv_open_journal_indices.add(row_index)
            pending_path = self._get_csv_pending_path()
            if not pending_path.exists() and (
                len(self._csv_open_journal_indices)
                >= self.CSV_SUCCESS_FLUSH_SIZE
            ):
                try:
                    self._rotate_csv_journal_locked()
                except Exception as exc:
                    rotation_error = exc
            should_commit = pending_path.exists()

        if rotation_error is not None and not should_commit:
            raise rotation_error
        if rotation_error is not None:
            self.log(f"ERROR: journal 轮转未完全完成: {rotation_error}")
        if should_commit:
            self._try_commit_csv_pending(blocking=False)
        return True

    def on_task_success(
        self,
        task: Dict[str, str],
        paths: Dict[str, str],
    ) -> None:
        del paths
        try:
            self._mark_csv_row_for_removal(task)
        except FatalIngestorError:
            raise
        except Exception as exc:
            self.log(f"ERROR: 批量更新 CSV 失败: {exc}")

    def on_task_failed(self, task: Dict[str, str], error: str) -> None:
        super().on_task_failed(task, error)
        if not self._should_remove_csv_on_failure(error):
            return

        try:
            if self._mark_csv_row_for_removal(task):
                self.log(
                    "失败（非 pcap/ssl key 过小、内容或截图缺失），"
                    "已从 CSV 标记删除记录: "
                    f"row_id={task.get('row_id', '')}, url={task.get('url', '')}"
                )
        except FatalIngestorError:
            raise
        except Exception as exc:
            self.log(f"ERROR: 删除失败记录的 CSV 更新失败: {exc}")

    def _find_csv_row_index(self, task: Dict[str, str]) -> int | None:
        expected_fields = {
            "id": (task.get("row_id", "") or "").strip(),
            "url": (task.get("url", "") or "").strip(),
            "domain": (task.get("domain", "") or "").strip(),
        }
        for row_index, row in enumerate(self._csv_rows):
            if self._is_csv_row_reserved(row_index):
                continue
            if all(
                self._get_csv_row_value(row, key) == value
                for key, value in expected_fields.items()
            ):
                return row_index
        return None

    def _get_csv_path(self) -> Path:
        return Path(self.CSV_PATH).expanduser().resolve()

    def _get_csv_base_dst(self) -> Path:
        return Path(self.BASE_DST).expanduser().resolve()

    def _get_csv_journal_path(self) -> Path:
        csv_path = self._get_csv_path()
        return self._get_csv_base_dst() / f".{csv_path.name}.journal"

    def _get_csv_pending_path(self) -> Path:
        csv_path = self._get_csv_path()
        return self._get_csv_base_dst() / f".{csv_path.name}.pending"

    def _is_csv_row_reserved(self, row_index: int) -> bool:
        return (
            row_index in self._csv_applied_indices
            or row_index in self._csv_open_journal_indices
            or row_index in self._csv_commit_indices
        )

    @staticmethod
    def _json_line(payload: Mapping[str, Any]) -> str:
        return (
            json.dumps(
                dict(payload),
                ensure_ascii=False,
                separators=(",", ":"),
            )
            + "\n"
        )

    def _repair_csv_state_truncated_tail(self, path: Path) -> bool:
        try:
            with path.open("r+b") as state_file:
                data = state_file.read()
                if not data or data.endswith((b"\n", b"\r")):
                    return False

                last_newline = data.rfind(b"\n")
                tail = data[last_newline + 1:].strip()
                valid_complete_json = False
                if tail:
                    try:
                        payload = json.loads(tail.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        pass
                    else:
                        valid_complete_json = isinstance(payload, dict)

                if valid_complete_json:
                    state_file.seek(0, os.SEEK_END)
                    state_file.write(b"\n")
                else:
                    state_file.seek(last_newline + 1)
                    state_file.truncate()
                state_file.flush()
                os.fsync(state_file.fileno())
                return True
        except FileNotFoundError:
            return False

    def _write_csv_state_file(
        self,
        path: Path,
        metadata: Mapping[str, Any] | None,
        entries: list[Dict[str, str]],
    ) -> None:
        original_stat: os.stat_result | None = None
        try:
            original_stat = path.stat()
        except FileNotFoundError:
            pass

        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_fd, tmp_path = tempfile.mkstemp(
            dir=path.parent,
            suffix=".tmp",
            prefix=f".{path.name}.",
        )
        try:
            with os.fdopen(
                tmp_fd,
                "w",
                encoding="utf-8",
                newline="\n",
            ) as state_file:
                if metadata is not None:
                    state_file.write(self._json_line(metadata))
                for entry in entries:
                    state_file.write(self._json_line(entry))
                state_file.flush()
                os.fsync(state_file.fileno())

            mode = (
                stat.S_IMODE(original_stat.st_mode)
                if original_stat is not None
                else 0o664
            )
            os.chmod(tmp_path, mode)
            if original_stat is not None and hasattr(os, "chown"):
                os.chown(
                    tmp_path,
                    original_stat.st_uid,
                    original_stat.st_gid,
                )
            os.replace(tmp_path, path)
            self._fsync_directory(path.parent)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def _initialize_csv_journal(self) -> None:
        self._write_csv_state_file(
            self._get_csv_journal_path(),
            self._build_journal_metadata(),
            [],
        )

    def _build_journal_metadata(self) -> Dict[str, Any]:
        return {
            "version": self.CSV_JOURNAL_VERSION,
            "kind": self.CSV_JOURNAL_KIND,
            "csv_path": str(self._get_csv_path()),
        }

    def _build_pending_metadata(
        self,
        *,
        batch_id: str,
        source_sha256: str,
        target_sha256: str,
        entry_count: int,
        source_rows: int,
        target_rows: int,
    ) -> Dict[str, Any]:
        return {
            "version": self.CSV_JOURNAL_VERSION,
            "kind": self.CSV_PENDING_KIND,
            "batch_id": batch_id,
            "csv_path": str(self._get_csv_path()),
            "source_sha256": source_sha256,
            "target_sha256": target_sha256,
            "entry_count": entry_count,
            "source_rows": source_rows,
            "target_rows": target_rows,
        }

    def _validate_pending_counts(
        self,
        metadata: Mapping[str, Any] | None,
        *,
        entry_count: int,
        source_rows: int,
        target_rows: int,
    ) -> None:
        actual_counts = {
            "entry_count": entry_count,
            "source_rows": source_rows,
            "target_rows": target_rows,
        }
        for field_name, actual_value in actual_counts.items():
            raw_expected = (metadata or {}).get(field_name)
            if raw_expected is None:
                continue
            expected_value = int(raw_expected)
            if expected_value != actual_value:
                raise RuntimeError(
                    "pending 行数校验失败: "
                    f"{field_name} expected={expected_value}, "
                    f"actual={actual_value}"
                )

    def _append_csv_journal_entry(self, task: Dict[str, str]) -> None:
        journal_path = self._get_csv_journal_path()
        if self._repair_csv_state_truncated_tail(journal_path):
            self.log(f"WARNING: 已修复 journal 截断尾部: {journal_path}")

        journal_entry = {
            "id": (task.get("row_id", "") or "").strip(),
            "url": (task.get("url", "") or "").strip(),
            "domain": (task.get("domain", "") or "").strip(),
        }
        journal_path.parent.mkdir(parents=True, exist_ok=True)
        with journal_path.open(
            "a+",
            encoding="utf-8",
            newline="\n",
        ) as journal_file:
            journal_file.seek(0, os.SEEK_END)
            journal_created = journal_file.tell() == 0
            if journal_created:
                journal_file.write(self._json_line(self._build_journal_metadata()))
            journal_file.write(self._json_line(journal_entry))
            journal_file.flush()
            os.fsync(journal_file.fileno())
        if journal_created:
            os.chmod(journal_path, 0o664)
            self._fsync_directory(journal_path.parent)

    def _read_csv_state_file(
        self,
        path: Path,
    ) -> tuple[Dict[str, Any] | None, list[Dict[str, str]]]:
        if not path.exists():
            return None, []

        if self._repair_csv_state_truncated_tail(path):
            self.log(f"WARNING: 已修复状态文件截断尾部: {path}")

        with path.open("r", encoding="utf-8") as state_file:
            lines = state_file.readlines()

        entries: list[Dict[str, str]] = []
        metadata: Dict[str, Any] | None = None
        for line_index, raw_line in enumerate(lines):
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"状态文件第 {line_index + 1} 行不是有效 JSON: {path}"
                ) from exc
            if not isinstance(payload, dict):
                raise ValueError(
                    f"状态文件第 {line_index + 1} 行不是 JSON 对象: {path}"
                )

            if payload.get("kind") in self.CSV_STATE_KINDS:
                if metadata is not None or entries:
                    raise ValueError(
                        f"状态文件元数据位置无效: 第 {line_index + 1} 行"
                    )
                version = payload.get("version")
                if version is not None and int(version) > self.CSV_JOURNAL_VERSION:
                    raise ValueError(
                        f"状态文件版本过高: version={version}, path={path}"
                    )
                metadata = payload
                continue

            entry = {
                "id": str(payload.get("id", "") or "").strip(),
                "url": str(payload.get("url", "") or "").strip(),
                "domain": str(payload.get("domain", "") or "").strip(),
            }
            if not entry["url"]:
                raise ValueError(
                    f"状态文件第 {line_index + 1} 行缺少 url: {path}"
                )
            entries.append(entry)
        return metadata, entries

    def _read_csv_journal(
        self,
    ) -> tuple[Dict[str, Any] | None, list[Dict[str, str]]]:
        return self._read_csv_state_file(self._get_csv_journal_path())

    def _read_csv_pending(
        self,
    ) -> tuple[Dict[str, Any] | None, list[Dict[str, str]]]:
        return self._read_csv_state_file(self._get_csv_pending_path())

    def _validate_state_csv_path(
        self,
        metadata: Mapping[str, Any] | None,
        path: Path,
    ) -> None:
        raw_csv_path = str((metadata or {}).get("csv_path", "") or "").strip()
        if not raw_csv_path:
            return
        state_csv_path = Path(raw_csv_path).expanduser().resolve()
        expected_csv_path = self._get_csv_path()
        if state_csv_path != expected_csv_path:
            raise RuntimeError(
                "状态文件属于其他 CSV，拒绝恢复: "
                f"state={state_csv_path}, expected={expected_csv_path}, path={path}"
            )

    def _clear_csv_state_file(self, path: Path) -> bool:
        try:
            path.unlink()
        except FileNotFoundError:
            return False
        self._fsync_directory(path.parent)
        return True

    def _clear_csv_journal(self) -> bool:
        return self._clear_csv_state_file(self._get_csv_journal_path())

    def _clear_csv_pending_after_commit(self, batch_id: str) -> bool:
        try:
            removed = self._clear_csv_state_file(self._get_csv_pending_path())
        except OSError as exc:
            self.log(
                f"WARNING: CSV 批次 {batch_id} 已提交，但 pending 删除失败，"
                f"将在后续恢复时仅做收尾: {exc}"
            )
            return False
        if removed:
            self.log(f"CSV 批次 {batch_id} 的 pending 已清理")
        return removed

    @staticmethod
    def _sha256_file(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as source_file:
            while True:
                chunk = source_file.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        if os.name == "nt":
            return
        directory_fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def _build_csv_row_index(
        self,
        wanted_keys: set[tuple[str, str, str]],
    ) -> dict[tuple[str, str, str], deque[int]]:
        row_index_by_key: dict[tuple[str, str, str], deque[int]] = {}
        for row_index, row in enumerate(self._csv_rows):
            if self._is_csv_row_reserved(row_index):
                continue
            key = (
                self._get_csv_row_value(row, "id"),
                self._get_csv_row_value(row, "url"),
                self._get_csv_row_value(row, "domain"),
            )
            if key not in wanted_keys:
                continue
            row_index_by_key.setdefault(key, deque()).append(row_index)
        return row_index_by_key

    def _remove_rows_for_entries(
        self,
        rows: list[Dict[str, str]],
        entries: list[Dict[str, str]],
    ) -> tuple[list[Dict[str, str]], int]:
        wanted_keys = {
            (entry["id"], entry["url"], entry["domain"])
            for entry in entries
        }
        row_indices_by_key: dict[tuple[str, str, str], deque[int]] = {}
        for row_index, row in enumerate(rows):
            key = (
                self._get_csv_row_value(row, "id"),
                self._get_csv_row_value(row, "url"),
                self._get_csv_row_value(row, "domain"),
            )
            if key in wanted_keys:
                row_indices_by_key.setdefault(key, deque()).append(row_index)

        removed_indices: set[int] = set()
        for entry in entries:
            key = (entry["id"], entry["url"], entry["domain"])
            candidates = row_indices_by_key.get(key)
            if not candidates:
                continue
            removed_indices.add(candidates.popleft())

        remaining_rows = [
            row
            for row_index, row in enumerate(rows)
            if row_index not in removed_indices
        ]
        return remaining_rows, len(removed_indices)

    def _write_csv_temp(
        self,
        csv_path: Path,
        header_fields: list[str],
        rows: list[Dict[str, str]],
    ) -> tuple[Path, os.stat_result]:
        original_stat = csv_path.stat()
        tmp_fd, tmp_name = tempfile.mkstemp(
            dir=csv_path.parent,
            suffix=".tmp",
            prefix=f".{csv_path.name}.commit.",
        )
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(
                tmp_fd,
                "w",
                encoding="utf-8-sig",
                newline="",
            ) as csv_file:
                writer = csv.DictWriter(
                    csv_file,
                    fieldnames=header_fields,
                    extrasaction="ignore",
                )
                writer.writeheader()
                writer.writerows(rows)
                csv_file.flush()
                os.fsync(csv_file.fileno())
        except Exception:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass
            raise
        return tmp_path, original_stat

    @staticmethod
    def _validate_csv_temp(
        temp_path: Path,
        expected_header: list[str],
        expected_rows: int,
    ) -> None:
        with temp_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.reader(csv_file)
            try:
                header_fields = next(reader)
            except StopIteration as exc:
                raise RuntimeError(f"临时 CSV 为空: {temp_path}") from exc
            if header_fields != expected_header:
                raise RuntimeError(
                    "临时 CSV 表头不一致: "
                    f"actual={header_fields}, expected={expected_header}"
                )
            actual_rows = sum(1 for row in reader if row)
        if actual_rows != expected_rows:
            raise RuntimeError(
                "临时 CSV 行数不一致: "
                f"actual={actual_rows}, expected={expected_rows}"
            )

    @staticmethod
    def _prepare_csv_temp_permissions(
        temp_path: Path,
        original_stat: os.stat_result,
    ) -> None:
        os.chmod(temp_path, stat.S_IMODE(original_stat.st_mode))
        if hasattr(os, "chown"):
            os.chown(temp_path, original_stat.st_uid, original_stat.st_gid)

    def _recover_csv_pending_before_load(self, csv_path: Path) -> None:
        pending_path = self._get_csv_pending_path()
        metadata, entries = self._read_csv_pending()
        if metadata is None and not entries:
            return
        self._validate_state_csv_path(metadata, pending_path)

        batch_id = str((metadata or {}).get("batch_id", "") or "").strip()
        if not batch_id:
            batch_id = f"{time.time_ns()}_{os.getpid()}"

        if not entries:
            self._clear_csv_state_file(pending_path)
            self.log(f"pending 无记录，已清理: {pending_path}")
            return
        if (
            not self.DELETE_CSV_FROM_JOURNAL_ON_START
            or not self.DELETE_CSV_RECORD_ON_SUCCESS
        ):
            self._clear_csv_state_file(pending_path)
            self.log(f"已按配置跳过 pending 恢复并清理: {pending_path}")
            return

        current_sha256 = self._sha256_file(csv_path)
        target_sha256 = str(
            (metadata or {}).get("target_sha256", "") or ""
        ).strip()
        if target_sha256 and current_sha256 == target_sha256:
            self._clear_csv_pending_after_commit(batch_id)
            self.log(
                f"pending 批次 {batch_id} 已在 CSV 中生效，仅清理 pending"
            )
            return

        source_sha256 = str(
            (metadata or {}).get("source_sha256", "") or ""
        ).strip()
        if source_sha256 and current_sha256 != source_sha256:
            raise RuntimeError(
                "pending 恢复前 CSV 已外部变化: "
                f"batch={batch_id}, current={current_sha256}, "
                f"source={source_sha256}, target={target_sha256 or '<empty>'}"
            )

        header_fields, rows = self._read_csv_records(csv_path)
        if not header_fields:
            raise RuntimeError(f"pending 恢复时 CSV 没有表头: {csv_path}")
        remaining_rows, matched_count = self._remove_rows_for_entries(rows, entries)
        if matched_count != len(entries):
            raise RuntimeError(
                "pending 记录无法与 CSV 一一匹配: "
                f"batch={batch_id}, matched={matched_count}, "
                f"entries={len(entries)}"
            )
        self._validate_pending_counts(
            metadata,
            entry_count=len(entries),
            source_rows=len(rows),
            target_rows=len(remaining_rows),
        )

        temp_path, original_stat = self._write_csv_temp(
            csv_path,
            header_fields,
            remaining_rows,
        )
        try:
            self._validate_csv_temp(temp_path, header_fields, len(remaining_rows))
            new_target_sha256 = self._sha256_file(temp_path)
            self._write_csv_state_file(
                pending_path,
                self._build_pending_metadata(
                    batch_id=batch_id,
                    source_sha256=current_sha256,
                    target_sha256=new_target_sha256,
                    entry_count=len(entries),
                    source_rows=len(rows),
                    target_rows=len(remaining_rows),
                ),
                entries,
            )
            if self._sha256_file(csv_path) != current_sha256:
                raise RuntimeError(
                    f"CSV 在 pending 写入期间被外部修改: {csv_path}"
                )
            self._prepare_csv_temp_permissions(temp_path, original_stat)
            os.replace(temp_path, csv_path)
            self._fsync_directory(csv_path.parent)
        except Exception:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass
            raise

        self._clear_csv_pending_after_commit(batch_id)
        self.log(
            f"启动前恢复 pending 批次 {batch_id}: "
            f"移除 {len(entries)} 条，剩余 {len(remaining_rows)} 条"
        )

    def _recover_csv_journal_after_load(self) -> None:
        journal_path = self._get_csv_journal_path()
        metadata, entries = self._read_csv_journal()
        if metadata is None and not entries:
            return
        self._validate_state_csv_path(metadata, journal_path)

        if not entries:
            return
        if (
            not self.DELETE_CSV_FROM_JOURNAL_ON_START
            or not self.DELETE_CSV_RECORD_ON_SUCCESS
        ):
            self._clear_csv_journal()
            self.log("已按配置跳过 journal 恢复并清理 journal")
            return

        wanted_keys = {
            (entry["id"], entry["url"], entry["domain"])
            for entry in entries
        }
        row_index_by_key = self._build_csv_row_index(wanted_keys)
        matched_entries: list[Dict[str, str]] = []
        matched_indices: set[int] = set()
        with self._csv_lock:
            for entry in entries:
                key = (entry["id"], entry["url"], entry["domain"])
                candidates = row_index_by_key.get(key)
                if not candidates:
                    continue
                row_index = candidates.popleft()
                matched_entries.append(entry)
                matched_indices.add(row_index)
            self._csv_open_journal_indices = matched_indices

        if not matched_entries:
            self._clear_csv_journal()
            self.log(
                f"WARNING: journal 中没有可匹配记录，已清理: {journal_path}"
            )
            return
        if len(matched_entries) != len(entries):
            self.log(
                f"WARNING: journal 有 {len(entries) - len(matched_entries)} 条"
                "记录无法匹配 CSV，已从 journal 丢弃"
            )
            self._write_csv_state_file(
                journal_path,
                self._build_journal_metadata(),
                matched_entries,
            )

        self.log(
            f"journal 恢复完成：读取 {len(entries)} 条，"
            f"匹配 CSV {len(matched_entries)} 条"
        )
        if (
            len(self._csv_open_journal_indices)
            >= self.CSV_SUCCESS_FLUSH_SIZE
        ):
            with self._csv_lock:
                self._rotate_csv_journal_locked()
            self._try_commit_csv_pending(blocking=True)

    def _rotate_csv_journal_locked(self) -> bool:
        pending_path = self._get_csv_pending_path()
        if pending_path.exists():
            return False
        if (
            len(self._csv_open_journal_indices)
            < self.CSV_SUCCESS_FLUSH_SIZE
        ):
            return False

        journal_path = self._get_csv_journal_path()
        metadata, entries = self._read_csv_journal()
        self._validate_state_csv_path(metadata, journal_path)
        if len(entries) != len(self._csv_open_journal_indices):
            raise RuntimeError(
                "journal 记录数与内存 open 集合不一致: "
                f"journal={len(entries)}, "
                f"open={len(self._csv_open_journal_indices)}"
            )

        os.replace(journal_path, pending_path)
        self._csv_commit_indices.update(self._csv_open_journal_indices)
        self._csv_open_journal_indices.clear()
        self._fsync_directory(pending_path.parent)

        try:
            self._initialize_csv_journal()
        except Exception as exc:
            self.log(
                f"ERROR: pending 已轮转，但新 journal 初始化失败，"
                f"后续追加时将重试: {exc}"
            )
        return True

    def _try_commit_csv_pending(self, *, blocking: bool) -> bool:
        if not self._csv_commit_lock.acquire(blocking=blocking):
            return False
        try:
            return self._commit_csv_pending_runtime()
        finally:
            self._csv_commit_lock.release()

    def _commit_csv_pending_runtime(self) -> bool:
        pending_path = self._get_csv_pending_path()
        if not pending_path.exists():
            return False

        metadata, entries = self._read_csv_pending()
        self._validate_state_csv_path(metadata, pending_path)
        if not entries:
            self._clear_csv_state_file(pending_path)
            return True

        batch_id = str((metadata or {}).get("batch_id", "") or "").strip()
        if not batch_id:
            batch_id = f"{time.time_ns()}_{os.getpid()}"

        csv_path = self._get_csv_path()
        if not csv_path.is_file():
            raise FileNotFoundError(f"CSV 不存在，无法提交 pending: {csv_path}")
        if not self._csv_loaded:
            raise RuntimeError("运行中提交 pending 前必须先加载 CSV")
        if not self._csv_loaded_sha256:
            raise RuntimeError("缺少加载后的 CSV 哈希，拒绝提交 pending")

        current_sha256 = self._sha256_file(csv_path)
        target_sha256 = str(
            (metadata or {}).get("target_sha256", "") or ""
        ).strip()
        if target_sha256 and current_sha256 == target_sha256:
            self._finalize_runtime_pending(batch_id, target_sha256)
            return True
        if current_sha256 != self._csv_loaded_sha256:
            raise RuntimeError(
                "CSV 在运行期间被外部修改，拒绝提交 pending: "
                f"current={current_sha256}, loaded={self._csv_loaded_sha256}"
            )

        source_sha256 = str(
            (metadata or {}).get("source_sha256", "") or ""
        ).strip()
        if source_sha256 and source_sha256 != self._csv_loaded_sha256:
            raise RuntimeError(
                "pending 源哈希与当前内存 CSV 哈希不一致: "
                f"pending={source_sha256}, loaded={self._csv_loaded_sha256}"
            )

        with self._csv_lock:
            if len(self._csv_commit_indices) != len(entries):
                raise RuntimeError(
                    "pending 记录数与内存 commit 集合不一致: "
                    f"pending={len(entries)}, "
                    f"commit={len(self._csv_commit_indices)}"
                )
            applied_count = len(self._csv_applied_indices)
            remaining_rows = [
                row
                for row_index, row in enumerate(self._csv_rows)
                if row_index not in self._csv_applied_indices
                and row_index not in self._csv_commit_indices
            ]

        source_rows = len(self._csv_rows) - applied_count
        target_rows = len(remaining_rows)
        if source_rows - target_rows != len(entries):
            raise RuntimeError(
                "pending 待删除数量不一致: "
                f"source={source_rows}, target={target_rows}, "
                f"entries={len(entries)}"
            )
        self._validate_pending_counts(
            metadata,
            entry_count=len(entries),
            source_rows=source_rows,
            target_rows=target_rows,
        )

        temp_path, original_stat = self._write_csv_temp(
            csv_path,
            self._csv_header_fields,
            remaining_rows,
        )
        replaced = False
        try:
            self._validate_csv_temp(
                temp_path,
                self._csv_header_fields,
                target_rows,
            )
            new_target_sha256 = self._sha256_file(temp_path)
            self._write_csv_state_file(
                pending_path,
                self._build_pending_metadata(
                    batch_id=batch_id,
                    source_sha256=self._csv_loaded_sha256,
                    target_sha256=new_target_sha256,
                    entry_count=len(entries),
                    source_rows=source_rows,
                    target_rows=target_rows,
                ),
                entries,
            )
            if self._sha256_file(csv_path) != self._csv_loaded_sha256:
                raise RuntimeError(
                    f"CSV 在 pending 写入期间被外部修改: {csv_path}"
                )
            self._prepare_csv_temp_permissions(temp_path, original_stat)
            os.replace(temp_path, csv_path)
            replaced = True
            self._fsync_directory(csv_path.parent)
        except Exception:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass
            if replaced:
                self._finalize_runtime_pending(batch_id, new_target_sha256)
            raise

        self._finalize_runtime_pending(batch_id, new_target_sha256)
        self.log(
            f"已提交 CSV 批次 {batch_id}: 移除 {len(entries)} 条，"
            f"剩余 {target_rows} 条"
            f"{self.build_success_csv_remove_log_suffix()}"
        )
        return True

    def _finalize_runtime_pending(
        self,
        batch_id: str,
        target_sha256: str,
    ) -> None:
        with self._csv_lock:
            self._csv_applied_indices.update(self._csv_commit_indices)
            self._csv_commit_indices.clear()
            self._csv_loaded_sha256 = target_sha256
        self._clear_csv_pending_after_commit(batch_id)

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
