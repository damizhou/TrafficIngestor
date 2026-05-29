#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
功能:
- 从 CSV 中识别第一轮不重复记录，并按目标轮数重新扩充输出。
- 默认处理 small_tools/result/test.csv，把第一轮记录扩充为 10 轮。
- 保留首行表头，保持原始记录顺序，默认原地覆盖并生成 .bak 备份。

使用方式:
- 修改脚本顶部的全局变量配置。
- 运行: python small_tools/code/adjust_csv_repeat_count.py

第一轮定义:
- 表头之后，从第一条数据开始，到首次遇到重复记录之前的连续唯一记录。
- 如果输入文件只有一轮、没有任何重复记录，则整份数据就是第一轮。
"""

from __future__ import annotations

import csv
import os
import shutil
import stat
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple, Union


SCRIPT_DIR = Path(__file__).resolve().parent
SMALL_TOOLS_DIR = SCRIPT_DIR.parent

# ===== 全局配置 =====
# 要调整重复次数的 CSV 路径。
CSV_PATH = SMALL_TOOLS_DIR / "result/test.csv"

# 输出 CSV 路径。设为 None 表示原地覆盖 CSV_PATH。
OUTPUT_CSV: Optional[Path] = SMALL_TOOLS_DIR / "result/test1.csv"

# 第一轮不重复记录要扩充输出的轮数。
# 例如原文件只有 1 轮唯一记录，设置为 10 后会输出 10 轮。
# 如果原文件已有 40 轮，设置为 10 后会用第一轮记录重建为 10 轮。
TARGET_REPEAT_COUNT = 1

# True 表示第一行是表头，表头会固定保留一次，不参与重复次数统计。
HAS_HEADER = True

# 如果后续行再次出现与表头完全相同的行，是否直接跳过。
SKIP_REPEATED_HEADER_ROWS = True

# True 时只统计，不写文件。
DRY_RUN = False

# 原地覆盖时是否先生成备份。
MAKE_BACKUP = True
BACKUP_SUFFIX = ".bak"


@dataclass
class RepeatAdjustResult:
    input_path: Path
    output_path: Path
    backup_path: Optional[Path]
    physical_rows: int
    header_rows_written: int
    data_rows: int
    first_round_rows: int
    output_data_rows: int
    ignored_input_rows: int
    repeated_header_rows: int
    distinct_records: int
    records_above_target: int
    max_seen_repeat_count: int
    target_repeat_count: int
    dry_run: bool
    changed: bool


def resolve_path(raw_path: Union[str, Path]) -> Path:
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    return path


def make_record_key(row: Sequence[str]) -> Tuple[str, ...]:
    return tuple(row)


def next_backup_path(input_path: Path, backup_suffix: str) -> Path:
    backup_path = Path(str(input_path) + backup_suffix)
    if not backup_path.exists():
        return backup_path

    for index in range(1, 1000):
        candidate = Path(f"{input_path}{backup_suffix}.{index}")
        if not candidate.exists():
            return candidate

    raise RuntimeError(f"too many existing backup files for: {input_path}")


def preserve_mode(tmp_path: Path, source_stat: Optional[os.stat_result]) -> None:
    if source_stat is None:
        return
    try:
        os.chmod(tmp_path, stat.S_IMODE(source_stat.st_mode))
    except OSError as exc:
        print(f"[WARN] failed to preserve file mode on {tmp_path}: {exc}", file=sys.stderr)


def adjust_csv_repeat_count(
    input_path: Path,
    output_path: Path,
    target_repeat_count: int,
    has_header: bool,
    skip_repeated_header_rows: bool,
    dry_run: bool,
    make_backup: bool,
    backup_suffix: str,
) -> RepeatAdjustResult:
    if not input_path.exists() or not input_path.is_file():
        raise FileNotFoundError(f"input file not found: {input_path}")
    if target_repeat_count < 1:
        raise ValueError("TARGET_REPEAT_COUNT must be >= 1")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    same_file = input_path.resolve() == output_path.resolve()
    source_stat = input_path.stat()
    backup_path: Optional[Path] = None
    tmp_path: Optional[Path] = None
    seen_counts: Dict[Tuple[str, ...], int] = {}
    first_round_rows: List[List[str]] = []
    first_round_keys: Set[Tuple[str, ...]] = set()
    physical_rows = 0
    header_rows_written = 0
    data_rows = 0
    repeated_header_rows = 0
    input_matches_first_round_pattern = True
    first_round_closed = False
    pattern_index = 0
    header: Optional[Sequence[str]] = None

    writer_file = None
    try:
        with input_path.open("r", encoding="utf-8-sig", newline="") as src:
            reader = csv.reader(src)

            if has_header:
                try:
                    header = next(reader)
                except StopIteration:
                    header = None
                else:
                    physical_rows += 1
                    header_rows_written = 1

            for row in reader:
                physical_rows += 1
                if (
                    skip_repeated_header_rows
                    and header is not None
                    and make_record_key(row) == make_record_key(header)
                ):
                    repeated_header_rows += 1
                    continue

                data_rows += 1
                key = make_record_key(row)
                seen_count = seen_counts.get(key, 0) + 1
                seen_counts[key] = seen_count

                if not first_round_closed and key not in first_round_keys:
                    first_round_keys.add(key)
                    first_round_rows.append(list(row))
                    continue

                if not first_round_closed:
                    first_round_closed = True
                    pattern_index = 0

                expected_row = first_round_rows[pattern_index] if first_round_rows else None
                if expected_row != row:
                    input_matches_first_round_pattern = False
                pattern_index = (pattern_index + 1) % len(first_round_rows)

        output_data_rows = len(first_round_rows) * target_repeat_count
        ignored_input_rows = max(data_rows - len(first_round_rows), 0)
        records_above_target = sum(1 for count in seen_counts.values() if count > target_repeat_count)
        max_seen_repeat_count = max(seen_counts.values(), default=0)
        input_matches_target = (
            repeated_header_rows == 0
            and input_matches_first_round_pattern
            and data_rows == output_data_rows
        )
        changed = not input_matches_target or not same_file

        if dry_run:
            return RepeatAdjustResult(
                input_path=input_path,
                output_path=output_path,
                backup_path=None,
                physical_rows=physical_rows,
                header_rows_written=header_rows_written,
                data_rows=data_rows,
                first_round_rows=len(first_round_rows),
                output_data_rows=output_data_rows,
                ignored_input_rows=ignored_input_rows,
                repeated_header_rows=repeated_header_rows,
                distinct_records=len(seen_counts),
                records_above_target=records_above_target,
                max_seen_repeat_count=max_seen_repeat_count,
                target_repeat_count=target_repeat_count,
                dry_run=True,
                changed=changed,
            )

        if not changed:
            if tmp_path is not None and tmp_path.exists():
                tmp_path.unlink()
            return RepeatAdjustResult(
                input_path=input_path,
                output_path=output_path,
                backup_path=None,
                physical_rows=physical_rows,
                header_rows_written=header_rows_written,
                data_rows=data_rows,
                first_round_rows=len(first_round_rows),
                output_data_rows=output_data_rows,
                ignored_input_rows=ignored_input_rows,
                repeated_header_rows=repeated_header_rows,
                distinct_records=len(seen_counts),
                records_above_target=records_above_target,
                max_seen_repeat_count=max_seen_repeat_count,
                target_repeat_count=target_repeat_count,
                dry_run=False,
                changed=False,
            )

        writer_file = tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8-sig",
            newline="",
            dir=str(output_path.parent),
            prefix=f".{output_path.name}.",
            suffix=".tmp",
            delete=False,
        )
        tmp_path = Path(writer_file.name)
        writer = csv.writer(writer_file)
        if header is not None:
            writer.writerow(header)
        for _ in range(target_repeat_count):
            writer.writerows(first_round_rows)
        writer_file.close()
        writer_file = None

        preserve_mode(tmp_path, source_stat if same_file else None)

        if same_file and make_backup:
            backup_path = next_backup_path(input_path, backup_suffix)
            shutil.copy2(input_path, backup_path)

        os.replace(tmp_path, output_path)
        tmp_path = None

        return RepeatAdjustResult(
            input_path=input_path,
            output_path=output_path,
            backup_path=backup_path,
            physical_rows=physical_rows,
            header_rows_written=header_rows_written,
            data_rows=data_rows,
            first_round_rows=len(first_round_rows),
            output_data_rows=output_data_rows,
            ignored_input_rows=ignored_input_rows,
            repeated_header_rows=repeated_header_rows,
            distinct_records=len(seen_counts),
            records_above_target=records_above_target,
            max_seen_repeat_count=max_seen_repeat_count,
            target_repeat_count=target_repeat_count,
            dry_run=False,
            changed=True,
        )
    finally:
        if writer_file is not None:
            writer_file.close()
        if tmp_path is not None and tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError as exc:
                print(f"[WARN] failed to remove temp file {tmp_path}: {exc}", file=sys.stderr)


def main() -> int:
    input_path = resolve_path(CSV_PATH)
    output_path = resolve_path(OUTPUT_CSV) if OUTPUT_CSV is not None else input_path

    try:
        result = adjust_csv_repeat_count(
            input_path=input_path,
            output_path=output_path,
            target_repeat_count=TARGET_REPEAT_COUNT,
            has_header=HAS_HEADER,
            skip_repeated_header_rows=SKIP_REPEATED_HEADER_ROWS,
            dry_run=DRY_RUN,
            make_backup=MAKE_BACKUP,
            backup_suffix=BACKUP_SUFFIX,
        )
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1

    print(
        f"[SUMMARY] input={result.input_path} output={result.output_path} "
        f"physical_rows={result.physical_rows} header_rows_written={result.header_rows_written} "
        f"data_rows={result.data_rows} first_round_rows={result.first_round_rows} "
        f"output_data_rows={result.output_data_rows} ignored_input_rows={result.ignored_input_rows} "
        f"repeated_header_rows={result.repeated_header_rows} "
        f"distinct_records={result.distinct_records} records_above_target={result.records_above_target} "
        f"max_seen_repeat_count={result.max_seen_repeat_count} "
        f"target_repeat_count={result.target_repeat_count} dry_run={result.dry_run}"
    )
    if result.backup_path is not None:
        print(f"[BACKUP] {result.backup_path}")
    if not result.changed:
        print("[DONE] input already matches the configured first-round repeat count; file unchanged")
    elif result.dry_run:
        print("[DONE] dry run only; file unchanged")
    else:
        print("[DONE] CSV rebuilt from first-round records")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
