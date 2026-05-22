#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
功能:
- 对 homeonly_merged.csv 按 CSV 解析后的整行内容清理重复记录。
- 按 MAX_OCCURRENCES_PER_RECORD 控制每条相同记录最多保留几次。
- 保持原始顺序，优先保留先出现的记录。
- 默认原地覆盖，并在同目录生成 .bak 备份。

使用方式:
- 修改脚本顶部的全局变量配置。
- 运行: python small_tools/code/dedupe_homeonly_merged_csv.py
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
from typing import Dict, Optional, Sequence, Tuple, Union


SCRIPT_DIR = Path(__file__).resolve().parent
SMALL_TOOLS_DIR = SCRIPT_DIR.parent

# ===== 全局配置 =====
# 输入 CSV 路径。
INPUT_CSV = SMALL_TOOLS_DIR / "result/homeonly_merged_10.csv"

# 输出 CSV 路径。设为 None 表示原地覆盖 INPUT_CSV。
OUTPUT_CSV: Optional[Path] = None

# 每条完全相同的 CSV 记录最多保留几次。
# 1 表示去重到不重复；2 表示每条相同记录最多保留 2 次。
MAX_OCCURRENCES_PER_RECORD = 10

# True 时只统计，不写文件。
DRY_RUN = False

# 原地覆盖时是否先生成备份。
MAKE_BACKUP = True
BACKUP_SUFFIX = ".bak"


@dataclass
class DedupeResult:
    input_path: Path
    output_path: Path
    backup_path: Optional[Path]
    total_rows: int
    kept_rows: int
    removed_rows: int
    distinct_records: int
    max_occurrences_per_record: int
    dry_run: bool
    changed: bool


def resolve_path(raw_path: Union[str, Path]) -> Path:
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    return path


def make_row_key(row: Sequence[str]) -> Tuple[str, ...]:
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


def dedupe_csv(
    input_path: Path,
    output_path: Path,
    max_occurrences_per_record: int,
    dry_run: bool,
    make_backup: bool,
    backup_suffix: str,
) -> DedupeResult:
    if not input_path.exists() or not input_path.is_file():
        raise FileNotFoundError(f"input file not found: {input_path}")
    if max_occurrences_per_record < 1:
        raise ValueError("MAX_OCCURRENCES_PER_RECORD must be >= 1")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    same_file = input_path.resolve() == output_path.resolve()
    source_stat = input_path.stat()
    backup_path: Optional[Path] = None
    tmp_path: Optional[Path] = None
    occurrence_counts: Dict[Tuple[str, ...], int] = {}
    total_rows = 0
    kept_rows = 0
    removed_rows = 0

    writer_file = None
    writer = None
    try:
        if not dry_run:
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

        with input_path.open("r", encoding="utf-8-sig", newline="") as src:
            reader = csv.reader(src)
            for row in reader:
                total_rows += 1
                key = make_row_key(row)
                current_count = occurrence_counts.get(key, 0)
                if current_count >= max_occurrences_per_record:
                    removed_rows += 1
                    continue

                occurrence_counts[key] = current_count + 1
                kept_rows += 1
                if writer is not None:
                    writer.writerow(row)

        if writer_file is not None:
            writer_file.close()
            writer_file = None

        changed = removed_rows > 0 or not same_file
        if dry_run:
            return DedupeResult(
                input_path=input_path,
                output_path=output_path,
                backup_path=None,
                total_rows=total_rows,
                kept_rows=kept_rows,
                removed_rows=removed_rows,
                distinct_records=len(occurrence_counts),
                max_occurrences_per_record=max_occurrences_per_record,
                dry_run=True,
                changed=changed,
            )

        if not changed:
            if tmp_path is not None and tmp_path.exists():
                tmp_path.unlink()
            return DedupeResult(
                input_path=input_path,
                output_path=output_path,
                backup_path=None,
                total_rows=total_rows,
                kept_rows=kept_rows,
                removed_rows=removed_rows,
                distinct_records=len(occurrence_counts),
                max_occurrences_per_record=max_occurrences_per_record,
                dry_run=False,
                changed=False,
            )

        assert tmp_path is not None
        preserve_mode(tmp_path, source_stat if same_file else None)

        if same_file and make_backup:
            backup_path = next_backup_path(input_path, backup_suffix)
            shutil.copy2(input_path, backup_path)

        os.replace(tmp_path, output_path)
        tmp_path = None

        return DedupeResult(
            input_path=input_path,
            output_path=output_path,
            backup_path=backup_path,
            total_rows=total_rows,
            kept_rows=kept_rows,
            removed_rows=removed_rows,
            distinct_records=len(occurrence_counts),
            max_occurrences_per_record=max_occurrences_per_record,
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
    input_path = resolve_path(INPUT_CSV)
    output_path = resolve_path(OUTPUT_CSV) if OUTPUT_CSV is not None else input_path

    try:
        result = dedupe_csv(
            input_path=input_path,
            output_path=output_path,
            max_occurrences_per_record=MAX_OCCURRENCES_PER_RECORD,
            dry_run=DRY_RUN,
            make_backup=MAKE_BACKUP,
            backup_suffix=BACKUP_SUFFIX,
        )
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1

    print(
        f"[SUMMARY] input={result.input_path} output={result.output_path} "
        f"total_rows={result.total_rows} kept_rows={result.kept_rows} "
        f"removed_rows={result.removed_rows} distinct_records={result.distinct_records} "
        f"max_occurrences_per_record={result.max_occurrences_per_record} "
        f"dry_run={result.dry_run}"
    )
    if result.backup_path is not None:
        print(f"[BACKUP] {result.backup_path}")
    if not result.changed:
        print("[DONE] no rows exceeded the configured occurrence limit; file unchanged")
    elif result.dry_run:
        print("[DONE] dry run only; file unchanged")
    elif result.removed_rows == 0:
        print("[DONE] output written; no rows exceeded the configured occurrence limit")
    else:
        print("[DONE] repeated rows trimmed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
