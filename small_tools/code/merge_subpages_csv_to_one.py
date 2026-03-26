#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
功能:
- 递归遍历 subpages 目录下所有 CSV 文件并合并到一个新 CSV。
- 原始行直接拼接写入，id 保持原文件中的值不变。

使用方式:
- 默认运行: python small_tools/merge_subpages_csv_to_one.py
- 指定路径: python small_tools/merge_subpages_csv_to_one.py --root-dir /netdisk/ww/top2000/subpages --output subpages_merged.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import List, Tuple


DEFAULT_ROOT_DIR = "/netdisk/ww/top2000/subpages"
DEFAULT_OUTPUT_NAME = "subpages_merged.csv"


def collect_csv_files(root_dir: Path, output_csv: Path) -> List[Path]:
    files: List[Path] = []
    output_resolved = output_csv.resolve()
    for path in root_dir.rglob("*.csv"):
        if not path.is_file():
            continue
        if path.resolve() == output_resolved:
            continue
        files.append(path)
    files.sort()
    return files


def merge_csv_files(root_dir: Path, output_csv: Path) -> Tuple[int, int]:
    csv_files = collect_csv_files(root_dir, output_csv)
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found under: {root_dir}")

    merged_files = 0
    merged_rows = 0
    fieldnames = None
    writer = None

    with output_csv.open("w", encoding="utf-8-sig", newline="") as dst:
        for csv_file in csv_files:
            with csv_file.open("r", encoding="utf-8-sig", newline="") as src:
                reader = csv.DictReader(src)
                if reader.fieldnames is None:
                    continue

                current_fields = [str(h).strip() for h in reader.fieldnames]
                if fieldnames is None:
                    fieldnames = current_fields
                    writer = csv.DictWriter(dst, fieldnames=fieldnames)
                    writer.writeheader()
                elif current_fields != fieldnames:
                    raise ValueError(
                        f"CSV header mismatch: {csv_file}\n"
                        f"expected={fieldnames}\n"
                        f"actual={current_fields}"
                    )

                assert writer is not None
                for row in reader:
                    # Keep original row values, including id.
                    writer.writerow({key: row.get(key, "") for key in fieldnames})
                    merged_rows += 1
                merged_files += 1

    return merged_files, merged_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge all CSV files under subpages into one CSV file."
    )
    parser.add_argument(
        "--root-dir",
        default=DEFAULT_ROOT_DIR,
        help=f"Subpages root directory (default: {DEFAULT_ROOT_DIR})",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_NAME,
        help=(
            "Output file path or file name. "
            f"If file name only, it is created under --root-dir (default: {DEFAULT_OUTPUT_NAME})"
        ),
    )
    return parser.parse_args()


def resolve_output_path(root_dir: Path, output_arg: str) -> Path:
    output_path = Path(output_arg)
    if output_path.is_absolute():
        return output_path
    return root_dir / output_path


def main() -> int:
    args = parse_args()
    root_dir = Path(args.root_dir)

    if not root_dir.exists() or not root_dir.is_dir():
        print(f"[ERROR] invalid --root-dir: {root_dir}", file=sys.stderr)
        return 2

    output_csv = resolve_output_path(root_dir, args.output)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    try:
        merged_files, merged_rows = merge_csv_files(root_dir, output_csv)
    except Exception as exc:
        print(f"[ERROR] merge failed: {exc}", file=sys.stderr)
        return 1

    print(
        f"[SUMMARY] merged_files={merged_files}, merged_rows={merged_rows}, "
        f"output={output_csv}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
