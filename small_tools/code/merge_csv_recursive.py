#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
功能:
- 递归遍历 ROOT_DIR 下所有 CSV，并合并成一个输出文件。

使用方式:
- 修改脚本内配置: ROOT_DIR、OUTPUT_CSV_NAME。
- 运行命令: python small_tools/merge_csv_recursive.py
"""

import csv
from pathlib import Path
from typing import List, Tuple

# Hardcoded config
ROOT_DIR = r"/netdisk/ww/top2000/subpages"
OUTPUT_CSV_NAME = "merged_url_list.csv"


def collect_csv_files(root_dir: Path, output_csv: Path) -> List[Path]:
    files: List[Path] = []
    output_resolved = output_csv.resolve()
    for p in root_dir.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix.lower() != ".csv":
            continue
        if p.resolve() == output_resolved:
            continue
        files.append(p)
    files.sort()
    return files


def merge_csv_files(root_dir: Path, output_csv: Path) -> Tuple[int, int]:
    csv_files = collect_csv_files(root_dir, output_csv)
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found under: {root_dir}")

    merged_rows = 0
    merged_files = 0
    fieldnames = None
    writer = None

    with output_csv.open("w", encoding="utf-8-sig", newline="") as dst:
        for csv_file in csv_files:
            with csv_file.open("r", encoding="utf-8-sig", newline="") as src:
                reader = csv.DictReader(src)
                if reader.fieldnames is None:
                    continue

                current_fields = [h.strip() for h in reader.fieldnames]
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
                    writer.writerow({k: row.get(k, "") for k in fieldnames})
                    merged_rows += 1
                merged_files += 1

    return merged_files, merged_rows


def main() -> None:
    root_dir = Path(ROOT_DIR).expanduser().resolve()
    if not root_dir.exists() or not root_dir.is_dir():
        raise NotADirectoryError(f"ROOT_DIR does not exist or is not a directory: {root_dir}")

    output_csv = (root_dir / OUTPUT_CSV_NAME).resolve()
    merged_files, merged_rows = merge_csv_files(root_dir, output_csv)
    print(
        f"Done: merged_files={merged_files}, merged_rows={merged_rows}, output={output_csv}"
    )


if __name__ == "__main__":
    main()
