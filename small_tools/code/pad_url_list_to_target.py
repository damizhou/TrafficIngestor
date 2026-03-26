#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
功能:
- 遍历目录下所有 url_list.csv。
- 若某文件行数少于目标值（默认 40），按原行顺序循环补齐到目标值。
- 保持原始行不变，因此 id 会循环复用，不会重排为 1~40。

使用方式:
- 预览修改: python small_tools/pad_url_list_to_target.py --dry-run
- 实际补齐: python small_tools/pad_url_list_to_target.py --base-dir /netdisk/ww/top2000/subpages --target-count 40
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


DEFAULT_BASE_DIR = "/netdisk/ww/top2000/subpages"
DEFAULT_URL_LIST_NAME = "url_list.csv"
DEFAULT_TARGET_COUNT = 40


def find_url_key(fieldnames: List[str]) -> Optional[str]:
    for key in fieldnames:
        if key.strip().lower() == "url":
            return key
    return None


def load_csv_rows(csv_path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return [], []

        fieldnames = [str(name) for name in reader.fieldnames]
        url_key = find_url_key(fieldnames)
        rows: List[Dict[str, str]] = []

        for row in reader:
            normalized = {k: str(row.get(k, "") or "") for k in fieldnames}
            if url_key is not None:
                if not normalized.get(url_key, "").strip():
                    continue
            else:
                if not any(value.strip() for value in normalized.values()):
                    continue
            rows.append(normalized)

    return fieldnames, rows


def build_padded_rows(rows: List[Dict[str, str]], target_count: int) -> List[Dict[str, str]]:
    if len(rows) >= target_count:
        return list(rows)

    padded: List[Dict[str, str]] = []
    idx = 0
    source_size = len(rows)
    while len(padded) < target_count:
        padded.append(dict(rows[idx % source_size]))
        idx += 1
    return padded


def write_csv_rows(csv_path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Traverse subpages url_list files. If row count is less than target, "
            "cycle existing rows to fill up to target."
        )
    )
    parser.add_argument(
        "--base-dir",
        default=DEFAULT_BASE_DIR,
        help=f"Base directory to scan (default: {DEFAULT_BASE_DIR})",
    )
    parser.add_argument(
        "--url-list-name",
        default=DEFAULT_URL_LIST_NAME,
        help=f"URL list filename to match (default: {DEFAULT_URL_LIST_NAME})",
    )
    parser.add_argument(
        "--target-count",
        type=int,
        default=DEFAULT_TARGET_COUNT,
        help=f"Target row count for each file (default: {DEFAULT_TARGET_COUNT})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show files that would be updated without modifying them.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_dir = Path(args.base_dir)
    target_count = args.target_count
    url_list_name = args.url_list_name

    if not base_dir.exists() or not base_dir.is_dir():
        print(f"[ERROR] invalid --base-dir: {base_dir}", file=sys.stderr)
        return 2
    if target_count <= 0:
        print("[ERROR] --target-count must be > 0", file=sys.stderr)
        return 2

    scanned = 0
    updated = 0
    skipped_enough = 0
    skipped_empty = 0
    failed = 0

    for csv_path in sorted(base_dir.rglob(url_list_name)):
        if not csv_path.is_file():
            continue
        scanned += 1

        try:
            fieldnames, rows = load_csv_rows(csv_path)
        except Exception as exc:
            failed += 1
            print(f"[WARN] failed to read {csv_path}: {exc}", file=sys.stderr)
            continue

        if not fieldnames or not rows:
            skipped_empty += 1
            print(f"[SKIP-EMPTY] {csv_path}")
            continue

        current_count = len(rows)
        if current_count >= target_count:
            skipped_enough += 1
            continue

        padded_rows = build_padded_rows(rows, target_count)
        print(f"[PAD] {csv_path} {current_count} -> {len(padded_rows)}")

        if args.dry_run:
            updated += 1
            continue

        try:
            write_csv_rows(csv_path, fieldnames, padded_rows)
            updated += 1
        except Exception as exc:
            failed += 1
            print(f"[WARN] failed to write {csv_path}: {exc}", file=sys.stderr)

    print(
        f"[SUMMARY] scanned={scanned}, updated={updated}, "
        f"skipped_enough={skipped_enough}, skipped_empty={skipped_empty}, "
        f"failed={failed}, dry_run={args.dry_run}"
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
