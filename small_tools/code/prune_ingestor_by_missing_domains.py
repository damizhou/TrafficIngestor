#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
功能:
- 读取 top2000_ingestor.csv（id,url,domain）。
- 若某条记录的 domain 在目标目录不存在同名文件夹，则删除该条记录。

使用方式:
- 预览删除: python small_tools/prune_ingestor_by_missing_domains.py --dry-run
- 原地覆盖: python small_tools/prune_ingestor_by_missing_domains.py
- 输出到新文件: python small_tools/prune_ingestor_by_missing_domains.py --output top2000_ingestor.only_existing.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse


DEFAULT_INPUT_CSV = "top4000_ingestor.csv"
DEFAULT_CHECK_DIR = "/netdisk/ww/top2000/subpages"

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent


def normalize_domain(text: str) -> str:
    value = (text or "").strip().lower().strip(".")
    if not value:
        return ""
    if "://" not in value:
        value = "https://" + value
    parsed = urlparse(value)
    return (parsed.hostname or "").strip().lower().strip(".")


def resolve_input_csv(input_arg: str) -> Path:
    p = Path(input_arg).expanduser()
    if p.is_absolute():
        return p
    for base in (Path.cwd(), SCRIPT_DIR, PROJECT_DIR):
        candidate = (base / p).resolve()
        if candidate.exists():
            return candidate
    return (SCRIPT_DIR / p).resolve()


def find_domain_key(fieldnames: List[str]) -> Optional[str]:
    for key in fieldnames:
        if key.strip().lower() == "domain":
            return key
    return None


def load_existing_domains(check_dir: Path) -> Set[str]:
    domains: Set[str] = set()
    for entry in check_dir.iterdir():
        if not entry.is_dir():
            continue
        domain = normalize_domain(entry.name)
        if domain:
            domains.add(domain)
    return domains


def filter_rows_keep_existing(
    input_csv: Path,
    existing_domains: Set[str],
) -> Tuple[List[str], List[Dict[str, str]], int, int]:
    with input_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {input_csv}")

        fieldnames = [str(name) for name in reader.fieldnames]
        domain_key = find_domain_key(fieldnames)
        if domain_key is None:
            raise ValueError(f"CSV missing 'domain' column: {input_csv}")

        kept_rows: List[Dict[str, str]] = []
        total = 0
        removed = 0

        for row in reader:
            total += 1
            normalized_row = {key: str(row.get(key, "") or "") for key in fieldnames}
            domain = normalize_domain(normalized_row.get(domain_key, ""))
            if not domain or domain not in existing_domains:
                removed += 1
                continue
            kept_rows.append(normalized_row)

    return fieldnames, kept_rows, total, removed


def write_rows(output_csv: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Remove rows when row domain does NOT exist as a folder under target directory. "
            "Input CSV format should be top2000_ingestor style: id,url,domain."
        )
    )
    parser.add_argument(
        "--input-csv",
        default=DEFAULT_INPUT_CSV,
        help=f"Input CSV path (default: {DEFAULT_INPUT_CSV})",
    )
    parser.add_argument(
        "--check-dir",
        default=DEFAULT_CHECK_DIR,
        help=f"Directory containing existing domain folders (default: {DEFAULT_CHECK_DIR})",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Output CSV path. If omitted, overwrite input CSV.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print summary, do not write file.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_csv = resolve_input_csv(args.input_csv)
    check_dir = Path(args.check_dir)

    if not input_csv.exists() or not input_csv.is_file():
        print(f"[ERROR] invalid --input-csv: {input_csv}", file=sys.stderr)
        return 2
    if not check_dir.exists() or not check_dir.is_dir():
        print(f"[ERROR] invalid --check-dir: {check_dir}", file=sys.stderr)
        return 2

    output_csv = Path(args.output).expanduser() if args.output else input_csv
    if not output_csv.is_absolute():
        output_csv = (Path.cwd() / output_csv).resolve()

    try:
        existing_domains = load_existing_domains(check_dir)
        fieldnames, kept_rows, total, removed = filter_rows_keep_existing(input_csv, existing_domains)
    except Exception as exc:
        print(f"[ERROR] failed to process: {exc}", file=sys.stderr)
        return 1

    kept = total - removed
    print(
        f"[SUMMARY] input={input_csv}, check_dir={check_dir}, "
        f"existing_domains={len(existing_domains)}, total_rows={total}, removed={removed}, kept={kept}, "
        f"dry_run={args.dry_run}"
    )

    if args.dry_run:
        return 0

    try:
        write_rows(output_csv, fieldnames, kept_rows)
    except Exception as exc:
        print(f"[ERROR] failed to write output: {exc}", file=sys.stderr)
        return 1

    print(f"[DONE] output={output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
