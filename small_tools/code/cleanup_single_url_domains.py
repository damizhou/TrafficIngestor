#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
功能:
- 遍历子域名目录，检查 url_list 文件中的 URL 数量。
- 当 URL 数量等于指定值（默认 1）时，删除该 url_list 文件和对应 domain 文件夹。

使用方式:
- 预览删除: python small_tools/cleanup_single_url_domains.py --dry-run
- 实际删除: python small_tools/cleanup_single_url_domains.py --base-dir /netdisk/ww/top2000/subpages
"""

from __future__ import annotations

import argparse
import csv
import shutil
import sys
from pathlib import Path
from typing import Tuple


DEFAULT_BASE_DIR = "/netdisk/ww/top2000/subpages"
DEFAULT_URL_LIST_NAME = "url_list.csv"


def count_urls_in_file(url_list_path: Path) -> int:
    """Count URL rows in a url_list file, preferring CSV 'url' column."""
    try:
        with url_list_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames:
                header_map = {}
                for name in reader.fieldnames:
                    if isinstance(name, str):
                        header_map[name.strip().lower()] = name
                url_key = header_map.get("url")
                if url_key:
                    count = 0
                    for row in reader:
                        value = str(row.get(url_key, "")).strip()
                        if value:
                            count += 1
                    return count
    except Exception:
        pass

    lines = url_list_path.read_text(encoding="utf-8-sig", errors="ignore").splitlines()
    rows = [line.strip() for line in lines if line.strip()]
    if rows and rows[0].replace(" ", "").lower() in {"id,url,domain", "url"}:
        rows = rows[1:]
    return len(rows)


def remove_domain_dir(domain_dir: Path, url_list_path: Path, dry_run: bool) -> bool:
    """Delete url_list file and the whole domain directory."""
    if dry_run:
        print(f"[DRY-RUN] delete file: {url_list_path}")
        print(f"[DRY-RUN] delete dir : {domain_dir}")
        return True

    try:
        if url_list_path.exists():
            url_list_path.unlink()
        shutil.rmtree(domain_dir)
        return True
    except Exception as exc:
        print(f"[WARN] failed to delete {domain_dir}: {exc}", file=sys.stderr)
        return False


def cleanup_single_url_domains(
    base_dir: Path,
    url_list_name: str,
    target_count: int,
    dry_run: bool,
) -> Tuple[int, int, int]:
    scanned = 0
    matched = 0
    deleted = 0

    for domain_dir in sorted(base_dir.iterdir()):
        if not domain_dir.is_dir():
            continue
        scanned += 1

        url_list_path = domain_dir / url_list_name
        if not url_list_path.is_file():
            continue

        try:
            url_count = count_urls_in_file(url_list_path)
        except Exception as exc:
            print(f"[WARN] failed to parse {url_list_path}: {exc}", file=sys.stderr)
            continue

        if url_count != target_count:
            continue

        matched += 1
        print(f"[MATCH] {domain_dir.name}: urls={url_count}")
        if remove_domain_dir(domain_dir, url_list_path, dry_run):
            deleted += 1

    return scanned, matched, deleted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Delete domain folders whose url_list has exactly one URL."
    )
    parser.add_argument(
        "--base-dir",
        default=DEFAULT_BASE_DIR,
        help=f"Root output directory from get_url_list.py (default: {DEFAULT_BASE_DIR})",
    )
    parser.add_argument(
        "--url-list-name",
        default=DEFAULT_URL_LIST_NAME,
        help=f"URL list filename inside each domain folder (default: {DEFAULT_URL_LIST_NAME})",
    )
    parser.add_argument(
        "--target-count",
        type=int,
        default=1,
        help="Delete domain when URL count equals this value (default: 1)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be deleted without deleting.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_dir = Path(args.base_dir)

    if not base_dir.exists() or not base_dir.is_dir():
        print(f"[ERROR] base dir does not exist or is not a directory: {base_dir}", file=sys.stderr)
        return 2
    if args.target_count < 0:
        print("[ERROR] --target-count must be >= 0", file=sys.stderr)
        return 2

    scanned, matched, deleted = cleanup_single_url_domains(
        base_dir=base_dir,
        url_list_name=args.url_list_name,
        target_count=args.target_count,
        dry_run=args.dry_run,
    )

    print(
        f"[SUMMARY] scanned_domains={scanned}, matched={matched}, "
        f"deleted={deleted}, dry_run={args.dry_run}"
    )

    if args.dry_run:
        return 0
    return 0 if deleted == matched else 1


if __name__ == "__main__":
    raise SystemExit(main())
