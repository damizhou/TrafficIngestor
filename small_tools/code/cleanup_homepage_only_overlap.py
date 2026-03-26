#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
功能:
- 对比 DEFAULT_SUBPAGES_DIR 与 DEFAULT_HOMEPAGE_ONLY_DIR 的一级子文件夹。
- 若同名文件夹同时存在，仅删除 DEFAULT_HOMEPAGE_ONLY_DIR 中对应文件夹。

使用方式:
- 预览删除: python small_tools/cleanup_homepage_only_overlap.py --dry-run
- 实际删除: python small_tools/cleanup_homepage_only_overlap.py
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path
from typing import Set, Tuple


DEFAULT_SUBPAGES_DIR = "/netdisk/ww/top2000/subpages"
DEFAULT_HOMEPAGE_ONLY_DIR = "/netdisk/ww/top200000"


def list_child_dirs(base_dir: Path) -> Set[str]:
    return {p.name for p in base_dir.iterdir() if p.is_dir()}


def remove_overlap_dirs(subpages_dir: Path, homepage_only_dir: Path, dry_run: bool) -> Tuple[int, int]:
    subpages_names = list_child_dirs(subpages_dir)
    homepage_names = list_child_dirs(homepage_only_dir)
    overlap_names = sorted(subpages_names.intersection(homepage_names))

    deleted = 0
    for name in overlap_names:
        target = homepage_only_dir / name
        if dry_run:
            print(f"[DRY-RUN] delete dir: {target}")
            deleted += 1
            continue

        try:
            shutil.rmtree(target)
            print(f"[DELETE] {target}")
            deleted += 1
        except Exception as exc:
            print(f"[WARN] failed to delete {target}: {exc}", file=sys.stderr)

    return len(overlap_names), deleted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare subpages and homepage_only. "
            "If a domain folder exists in both, delete it from homepage_only."
        )
    )
    parser.add_argument(
        "--subpages-dir",
        default=DEFAULT_SUBPAGES_DIR,
        help=f"Subpages base directory (default: {DEFAULT_SUBPAGES_DIR})",
    )
    parser.add_argument(
        "--homepage-only-dir",
        default=DEFAULT_HOMEPAGE_ONLY_DIR,
        help=f"Homepage-only base directory (default: {DEFAULT_HOMEPAGE_ONLY_DIR})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print folders to delete without deleting.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    subpages_dir = Path(args.subpages_dir)
    homepage_only_dir = Path(args.homepage_only_dir)

    if not subpages_dir.exists() or not subpages_dir.is_dir():
        print(f"[ERROR] invalid --subpages-dir: {subpages_dir}", file=sys.stderr)
        return 2
    if not homepage_only_dir.exists() or not homepage_only_dir.is_dir():
        print(f"[ERROR] invalid --homepage-only-dir: {homepage_only_dir}", file=sys.stderr)
        return 2

    overlap_count, deleted_count = remove_overlap_dirs(
        subpages_dir=subpages_dir,
        homepage_only_dir=homepage_only_dir,
        dry_run=args.dry_run,
    )

    print(
        f"[SUMMARY] overlap={overlap_count}, deleted={deleted_count}, dry_run={args.dry_run}"
    )

    if args.dry_run:
        return 0
    return 0 if overlap_count == deleted_count else 1


if __name__ == "__main__":
    raise SystemExit(main())
