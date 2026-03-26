#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将 `subpages_temp/<domain>/url_list.csv` 复制到 `subpages/<domain>/url_list.csv`。

默认路径：
- 源目录：/netdisk2/ww/top2000/subpages_temp
- 目的目录：/netdisk2/ww/top2000/subpages
- 文件名：url_list.csv

示例：
- python small_tools/copy_subpages_temp_csv.py
- python small_tools/copy_subpages_temp_csv.py --dry-run
- python small_tools/copy_subpages_temp_csv.py --src-root /a/subpages_temp --dst-root /b/subpages
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


DEFAULT_SRC_ROOT = "/netdisk2/ww/top2000/subpages_temp"
DEFAULT_DST_ROOT = "/netdisk2/ww/top2000/subpages"
DEFAULT_FILENAME = "url_list.csv"


def resolve_path(raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "遍历目的目录下的域名目录，并将源目录中同名域名下的 url_list.csv "
            "复制到目的目录域名根目录。"
        )
    )
    parser.add_argument(
        "--src-root",
        default=str(DEFAULT_SRC_ROOT),
        help=f"CSV 源根目录，默认：{DEFAULT_SRC_ROOT}",
    )
    parser.add_argument(
        "--dst-root",
        default=str(DEFAULT_DST_ROOT),
        help=f"域名目的根目录，默认：{DEFAULT_DST_ROOT}",
    )
    parser.add_argument(
        "--filename",
        default=DEFAULT_FILENAME,
        help=f"要复制的 CSV 文件名，默认：{DEFAULT_FILENAME}",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅打印将要执行的复制操作，不实际写入文件。",
    )
    return parser


def iter_domain_dirs(dst_root: Path) -> list[Path]:
    domain_dirs = [path for path in dst_root.iterdir() if path.is_dir()]
    domain_dirs.sort(key=lambda path: path.name)
    return domain_dirs


def copy_domain_csvs(src_root: Path, dst_root: Path, filename: str, dry_run: bool) -> tuple[int, int, int]:
    copied = 0
    missing = 0
    total = 0

    for domain_dir in iter_domain_dirs(dst_root):
        total += 1
        src_csv = src_root / domain_dir.name / filename
        dst_csv = domain_dir / filename

        if not src_csv.is_file():
            missing += 1
            print(f"[MISS] {domain_dir.name}: source not found: {src_csv}")
            continue

        if dry_run:
            print(f"[DRY ] {src_csv} -> {dst_csv}")
        else:
            shutil.copy2(src_csv, dst_csv)
            print(f"[COPY] {src_csv} -> {dst_csv}")
        copied += 1

    return total, copied, missing


def main() -> int:
    args = build_parser().parse_args()

    src_root = resolve_path(args.src_root)
    dst_root = resolve_path(args.dst_root)
    filename = args.filename.strip()

    if not filename:
        raise ValueError("filename cannot be empty")
    if not src_root.exists() or not src_root.is_dir():
        raise NotADirectoryError(f"src_root does not exist or is not a directory: {src_root}")
    if not dst_root.exists() or not dst_root.is_dir():
        raise NotADirectoryError(f"dst_root does not exist or is not a directory: {dst_root}")

    if not iter_domain_dirs(dst_root):
        raise FileNotFoundError(f"no domain directories found under dst_root: {dst_root}")

    total, copied, missing = copy_domain_csvs(src_root, dst_root, filename, args.dry_run)
    print(
        f"[DONE] total_domains={total} copied={copied} missing={missing} "
        f"dry_run={args.dry_run} src_root={src_root} dst_root={dst_root}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
