#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将 x_url.txt 转为 traffic ingestor 使用的 id,url,domain CSV。

默认：
- 输入: small_tools/x_url.txt
- 输出: small_tools/x_url_firefox.csv
- 重复次数: 1

输出格式与 wiki_firefox.csv 一致：
id,url,domain
1,https://x.com/example,x.com

重复逻辑：
- repeat_count=1: 输出 1 轮原始列表
- repeat_count=50: 按“1 到结束，再 1 到结束”输出 50 轮
- 每一轮中的 id,url,domain 保持完全相同
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urlparse


SCRIPT_DIR = Path(__file__).resolve().parent
SMALL_TOOLS_DIR = SCRIPT_DIR.parent
DEFAULT_INPUT = SMALL_TOOLS_DIR / "origin" / "bsky_urls_1000.txt"
DEFAULT_OUTPUT = SMALL_TOOLS_DIR / "result" / f"{DEFAULT_INPUT.stem}.csv"
DEFAULT_REPEAT_COUNT = 50
FIELDNAMES = ["id", "url", "domain"]


def resolve_path(raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    return path


def normalize_url(raw_url: str) -> str:
    url = raw_url.strip()
    if not url:
        return ""
    if "://" not in url:
        url = f"https://{url}"
    return url


def extract_domain(url: str) -> str:
    parsed = urlparse(url)
    return (parsed.hostname or "").strip().lower()


def load_rows(input_path: Path, start_id: int, repeat_count: int) -> Tuple[List[dict], int]:
    base_rows: List[dict] = []
    blank_lines = 0

    with input_path.open("r", encoding="utf-8-sig") as f:
        for line_number, raw_line in enumerate(f, start=1):
            url = normalize_url(raw_line)
            if not url:
                blank_lines += 1
                continue

            domain = extract_domain(url)
            if not domain:
                raise ValueError(f"invalid URL at line {line_number}: {raw_line.rstrip()}")

            row_id = start_id + len(base_rows)
            base_rows.append({
                "id": str(row_id),
                "url": url,
                "domain": domain,
            })

    rows: List[dict] = []
    for _ in range(repeat_count):
        rows.extend(dict(row) for row in base_rows)

    return rows, blank_lines


def write_rows(output_path: Path, rows: List[dict]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="将 x_url.txt 转为 id,url,domain CSV。"
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_INPUT),
        help=f"输入 txt 路径，默认 {DEFAULT_INPUT}",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help=f"输出 csv 路径，默认 {DEFAULT_OUTPUT}",
    )
    parser.add_argument(
        "--start-id",
        type=int,
        default=1,
        help="起始 id，默认 1。",
    )
    parser.add_argument(
        "--repeat-count",
        type=int,
        default=DEFAULT_REPEAT_COUNT,
        help=(
            f"整份 URL 列表重复输出的轮数，默认 {DEFAULT_REPEAT_COUNT}。"
            "每轮中的 id,url,domain 保持不变。"
        ),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    input_path = resolve_path(args.input)
    output_path = resolve_path(args.output)

    if not input_path.exists() or not input_path.is_file():
        raise FileNotFoundError(f"input file not found: {input_path}")

    rows, blank_lines = load_rows(input_path, args.start_id, args.repeat_count)
    write_rows(output_path, rows)

    print(
        f"[DONE] input={input_path} output={output_path} "
        f"rows={len(rows)} blank_lines={blank_lines} repeat_count={args.repeat_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
