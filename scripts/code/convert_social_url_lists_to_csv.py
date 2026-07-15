#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""批量将社交平台 URL 文本转换为 trafficIngestor ingestor 使用的 CSV。"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from urllib.parse import urlparse


SCRIPT_DIR = Path(__file__).resolve().parent
SMALL_TOOLS_DIR = SCRIPT_DIR.parent
DEFAULT_OUTPUT_DIR = SMALL_TOOLS_DIR / "result"
DEFAULT_INPUTS = [
    SMALL_TOOLS_DIR / "origin" / "mastodon_social_urls_4000_non_monitored.txt",
    SMALL_TOOLS_DIR / "origin" / "bsky_urls_4000_non_monitored(1).txt",
    SMALL_TOOLS_DIR / "origin" / "threads_urls_4000_non_monitored(1).txt",
    SMALL_TOOLS_DIR / "origin" / "tumblr_urls_4000_non_monitored.txt",
    SMALL_TOOLS_DIR / "origin" / "x_url_4000_non_monitored(1).txt",
]
OUTPUT_FIELDS = ["id", "url", "domain"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="批量将每行一个 URL 的文本文件转换为 id,url,domain CSV。"
    )
    parser.add_argument(
        "inputs",
        nargs="*",
        type=Path,
        default=DEFAULT_INPUTS,
        help="输入 TXT 路径；不传时处理脚本内配置的 5 个默认文件",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"输出目录，默认 {DEFAULT_OUTPUT_DIR}",
    )
    return parser.parse_args()


def resolve_path(path: Path) -> Path:
    path = path.expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path.resolve()


def extract_domain(url: str, input_path: Path, line_number: int) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError(f"{input_path} 第 {line_number} 行不是有效 URL: {url!r}")

    domain = parsed.hostname.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def convert_file(input_path: Path, output_path: Path) -> int:
    if not input_path.is_file():
        raise FileNotFoundError(f"输入文件不存在: {input_path}")

    temporary_output = output_path.with_suffix(f"{output_path.suffix}.tmp")
    converted_rows = 0

    try:
        with input_path.open("r", encoding="utf-8-sig") as source, (
            temporary_output.open("w", encoding="utf-8", newline="")
        ) as destination:
            writer = csv.DictWriter(destination, fieldnames=OUTPUT_FIELDS)
            writer.writeheader()

            for line_number, raw_line in enumerate(source, start=1):
                url = raw_line.strip()
                if not url:
                    raise ValueError(f"{input_path} 第 {line_number} 行为空")

                converted_rows += 1
                writer.writerow(
                    {
                        "id": converted_rows,
                        "url": url,
                        "domain": extract_domain(url, input_path, line_number),
                    }
                )

        if converted_rows == 0:
            raise ValueError(f"输入文件没有数据: {input_path}")

        temporary_output.replace(output_path)
    finally:
        temporary_output.unlink(missing_ok=True)

    return converted_rows


def main() -> int:
    args = parse_args()
    input_paths = [resolve_path(path) for path in args.inputs]
    output_dir = resolve_path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_paths = [output_dir / f"{path.stem}.csv" for path in input_paths]
    if len(set(output_paths)) != len(output_paths):
        raise ValueError("多个输入文件会产生相同的输出文件名")

    total_rows = 0
    for input_path, output_path in zip(input_paths, output_paths):
        converted_rows = convert_file(input_path, output_path)
        total_rows += converted_rows
        print(f"转换完成: rows={converted_rows}, output={output_path}")

    print(f"全部完成: files={len(input_paths)}, rows={total_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
