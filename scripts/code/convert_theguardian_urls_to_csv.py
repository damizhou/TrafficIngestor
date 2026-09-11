#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""将 The Guardian URL TSV 转换为 trafficIngestor 使用的 CSV。"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from urllib.parse import urlparse


SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = SCRIPT_DIR.parent
DEFAULT_INPUT = SCRIPTS_DIR / "origin" / "theguardian_sensitive1_urls.tsv"
DEFAULT_OUTPUT = SCRIPTS_DIR / "result" / "theguardian_sensitive1_urls.csv"
SOURCE_FIELDS = ["index", "url"]
OUTPUT_FIELDS = ["id", "url", "domain"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="将 The Guardian 的 TSV URL 清单转换为 id,url,domain CSV。"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"输入 TSV 路径，默认 {DEFAULT_INPUT}",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"输出 CSV 路径，默认 {DEFAULT_OUTPUT}",
    )
    return parser.parse_args()


def resolve_path(path: Path) -> Path:
    path = path.expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path.resolve()


def extract_domain(url: str, line_number: int) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError(f"第 {line_number} 行不是有效 HTTP(S) URL: {url!r}")
    return parsed.hostname.lower()


def convert(input_path: Path, output_path: Path) -> int:
    if not input_path.is_file():
        raise FileNotFoundError(f"输入文件不存在: {input_path}")
    if input_path == output_path:
        raise ValueError("输入文件与输出文件不能相同")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = output_path.with_suffix(f"{output_path.suffix}.tmp")
    converted_rows = 0

    try:
        with input_path.open("r", encoding="utf-8-sig", newline="") as source, (
            temporary_output.open("w", encoding="utf-8", newline="")
        ) as destination:
            reader = csv.DictReader(source, delimiter="\t")
            if reader.fieldnames != SOURCE_FIELDS:
                raise ValueError(
                    f"输入文件表头必须为 {SOURCE_FIELDS}，实际为 {reader.fieldnames}"
                )

            writer = csv.DictWriter(destination, fieldnames=OUTPUT_FIELDS)
            writer.writeheader()

            for line_number, row in enumerate(reader, start=2):
                if row.get(None):
                    raise ValueError(f"第 {line_number} 行包含多余字段: {row[None]!r}")

                source_id = (row.get("index") or "").strip()
                url = (row.get("url") or "").strip()
                if not source_id:
                    raise ValueError(f"第 {line_number} 行的 index 不能为空")
                if not url:
                    raise ValueError(f"第 {line_number} 行的 url 不能为空")

                writer.writerow(
                    {
                        "id": source_id,
                        "url": url,
                        "domain": extract_domain(url, line_number),
                    }
                )
                converted_rows += 1

        if converted_rows == 0:
            raise ValueError(f"输入文件没有数据行: {input_path}")

        temporary_output.replace(output_path)
    finally:
        temporary_output.unlink(missing_ok=True)

    return converted_rows


def main() -> int:
    args = parse_args()
    input_path = resolve_path(args.input)
    output_path = resolve_path(args.output)
    converted_rows = convert(input_path, output_path)
    print(f"转换完成: rows={converted_rows}, output={output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
