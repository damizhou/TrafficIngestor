#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""将 The Guardian 分类 URL CSV 文件整合为 id,url,domain CSV。"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from urllib.parse import urlparse


SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = SCRIPT_DIR.parent
DEFAULT_INPUT_DIR = SCRIPTS_DIR / "origin" / "theguardian_all_reformat"
DEFAULT_OUTPUT = SCRIPTS_DIR / "result" / "theguardian2025_all_reformat.csv"
SOURCE_FIELDS = ["Section", "ID", "URL"]
OUTPUT_FIELDS = ["id", "url", "domain"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "整合 The Guardian 分类 URL 文件，并输出 id,url,domain CSV；"
            "其中 id 为 Section_ID。"
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"输入目录，默认 {DEFAULT_INPUT_DIR}",
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


def extract_domain(url: str, source_path: Path, line_number: int) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError(
            f"{source_path} 第 {line_number} 行不是有效 HTTP(S) URL: {url!r}"
        )
    return parsed.hostname.lower()


def convert(input_dir: Path, output_path: Path) -> int:
    if not input_dir.is_dir():
        raise NotADirectoryError(f"输入目录不存在: {input_dir}")
    if input_dir == output_path or input_dir in output_path.parents:
        raise ValueError("输出文件不能位于输入目录中")

    input_paths = sorted(input_dir.glob("*.csv"))
    if not input_paths:
        raise ValueError(f"输入目录没有 CSV 文件: {input_dir}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = output_path.with_suffix(f"{output_path.suffix}.tmp")
    converted_rows = 0
    seen_ids: set[str] = set()

    try:
        with temporary_output.open("w", encoding="utf-8", newline="") as destination:
            writer = csv.DictWriter(destination, fieldnames=OUTPUT_FIELDS)
            writer.writeheader()

            for input_path in input_paths:
                with input_path.open(
                    "r", encoding="utf-8-sig", newline=""
                ) as source:
                    reader = csv.DictReader(source, delimiter="\t")
                    if reader.fieldnames != SOURCE_FIELDS:
                        raise ValueError(
                            f"{input_path} 表头必须为 {SOURCE_FIELDS}，"
                            f"实际为 {reader.fieldnames}"
                        )

                    file_rows = 0
                    for line_number, row in enumerate(reader, start=2):
                        if row.get(None):
                            raise ValueError(
                                f"{input_path} 第 {line_number} 行包含多余字段: "
                                f"{row[None]!r}"
                            )

                        section = (row.get("Section") or "").strip()
                        source_id = (row.get("ID") or "").strip()
                        url = (row.get("URL") or "").strip()
                        if not section:
                            raise ValueError(
                                f"{input_path} 第 {line_number} 行的 Section 不能为空"
                            )
                        if not source_id:
                            raise ValueError(
                                f"{input_path} 第 {line_number} 行的 ID 不能为空"
                            )
                        if not url:
                            raise ValueError(
                                f"{input_path} 第 {line_number} 行的 URL 不能为空"
                            )

                        output_id = f"{section}_{source_id}"
                        if output_id in seen_ids:
                            raise ValueError(
                                f"发现重复的 Section_ID: {output_id!r}"
                            )

                        writer.writerow(
                            {
                                "id": output_id,
                                "url": url,
                                "domain": extract_domain(url, input_path, line_number),
                            }
                        )
                        seen_ids.add(output_id)
                        converted_rows += 1
                        file_rows += 1

                    if file_rows == 0:
                        raise ValueError(f"输入文件没有数据行: {input_path}")

        if converted_rows == 0:
            raise ValueError(f"输入目录没有数据: {input_dir}")

        temporary_output.replace(output_path)
    finally:
        temporary_output.unlink(missing_ok=True)

    return converted_rows


def main() -> int:
    args = parse_args()
    input_dir = resolve_path(args.input_dir)
    output_path = resolve_path(args.output)
    converted_rows = convert(input_dir, output_path)
    print(
        f"整合完成: files={len(sorted(input_dir.glob('*.csv')))}, "
        f"rows={converted_rows}, output={output_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
