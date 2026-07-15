#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""将 GitHub 仓库清单转换为 trafficIngestor ingestor 使用的 CSV。"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
SMALL_TOOLS_DIR = SCRIPT_DIR.parent
DEFAULT_INPUT = SMALL_TOOLS_DIR / "origin" / "github_repose_10w.csv"
DEFAULT_OUTPUT = SMALL_TOOLS_DIR / "result" / "github_repose_10w.csv"
SOURCE_ID_FIELD = "id"
SOURCE_URL_FIELD = "html_url"
OUTPUT_DOMAIN = "github.com"
OUTPUT_FIELDS = ["id", "url", "domain"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="将制表符分隔的 GitHub 仓库清单转换为 id,url,domain CSV。"
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="输入 CSV 路径")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="输出 CSV 路径")
    return parser.parse_args()


def resolve_path(path: Path) -> Path:
    path = path.expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path.resolve()


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
            if reader.fieldnames is None:
                raise ValueError("输入文件缺少表头")
            missing_fields = {
                SOURCE_ID_FIELD,
                SOURCE_URL_FIELD,
            }.difference(reader.fieldnames)
            if missing_fields:
                raise ValueError(
                    f"输入文件缺少字段 {sorted(missing_fields)}，"
                    f"实际字段: {reader.fieldnames}"
                )

            writer = csv.DictWriter(destination, fieldnames=OUTPUT_FIELDS)
            writer.writeheader()

            for line_number, row in enumerate(reader, start=2):
                source_id = (row.get(SOURCE_ID_FIELD) or "").strip()
                url = (row.get(SOURCE_URL_FIELD) or "").strip()
                if not source_id:
                    raise ValueError(
                        f"第 {line_number} 行的 {SOURCE_ID_FIELD} 不能为空"
                    )
                if not url:
                    raise ValueError(
                        f"第 {line_number} 行的 {SOURCE_URL_FIELD} 不能为空"
                    )

                converted_rows += 1
                writer.writerow(
                    {
                        "id": source_id,
                        "url": url,
                        "domain": OUTPUT_DOMAIN,
                    }
                )

        if converted_rows == 0:
            raise ValueError("输入文件没有数据行")

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
