#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""合并多个 CSV 的前 N 条数据行，保留表头、字段内容和输入顺序。"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent.parent
DEFAULT_INPUTS = [
    SCRIPTS_DIR / "result" / "bbc_sensitive.csv",
    SCRIPTS_DIR / "result" / "theguardian_sensitive1_urls.csv",
    SCRIPTS_DIR / "result" / "voa_sensitive.csv",
]
DEFAULT_OUTPUT = SCRIPTS_DIR / "result" / "merged_sensitive_top200.csv"


def resolve_path(path: Path) -> Path:
    path = path.expanduser()
    return path if path.is_absolute() else (Path.cwd() / path).resolve()


def read_top_rows(input_path: Path, limit: int) -> tuple[list[str], list[dict[str, str]]]:
    input_path = resolve_path(input_path)
    if not input_path.is_file():
        raise FileNotFoundError(f"输入文件不存在：{input_path}")
    with input_path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames:
            raise ValueError(f"CSV 文件缺少表头：{input_path}")
        return list(reader.fieldnames), list(reader)[:limit]


def merge_top_rows(input_paths: list[Path], output_path: Path, limit: int = 200) -> int:
    if not input_paths:
        raise ValueError("至少需要一个输入文件")
    if limit < 1:
        raise ValueError("limit 必须大于 0")

    resolved_inputs = [resolve_path(path) for path in input_paths]
    output_path = resolve_path(output_path)
    if output_path in resolved_inputs:
        raise ValueError("输出文件不能与输入文件相同")

    header, first_rows = read_top_rows(resolved_inputs[0], limit)
    rows = list(first_rows)
    counts = [len(first_rows)]
    for input_path in resolved_inputs[1:]:
        current_header, current_rows = read_top_rows(input_path, limit)
        if current_header != header:
            raise ValueError(
                f"CSV 表头不一致：{input_path}\n"
                f"预期：{header}\n实际：{current_header}"
            )
        rows.extend(current_rows)
        counts.append(len(current_rows))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(output_path.name + ".tmp")
    try:
        with temporary.open("w", encoding="utf-8-sig", newline="") as destination:
            writer = csv.DictWriter(destination, fieldnames=header)
            writer.writeheader()
            writer.writerows(rows)
        temporary.replace(output_path)
    finally:
        temporary.unlink(missing_ok=True)

    for input_path, count in zip(resolved_inputs, counts):
        print(f"{input_path}: {count} 条")
    print(f"合并完成：{output_path}，共 {len(rows)} 条")
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", type=Path, nargs="+", default=DEFAULT_INPUTS,
        help="输入 CSV，可指定多个；默认使用 BBC、The Guardian 和 VOA 敏感 URL 文件",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help=f"输出 CSV，默认：{DEFAULT_OUTPUT}")
    parser.add_argument("--limit", type=int, default=200, help="每个输入文件取前多少条数据，默认 200")
    args = parser.parse_args()
    merge_top_rows(args.input, args.output, args.limit)


if __name__ == "__main__":
    main()
