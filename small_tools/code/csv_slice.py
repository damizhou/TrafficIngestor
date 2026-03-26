#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
功能:
- 从输入 CSV 按行号区间切片。
- 直接输出 ingestor 三列格式: id,url,domain（不再需要二次转换脚本）。

使用方式:
- 修改脚本内配置: INPUT_CSV、START_LINE、END_LINE、OUTPUT_CSV、DEFAULT_SCHEME。
- 运行命令: python small_tools/csv_slice.py
"""

from __future__ import annotations

import csv
from pathlib import Path

# Hardcoded config
INPUT_CSV = "tranco_ZWZ5G.csv"
START_LINE = 300001
END_LINE = 330001
OUTPUT_CSV = "top330000_ingestor.csv"
DEFAULT_SCHEME = "https://"

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent


def resolve_input_path(input_csv_raw: str) -> Path:
    input_path = Path(input_csv_raw).expanduser()
    if input_path.is_absolute() and input_path.exists():
        return input_path.resolve()
    for base in (Path.cwd(), SCRIPT_DIR, PROJECT_DIR):
        candidate = (base / input_path).resolve()
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Input file not found: {input_csv_raw}")


def resolve_output_path(input_path: Path, output_raw: str) -> Path:
    output_path = Path(output_raw).expanduser()
    if output_path.is_absolute():
        return output_path
    return (input_path.parent / output_path).resolve()


def normalize_url(domain_or_url: str) -> tuple[str, str]:
    text = (domain_or_url or "").strip()
    if not text:
        return "", ""
    if text.startswith("http://") or text.startswith("https://"):
        domain = text.split("://", 1)[1].split("/", 1)[0].strip()
        return text, domain
    domain = text.split("/", 1)[0].strip()
    return f"{DEFAULT_SCHEME}{domain}", domain


def main() -> None:
    if START_LINE < 1 or END_LINE < START_LINE:
        raise ValueError("START_LINE/END_LINE is invalid.")

    input_csv = resolve_input_path(INPUT_CSV)
    if not input_csv.is_file():
        raise ValueError(f"Input path is not a file: {input_csv}")
    output_csv = resolve_output_path(input_csv, OUTPUT_CSV)

    total_lines = 0
    selected_lines = 0
    converted = 0
    skipped = 0

    with input_csv.open("r", encoding="utf-8-sig", newline="") as src, output_csv.open(
        "w", encoding="utf-8-sig", newline=""
    ) as dst:
        reader = csv.reader(src)
        writer = csv.writer(dst)
        writer.writerow(["id", "url", "domain"])

        for line_no, row in enumerate(reader, start=1):
            total_lines = line_no
            if not (START_LINE <= line_no <= END_LINE):
                continue
            selected_lines += 1

            if len(row) < 2:
                skipped += 1
                continue

            row_id = (row[0] or "").strip()
            domain_raw = (row[1] or "").strip()
            if not row_id or not domain_raw:
                skipped += 1
                continue

            if row_id.lower() in {"id", "rank", "index"} and "domain" in domain_raw.lower():
                skipped += 1
                continue

            url, domain = normalize_url(domain_raw)
            if not domain:
                skipped += 1
                continue

            writer.writerow([row_id, url, domain])
            converted += 1

    if total_lines == 0:
        output_csv.unlink(missing_ok=True)
        raise ValueError("Input CSV is empty.")
    if START_LINE > total_lines:
        output_csv.unlink(missing_ok=True)
        raise ValueError(f"START_LINE ({START_LINE}) > total lines ({total_lines}).")
    if selected_lines == 0:
        output_csv.unlink(missing_ok=True)
        raise ValueError("No lines were selected.")
    if converted == 0:
        output_csv.unlink(missing_ok=True)
        raise ValueError("No valid rows converted in selected range.")

    print(
        f"Done: selected_lines={selected_lines}, converted={converted}, skipped={skipped}, "
        f"total_input_lines={total_lines}, output={output_csv}"
    )


if __name__ == "__main__":
    main()
