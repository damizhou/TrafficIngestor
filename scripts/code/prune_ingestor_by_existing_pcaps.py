#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
根据已有 pcap 删除单 CSV 中对应的待采集记录。

匹配规则:
- 以 CSV 行的 id + domain 为键。
- 在结果目录的 <domain>/pcap/ 下查找以 <id>_ 开头的 .pcap 文件。
- 每存在一个匹配 pcap，就从 CSV 中删除一行对应记录。
- 不检查其他产物，也不推算原始重复次数。

默认只预览。确认后使用 --apply 原地更新 CSV。

示例:
    python scripts/code/prune_ingestor_by_existing_pcaps.py \
        --input-csv scripts/result/top15000_50_yjn.csv \
        --result-dir /netdisk/yjn/20260914/us/chrome \
        --dry-run

    python scripts/code/prune_ingestor_by_existing_pcaps.py \
        --input-csv scripts/result/top15000_50_yjn.csv \
        --result-dir /netdisk/yjn/20260914/us/chrome \
        --apply
"""

from __future__ import annotations

import argparse
import csv
import os
import stat
import sys
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlparse


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]
DEFAULT_INPUT_CSV = (
    PROJECT_ROOT / "scripts" / "result" / "top15000_50_yjn.csv"
)
DEFAULT_RESULT_DIR = Path("/netdisk/yjn/20260914/us/chrome")
DATA_DIR_NAME = "data"
PCAP_DIR_NAME = "pcap"
PCAP_SUFFIX = ".pcap"

RowKey = Tuple[str, str]


def normalize_domain(value: str) -> str:
    domain = str(value or "").strip().lower().strip(".")
    if not domain:
        return ""
    if "://" in domain:
        parsed = urlparse(domain)
        domain = str(parsed.hostname or "").strip().lower().strip(".")
    return domain


def find_column(fieldnames: Sequence[str], target: str) -> Optional[str]:
    normalized_target = target.strip().lower()
    for fieldname in fieldnames:
        if str(fieldname).strip().lower() == normalized_target:
            return str(fieldname)
    return None


def resolve_result_domain_root(result_dir: Path) -> Path:
    data_dir = result_dir / DATA_DIR_NAME
    if data_dir.is_dir():
        return data_dir
    if result_dir.is_dir():
        return result_dir
    raise FileNotFoundError(f"结果目录不存在: {result_dir}")


def read_csv_rows(
    input_csv: Path,
) -> Tuple[List[str], List[Dict[str, str]], List[Optional[RowKey]]]:
    with input_csv.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames is None:
            raise ValueError(f"CSV 缺少表头: {input_csv}")

        fieldnames = [str(fieldname) for fieldname in reader.fieldnames]
        id_column = find_column(fieldnames, "id")
        domain_column = find_column(fieldnames, "domain")
        if id_column is None:
            raise ValueError(f"CSV 缺少 id 列: {input_csv}")
        if domain_column is None:
            raise ValueError(f"CSV 缺少 domain 列: {input_csv}")

        rows: List[Dict[str, str]] = []
        row_keys: List[Optional[RowKey]] = []
        for raw_row in reader:
            row = {
                fieldname: str(raw_row.get(fieldname, "") or "")
                for fieldname in fieldnames
            }
            row_id = row[id_column].strip()
            domain = normalize_domain(row[domain_column])
            rows.append(row)
            row_keys.append((row_id, domain) if row_id and domain else None)

    return fieldnames, rows, row_keys


def index_domain_directories(domain_root: Path) -> Dict[str, List[Path]]:
    domain_directories: Dict[str, List[Path]] = defaultdict(list)
    with os.scandir(domain_root) as entries:
        for entry in entries:
            if not entry.is_dir():
                continue
            domain = normalize_domain(entry.name)
            if domain:
                domain_directories[domain].append(Path(entry.path))
    return domain_directories


def count_existing_pcaps(
    row_keys: Iterable[Optional[RowKey]],
    domain_directories: Dict[str, List[Path]],
) -> Counter:
    ids_by_domain: Dict[str, Set[str]] = defaultdict(set)
    for key in row_keys:
        if key is not None:
            row_id, domain = key
            ids_by_domain[domain].add(row_id)

    pcap_counts: Counter = Counter()
    for domain, expected_ids in ids_by_domain.items():
        for domain_directory in domain_directories.get(domain, ()):
            pcap_directory = domain_directory / PCAP_DIR_NAME
            if not pcap_directory.is_dir():
                continue

            with os.scandir(pcap_directory) as entries:
                for entry in entries:
                    if (
                        not entry.is_file()
                        or not entry.name.lower().endswith(PCAP_SUFFIX)
                    ):
                        continue
                    row_id, separator, _ = entry.name.partition("_")
                    if separator and row_id in expected_ids:
                        pcap_counts[(row_id, domain)] += 1

    return pcap_counts


def filter_rows_by_pcap_count(
    rows: Sequence[Dict[str, str]],
    row_keys: Sequence[Optional[RowKey]],
    pcap_counts: Counter,
) -> Tuple[List[Dict[str, str]], int, int]:
    remaining_pcaps = pcap_counts.copy()
    kept_rows: List[Dict[str, str]] = []
    removed_rows = 0

    for row, key in zip(rows, row_keys):
        if key is not None and remaining_pcaps[key] > 0:
            remaining_pcaps[key] -= 1
            removed_rows += 1
            continue
        kept_rows.append(row)

    unused_pcaps = sum(remaining_pcaps.values())
    return kept_rows, removed_rows, unused_pcaps


def write_rows_atomic(
    output_csv: Path,
    fieldnames: Sequence[str],
    rows: Sequence[Dict[str, str]],
) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    original_stat = output_csv.stat() if output_csv.exists() else None
    temp_fd, temp_path_text = tempfile.mkstemp(
        dir=output_csv.parent,
        prefix=f".{output_csv.name}.",
        suffix=".tmp",
    )
    temp_path = Path(temp_path_text)
    try:
        with os.fdopen(temp_fd, "w", encoding="utf-8-sig", newline="") as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=fieldnames,
                extrasaction="ignore",
            )
            writer.writeheader()
            writer.writerows(rows)
            csv_file.flush()
            os.fsync(csv_file.fileno())

        if original_stat is not None:
            os.chmod(temp_path, stat.S_IMODE(original_stat.st_mode))
        os.replace(temp_path, output_csv)
    except Exception:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass
        raise


def resolve_cli_path(value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path.resolve()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "根据 id+domain 下已有 pcap 的数量，从单 CSV 中删除对应数量的记录。"
        )
    )
    parser.add_argument(
        "--input-csv",
        default=str(DEFAULT_INPUT_CSV),
        help=f"输入 CSV（默认: {DEFAULT_INPUT_CSV}）",
    )
    parser.add_argument(
        "--result-dir",
        default=str(DEFAULT_RESULT_DIR),
        help=(
            "采集结果根目录；会自动识别其 data 子目录"
            f"（默认: {DEFAULT_RESULT_DIR}）"
        ),
    )
    parser.add_argument(
        "--output",
        default="",
        help="输出 CSV；省略时原地覆盖输入 CSV。",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只预览，不写文件（默认行为）。",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="实际写入 CSV。",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.dry_run and args.apply:
        print("[ERROR] --dry-run 与 --apply 不能同时使用", file=sys.stderr)
        return 2

    input_csv = resolve_cli_path(args.input_csv)
    result_dir = resolve_cli_path(args.result_dir)
    output_csv = resolve_cli_path(args.output) if args.output else input_csv

    if not input_csv.is_file():
        print(f"[ERROR] 输入 CSV 不存在: {input_csv}", file=sys.stderr)
        return 2

    try:
        domain_root = resolve_result_domain_root(result_dir)
        fieldnames, rows, row_keys = read_csv_rows(input_csv)
        domain_directories = index_domain_directories(domain_root)
        pcap_counts = count_existing_pcaps(row_keys, domain_directories)
        kept_rows, removed_rows, unused_pcaps = filter_rows_by_pcap_count(
            rows,
            row_keys,
            pcap_counts,
        )
    except Exception as exc:
        print(f"[ERROR] 处理失败: {exc}", file=sys.stderr)
        return 1

    invalid_rows = sum(1 for key in row_keys if key is None)
    matched_pcaps = sum(pcap_counts.values())
    matched_keys = len(pcap_counts)
    should_write = args.apply
    print(
        "[SUMMARY] "
        f"input={input_csv}, result_dir={result_dir}, domain_root={domain_root}, "
        f"total_rows={len(rows)}, valid_keys={len(rows) - invalid_rows}, "
        f"invalid_rows={invalid_rows}, matched_keys={matched_keys}, "
        f"matched_pcaps={matched_pcaps}, removed={removed_rows}, "
        f"kept={len(kept_rows)}, unused_pcaps={unused_pcaps}, "
        f"apply={should_write}"
    )

    if not should_write:
        print("[DRY-RUN] 未写入 CSV；确认后添加 --apply。")
        return 0

    try:
        write_rows_atomic(output_csv, fieldnames, kept_rows)
    except Exception as exc:
        print(f"[ERROR] 写入 CSV 失败: {exc}", file=sys.stderr)
        return 1

    print(f"[DONE] output={output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
