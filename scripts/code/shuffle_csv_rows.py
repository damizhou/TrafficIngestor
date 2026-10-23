#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""随机打乱 CSV 数据行，保留表头和每行字段内容。"""

from __future__ import annotations

import argparse
import csv
import random
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = SCRIPT_DIR.parent
DEFAULT_INPUT = SCRIPTS_DIR / "result" / "dailymirror&nytimes.csv"
DEFAULT_OUTPUT = SCRIPTS_DIR / "result" / "dailymirror&nytimes_shuffled.csv"


def resolve_path(path: Path) -> Path:
    path = path.expanduser()
    return path if path.is_absolute() else (Path.cwd() / path).resolve()


def shuffle_csv(input_path: Path, output_path: Path, seed: int | None = None) -> int:
    input_path = resolve_path(input_path)
    output_path = resolve_path(output_path)
    if not input_path.is_file():
        raise FileNotFoundError(f"输入文件不存在：{input_path}")
    if input_path == output_path:
        raise ValueError("输入和输出文件不能相同；如需覆盖，请先指定临时输出文件并确认结果")

    with input_path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames:
            raise ValueError("CSV 文件缺少表头")
        header = list(reader.fieldnames)
        rows = list(reader)

    randomizer = random.Random(seed)
    randomizer.shuffle(rows)

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
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help=f"输入 CSV，默认：{DEFAULT_INPUT}")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help=f"输出 CSV，默认：{DEFAULT_OUTPUT}")
    parser.add_argument("--seed", type=int, default=None, help="可选随机种子；指定后可复现相同顺序")
    args = parser.parse_args()
    count = shuffle_csv(args.input, args.output, args.seed)
    print(f"已打乱 {count} 条数据行")
    print(f"输入文件：{resolve_path(args.input)}")
    print(f"输出文件：{resolve_path(args.output)}")


if __name__ == "__main__":
    main()
