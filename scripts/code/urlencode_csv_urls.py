#!/usr/bin/env python3

import csv
import sys
from pathlib import Path
from urllib.parse import quote


SCRIPT_DIR = Path(__file__).resolve().parent
SMALL_TOOLS_DIR = SCRIPT_DIR.parent
DEFAULT_INPUT = SMALL_TOOLS_DIR / "result" / "wiki5.csv"
DEFAULT_OUTPUT = SMALL_TOOLS_DIR / "result" / f"{DEFAULT_INPUT.stem}_encode.csv"


def encode_url(url: str) -> str:
    """
    对 URL 中的中文、空格等字符进行 URL Encode，
    保留 URL 本身的结构字符。
    """
    return quote(
        url,
        safe=":/?&=#%[]@!$'()*+,;~-._"
    )


def encode_csv(input_path: Path, output_path: Path):
    if not input_path.exists():
        print(f"错误：输入文件不存在：{input_path}")
        sys.exit(1)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with input_path.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as fin, output_path.open(
        "w",
        encoding="utf-8",
        newline=""
    ) as fout:

        reader = csv.DictReader(fin)

        if not reader.fieldnames:
            raise ValueError("CSV 文件没有表头")

        if "url" not in reader.fieldnames:
            raise ValueError(
                f"CSV 中不存在 url 列，当前列为：{reader.fieldnames}"
            )

        writer = csv.DictWriter(
            fout,
            fieldnames=reader.fieldnames
        )

        writer.writeheader()

        count = 0

        for row in reader:
            url = row.get("url", "").strip()

            if url:
                row["url"] = encode_url(url)

            writer.writerow(row)
            count += 1

    print(f"处理完成：{count} 条")
    print(f"输入文件：{input_path}")
    print(f"输出文件：{output_path}")


if __name__ == "__main__":
    # 不传参数：
    #   使用 DEFAULT_INPUT 和 DEFAULT_OUTPUT
    #
    # 传 1 个参数：
    #   参数作为输入文件，输出自动生成 xxx_encode.csv
    #
    # 传 2 个参数：
    #   分别作为输入文件和输出文件

    if len(sys.argv) == 1:
        input_file = DEFAULT_INPUT
        output_file = DEFAULT_OUTPUT

    elif len(sys.argv) == 2:
        input_file = Path(sys.argv[1]).resolve()
        output_file = input_file.with_name(
            f"{input_file.stem}_encode{input_file.suffix}"
        )

    elif len(sys.argv) == 3:
        input_file = Path(sys.argv[1]).resolve()
        output_file = Path(sys.argv[2]).resolve()

    else:
        print(
            f"用法：\n"
            f"  python3 {Path(sys.argv[0]).name}\n"
            f"  python3 {Path(sys.argv[0]).name} <input.csv>\n"
            f"  python3 {Path(sys.argv[0]).name} <input.csv> <output.csv>"
        )
        sys.exit(1)

    encode_csv(input_file, output_file)