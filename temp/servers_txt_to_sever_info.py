#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将 temp/servers.txt 中的节点链接转换为 vpns_info 的 Python 配置格式。

默认打印到 stdout；如需写入文件，显式传入 --output。
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Iterable
from urllib.parse import unquote, urlsplit


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "temp" / "servers.txt"

PROXY_TYPE_MAP = {
    "trojan-go": "trojan",
    "trojan": "trojan",
}

IGNORED_FRAGMENT_NAMES = {
    "trojan",
    "trojan-go",
    "ss",
    "ssr",
    "vmess",
    "vless",
}

FIELD_ORDER = (
    "name",
    "type",
    "server",
    "port",
    "password",
    "sni",
    "udp",
    "skip-cert-verify",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert servers.txt links into vpns_info Python config format."
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_INPUT),
        help=f"input file path, default: {DEFAULT_INPUT}",
    )
    parser.add_argument(
        "--output",
        help="optional output file path; prints to stdout when omitted",
    )
    return parser.parse_args()


def normalize_name(hostname: str, fragment: str, index: int) -> str:
    candidate = fragment.strip()
    if not candidate or candidate.lower() in IGNORED_FRAGMENT_NAMES:
        candidate = hostname.split(".", 1)[0]
    candidate = re.sub(r"[^0-9A-Za-z_]+", "_", candidate)
    candidate = re.sub(r"_+", "_", candidate).strip("_")
    return candidate or f"vpn_{index}"


def parse_server_lines(lines: Iterable[str]) -> list[dict[str, object]]:
    vpns_info: list[dict[str, object]] = []

    for index, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        split = urlsplit(line)
        scheme = split.scheme.lower()
        proxy_type = PROXY_TYPE_MAP.get(scheme, scheme)
        hostname = split.hostname or ""
        port = str(split.port or "")
        password = unquote(split.username or "")
        fragment = unquote(split.fragment or "")

        if not hostname or not port or not password:
            raise ValueError(f"line {index} is missing host/port/password: {line}")

        vpns_info.append(
            {
                "name": normalize_name(hostname, fragment, index),
                "type": proxy_type,
                "server": hostname,
                "port": port,
                "password": password,
                "sni": hostname,
                "udp": True,
                "skip-cert-verify": True,
            }
        )

    return vpns_info


def py_literal(value: object) -> str:
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, bool):
        return "True" if value else "False"
    return repr(value)


def render_python(vpns_info: list[dict[str, object]]) -> str:
    lines = ["vpns_info = ["]

    for item in vpns_info:
        lines.append("    {")
        for field in FIELD_ORDER:
            lines.append(f'        "{field}": {py_literal(item[field])},')
        lines.append("    },")

    lines.append("]")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.is_absolute():
        input_path = PROJECT_ROOT / input_path

    if not input_path.exists():
        raise FileNotFoundError(f"input file not found: {input_path}")

    vpns_info = parse_server_lines(input_path.read_text(encoding="utf-8").splitlines())
    content = render_python(vpns_info)

    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = PROJECT_ROOT / output_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content, encoding="utf-8")
        print(f"written {len(vpns_info)} items to {output_path}")
        return

    print(content, end="")


if __name__ == "__main__":
    main()
