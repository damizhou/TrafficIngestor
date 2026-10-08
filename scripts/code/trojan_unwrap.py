#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parse the first decrypted Trojan client request and strip its protocol header.

Typical usage:
1. Use Wireshark/TShark with the Trojan outer TLS key log to decrypt the outer pcap.
2. Export the first client-to-server decrypted payload as raw bytes or plain hex text.
3. Run this script to parse the Trojan header and write the inner TCP payload.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
from pathlib import Path


CRLF = b"\r\n"
HEX_DIGITS = set(b"0123456789abcdefABCDEF")
CMD_NAMES = {
    0x01: "CONNECT",
    0x03: "UDP_ASSOCIATE",
}
ATYP_NAMES = {
    0x01: "IPv4",
    0x03: "DOMAIN",
    0x04: "IPv6",
}


def normalize_payload(raw: bytes) -> bytes:
    """Accept raw binary or whitespace-separated hex text."""
    compact = b"".join(raw.split())
    if compact and len(compact) % 2 == 0 and all(byte in HEX_DIGITS for byte in compact):
        return bytes.fromhex(compact.decode("ascii"))
    return raw


def _read_addr(payload: bytes, offset: int, atyp: int) -> tuple[str, int]:
    if atyp == 0x01:
        end = offset + 4
        return str(ipaddress.IPv4Address(payload[offset:end])), end
    if atyp == 0x03:
        if offset >= len(payload):
            raise ValueError("domain length byte missing")
        size = payload[offset]
        start = offset + 1
        end = start + size
        if end > len(payload):
            raise ValueError("domain bytes truncated")
        return payload[start:end].decode("utf-8", errors="replace"), end
    if atyp == 0x04:
        end = offset + 16
        return str(ipaddress.IPv6Address(payload[offset:end])), end
    raise ValueError(f"unsupported ATYP: 0x{atyp:02x}")


def parse_trojan_request(payload: bytes) -> dict[str, object]:
    if len(payload) < 60:
        raise ValueError("payload too short to contain a Trojan request")
    if payload[56:58] != CRLF:
        raise ValueError("expected CRLF after 56-byte password digest")

    password_digest = payload[:56].decode("ascii", errors="strict")
    if any(ch not in "0123456789abcdef" for ch in password_digest.lower()):
        raise ValueError("password digest is not 56 hex characters")

    cursor = 58
    if cursor + 4 > len(payload):
        raise ValueError("Trojan request header truncated before SOCKS5 target")

    cmd = payload[cursor]
    cursor += 1
    atyp = payload[cursor]
    cursor += 1
    host, cursor = _read_addr(payload, cursor, atyp)

    if cursor + 2 > len(payload):
        raise ValueError("destination port truncated")
    port = int.from_bytes(payload[cursor:cursor + 2], "big")
    cursor += 2

    if payload[cursor:cursor + 2] != CRLF:
        raise ValueError("expected CRLF after destination address and port")
    cursor += 2

    inner_payload = payload[cursor:]
    return {
        "password_digest": password_digest,
        "command": cmd,
        "command_name": CMD_NAMES.get(cmd, f"UNKNOWN_0x{cmd:02x}"),
        "atyp": atyp,
        "atyp_name": ATYP_NAMES.get(atyp, f"UNKNOWN_0x{atyp:02x}"),
        "host": host,
        "port": port,
        "payload_offset": cursor,
        "payload_length": len(inner_payload),
        "inner_payload": inner_payload,
    }


def build_default_output_path(input_path: Path) -> Path:
    if input_path.suffix:
        return input_path.with_suffix(f"{input_path.suffix}.inner.bin")
    return input_path.with_name(f"{input_path.name}.inner.bin")


def main() -> int:
    parser = argparse.ArgumentParser(description="Strip a Trojan request header from decrypted client payload bytes.")
    parser.add_argument("input", help="Raw binary or plain hex text exported from a decrypted Trojan client stream")
    parser.add_argument(
        "--payload-out",
        help="Optional path for the inner TCP payload after the Trojan header; defaults to <input>.inner.bin",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    raw = input_path.read_bytes()
    payload = normalize_payload(raw)
    parsed = parse_trojan_request(payload)

    payload_out = Path(args.payload_out) if args.payload_out else build_default_output_path(input_path)
    payload_out.write_bytes(parsed.pop("inner_payload"))  # type: ignore[arg-type]

    print(json.dumps({**parsed, "payload_out": str(payload_out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
