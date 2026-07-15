#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Decrypt Trojan outer TLS traffic from a pcap + SSLKEYLOGFILE pair, then strip the
Trojan request header from the first matching decrypted client segment.

This tool depends on TShark's `follow,tls,hex,<stream>` output. It writes:
- the raw TShark follow output for each matching TLS stream
- the decrypted Trojan request segment bytes
- the inner payload bytes after removing the Trojan header
- a JSON manifest with stream metadata
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Optional

from trojan_unwrap import parse_trojan_request


NODE_RE = re.compile(r"^Node\s+(\d+):\s+(.+)$")
HEX_LINE_RE = re.compile(r"^\s*([0-9A-Fa-f]{8})\s{2,}(.*)$")


def find_tshark(explicit: Optional[str]) -> str:
    candidates = []
    if explicit:
        candidates.append(explicit)
    candidates.extend(
        [
            "tshark",
            r"C:\Program Files\Wireshark\tshark.exe",
        ]
    )
    for candidate in candidates:
        resolved = shutil.which(candidate) if candidate == "tshark" else candidate
        if resolved and Path(resolved).exists():
            return str(resolved)
    raise FileNotFoundError("tshark not found; pass --tshark or install Wireshark/TShark")


def run_tshark(tshark: str, args: list[str], cwd: Path) -> str:
    cp = subprocess.run(
        [tshark, *args],
        cwd=str(cwd),
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if cp.returncode != 0:
        raise RuntimeError(f"tshark failed ({cp.returncode}): {cp.stderr.strip() or cp.stdout.strip()}")
    return cp.stdout


def list_tls_client_streams(tshark: str, pcap: Path, keylog: Path) -> list[int]:
    output = run_tshark(
        tshark,
        [
            "-r",
            str(pcap),
            "-o",
            f"tls.keylog_file:{keylog}",
            "-Y",
            "tls.handshake.type==1",
            "-T",
            "fields",
            "-e",
            "tcp.stream",
        ],
        cwd=pcap.parent,
    )
    streams = []
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            streams.append(int(line))
        except ValueError:
            continue
    return sorted(set(streams))


def parse_follow_tls_hex(output: str) -> tuple[dict[int, str], list[bytes]]:
    nodes: dict[int, str] = {}
    segments: list[bytes] = []
    current = bytearray()
    previous_offset: Optional[int] = None

    for raw_line in output.splitlines():
        line = raw_line.rstrip("\n")
        node_match = NODE_RE.match(line.strip())
        if node_match:
            nodes[int(node_match.group(1))] = node_match.group(2)
            continue

        hex_match = HEX_LINE_RE.match(line)
        if not hex_match:
            continue

        offset = int(hex_match.group(1), 16)
        remainder = hex_match.group(2).rstrip()
        parts = re.split(r"\s{2,}", remainder)
        if len(parts) >= 2:
            hex_parts = parts[:-1]
        else:
            hex_parts = parts

        tokens: list[str] = []
        for part in hex_parts:
            for token in part.split():
                if re.fullmatch(r"[0-9A-Fa-f]{2}", token):
                    tokens.append(token)
        if not tokens:
            continue

        if previous_offset is not None and offset < previous_offset and current:
            segments.append(bytes(current))
            current = bytearray()

        chunk = bytes.fromhex("".join(tokens))
        current.extend(chunk)
        previous_offset = offset + len(chunk)

    if current:
        segments.append(bytes(current))

    return nodes, segments


def build_output_dir(pcap: Path, outdir: Optional[Path]) -> Path:
    if outdir is not None:
        return outdir
    return pcap.parent / f"{pcap.stem}_trojan_outer_unpack"


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def process_stream(
    tshark: str,
    pcap: Path,
    keylog: Path,
    stream_id: int,
    outdir: Path,
) -> Optional[dict]:
    follow_text = run_tshark(
        tshark,
        [
            "-r",
            str(pcap),
            "-o",
            f"tls.keylog_file:{keylog}",
            "-q",
            "-z",
            f"follow,tls,hex,{stream_id}",
        ],
        cwd=pcap.parent,
    )
    nodes, segments = parse_follow_tls_hex(follow_text)
    if not segments:
        return None

    trojan_segment_index: Optional[int] = None
    parsed_trojan: Optional[dict] = None
    for index, segment in enumerate(segments):
        try:
            parsed = parse_trojan_request(segment)
        except ValueError:
            continue
        trojan_segment_index = index
        parsed_trojan = parsed
        break

    if trojan_segment_index is None or parsed_trojan is None:
        return None

    prefix = f"{pcap.stem}.stream_{stream_id}"
    follow_path = outdir / f"{prefix}.follow_tls.hex.txt"
    request_path = outdir / f"{prefix}.trojan_request_segment.bin"
    inner_path = outdir / f"{prefix}.inner_first_payload.bin"
    manifest_path = outdir / f"{prefix}.json"

    follow_path.write_text(follow_text, encoding="utf-8")
    request_path.write_bytes(segments[trojan_segment_index])
    inner_path.write_bytes(parsed_trojan["inner_payload"])  # type: ignore[index]

    manifest = {
        "pcap": str(pcap),
        "keylog": str(keylog),
        "stream_id": stream_id,
        "nodes": nodes,
        "segment_count": len(segments),
        "trojan_request_segment_index": trojan_segment_index,
        "trojan_request_segment_size": len(segments[trojan_segment_index]),
        "host": parsed_trojan["host"],
        "port": parsed_trojan["port"],
        "command": parsed_trojan["command"],
        "command_name": parsed_trojan["command_name"],
        "atyp": parsed_trojan["atyp"],
        "atyp_name": parsed_trojan["atyp_name"],
        "payload_offset": parsed_trojan["payload_offset"],
        "inner_payload_length": parsed_trojan["payload_length"],
        "follow_path": str(follow_path),
        "trojan_request_segment_path": str(request_path),
        "inner_payload_path": str(inner_path),
        "note": (
            "inner_first_payload.bin is only the first decrypted client segment after removing "
            "the Trojan header. It is typically the inner website TLS ClientHello for HTTPS targets."
        ),
    }
    write_json(manifest_path, manifest)
    manifest["manifest_path"] = str(manifest_path)
    return manifest


def iter_manifests(
    tshark: str,
    pcap: Path,
    keylog: Path,
    stream_ids: Iterable[int],
    outdir: Path,
) -> list[dict]:
    results = []
    for stream_id in stream_ids:
        manifest = process_stream(tshark, pcap, keylog, stream_id, outdir)
        if manifest is not None:
            results.append(manifest)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Decrypt Trojan outer TLS traffic from a pcap/keylog pair.")
    parser.add_argument("pcap", help="Path to the Trojan outer pcap")
    parser.add_argument("keylog", help="Path to the matching Trojan outer SSLKEYLOGFILE")
    parser.add_argument("--stream", type=int, help="Optional explicit tls.stream id to process")
    parser.add_argument("--outdir", help="Output directory; defaults to <pcap>.stem + _trojan_outer_unpack")
    parser.add_argument("--tshark", help="Optional path to tshark")
    args = parser.parse_args()

    pcap = Path(args.pcap).resolve()
    keylog = Path(args.keylog).resolve()
    outdir = build_output_dir(pcap, Path(args.outdir).resolve() if args.outdir else None)
    outdir.mkdir(parents=True, exist_ok=True)

    tshark = find_tshark(args.tshark)

    if args.stream is not None:
        stream_ids = [args.stream]
    else:
        stream_ids = list_tls_client_streams(tshark, pcap, keylog)

    if not stream_ids:
        raise SystemExit("No TLS client streams found in the pcap")

    manifests = iter_manifests(tshark, pcap, keylog, stream_ids, outdir)
    summary = {
        "pcap": str(pcap),
        "keylog": str(keylog),
        "tshark": tshark,
        "outdir": str(outdir),
        "stream_ids_considered": stream_ids,
        "matched_stream_count": len(manifests),
        "matched_streams": manifests,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
