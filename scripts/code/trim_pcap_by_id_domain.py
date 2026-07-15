#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Limit pcap copies per id+domain group and keep side artifacts aligned.

Default behavior is dry-run. Pass --apply to delete files beyond the limit.
When pcap files are trimmed, matching ssl_key/content/html/screenshot files are
deleted at the same time. If pcap files were already trimmed by an older version,
use --clean-orphans to delete side artifacts that no longer have a matching pcap.

Examples:
- Preview current Firefox output:
  python scripts/code/trim_pcap_by_id_domain.py
- Delete extra pcaps:
  python scripts/code/trim_pcap_by_id_domain.py --apply
- Delete side artifacts left after a previous pcap-only trim:
  python scripts/code/trim_pcap_by_id_domain.py --clean-orphans --apply
- Use a custom output directory:
  python scripts/code/trim_pcap_by_id_domain.py --base-dir /netdisk/mlj/20260626/temp --limit 120
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List, Optional, Sequence, Set, Tuple


DEFAULT_BASE_DIRS = [
    "/netdisk/mlj/20260626/temp",
    "/netdisk/mlj/20260626/temp_disableML-KEM",
]
DEFAULT_LIMIT = 120
SIDE_ARTIFACT_SUFFIXES = {
    "ssl_key": "_ssl_key.log",
    "content": ".text",
    "html": ".html",
    "screenshot": ".png",
}

ARTIFACT_NAME_RE = re.compile(
    r"^(?P<url_id>[^_]+)_(?P<timestamp>\d{8}_\d{2}_\d{2}_\d{2})_"
    r"(?:(?:chrome|edge|firefox|chromium)_?\d+_)?(?P<domain>.+)\.pcap$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class PcapRecord:
    path: Path
    url_id: str
    domain: str
    timestamp: str

    @property
    def group_key(self) -> Tuple[str, str]:
        return self.url_id, self.domain

    @property
    def sort_key(self) -> Tuple[int, str, str]:
        if self.timestamp:
            return 0, self.timestamp, self.path.name
        return 1, self.path.name, str(self.path)


@dataclass(frozen=True)
class DeleteCandidate:
    path: Path
    category: str
    reason: str


def normalize_domain(value: str) -> str:
    return (value or "").strip().lower().strip(".")


def parse_filename(filename: str) -> Tuple[Optional[str], str, str]:
    matched = ARTIFACT_NAME_RE.match(filename)
    if matched:
        return (
            matched.group("url_id"),
            matched.group("timestamp"),
            normalize_domain(matched.group("domain")),
        )

    if not filename.lower().endswith(".pcap"):
        return None, "", ""
    stem = filename[:-5]
    parts = stem.split("_", 1)
    if len(parts) != 2 or not parts[0]:
        return None, "", ""
    return parts[0], "", ""


def domain_from_standard_path(path: Path) -> str:
    parent = path.parent
    if parent.name.lower() == "pcap" and parent.parent != parent:
        return normalize_domain(parent.parent.name)
    return ""


def side_artifact_path(pcap_path: Path, category: str, suffix: str) -> Path:
    domain_root = pcap_path.parent.parent
    return domain_root / category / f"{pcap_path.stem}{suffix}"


def side_artifact_paths_for_pcap(pcap_path: Path) -> List[Tuple[str, Path]]:
    return [
        (category, side_artifact_path(pcap_path, category, suffix))
        for category, suffix in SIDE_ARTIFACT_SUFFIXES.items()
    ]


def strip_known_suffix(filename: str, suffix: str) -> Optional[str]:
    if not filename.endswith(suffix):
        return None
    return filename[: -len(suffix)]


def iter_pcap_files(base_dir: Path) -> Iterable[Path]:
    for root, dirs, files in os.walk(base_dir):
        root_path = Path(root)
        if root_path.name.lower() != "pcap":
            continue

        dirs[:] = []
        for filename in files:
            if filename.lower().endswith(".pcap"):
                yield root_path / filename


def iter_side_artifact_files(base_dir: Path) -> Iterable[Tuple[str, Path]]:
    artifact_dirs = set(SIDE_ARTIFACT_SUFFIXES)
    for root, dirs, files in os.walk(base_dir):
        root_path = Path(root)
        category = root_path.name.lower()
        if category not in artifact_dirs:
            continue

        dirs[:] = []
        suffix = SIDE_ARTIFACT_SUFFIXES[category]
        for filename in files:
            if filename.endswith(suffix):
                yield category, root_path / filename


def scan_base_dir(base_dir: Path) -> Tuple[Dict[Tuple[str, str], List[PcapRecord]], int, int, int]:
    groups: DefaultDict[Tuple[str, str], List[PcapRecord]] = defaultdict(list)
    parsed = 0
    skipped = 0
    domain_mismatch = 0

    for path in iter_pcap_files(base_dir):
        url_id, timestamp, filename_domain = parse_filename(path.name)
        if not url_id:
            skipped += 1
            continue

        path_domain = domain_from_standard_path(path)
        domain = path_domain or filename_domain
        if not domain:
            skipped += 1
            continue
        if path_domain and filename_domain and path_domain != filename_domain:
            domain_mismatch += 1

        record = PcapRecord(
            path=path,
            url_id=str(url_id).strip(),
            domain=domain,
            timestamp=timestamp,
        )
        groups[record.group_key].append(record)
        parsed += 1

    return dict(groups), parsed, skipped, domain_mismatch


def build_pcap_stems_by_domain(base_dir: Path) -> Dict[str, Set[str]]:
    stems_by_domain: DefaultDict[str, Set[str]] = defaultdict(set)
    for path in iter_pcap_files(base_dir):
        _, _, filename_domain = parse_filename(path.name)
        domain = domain_from_standard_path(path) or filename_domain
        if domain:
            stems_by_domain[domain].add(path.stem)
    return dict(stems_by_domain)


def select_extra_records(
    groups: Dict[Tuple[str, str], List[PcapRecord]],
    limit: int,
) -> Tuple[List[Tuple[Tuple[str, str], int, int]], List[PcapRecord]]:
    over_limit: List[Tuple[Tuple[str, str], int, int]] = []
    to_delete: List[PcapRecord] = []

    for key, records in sorted(groups.items(), key=lambda item: item[0]):
        ordered = sorted(records, key=lambda item: item.sort_key)
        extra = ordered[limit:]
        if not extra:
            continue
        over_limit.append((key, len(records), len(extra)))
        to_delete.extend(extra)

    return over_limit, to_delete


def add_candidate(
    candidates: List[DeleteCandidate],
    seen_paths: Set[str],
    path: Path,
    category: str,
    reason: str,
) -> None:
    key = str(path.absolute())
    if key in seen_paths:
        return
    seen_paths.add(key)
    candidates.append(DeleteCandidate(path=path, category=category, reason=reason))


def build_trim_delete_candidates(records: Sequence[PcapRecord]) -> List[DeleteCandidate]:
    candidates: List[DeleteCandidate] = []
    seen_paths: Set[str] = set()

    for record in records:
        add_candidate(candidates, seen_paths, record.path, "pcap", "trim")
        for category, path in side_artifact_paths_for_pcap(record.path):
            if path.exists():
                add_candidate(candidates, seen_paths, path, category, "trim_side_artifact")

    return candidates


def find_orphan_side_artifacts(base_dir: Path) -> List[DeleteCandidate]:
    pcap_stems_by_domain = build_pcap_stems_by_domain(base_dir)
    candidates: List[DeleteCandidate] = []
    seen_paths: Set[str] = set()

    for category, path in iter_side_artifact_files(base_dir):
        parent = path.parent
        domain = normalize_domain(parent.parent.name) if parent.parent != parent else ""
        suffix = SIDE_ARTIFACT_SUFFIXES[category]
        stem = strip_known_suffix(path.name, suffix)
        if not domain or not stem:
            continue
        if stem in pcap_stems_by_domain.get(domain, set()):
            continue
        add_candidate(candidates, seen_paths, path, category, "orphan_side_artifact")

    return candidates


def count_candidates_by_category(candidates: Sequence[DeleteCandidate]) -> Dict[str, int]:
    counts: DefaultDict[str, int] = defaultdict(int)
    for candidate in candidates:
        counts[candidate.category] += 1
    return dict(counts)


def format_category_counts(candidates: Sequence[DeleteCandidate]) -> str:
    counts = count_candidates_by_category(candidates)
    parts = [f"{category}={counts.get(category, 0)}" for category in ["pcap", *SIDE_ARTIFACT_SUFFIXES]]
    return ", ".join(parts)


def delete_candidates(candidates: Sequence[DeleteCandidate], verbose: bool) -> Tuple[int, int]:
    deleted = 0
    failed = 0
    for candidate in candidates:
        try:
            candidate.path.unlink()
            deleted += 1
            if verbose:
                print(f"[DELETE] {candidate.category} {candidate.path}")
        except Exception as exc:
            failed += 1
            print(f"[WARN] failed to delete {candidate.path}: {exc}", file=sys.stderr)
    return deleted, failed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Keep only the first N pcap files for each id+domain group."
    )
    parser.add_argument(
        "--base-dir",
        dest="base_dirs",
        action="append",
        default=[],
        help=(
            "Dataset root containing <domain>/pcap/*.pcap. "
            "Can be provided multiple times. Defaults to current Firefox output dirs."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Max pcap count to keep for each id+domain group (default: {DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Actually delete extra pcap files and matching side artifacts. "
            "Without this flag only prints a dry-run summary."
        ),
    )
    parser.add_argument(
        "--clean-orphans",
        action="store_true",
        help=(
            "Delete ssl_key/content/html/screenshot files that no longer have a matching pcap. "
            "Use this after an older pcap-only --apply run."
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print every deleted file when --apply is used.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.limit < 1:
        print(f"[ERROR] invalid --limit: {args.limit}", file=sys.stderr)
        return 2

    base_dirs = [Path(value).expanduser() for value in (args.base_dirs or DEFAULT_BASE_DIRS)]
    total_parsed = 0
    total_skipped = 0
    total_domain_mismatch = 0
    total_groups = 0
    total_over_groups = 0
    total_extra_pcaps = 0
    all_to_delete: List[DeleteCandidate] = []
    invalid_base_dirs = 0

    for base_dir in base_dirs:
        if not base_dir.exists() or not base_dir.is_dir():
            invalid_base_dirs += 1
            print(f"[ERROR] invalid --base-dir: {base_dir}", file=sys.stderr)
            continue

        groups, parsed, skipped, domain_mismatch = scan_base_dir(base_dir)
        over_limit, pcap_records_to_trim = select_extra_records(groups, args.limit)
        trim_delete_candidates = build_trim_delete_candidates(pcap_records_to_trim)
        orphan_delete_candidates = find_orphan_side_artifacts(base_dir) if args.clean_orphans else []
        to_delete = trim_delete_candidates + orphan_delete_candidates

        total_parsed += parsed
        total_skipped += skipped
        total_domain_mismatch += domain_mismatch
        total_groups += len(groups)
        total_over_groups += len(over_limit)
        total_extra_pcaps += len(pcap_records_to_trim)
        all_to_delete.extend(to_delete)

        print(
            f"[BASE] {base_dir} parsed={parsed}, groups={len(groups)}, "
            f"over_limit_groups={len(over_limit)}, extra_pcaps={len(pcap_records_to_trim)}, "
            f"delete_targets={len(to_delete)} ({format_category_counts(to_delete)}), "
            f"orphan_targets={len(orphan_delete_candidates)}, skipped={skipped}, "
            f"domain_mismatch={domain_mismatch}"
        )
        for (url_id, domain), count, extra_count in over_limit:
            print(f"[TRIM] id={url_id}, domain={domain}, count={count}, delete={extra_count}")

    if invalid_base_dirs:
        return 2

    print(
        f"[SUMMARY] limit={args.limit}, base_dirs={len(base_dirs)}, parsed={total_parsed}, "
        f"groups={total_groups}, over_limit_groups={total_over_groups}, "
        f"extra_pcaps={total_extra_pcaps}, delete_targets={len(all_to_delete)} "
        f"({format_category_counts(all_to_delete)}), skipped={total_skipped}, "
        f"domain_mismatch={total_domain_mismatch}, clean_orphans={args.clean_orphans}, "
        f"apply={args.apply}"
    )

    if not args.apply:
        print("[DRY-RUN] no files deleted. Re-run with --apply to delete listed targets.")
        return 0

    deleted, failed = delete_candidates(all_to_delete, args.verbose)
    print(f"[DONE] deleted={deleted}, failed={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
