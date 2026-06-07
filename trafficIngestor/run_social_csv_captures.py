#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import sys
from pathlib import Path


SCRIPTS = [
    "traffic_capture_single_csv_bsky.py",
    "traffic_capture_single_csv_mastodon.py",
    "traffic_capture_single_csv_threads.py",
]


def main() -> int:
    work_dir = Path(__file__).resolve().parent

    for script in SCRIPTS:
        print(f"\n===== Running {script} =====", flush=True)
        result = subprocess.run([sys.executable, script], cwd=work_dir)
        if result.returncode != 0:
            print(f"ERROR: {script} failed with exit code {result.returncode}", flush=True)
            return result.returncode

    print("\nAll capture scripts finished.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
