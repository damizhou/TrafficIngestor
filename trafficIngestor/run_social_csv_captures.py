#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import sys
from pathlib import Path


CONFIG_PATHS = [
    "single_csv_configs/bsky.py",
    "single_csv_configs/mastodon.py",
    "single_csv_configs/threads.py",
]


def main() -> int:
    project_root = Path(__file__).resolve().parent.parent

    for config_path in CONFIG_PATHS:
        print(f"\n===== Running config: {config_path} =====", flush=True)
        result = subprocess.run(
            [
                sys.executable,
                "trafficIngestor/single_csv_profiles.py",
                config_path,
            ],
            cwd=project_root,
        )
        if result.returncode != 0:
            print(
                "ERROR: config "
                f"{config_path} failed with exit code {result.returncode}",
                flush=True,
            )
            return result.returncode

    print("\nAll capture scripts finished.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
