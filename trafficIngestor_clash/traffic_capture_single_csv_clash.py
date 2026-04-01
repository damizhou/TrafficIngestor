#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
traffic_capture_single_csv_clash.py

Read URLs from CSV and capture traffic with a container pool.
Each container is assigned a dedicated Clash node.
"""

import os
import sys
from typing import Dict, List

_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from trafficIngestor_clash.base_clash_traffic_ingestor import BaseClashTrafficIngestor


class TrafficIngestor(BaseClashTrafficIngestor):
    CONTAINER_COUNT = 1
    BASE_DST = "/netdisk2/test"
    DOCKER_IMAGE = "chuanzhoupan/trace_spider:250912"
    RETRY = 5
    DELETE_INVALID_FILES_ON_FAIL = False
    # CSV_PATH = os.path.join(_project_root, "small_tools", "result", "top300000_ingestor.csv")

    CSV_PATH = os.path.join(_project_root, "small_tools", "result", "test.csv")
    def __init__(self):
        super().__init__()
        self._has_jobs = True

    def fetch_jobs(self) -> List[Dict[str, str]]:
        if not self._has_jobs:
            return []

        jobs, _ = self.read_jobs_from_csv(self.CSV_PATH)
        if not jobs:
            self._has_jobs = False
        return jobs

    def on_task_success(self, task: Dict[str, str], paths: Dict[str, str]) -> None:
        # row_id = task.get("row_id", "")
        # if row_id:
        #     try:
        #         self.remove_from_csv(self.CSV_PATH, row_id)
        #     except Exception as e:
        #         self.log(f"ERROR: 删除 CSV 记录失败: {e}")
        pass

    def on_task_failed(self, task: Dict[str, str], error: str) -> None:
        pass

    def should_continue(self) -> bool:
        return False

    def cleanup(self) -> None:
        import time

        time.sleep(60)
        self.remove_containers()


if __name__ == "__main__":
    TrafficIngestor.main()
