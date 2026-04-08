#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
traffic_capture_single_csv_edge_clash.py

Read URLs from CSV and capture traffic with Edge containers routed through Clash.
"""

from datetime import datetime
import os
import sys
from typing import Dict, List

_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from trafficIngestor_clash.base_clash_traffic_ingestor import BaseClashTrafficIngestor

BASE_DST_DATE = datetime.now().strftime("%y%m%d")


class TrafficIngestor(BaseClashTrafficIngestor):
    CONTAINER_COUNT = 15 * 10
    BASE_DST = f"/netdisk2/ww/trojan/top2000/homepage_only/{BASE_DST_DATE}/edge"
    DOCKER_IMAGE = "chuanzhoupan/trace_spider_edge:260309"
    RETRY = 5
    CSV_PATH = os.path.join(_project_root, "small_tools", "result", "homeonly_merged_edge.csv")

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
        row_id = task.get("row_id", "")
        if row_id:
            try:
                self.remove_from_csv(self.CSV_PATH, row_id)
            except Exception as e:
                self.log(f"ERROR: 删除 CSV 记录失败: {e}")

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
    TrafficIngestor.main()
    TrafficIngestor.main()
    TrafficIngestor.main()
    TrafficIngestor.main()
