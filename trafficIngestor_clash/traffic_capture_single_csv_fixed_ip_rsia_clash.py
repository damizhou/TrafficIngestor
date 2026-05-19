#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
traffic_capture_single_csv_fixed_ip_rsia_clash.py

Read URLs from CSV and capture traffic through Clash nodes from a selected
config/sever_info.py node list.
"""

import os
import sys
import time
from typing import Dict, List

_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from trafficIngestor_clash.base_clash_traffic_ingestor import BaseClashTrafficIngestor


class TrafficIngestor(BaseClashTrafficIngestor):
    """Rsia Clash traffic collector."""

    VPN_INFO_NAME = "vpns_info_rsia"

    CONTAINER_COUNT = 2 * 40
    BASE_DST = "/netdisk2/ww/trojan/top2000/homepage_only/260518/chrome/sgp"
    DOCKER_IMAGE = "chuanzhoupan/trace_spider:250912"
    RETRY = 5
    DELETE_INVALID_FILES_ON_FAIL = False

    CSV_PATH = os.path.join(_project_root, "small_tools", "result", "homeonly_merged_chrome_sgp.csv")

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
                self.remove_first_matching_row_from_csv(
                    self.CSV_PATH,
                    {
                        "id": task.get("row_id", ""),
                        "url": task.get("url", ""),
                        "domain": task.get("domain", ""),
                    },
                )
            except Exception as e:
                self.log(f"ERROR: 删除 CSV 记录失败: {e}")

    def should_continue(self) -> bool:
        return False

    def cleanup(self) -> None:
        time.sleep(60)
        self.remove_containers()


if __name__ == "__main__":
    for i in range(5):
        TrafficIngestor.main()
        time.sleep(3600)
