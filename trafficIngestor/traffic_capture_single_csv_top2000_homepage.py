#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
x_traffic.py

从 CSV 读取 X(Twitter) 用户 URL，使用容器池并发采集流量数据。
"""

import os
import sys
from typing import List, Dict

# 添加项目根目录到路径
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from trafficIngestor.base_traffic_ingestor import BaseTrafficIngestor


class TrafficIngestor(BaseTrafficIngestor):
    """流量采集器"""

    # ============== 配置 ==============
    CONTAINER_COUNT = 15 * 10
    HOST_CODE_PATH = os.path.join(_project_root, 'traffic_capture_single_csv_top2000_homepage')
    BASE_DST = '/netdisk/ww/top2000/homepage_only'
    DOCKER_IMAGE = "chuanzhoupan/trace_spider:250912"
    RETRY = 5

    # CSV 必须包含表头，字段名（大小写不敏感）：
    # - id: 唯一标识，用于任务完成/失败后从 CSV 删除对应行
    # - url: 访问地址（建议完整 URL，包含 http:// 或 https://）
    # - domain: 域名（用于日志与流量采集标识）
    # 示例：
    # id,url,domain
    # 1,https://vox-cdn.com,vox-cdn.com
    CSV_PATH = os.path.join(_project_root, 'small_tools', 'homeonly_merged.csv')

    def __init__(self):
        super().__init__()
        self._has_jobs = True

    def fetch_jobs(self) -> List[Dict[str, str]]:
        """从 CSV 读取任务"""
        if not self._has_jobs:
            return []

        jobs, _ = self.read_jobs_from_csv(self.CSV_PATH)
        if not jobs:
            self._has_jobs = False
        return jobs

    def on_task_success(self, task: Dict[str, str], paths: Dict[str, str]) -> None:
        """任务成功后从 CSV 删除记录"""
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
        """只运行一次"""
        return False

    def cleanup(self) -> None:
        """清理容器"""
        import time
        time.sleep(60)
        self.remove_containers()


if __name__ == "__main__":
    TrafficIngestor.main()
