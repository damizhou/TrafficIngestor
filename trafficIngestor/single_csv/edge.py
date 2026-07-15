"""Edge 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "BASE_DST": "/netdisk2/ww/wiki/0325/edge",
    "DOCKER_IMAGE": "chuanzhoupan/trace_spider_edge:260309",
    "BROWSER_NAME": "edge",
    "BROWSER_VERSION_COMMANDS": (("microsoft-edge", "--version"),),
    "CSV_PATH": os.path.join(PROJECT_ROOT, "scripts", "wiki_edge.csv"),
}
RUNTIME_NAME = "traffic_capture_single_csv_edge"
ACTION_PROFILE = "edge"
