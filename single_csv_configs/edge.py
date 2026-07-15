"""Edge 单 CSV 采集配置。"""

import os

from single_csv_configs._common import PROJECT_ROOT, RUN_UP_TO_FIVE


CONFIG = {
    "HOST_CODE_PATH": os.path.join(
        PROJECT_ROOT,
        "traffic_capture_single_csv_edge",
    ),
    "BASE_DST": "/netdisk2/ww/wiki/0325/edge",
    "DOCKER_IMAGE": "chuanzhoupan/trace_spider_edge:260309",
    "BROWSER_NAME": "edge",
    "BROWSER_VERSION_COMMANDS": (("microsoft-edge", "--version"),),
    "RETRY": 5,
    "CSV_PATH": os.path.join(PROJECT_ROOT, "small_tools", "wiki_edge.csv"),
}
RUN_POLICY = RUN_UP_TO_FIVE
RUNTIME_NAME = "traffic_capture_single_csv_edge"
ACTION_PROFILE = "edge"
