"""默认 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "DOCKER_IMAGE": "chuanzhoupan/trace_spider:250912",
    "CONTAINER_COUNT": 150,
    "BASE_DST": "/netdisk/cl/20260820/madrid/chrome",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "cl_url_madrid.csv",
    ),
    "DOCKER_NETWORK": "traffic_ingestor_fixed_ip_madrid_net",
    "DOCKER_NETWORK_SUBNET_PREFIX": 22,
    "DOCKER_NETWORK_GATEWAY": "172.23.0.1",
    "CONTAINER_IP_START": "172.23.0.2",
}
ACTION_PROFILE = "tools/browsers/chrome.py"
