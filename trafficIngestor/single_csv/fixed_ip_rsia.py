"""Rsia 固定 IP Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "SHARED_FIXED_IP_NETWORK": "traffic_ingestor_geo_sgp_net",
    "CONTAINER_COUNT": 20,
    "BASE_DST": "/netdisk2/ww/wiki/20261112/chrome/sgp",
    "DOCKER_NETWORK": "traffic_ingestor_geo_sgp_net",
    "DOCKER_NETWORK_OUTPUT_NODES": 2,
    "DOCKER_NETWORK_SUBNET_PREFIX": 24,
    "DOCKER_NETWORK_GATEWAY": "172.18.0.1",
    "CONTAINER_IP_START": "172.18.0.2",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "ww",
        "wiki20x3.csv",
    ),
}
ACTION_PROFILE = "tools/browsers/chrome.py"
