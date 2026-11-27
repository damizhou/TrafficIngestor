"""Rsia 固定 IP Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "SITE_COOLDOWN_SECONDS": 0,
    "CONTAINER_COUNT": 10,
    "BASE_DST": "/netdisk/pcz/test/rsia/20261127/fixed_ip",
    "DOCKER_NETWORK": "traffic_ingestor_geo_sgp_net",
    "DOCKER_NETWORK_OUTPUT_NODES": 10,
    "DOCKER_NETWORK_SUBNET_PREFIX": 24,
    "DOCKER_NETWORK_GATEWAY": "172.18.0.1",
    "CONTAINER_IP_START": "172.18.0.2",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts/result/archive/top200000_ingestor_subpage.csv",
    ),
}
ACTION_PROFILE = "tools/browsers/chrome.py"
