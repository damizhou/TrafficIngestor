"""默认 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "SHARED_FIXED_IP_NETWORK": "traffic_ingestor_fixed_ip_europe_net",
    "CONTAINER_COUNT": 150,
    "DOCKER_IMAGE": "chuanzhoupan/trace_spider:250912",
    "BASE_DST": "/netdisk/pcz/experiment_E2_fra",
    "DOCKER_NETWORK": "traffic_ingestor_fixed_ip_europe_net",
    "DOCKER_NETWORK_SUBNET_PREFIX": 23,
    "DOCKER_NETWORK_GATEWAY": "172.18.0.1",
    "CONTAINER_IP_START": "172.18.0.2",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "merged_shuffled_E2.csv",
    ),
}
ACTION_PROFILE = "tools/browsers/chrome.py"
