"""默认 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "SHARED_FIXED_IP_NETWORK": "traffic_ingestor_experiment_e1_sgp_net",
    "DOCKER_IMAGE": "chuanzhoupan/trace_spider:250912",
    "BASE_DST": "/netdisk/pcz/experiment_E1_sgp",
    "CONTAINER_COUNT": 900,
    "DOCKER_NETWORK": "traffic_ingestor_experiment_e1_sgp_net",
    "DOCKER_NETWORK_SUBNET_PREFIX": 22,
    "DOCKER_NETWORK_GATEWAY": "172.18.4.1",
    "CONTAINER_IP_START": "172.18.4.2",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "merged_shuffled_E1.csv",
    ),
}
ACTION_PROFILE = "tools/browsers/chrome.py"
