"""欧洲固定 IP Chrome 单 CSV 采集配置。"""

import os

from configs.single_csv._common import PROJECT_ROOT, RUN_ONCE


CONFIG = {
    "SHARED_FIXED_IP_NETWORK": "traffic_ingestor_fixed_ip_europe_net",
    "CONTAINER_COUNT": 2 * 40,
    "BASE_DST": "/netdisk2/pcz/TrafficSimulation/260528/fra",
    "RETRY": 5,
    "DOCKER_NETWORK": "traffic_ingestor_fixed_ip_europe_net",
    "DOCKER_NETWORK_SUBNET_PREFIX": 23,
    "DOCKER_NETWORK_GATEWAY": "172.18.0.1",
    "CONTAINER_IP_START": "172.18.0.2",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts",
        "result",
        "test10_fra.csv",
    ),
}
RUN_POLICY = RUN_ONCE
RUNTIME_NAME = "traffic_capture_single_csv_fixed_ip_europe"
ACTION_PROFILE = "chrome"
