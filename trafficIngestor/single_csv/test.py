"""默认 Chrome 单 CSV 采集配置。"""

import os

from single_csv._common import PROJECT_ROOT


CONFIG = {
    "DELETE_CSV_RECORD_ON_SUCCESS": True,
    "BASE_DST": "/netdisk/pcz/test20261126/",
    # 容器数不设置时自动计算：出口节点数 × ⌈网站数 × 预估耗时 ÷ 冷却时间⌉，默认即 15 × 网站数，
    # 自动值最多 210 个（AUTO_CONTAINER_COUNT_MAX）；设置后作为上限。
    "CONTAINER_COUNT": 100,
    # 同一出口节点访问同一网站的最小间隔（秒）；不设置时默认 180，设为 0 关闭。
    "SITE_COOLDOWN_SECONDS": 0,
    # 单个任务的预估耗时（秒），用于自动计算容器数；不设置时默认 180。
    # "TASK_DURATION_ESTIMATE_SECONDS": 180,
    "DOCKER_NETWORK": "traffic_ingestor_geo_test_net",
    "DOCKER_NETWORK_OUTPUT_NODES": 2,
    "DOCKER_NETWORK_SUBNET_PREFIX": 24,
    "DOCKER_NETWORK_GATEWAY": "172.19.0.1",
    "CONTAINER_IP_START": "172.19.0.2",
    "CSV_PATH": os.path.join(
        PROJECT_ROOT,
        "scripts/result/archive/top200000_ingestor_subpage.csv",
    ),
}
ACTION_PROFILE = "tools/browsers/chrome.py"
