#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""urlFind 全部运行参数。

所有参数都以全局变量的形式集中在这里，修改后直接运行
``python trafficIngestor/urlFind/url_find.py`` 即可，不从外部传参。
"""

from __future__ import annotations

import os
from typing import List, Optional

# 源码根目录（trafficIngestor/）与仓库根目录（TrafficIngestor/）
SOURCE_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT: str = os.path.dirname(SOURCE_ROOT)

# ============== 输入与输出 ==============
# 输入 CSV：两列 id,domain，或单列 domain；domain 原样作为站点 key。
SOURCE_CSV: str = os.path.join(PROJECT_ROOT, "scripts", "result", "yjn", "yjn_32_domains_seeds.csv")
# 每个站点输出到 {OUTPUT_BASE_DIR}/{site}/url_list.csv，列为 id,url,domain。
OUTPUT_BASE_DIR: str = "/netdisk/yjn/website30/subpages"
# 运行目录名：状态快照、冷却表、每轮日志都放在 runtime/url_find/{RUN_NAME}/ 下。
RUN_NAME: str = "yjn_32"
RUNTIME_DIR: str = os.path.join(PROJECT_ROOT, "runtime", "url_find", RUN_NAME)
# 只运行这些站点（站点 key 与输入 CSV 一致）；为 None 时运行 CSV 中全部站点。
# 未选中的站点仍参与站点归属判断，属于它们的链接不会进入选中站点的队列。
ONLY_SITES: Optional[List[str]] = ["wikipedia.org", "vmware.com", "worldbank.org", "mobile-tracker-free.com"]

# ============== 采集目标 ==============
# 每个站点需要采集的 URL 数（只统计实际打开并校验通过的页面）。
TARGET_URLS_PER_SITE: int = 600
# 访问次数上限 = TARGET_URLS_PER_SITE * MAX_VISIT_FACTOR，超过后站点结束为 partial。
MAX_VISIT_FACTOR: int = 3
# 种子阶段最多尝试次数（首次 + 5 次重试，每次换出口组；依次轮换 https://domain、https://www.domain、http://domain）。
# 运行阶段队列耗尽后重访首页也使用这个次数。
SEED_MAX_ATTEMPTS: int = 6
# 每个 URL 失败后的最大重试次数；每次重试都派到该 URL 尚未使用过的出口组（全部用过后才放开）。
URL_MAX_RETRIES: int = 5
# 与出口 IP 无关、重试无意义的拒绝原因（按前缀匹配），这些不重试；其余 rejected/error/timeout 都会重试。
NON_RETRYABLE_REJECTIONS: List[str] = [
    "http_status:404", "http_status:410", "login_page", "non_html",
    "download_attachment", "resource_or_non_http_url",
]
# 每个桶（子域名 + 第一级路径）最多采集多少条，0 表示不限制。
MAX_URLS_PER_BUCKET: int = 0
# 每个站点待访问队列的上限，超出的新链接直接丢弃。
MAX_FRONTIER_PER_SITE: int = 50000
# 单个页面最多返回多少条链接。
MAX_LINKS_PER_PAGE: int = 2000
# URL 中包含任一关键字则不采集、不入队。
EXCLUDE_KEYWORDS: List[str] = []
# URL 匹配任一正则（re.search）则不采集、不入队；从状态快照恢复时也会清理队列中的匹配项。
SKIP_URL_PATTERNS: List[str] = [
    r"[?&]returnto=",      # 登录/注册页（MediaWiki 等），通常还会跳转到站外认证域名
    r"/w/index\.php\?",    # MediaWiki 工具页：编辑、历史、旧版本、特殊页面等
]
# 归一化时去掉的跟踪参数（utm_* 前缀另外处理）。
TRACKING_QUERY_PARAMS: List[str] = [
    "fbclid", "gclid", "msclkid", "mc_cid", "mc_eid", "_ga", "ref_src", "spm",
]

# ============== 出口 IP 与冷却 ==============
# 默认 bridge 网络的出口节点数；容器 IP 最后 8 位 % 节点数 相同的容器共用同一出口。
DEFAULT_BRIDGE_OUTPUT_NODES: int = 15
# 同一出口组访问同一站点的最小间隔（秒），从访问开始时计，失败也计入。
SITE_COOLDOWN_SECONDS: int = 180

# ============== 单页访问 ==============
# 页面打开后的等待时间（秒），再做页面校验和链接提取。
PAGE_SETTLE_SECONDS: int = 15
# Chrome 页面加载超时（秒）。
PAGE_LOAD_TIMEOUT_SECONDS: int = 60
# 单次访问在容器内的硬超时（秒），包含 Chrome 启动、加载、等待和提取。
VISIT_TIMEOUT_SECONDS: int = 150

# ============== 容器池 ==============
# 容器数量；主进程启动时一次性创建，运行中不再创建或重建。
CONTAINER_COUNT: int = 150
DOCKER_IMAGE: str = "chuanzhoupan/trace_spider_chrome:153_260909"
# 启动时是否删除同前缀旧容器后重新创建（保证 IP 连续）。
CLEAN_AND_RECREATE_CONTAINERS: bool = True
# 结束后是否删除容器。
REMOVE_CONTAINERS_ON_EXIT: bool = False
# 同一容器连续发生基础设施错误（docker exec 失败、无结果输出、宿主机超时）多少次后下线。
CONTAINER_MAX_CONSECUTIVE_FAILURES: int = 5
# 容器内 Python 解释器。
CONTAINER_PYTHON: str = "python"

# 固定 IP 网络（不设置则使用 Docker 默认 bridge，IP 由 Docker 连续分配）。
# 示例：
#   DOCKER_NETWORK = "traffic_ingestor_geo_sgp_net"
#   DOCKER_NETWORK_SUBNET_PREFIX = 24
#   DOCKER_NETWORK_GATEWAY = "172.18.0.1"
#   CONTAINER_IP_START = "172.18.0.2"
DOCKER_NETWORK: Optional[str] = None
# 自建网络的出口节点数；设置了 DOCKER_NETWORK 时必须给出。
DOCKER_NETWORK_OUTPUT_NODES: Optional[int] = None
DOCKER_NETWORK_SUBNET_PREFIX: int = 24
DOCKER_NETWORK_GATEWAY: Optional[str] = None
CONTAINER_IP_START: Optional[str] = None
DOCKER_DNS: Optional[str] = None

# 实际使用的出口节点数（由上面的网络配置决定，无需修改）
if DOCKER_NETWORK:
    if not isinstance(DOCKER_NETWORK_OUTPUT_NODES, int) or DOCKER_NETWORK_OUTPUT_NODES <= 0:
        raise ValueError(f"DOCKER_NETWORK={DOCKER_NETWORK} 必须同时给出正整数 DOCKER_NETWORK_OUTPUT_NODES")
    EGRESS_IP_COUNT: int = DOCKER_NETWORK_OUTPUT_NODES
else:
    EGRESS_IP_COUNT = DEFAULT_BRIDGE_OUTPUT_NODES

# ============== 运行状态 ==============
# 状态快照最短保存间隔（秒）；站点结束和程序退出时一定会保存。
STATE_SAVE_INTERVAL_SECONDS: int = 30
# 进度日志输出间隔（秒）。
STATUS_LOG_INTERVAL_SECONDS: int = 60
