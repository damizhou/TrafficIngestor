# TrafficIngestor

最后更新：2026-11-24 15:46:09

## 项目简介
TrafficIngestor 用于批量采集网页访问流量与页面内容。宿主机脚本负责管理 Docker 容器池、分发任务；容器内脚本负责驱动浏览器执行访问，并输出抓包文件、TLS 密钥日志、HTML、截图和文本内容。

仓库主要覆盖两类任务：

- 流量采集：使用 Chrome、Edge、Firefox 访问目标 URL，生成 `pcap`、`ssl_key.log`、页面源码和截图。
- URL 收集（urlFind）：按站点调度，每次在容器内用 Chrome 访问一个页面并校验，收集站点子页面 URL，供后续流量采集使用。

改动前先判断属于“宿主机调度层”“容器内执行层”还是“URL 收集层”，避免修改范围扩散。

## 核心结构
```text
trafficIngestor/                    源码根目录
trafficIngestor/host_scheduler/     非 Clash 宿主机调度、单 CSV 入口、出口冷却、出口 IP 探测、Server酱通知
trafficIngestor/host_scheduler_clash/
                                    Clash 宿主机调度入口
trafficIngestor/traffic_capture_single_csv/
                                    公共浏览器容器执行入口 action.py（本地维护，未入库，见下方说明）
trafficIngestor/traffic_capture_single_db/
                                    数据库任务容器执行入口（本地维护，未入库）
trafficIngestor/multi_csv_traffic_ingestor/
                                    目录内多 CSV 滚动采集入口
trafficIngestor/urlFind/            URL 发现流程（参数集中在 urlFind/config.py）
trafficIngestor/tools/              公共抓包、日志、pcap 校验与 Action 基类
trafficIngestor/tools/browsers/     浏览器实现：chrome、chrome_video、chrome_scroll、edge、firefox、firefox_ech、firefox_noech
trafficIngestor/single_csv/         HTTPS 与 Clash 单 CSV 配置目录
configs/clash/                      Clash 模板与节点配置（config.yaml、sever_info.py 本地维护）
scripts/code/                       数据维护与离线分析脚本
scripts/system/                     宿主机维护脚本
scripts/result/、scripts/origin/    输入 CSV（不入库）
tests/                              unittest 单元测试
docs/                               设计文档、月报和专题说明
runtime/                            运行工作区和临时产物（不入库）
legacy/                             尚未移除的旧执行代码
```

> 注意：`.gitignore` 中的 `trafficIngestor/traffic_capture_*` 会让 `traffic_capture_single_csv/action.py` 和 `traffic_capture_single_db/action.py` 不进入版本库，但宿主调度器（`BaseTrafficIngestor.get_default_action_source`）仍从 `traffic_capture_single_csv/action.py` 同步 action。新环境部署时需要单独拷贝这两个文件。

## 运行依赖
建议在 Linux 宿主机上运行，并具备以下条件：

- Python 3.10+
- Docker，可执行 `docker version`
- `tcpdump`，且当前用户具备抓包权限；`ethtool` 具备 `CAP_NET_ADMIN`（见下文）
- 浏览器、驱动和容器内 Python 依赖已打进 Docker 镜像

宿主机 Python 依赖见 `requirements.txt`，统一使用仓库内虚拟环境（`venv/` 不入库）：

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

并发容器数较高时，建议先执行：

```bash
bash scripts/system/set_nofile_limits.sh
```

远程服务器上的虚拟环境与 tmux 运行示例见本地文档 `REMOTE_SERVER_ACCESS.md`（不进入 Git 仓库）。

## 流量采集

### 1. 准备输入
CSV 任务文件通常放在 `scripts/result/`，字段至少包含：

```csv
id,url,domain
1,https://example.com,example.com
```

`domain` 列同时用于结果目录归类和出口冷却（缺失时取 URL 主机名）。

### 2. 修改配置并运行
单 CSV 配置位于 `trafficIngestor/single_csv/`，一个 `.py` 文件对应一种配置，只需定义 `CONFIG` 和 `ACTION_PROFILE`。配置文件不能直接运行，必须作为参数传给调度入口。以下命令均在项目根目录执行，相对路径按当前工作目录解析：

```bash
# 非 Clash
python trafficIngestor/host_scheduler/single_csv_profiles.py trafficIngestor/single_csv/base.py
# Clash
python trafficIngestor/host_scheduler_clash/single_csv_profiles.py trafficIngestor/single_csv/clash.py
```

常用配置：

| 配置文件 | 入口 | 说明 |
| --- | --- | --- |
| `base.py` / `chrome.py` | host_scheduler | Chrome 批量采集，默认 bridge 网络 |
| `geo_sgp_chrome.py`、`geo_fra_chrome.py`、`geo_lon_chrome.py`、`cu_ams_chrome.py`、`cu_madrid_chrome.py` | host_scheduler | Chrome 固定 IP 采集，分别挂到对应地区网络（见“固定 IP 网络”） |
| `fixed_ip_rsia.py` / `fixed_ip_europe.py` | host_scheduler | Chrome 固定 IP 采集，分别使用 SGP / FRA 网络 |
| `cu_us_chrome.py`、`cu_us_edge.py`、`cu_us_firefox.py` | host_scheduler | 美国地区三种浏览器采集 |
| `experiment_E0_us.py`、`experiment_E1_sgp.py`、`experiment_E2_fra.py` | host_scheduler | 实验组采集 |
| `video.py` | host_scheduler | 使用 `chrome_video.py`，处理 consent 与视频起播 |
| `edge.py` / `firefox.py` | host_scheduler | Edge / Firefox 采集 |
| `firefox_ech.py` / `firefox_noech_mlj.py` 等 | host_scheduler | Firefox 开启 / 关闭 ECH 对照采集 |
| `batch.py` | host_scheduler | 按网站分组采集，10 页共享一份抓包（见下文） |
| `mastodon.py`、`bsky.py`、`x.py`、`threads.py`、`theguardian.py` 等 | host_scheduler | 各站点专项采集 |
| `clash.py`、`chrome_clash.py`、`chrome_subpage_clash.py` | host_scheduler_clash | Chrome + Clash 采集 |
| `edge_clash.py` / `firefox_clash.py` | host_scheduler_clash | Edge / Firefox + Clash 采集 |
| `fixed_ip_europe_clash.py` / `fixed_ip_rsia_clash.py` | host_scheduler_clash | 使用 `configs/clash/sever_info.py` 中指定节点数组（如 `VPN_INFO_NAME = "vpns_info_europ"`） |
| `github_trojan.py` | host_scheduler_clash | GitHub Trojan 采集，配合外层解密使用 |

其他入口：

- `python trafficIngestor/host_scheduler/traffic_capture_single_db.py`：从数据库读取新闻 URL 批量采集，配置为 `configs/database/db_config.ini`（`mysql` 节，含 `host`、`port`、`user`、`password`、`database`、`charset`，不入库）。
- `python trafficIngestor/multi_csv_traffic_ingestor/traffic_capture_csv_directory.py`：滚动处理目录内多个 CSV，直到每个 CSV 达到指定成功 pcap 数。

### 3. 查看结果
运行期间，中间文件先写入容器挂载目录：

- `data/YYYYMMDD/*.pcap`
- `ssl_key/YYYYMMDD/*_ssl_key.log`
- `content/YYYYMMDD/*.txt`
- `html/YYYYMMDD/*.html`
- `screenshot/YYYYMMDD/*.png`
- `meta/{container}_last.json`

任务成功后，调度器把结果移动到配置的 `BASE_DST/data/<domain>/` 下。

## 配置说明

### 单 CSV 配置项
公共配置加载、任务源和运行策略位于 `trafficIngestor/host_scheduler/csv_ingestor_common.py`，默认值在 `BaseTrafficIngestor` 中统一维护。常用项：

- `CSV_PATH`：输入任务 CSV
- `BASE_DST`：最终输出目录
- `CONTAINER_COUNT`：容器池规模上限，默认 200；未开启冷却时实际数量取它与任务数的较小值
- `DOCKER_IMAGE`：容器镜像；只有非默认镜像（Edge、Firefox 等）需要覆盖
- `DELETE_CSV_RECORD_ON_SUCCESS`：任务成功后是否删除输入 CSV 中的对应记录，默认 `True`
- `DELETE_CSV_FROM_JOURNAL_ON_START`：启动时是否按上次未完成的删行 journal / pending 文件补删 CSV 记录，默认 `True`；设为 `False` 时直接清理 journal
- `CLEAN_AND_RECREATE_CONTAINERS`：启动时删除同前缀旧容器并重建（保证 IP 连续），默认 `True`；设为 `False` 时复用已运行容器，缺少容器会直接报错
- `SITE_COOLDOWN_SECONDS`：同一出口节点访问同一网站的最小启动间隔，默认 180 秒，`0` 关闭（见“出口冷却”）
- `DOCKER_NETWORK`、`DOCKER_NETWORK_SUBNET_PREFIX`、`DOCKER_NETWORK_GATEWAY`、`CONTAINER_IP_START`：固定 IP 模式网络参数，网络不存在时自动创建
- `DOCKER_NETWORK_OUTPUT_NODES`：自建网络的出口节点数；设置了 `DOCKER_NETWORK` 且开启冷却时必填
- `DELETE_INVALID_FILES_ON_FAIL`：失败时是否删除失败产物；Clash 相关 profile 设为 `False` 以便排查
- `DOCKER_DNS`：仅在必须强制指定容器 DNS 时设置，否则使用 Docker daemon 的 DNS 配置
- `RUNTIME_NAME`：默认 `traffic_capture_single_csv_<配置文件名>`，可显式覆盖

`RETRY = 5`、`RESULT_DOMAIN_ROOT_DIR = "data"`、`TASK_CSV_DATA_ROOT_LAYOUT = True` 和 `SUCCESS_DELETE_GUARD_FIELD = "url"` 已固定在基类中，配置文件无需重复声明。HTTPS 入口最多执行 5 轮，每轮间隔 1200 秒，没有待处理任务时停止；Clash 入口执行 5 轮，每轮间隔 3600 秒。工作目录统一写入 `runtime/workspaces/`。

`ACTION_PROFILE` 填写源码根目录下的浏览器文件路径，例如 `tools/browsers/chrome.py`、`tools/browsers/chrome_video.py`、`tools/browsers/edge.py`、`tools/browsers/firefox_ech.py`。入口启动前检查文件是否存在，并根据模块暴露的驱动函数识别浏览器类型，新增浏览器变体无需修改注册表。所有浏览器共用 `traffic_capture_single_csv/action.py`，宿主通过 `TRAFFIC_ACTION_PROFILE` 传入浏览器模块路径，运行前同步到该 profile 的独立工作目录。Firefox ECH 任务还会检查密钥日志中的 `ECH_SECRET`、`ECH_CONFIG` 及后续 TLS 1.3 流量密钥标签，缺失时任务失败。

### 出口冷却与自动容器数
`host_scheduler/egress_cooldown.py` 保证同一出口组访问同一网站的两次任务启动间隔不小于 `SITE_COOLDOWN_SECONDS`：

- 出口组 = 容器 IP 最后 8 位 % 出口节点数；默认 bridge 为 15 个节点（`DEFAULT_BRIDGE_OUTPUT_NODES`），自建网络使用 `DOCKER_NETWORK_OUTPUT_NODES`。
- 冷却从任务被容器领取时开始计算，失败也计入；重试优先派给该任务尚未使用过的出口组。
- 开启冷却后容器数自动估算：`出口节点数 × ⌈网站数 × TASK_DURATION_ESTIMATE_SECONDS ÷ SITE_COOLDOWN_SECONDS⌉`，不超过 `AUTO_CONTAINER_COUNT_MAX`（默认 210）、任务数以及配置的 `CONTAINER_COUNT`。实际取值会打印在日志 `容器数=...` 中。

### 出口 IP 探测与告警通知
`LOG_CONTAINER_EXIT_IPS = True`（默认）时，采集前对前 `min(容器数, 出口节点数)` 个容器探测公网出口 IPv4（`host_scheduler/exit_ip_probe.py`，超时 `EXIT_IP_PROBE_TIMEOUT` 秒），结果写入日志。探测只统计 IPv4：端点使用仅解析 A 记录的 `api4.ipify.org`、`ipv4.icanhazip.com`、`ipv4.ident.me`，容器内用 `curl -4` 限定 IPv4，回包不是 IPv4 时继续换下一个端点，因此出口节点的 IPv6 地址不会被判为失败。独立检测可用 `python scripts/code/check_container_exit_ips.py`，它复用同一份探测实现。

探测失败时通过 Server酱发送告警（`host_scheduler/serverchan_notify.py`）。SendKey 按顺序读取环境变量 `SERVERCHAN_SENDKEY`、`SERVERCHAN_SENDKEY_FILE` 指向的文件、仓库根目录 `.serverchan_sendkey`（不入库）；未配置时只记录日志，不影响采集。

### 按网站分组采集（10 页共享一份抓包）
`trafficIngestor/single_csv/batch.py` 把 CSV 中同一域名的连续 10 行作为一组：一次 `docker exec` 顺序访问 10 个页面，共用一份 pcap 和一份 SSL keylog。组内任一页面失败（HTTP 4xx/5xx、人机验证、浏览器异常）或产物校验不通过，整组失败并保留 10 行等待整组重试；整组成功才归档 pcap、keylog 和 `manifest`，并删除这 10 行。

- `BATCH_BY_DOMAIN`：开启分组；只支持非 Clash 入口和 `tools/browsers/chrome.py`。
- `BATCH_GROUP_SIZE`：每组页数（默认 10），每个域名的行数必须是它的整数倍。
- `BATCH_PAGE_WAIT_SECONDS` / `BATCH_PAGE_GAP_SECONDS`：单页驻留时间与页间隔（默认 120 / 15 秒），也决定 `DOCKER_EXEC_TIMEOUT` 下限。
- 各网站第 1 组依次入队后再安排第 2 组，同轮按 CSV 行序；重试优先使用未尝试过的容器，`container_attempt` / `container_history` 写入 manifest。

产物路径为 `BASE_DST/data/<domain>/{pcap,ssl_key,manifest}/`；运行结束打印 `分组任务完整度校验`。

### 固定 IP 网络
固定 IP 网络统一命名为 `traffic_ingestor_geo_<地区>_net`，从 `172.18.0.0` 起按地区划分 `/24`，网关为 `.1`，容器 IP 为 `.2`–`.254`（每个子网最多 253 个容器）。Docker 默认 bridge 保留 `172.17.0.0/16`。

| 地区 | 网络 | 子网 | 网关 | 容器 IP 起点 |
| --- | --- | --- | --- | --- |
| SGP（新加坡） | `traffic_ingestor_geo_sgp_net` | `172.18.0.0/24` | `172.18.0.1` | `172.18.0.2` |
| FRA（法兰克福） | `traffic_ingestor_geo_fra_net` | `172.18.1.0/24` | `172.18.1.1` | `172.18.1.2` |
| LON（伦敦） | `traffic_ingestor_geo_lon_net` | `172.18.2.0/24` | `172.18.2.1` | `172.18.2.2` |
| AMS（阿姆斯特丹） | `traffic_ingestor_geo_ams_net` | `172.18.3.0/24` | `172.18.3.1` | `172.18.3.2` |
| Madrid（马德里） | `traffic_ingestor_geo_madrid_net` | `172.18.4.0/24` | `172.18.4.1` | `172.18.4.2` |

多个配置共用同一地区网络时，不能同时运行并分别创建网络，且容器总数不能超过 253。

### Clash 运行命名空间
Clash 入口按运行名称隔离 `BASE_NAME`、`HOST_CODE_PATH`、`CONTAINER_PREFIX` 和 `DOCKER_NETWORK`。运行名称默认 `traffic_capture_single_csv_<配置文件名>`，可通过配置中的 `RUNTIME_NAME` 或环境变量 `TRAFFIC_INGESTOR_RUN_NAME` 覆盖，适合同一配置并行跑多批任务：

```bash
TRAFFIC_INGESTOR_RUN_NAME=my_firefox_batch python trafficIngestor/host_scheduler_clash/single_csv_profiles.py trafficIngestor/single_csv/firefox_clash.py
```

Clash 网络使用独立地址池 `172.19.0.0/16`，调度器按顺序扫描可用 `/22` 子网（`172.19.0.0/22`、`172.19.4.0/22`……），网关 `.1`，首个容器 IP `.2`。建议在宿主机代理或 `v2raya` 中将 `172.19.0.0/16` 设为直连。

### Docker 网络排查

```bash
docker network ls
docker network inspect bridge --format '{{json .IPAM.Config}}'
docker network ls --format '{{.Name}}' | grep '^traffic_ingestor_' | while read -r n; do
    docker network inspect "$n" --format '{{.Name}} {{range .IPAM.Config}}{{.Subnet}} gw={{.Gateway}}{{end}} attached={{len .Containers}}'
done
docker network inspect traffic_ingestor_geo_sgp_net   # 查看已挂载容器及其 IP
```

### 宿主机网卡 offload 权限
`docker0` 的 `TSO/GSO/GRO` 应由宿主机 systemd oneshot 服务在 Docker 启动后关闭；采集器运行时只处理自定义 bridge 和容器对应的 `veth*` 接口（`ethtool -K vethxxxx tso off gso off gro off`），以减少合包造成的分段偏差。

`ethtool -K` 需要 `CAP_NET_ADMIN`，普通用户运行时可授予：

```bash
setcap cap_net_admin+ep "$(command -v ethtool)"
getcap "$(command -v ethtool)"     # 期望：/usr/sbin/ethtool cap_net_admin=ep
setcap -r "$(command -v ethtool)"  # 取消授权
```

`ethtool` 升级或重装、二进制被替换、文件系统不支持 xattr、快照回滚等情况会导致 capability 丢失。授权后，能执行该文件的用户都可以修改网卡 offload 配置。

## URL 收集（urlFind）
`url_find.py` 不接收命令行参数，**唯一的配置文件是 `trafficIngestor/urlFind/config.py`**。改完配置后在项目根目录运行：

```bash
python trafficIngestor/urlFind/url_find.py
```

流程：读取站点列表 → 从状态快照恢复 → 一次性创建容器池 → 总调度按站点分发单页访问。容器内 `urlFind/worker/visit_page.py` 每次只访问一个页面，浏览器复用 `tools/browsers/chrome.py`，页面校验在 `urlFind/page_validation.py`。

### 每次任务需要修改的参数
这几个参数位于 `config.py` 最上方的“每次任务需要修改的参数”区块：

| 参数 | 说明 |
| --- | --- |
| `SOURCE_CSV` | 输入站点列表：`id,domain` 两列或单列 `domain`（也可以写完整 URL）。表头可省略；有第二列时读第二列。站点 key 为小写主机名，`www.example.com` 与 `example.com` 是两个站点 |
| `OUTPUT_BASE_DIR` | 输出目录，每个站点写入 `{OUTPUT_BASE_DIR}/{site}/url_list.csv`（列 `id,url,domain`） |
| `RUN_NAME` | 运行目录名，对应 `runtime/url_find/{RUN_NAME}/`，保存站点状态快照 `state/{site}.json`、冷却表 `state/cooldowns.json` 和每次运行的 `runs/<时间>/run.log`、`visits.csv`、`summary.csv`。同名目录会断点续跑，换一批站点时请换名字 |
| `TARGET_URLS_PER_SITE` | 每个站点需要采集的 URL 数（只统计打开并校验通过的页面） |
| `CONTAINER_COUNT` | 容器池大小，启动时一次性创建 |

输入 CSV 示例：

```csv
id,domain
1,abc.net.au
2,dhl.com
```

### 其他参数（按需调整）

- `ONLY_SITES`：只运行指定站点（填站点 key）；未选中站点仍参与站点归属判断。首次验证时建议只跑少量站点。
- `MAX_VISIT_FACTOR`：访问次数上限 = `TARGET_URLS_PER_SITE × MAX_VISIT_FACTOR`。
- `SEED_MAX_ATTEMPTS`、`URL_MAX_RETRIES`、`NON_RETRYABLE_REJECTIONS`：种子与 URL 重试策略，重试会换到未用过的出口组。
- `EXCLUDE_KEYWORDS`、`SKIP_URL_PATTERNS`、`TRACKING_QUERY_PARAMS`：URL 过滤与归一化。
- `SITE_COOLDOWN_SECONDS`、`DEFAULT_BRIDGE_OUTPUT_NODES`：同一出口组访问同一站点的最小间隔与出口节点数。
- `PAGE_SETTLE_SECONDS`、`PAGE_LOAD_TIMEOUT_SECONDS`、`VISIT_TIMEOUT_SECONDS`：单页访问时序。
- `DOCKER_IMAGE`、`CLEAN_AND_RECREATE_CONTAINERS`、`REMOVE_CONTAINERS_ON_EXIT`、`DOCKER_NETWORK`、`DOCKER_NETWORK_OUTPUT_NODES` 等：容器池与网络。

### 常见失败
- `seed_unreachable` 且 `top_rejections` 为 `offsite_redirect`：输入域名整站跳转到了另一个域名（例如 `theregister.co.uk` → `theregister.com`），跳转后的页面被判为站外。把 CSV 中的域名改成跳转后的域名再运行。

## Chrome 逐段下滑采集（实验）
`tools/browsers/chrome_scroll.py` 在 `chrome.py` 的驱动基础上实现“打开 → 逐段下滑到底 → 保存快照”的访问流程，单次访问约 120 秒。它目前只供本地测试入口 `trafficIngestor/traffic_capture_scroll_test/run_capture.py` 使用（该目录不入库），尚未接入 single_csv / `BaseAction` 流程。

## Trojan 外层解密

- `host_scheduler_clash/` 基类在容器启动 Clash 时导出 `SSLKEYLOGFILE`，默认路径为 `HOST_CODE_PATH/clash_runtime/<container>/trojan_outer_sslkey.log`。
- 每个成功任务在结果目录额外保存 `trojan_outer_ssl_key/<pcap-basename>_trojan_outer_sslkey.log`，只包含本次任务开始后新增的 keylog 字节。
- 能否生成外层 keylog，取决于替换进去的 `vendor/clash-for-linux/bin/clash-linux-*` 是否实现了 TLS `KeyLogWriter`；预编译二进制通常不支持。
- Clash 复用已有外层 TLS 会话时快照可能为空，表示没有新的外层握手，而不是保存失败。
- 外层解密：`tshark -r outer.pcap -o tls.keylog_file:trojan_outer_sslkey.log`。
- 外层解开后，首个客户端明文仍带 Trojan 头（56 字节十六进制密码摘要、`\r\n`、SOCKS5 风格目标地址、`\r\n`，之后才是原始 TCP 载荷）。
- 离线工具：`python scripts/code/trojan_unwrap.py trojan_client_payload.bin` 剥离单个明文块的 Trojan 头；`python scripts/code/decrypt_trojan_outer_pcap.py outer.pcap trojan_outer_sslkey.log` 自动枚举 TLS 流、识别 Trojan 请求头并输出 `follow_tls.hex.txt`、`trojan_request_segment.bin`、`inner_first_payload.bin` 和 JSON 清单。

## 开发与验证
修改后至少对改动文件执行语法校验：

```bash
python -m py_compile trafficIngestor/tools/base_action.py trafficIngestor/tools/browsers/chrome.py trafficIngestor/host_scheduler/base_traffic_ingestor.py trafficIngestor/host_scheduler/csv_ingestor_common.py
```

单元测试（unittest，不依赖 Docker）：

```bash
python -m unittest discover -s tests
```

涉及具体采集器时，再用只有几行的小 CSV（配合 `trafficIngestor/single_csv/test.py`）跑一次定向冒烟验证；urlFind 用缩小版 CSV 或 `ONLY_SITES`。

### 数据维护工具
`scripts/code/` 下的脚本大多支持 `--help`，常用的有：

- `check_pcap_dataset.py`：检查 pcap 数据集完整性；`--remove-unqualified` 会删除目录和 CSV 记录，使用前必须核对路径。
- `check_container_exit_ips.py`：检查容器出口 IP。
- `merge_csv.py` / `merge_csv_recursive.py`：按 domain 合并 CSV，支持 `--dry-run`。
- `shuffle_csv_rows.py`、`csv_slice.py`、`adjust_csv_repeat_count.py`、`pad_url_list_to_target.py`：CSV 打乱、切分、调整重复次数和补齐。
- `prune_ingestor_by_existing_pcaps.py` 等 `prune_*`：按已有产物裁剪待采集 CSV。
- `cleanup_single_url_domains.py`、`cleanup_homepage_only_overlap.py`：清理不合格域名。
- `move_categories.py`：按 pcap 总体积保留前 N 个类别，确认后移动其余目录。
- `upgrade_chrome_docker_image.py`：升级 Chrome 镜像。

## 常见注意事项

- 浏览器路径和驱动路径写在 `trafficIngestor/tools/browsers/` 下各模块中，更换镜像时要同步检查。
- 抓包依赖 `tcpdump` 和 `pkill -f tcpdump`，受限环境中可能失败。
- 调度脚本会创建和删除当前 profile 对应的容器池；Clash profile 通过运行命名空间隔离，避免互相清理容器或网络。
- 输出目录大量使用 `/netdisk/...` 绝对路径，迁移环境时必须先改配置。
- 不要提交真实流量数据、密钥、日志、抓包产物及 `configs/database/db_config.ini`、`.serverchan_sendkey` 等敏感文件。
