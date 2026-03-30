# TrafficIngestor

## 项目简介
TrafficIngestor 用于批量采集网页访问流量与页面内容。宿主机脚本负责管理 Docker 容器池、分发任务；容器内脚本负责驱动浏览器或 Scrapy 执行访问，并输出抓包文件、TLS 密钥日志、HTML、截图和文本内容。

当前仓库主要覆盖两类任务：

- 流量采集：使用 Chrome、Edge、Firefox 访问目标 URL，生成 `pcap`、`ssl_key.log`、页面源码和截图。
- URL 收集：使用 Scrapy 从站点主页采集候选子链接，供后续流量采集使用。

## 核心结构
```text
trafficIngestor/                    宿主机侧调度脚本
tools/                              公共浏览器、抓包、日志与 Action 基类
traffic_capture_single_csv*/        容器内执行目录，通常以 action.py 为入口
traffic_capture_single_db/          容器内数据库任务执行目录
url_list_collector/                 Scrapy URL 采集子项目
small_tools/                        输入 CSV、测试数据、临时辅助文件
db/                                 数据库配置
set_nofile_limits.sh                高并发场景下的文件句柄调优脚本
```

## 运行依赖
建议在 Linux 宿主机或兼容环境中运行，且具备以下条件：

- Python 3.10+
- Docker，可执行 `docker version`
- `tcpdump`，且当前用户具备抓包权限
- 对应浏览器和驱动已安装在容器镜像中
- Python 依赖已安装：`selenium`、`tqdm`、`psutil`、`scrapy`、`sqlalchemy`、`pymysql`

可参考：

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install selenium tqdm psutil scrapy sqlalchemy pymysql
```

如果并发容器数较高，建议先执行：

```bash
sudo bash set_nofile_limits.sh
```

## 快速开始

### 1. 准备输入
CSV 任务文件通常放在 `small_tools/`，字段至少包含：

```csv
id,url,domain
1,https://example.com,example.com
```

不同采集器会在各自脚本中定义 `CSV_PATH`、`BASE_DST`、`CONTAINER_COUNT`、`DOCKER_IMAGE` 等常量，运行前请先检查。

### 2. 选择入口脚本
常用入口：

- `python trafficIngestor/traffic_capture_single_csv.py`
  Chrome 批量流量采集
- `python trafficIngestor/traffic_capture_single_csv_fixed_ip_europe.py`
  Chrome 批量流量采集，容器挂到独立网络 `traffic_ingestor_fixed_ip_europe_net`，IP 从 `172.19.10.10` 开始递增
- `python trafficIngestor/traffic_capture_single_csv_fixed_ip_rsia.py`
  Chrome 批量流量采集，容器挂到独立网络 `traffic_ingestor_fixed_ip_rsia_net`，IP 从 `172.19.20.10` 开始递增
- `python trafficIngestor_clash/traffic_capture_single_csv_clash.py`
  Chrome + Clash 批量流量采集，容器挂到独立网络 `traffic_ingestor_chrome_clash_net`，IP 从 `172.19.60.10` 起始段自动顺延
- `python trafficIngestor/traffic_capture_single_csv_edge.py`
  Edge 测试或定向采集
- `python trafficIngestor/traffic_capture_single_csv_firefox.py`
  Firefox 测试或定向采集
- `python trafficIngestor/traffic_capture_single_db.py`
  从数据库读取新闻 URL 批量采集
- `python trafficIngestor/get_url_list.py`
  批量采集站点子页面 URL

### 3. 查看结果
运行期间，中间文件会先写入容器挂载目录，例如：

- `data/YYYYMMDD/*.pcap`
- `ssl_key/YYYYMMDD/*_ssl_key.log`
- `content/YYYYMMDD/*.text`
- `html/YYYYMMDD/*.html`
- `screenshot/YYYYMMDD/*.png`
- `meta/{container}_last.json`

宿主机调度脚本成功后会把结果移动到对应采集器的 `BASE_DST` 目录。

## 配置说明

### CSV 采集任务
每个 `trafficIngestor/traffic_capture_single_*.py` 都是一个具体采集器。常改配置包括：

`trafficIngestor/` 下基于 `BaseTrafficIngestor` 的入口默认按脚本文件名自动推导 `BASE_NAME` 和 `CONTAINER_PREFIX`；多数入口也不再需要显式维护这两个字段，只保留 `HOST_CODE_PATH`、CSV、镜像、并发等实际业务配置。

- `CSV_PATH`：输入任务 CSV
- `BASE_DST`：最终输出目录
- `CONTAINER_COUNT`：容器并发数
- `DOCKER_IMAGE`：容器镜像
- `RETRY`：失败重试次数
- `DOCKER_NETWORK`：固定 IP 模式使用的 Docker 自定义网络名
- `DOCKER_NETWORK_SUBNET_PREFIX`：固定 IP 模式的 Docker 子网前缀长度
- `DOCKER_NETWORK_GATEWAY`：固定 IP 模式的 Docker 网关地址
- `CONTAINER_IP_START`：可选，按容器序号递增分配固定 IPv4
- `DELETE_INVALID_FILES_ON_FAIL`：可选，容器内任务失败或校验失败时是否删除失败产物；`traffic_capture_single_csv_clash.py` 可将其设为 `False` 以保留 `pcap/html/ssl_key` 便于排查

固定 IP 入口默认使用各自独立的 Docker 网络；若目标网络不存在，基类会按 `CONTAINER_IP_START`、`DOCKER_NETWORK_SUBNET_PREFIX` 和 `DOCKER_NETWORK_GATEWAY` 自动创建。当前示例入口分别使用 `traffic_ingestor_fixed_ip_europe_net`(`172.19.10.0/24`)、`traffic_ingestor_fixed_ip_rsia_net`(`172.19.20.0/24`) 等网络；这样可以避免与历史共享网段 `172.18.0.0/16` 重叠，并减少多个大容器池复用同一 bridge 时触发 `exchange full`。

`trafficIngestor_clash/` 下的入口额外启用了“运行命名空间”隔离：默认按入口脚本文件名自动推导 `BASE_NAME`、`HOST_CODE_PATH`、`CONTAINER_PREFIX` 和 `DOCKER_NETWORK`。基类不会创建 `172.19.0.0/16` 这个大网段，而是把它当作地址池，按顺序扫描可用的 `/22` 子网并依次使用 `172.19.0.0/22`、`172.19.4.0/22`、`172.19.8.0/22`……；新建的自动子网默认使用 `.1` 作为网关、`.2` 作为首个容器 IP。这样各个 clash 入口脚本本身不再显式配置网络和 IP，只保留任务规模、镜像、CSV 路径等业务参数。若需要显式指定命名空间，可设置环境变量 `TRAFFIC_INGESTOR_RUN_NAME`。

### 数据库采集任务
数据库模式使用 `db/db_config.ini`。需要提供 `mysql` 配置节，并包含：

- `host`
- `port`
- `user`
- `password`
- `database`
- `charset`

不要将真实凭据提交到版本库。

## 开发与验证
仓库当前没有正式的自动化测试套件。修改后至少执行：

```powershell
python -m py_compile tools\base_action.py tools\chrome.py tools\edge.py tools\firefox.py
```

如果改动涉及具体采集器，再对对应入口跑一次最小冒烟验证。建议使用小规模 CSV，例如 `small_tools/test.csv`，避免直接对大批量任务做首轮验证。

## 常见注意事项

- 浏览器路径和驱动路径主要写死在 `tools/chrome.py`、`tools/edge.py`、`tools/firefox.py`，更换镜像时要同步检查。
- 抓包逻辑依赖 `tcpdump` 和 `sudo pkill -f tcpdump`，受限环境中可能失败。
- 调度脚本会创建和删除当前入口对应的容器池；`trafficIngestor_clash/` 默认按脚本名隔离运行命名空间，避免复制脚本后误删其他任务。
- 输出目录大量使用 `/netdisk/...` 这类绝对路径，迁移环境时必须先改配置。
- `db/db_config.ini` 当前属于敏感文件，建议本地维护或改为环境变量注入。

## Clash 浏览器变体

- `python trafficIngestor_clash/traffic_capture_single_csv_edge_clash.py`
  Edge + Clash 采集入口；默认按入口脚本名自动生成独立的运行目录、容器名前缀和 Docker 网络。
- `python trafficIngestor_clash/traffic_capture_single_csv_firefox_clash.py`
  Firefox + Clash 采集入口；默认按入口脚本名自动生成独立的运行目录、容器名前缀和 Docker 网络。
- `TRAFFIC_INGESTOR_RUN_NAME=my_firefox_batch python trafficIngestor_clash/traffic_capture_single_csv_firefox_clash.py`
  可选，用显式运行名覆盖默认脚本名隔离规则；适合同一入口脚本并行跑多批任务。
- 容器内对应目录分别为 `traffic_capture_single_csv_edge_clash/` 与 `traffic_capture_single_csv_firefox_clash/`，仅在原 Edge / Firefox action 基础上额外注入 Clash 代理配置，原有非 Clash 入口不受影响。
