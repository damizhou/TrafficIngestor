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

- `CSV_PATH`：输入任务 CSV
- `BASE_DST`：最终输出目录
- `CONTAINER_COUNT`：容器并发数
- `DOCKER_IMAGE`：容器镜像
- `RETRY`：失败重试次数

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
- 调度脚本会创建和删除同前缀 Docker 容器，运行前确认不会影响其他任务。
- 输出目录大量使用 `/netdisk/...` 这类绝对路径，迁移环境时必须先改配置。
- `db/db_config.ini` 当前属于敏感文件，建议本地维护或改为环境变量注入。
