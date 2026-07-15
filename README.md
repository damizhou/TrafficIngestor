# TrafficIngestor

最后更新：2026-07-15 17:03:09

## 项目简介
TrafficIngestor 用于批量采集网页访问流量与页面内容。宿主机脚本负责管理 Docker 容器池、分发任务；容器内脚本负责驱动浏览器或 Scrapy 执行访问，并输出抓包文件、TLS 密钥日志、HTML、截图和文本内容。

当前仓库主要覆盖两类任务：

- 流量采集：使用 Chrome、Edge、Firefox 访问目标 URL，生成 `pcap`、`ssl_key.log`、页面源码和截图。
- URL 收集：使用 Scrapy 从站点主页采集候选子链接，供后续流量采集使用。

## 核心结构
```text
trafficIngestor/                    项目源码根目录
trafficIngestor/trafficIngestor/    非 Clash 宿主机调度与单 CSV 入口
trafficIngestor/trafficIngestor_clash/
                                    Clash 宿主机调度入口
trafficIngestor/traffic_capture_single_csv/
                                    公共浏览器容器执行入口
trafficIngestor/traffic_capture_single_db/
                                    数据库容器执行入口
trafficIngestor/multi_csv_traffic_ingestor/
                                    多 CSV 调度入口
trafficIngestor/url_list_collector/ Scrapy URL 收集子项目
trafficIngestor/tools/              公共浏览器、抓包、日志与 Action 基类
trafficIngestor/single_csv/         非 Clash 单 CSV 独立配置目录
configs/clash/                      Clash 模板与节点配置
configs/database/                   数据库配置
scripts/                            输入数据和数据维护脚本
scripts/system/                     宿主机维护脚本
docs/                               设计文档、月报和历史报告
vendor/                             Clash 等第三方程序及构建目录
runtime/                            运行工作区和临时产物
legacy/                             尚未移除的旧执行代码
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
bash scripts/system/set_nofile_limits.sh
```

## 快速开始

### 1. 准备输入
CSV 任务文件通常放在 `scripts/`，字段至少包含：

```csv
id,url,domain
1,https://example.com,example.com
```

非 Clash 单 CSV 采集器的配置位于 `trafficIngestor/single_csv/`，一个 `.py` 文件对应一种配置；`trafficIngestor/trafficIngestor/single_csv_profiles.py` 只负责加载命令行指定的配置文件并启动任务。Clash 配置暂时仍在 `trafficIngestor/trafficIngestor_clash/single_csv_profiles.py`。默认 Docker 镜像由 `BaseTrafficIngestor.DOCKER_IMAGE` 提供，Edge / Firefox、固定 IP 和 Clash 等特殊配置会显式覆盖所需字段。

### 2. 修改配置并直接运行

非 Clash 任务必须显式传入配置文件路径。修改 `trafficIngestor/single_csv/base.py` 中的 `CONFIG`、`RUN_POLICY`、`RUNTIME_NAME` 或 `ACTION_PROFILE` 后执行：

```powershell
python trafficIngestor/trafficIngestor/single_csv_profiles.py trafficIngestor/single_csv/base.py
```

Clash 任务仍从其入口文件中的 profile 配置启动：

```powershell
python trafficIngestor/trafficIngestor_clash/single_csv_profiles.py clash
```

其他配置同样传入完整相对路径：

- `python trafficIngestor/trafficIngestor/single_csv_profiles.py trafficIngestor/single_csv/base.py`
  Chrome 批量流量采集
- `python trafficIngestor/trafficIngestor/single_csv_profiles.py trafficIngestor/single_csv/fixed_ip_europe.py`
  Chrome 批量流量采集，容器挂到独立网络 `traffic_ingestor_fixed_ip_europe_net`，IP 从 `172.18.0.2` 开始递增
- `python trafficIngestor/trafficIngestor/single_csv_profiles.py trafficIngestor/single_csv/fixed_ip_rsia.py`
  Chrome 批量流量采集，容器挂到独立网络 `traffic_ingestor_fixed_ip_rsia_net`，IP 从 `172.18.2.2` 开始递增
- `python trafficIngestor/trafficIngestor_clash/single_csv_profiles.py clash`
  Chrome + Clash 批量流量采集，运行时按 profile 的固定命名空间创建独立 Docker 网络，并从 `172.19.0.0/16` 地址池中选择可用 `/22` 子网
- `python trafficIngestor/trafficIngestor_clash/single_csv_profiles.py fixed_ip_europe`
  Chrome + Clash 欧洲入口，使用 `configs/clash/sever_info.py` 中的 `vpns_info_europ` 节点数组；网络隔离逻辑与普通 Clash 入口一致
- `python trafficIngestor/trafficIngestor/single_csv_profiles.py trafficIngestor/single_csv/edge.py`
  Edge 测试或定向采集
- `python trafficIngestor/trafficIngestor/single_csv_profiles.py trafficIngestor/single_csv/firefox.py`
  Firefox 测试或定向采集
- `python trafficIngestor/trafficIngestor/traffic_capture_single_db.py`
  从数据库读取新闻 URL 批量采集
- `python trafficIngestor/trafficIngestor/get_url_list.py`
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
原有 22 个单 CSV 宿主入口已经合并。实际任务读取、成功删行和外层循环策略由公共实现统一提供，配置位置为：

- `trafficIngestor/single_csv/`：15 个非 Clash 配置，一个配置一个文件
- `trafficIngestor/trafficIngestor/single_csv_profiles.py`：非 Clash 指定配置文件的加载、校验与运行入口
- `trafficIngestor/trafficIngestor_clash/single_csv_profiles.py`：7 个 Clash profile

公共任务源和运行策略位于 `trafficIngestor/trafficIngestor/csv_ingestor_common.py`。非 Clash 配置文件必须定义 `CONFIG`、`RUN_POLICY`、`RUNTIME_NAME` 和 `ACTION_PROFILE`，运行时必须显式传入该 `.py` 文件的路径。每个配置显式保存运行命名空间，动态工作目录统一写入 `runtime/workspaces/`。

Chrome、Edge、Firefox、Firefox Disable 及其 Clash 变体统一使用 `trafficIngestor/traffic_capture_single_csv/action.py`。宿主 profile 通过 `TRAFFIC_ACTION_PROFILE` 向容器传入浏览器后端和代理模式；运行前会把公共 action 同步到 `runtime/workspaces/` 中该 profile 的独立目录。

- `CSV_PATH`：输入任务 CSV
- `BASE_DST`：最终输出目录
- `CONTAINER_COUNT`：容器并发数
- `DOCKER_IMAGE`：容器镜像；默认值在 `BaseTrafficIngestor` 中统一维护，只有 Edge / Firefox 等非默认镜像入口需要覆盖
- `RETRY`：失败重试次数
- `DOCKER_NETWORK`：固定 IP 模式使用的 Docker 自定义网络名
- `DOCKER_NETWORK_SUBNET_PREFIX`：固定 IP 模式的 Docker 子网前缀长度
- `DOCKER_NETWORK_GATEWAY`：固定 IP 模式的 Docker 网关地址
- `CONTAINER_IP_START`：可选，按容器序号递增分配固定 IPv4
- `DELETE_INVALID_FILES_ON_FAIL`：可选，容器内任务失败或校验失败时是否删除失败产物；Clash 的 `clash`、`chrome`、`chrome_subpage` 和固定节点 profile 将其设为 `False` 以保留 `pcap/html/ssl_key` 便于排查

固定 IP 入口默认使用各自独立的 Docker 网络；若目标网络不存在，基类会按 `CONTAINER_IP_START`、`DOCKER_NETWORK_SUBNET_PREFIX` 和 `DOCKER_NETWORK_GATEWAY` 自动创建。当前示例入口分别使用 `traffic_ingestor_fixed_ip_europe_net`(`172.18.0.0/23`) 和 `traffic_ingestor_fixed_ip_rsia_net`(`172.18.2.0/23`)。网段规划约定为：Docker 默认网络保留 `172.17.0.0/16`；需要手动固定 IP 的采集器统一使用 `172.18.0.0/16`；这样可以在不触碰默认 bridge 的前提下，为特殊任务提供稳定容器地址，并减少多个大容器池复用同一 bridge 时触发 `exchange full`。

`trafficIngestor/trafficIngestor_clash/single_csv_profiles.py` 中的 profile 额外启用了“运行命名空间”隔离：每个 profile 显式保存原入口名称，并据此生成 `BASE_NAME`、`HOST_CODE_PATH`、`CONTAINER_PREFIX` 和 `DOCKER_NETWORK`。基类不会创建整个 `172.19.0.0/16`，而是把它当作地址池，按顺序扫描可用的 `/22` 子网并依次使用 `172.19.0.0/22`、`172.19.4.0/22`、`172.19.8.0/22`……；新建的自动子网默认使用 `.1` 作为网关、`.2` 作为首个容器 IP。建议在宿主机代理或 `v2raya` 配置中将 `172.19.0.0/16` 整段设为直连。若需要显式覆盖命名空间，可设置环境变量 `TRAFFIC_INGESTOR_RUN_NAME`。

### Docker 网络排查
需要核对宿主机上的 Docker 网段时，可先执行：

```powershell
docker network ls
docker network inspect bridge --format '{{json .IPAM.Config}}'
docker network inspect traffic_ingestor_fixed_ip_europe_net --format '{{json .IPAM.Config}}'
docker network inspect traffic_ingestor_fixed_ip_rsia_net --format '{{json .IPAM.Config}}'
docker network ls --format '{{.Name}}' | ForEach-Object {
    docker network inspect $_ --format '{{.Name}} {{range .IPAM.Config}}{{.Subnet}} gw={{.Gateway}}{{end}}'
}
docker network ls --format '{{.Name}}' | Select-String 'traffic_ingestor_clash'
```

若要进一步查看网络里已挂载的容器及其固定 IP，可执行：

```powershell
docker network inspect traffic_ingestor_fixed_ip_europe_net
docker network inspect traffic_ingestor_fixed_ip_rsia_net
```

如果宿主机使用的是 bash，可改用：

```bash
docker network ls
docker network inspect bridge --format '{{json .IPAM.Config}}'
docker network inspect traffic_ingestor_fixed_ip_europe_net --format '{{json .IPAM.Config}}'
docker network inspect traffic_ingestor_fixed_ip_rsia_net --format '{{json .IPAM.Config}}'
docker network ls --format '{{.Name}}' | while read -r n; do
    docker network inspect "$n" --format '{{.Name}} {{range .IPAM.Config}}{{.Subnet}} gw={{.Gateway}}{{end}}'
done
docker network ls --format '{{.Name}}' | grep '^traffic_ingestor_clash'
docker network ls --format '{{.Name}}' | grep '^traffic_ingestor_clash' | while read -r n; do
    docker network inspect "$n" --format '{{.Name}} {{range .IPAM.Config}}{{.Subnet}} gw={{.Gateway}}{{end}} attached={{len .Containers}}'
done
```

### 宿主机网卡 offload 权限
`docker0` 的 `TSO/GSO/GRO` 应由宿主机 systemd oneshot 服务在 Docker 启动后关闭。采集器运行时只处理自定义 Docker bridge 和容器对应 `veth*` 接口，以减少抓包中出现网卡合包带来的分段偏差。典型命令如下：

```bash
ethtool -K vethxxxx tso off gso off gro off
```

`ethtool -K` 需要 `CAP_NET_ADMIN`。如果普通用户运行采集器时缺少该能力，可以给宿主机上的 `ethtool` 二进制授予 capability：

```bash
setcap cap_net_admin+ep "$(command -v ethtool)"
getcap "$(command -v ethtool)"
```

期望输出类似：

```bash
/usr/sbin/ethtool cap_net_admin=ep
```

`setcap` 写入的是文件 extended attributes，正常情况下重启后仍然生效。以下情况可能导致 capability 丢失：`ethtool` 软件包升级或重装、二进制文件被替换、文件系统不支持或未启用 extended attributes、手动执行 `setcap -r`、系统或镜像快照回滚、安全策略或部署脚本重置文件权限。

取消授权可执行：

```bash
setcap -r "$(command -v ethtool)"
```

注意：授予该 capability 后，能执行该 `ethtool` 文件的用户就具备修改网卡 offload 配置的能力。采集器代码会直接执行 `ethtool`，失败时按原错误返回。

### 数据库采集任务
数据库模式使用 `configs/database/db_config.ini`。需要提供 `mysql` 配置节，并包含：

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
python -m py_compile trafficIngestor\tools\base_action.py trafficIngestor\tools\chrome.py trafficIngestor\tools\edge.py trafficIngestor\tools\firefox.py trafficIngestor\traffic_capture_single_csv\action.py
```

如果改动涉及具体采集器，再对对应入口跑一次最小冒烟验证。建议使用小规模 CSV，例如 `scripts/test.csv`，避免直接对大批量任务做首轮验证。

### 数据维护工具

- `python scripts/code/check_pcap_dataset.py --help`：检查 pcap 数据集完整性；`--remove-unqualified` 会删除目录和 CSV 记录，使用前必须核对路径。
- `python scripts/code/merge_csv.py --help`：按 domain 合并主 CSV 与临时 CSV，支持 `--dry-run`。
- `python scripts/code/move_categories.py`：按 pcap 总体积保留前 N 个类别，并在确认后移动其余目录。

## 常见注意事项

- 浏览器路径和驱动路径主要写死在 `trafficIngestor/tools/chrome.py`、`trafficIngestor/tools/edge.py`、`trafficIngestor/tools/firefox.py`，更换镜像时要同步检查。
- 抓包逻辑依赖 `tcpdump` 和 `pkill -f tcpdump`，受限环境中可能失败。
- 调度脚本会创建和删除当前 profile 对应的容器池；Clash profile 使用显式运行命名空间隔离不同任务，避免互相清理容器或网络。
- 输出目录大量使用 `/netdisk/...` 这类绝对路径，迁移环境时必须先改配置。
- `configs/database/db_config.ini` 当前属于敏感文件，建议本地维护或改为环境变量注入。

## Clash 浏览器变体

- `python trafficIngestor/trafficIngestor_clash/single_csv_profiles.py edge`
  Edge + Clash 采集；使用 Edge 镜像、Edge 版本探测和原 Edge Clash 运行命名空间。
- `python trafficIngestor/trafficIngestor_clash/single_csv_profiles.py firefox`
  Firefox + Clash 采集；使用 Firefox 镜像、Firefox 版本探测和原 Firefox Clash 运行命名空间。
- `python trafficIngestor/trafficIngestor_clash/single_csv_profiles.py fixed_ip_europe`
  Chrome + Clash 欧洲采集；profile 通过 `VPN_INFO_NAME = "vpns_info_europ"` 指定 `configs/clash/sever_info.py` 里的节点数组。
- `TRAFFIC_INGESTOR_RUN_NAME=my_firefox_batch python trafficIngestor/trafficIngestor_clash/single_csv_profiles.py firefox`
  可选，用显式运行名覆盖 profile 的默认隔离规则；适合同一 profile 并行跑多批任务。
- Edge / Firefox Clash 不再维护独立 action 源文件；统一 action 根据 `edge_clash`、`firefox_clash` 模式选择浏览器后端并注入 Clash 代理。各 profile 仍使用原运行命名空间和独立运行目录。
## Trojan 外层解密

- `trafficIngestor/trafficIngestor_clash/` 基类现在会在容器启动 Clash 时导出 `SSLKEYLOGFILE`，默认路径为 `HOST_CODE_PATH/clash_runtime/<container>/trojan_outer_sslkey.log`。
- 每个成功任务会额外在结果目录下保存一份 `trojan_outer_ssl_key/<pcap-basename>_trojan_outer_sslkey.log` 快照。该快照只截取“本次任务开始后新增的 keylog 字节”，便于和同名前缀的外层 `pcap` 一起离线解密。
- 这一步只负责“传递环境变量并保存 keylog”。真正能否生成外层 keylog，取决于你替换进去的 `vendor/clash-for-linux/bin/clash-linux-*` 是否已经基于源码实现了 TLS `KeyLogWriter` 或等价能力；仓库自带的预编译二进制通常不会自动支持。
- 如果 Clash 在多个任务之间复用了已经建立好的外层 TLS 会话，那么某个任务对应的快照可能是空文件；这表示该任务没有触发新的外层 TLS 握手，而不是保存逻辑失败。
- 外层解密命令示例：

```powershell
tshark -r outer.pcap -o tls.keylog_file:trojan_outer_sslkey.log
```

- 外层 TLS 解开后，首个客户端明文请求仍然带有 Trojan 协议头，需要再剥一次头部。常见格式是 `56` 字节十六进制密码摘要、`\r\n`、SOCKS5 风格目标地址段、`\r\n`，其后才是原始 TCP 载荷；如果目标端口是 `443`，后续字节通常就是你要的 HTTPS/TLS 流量。
- 仓库附带了一个离线小工具：先导出“解密后的首个客户端明文块”，再运行 `python scripts/trojan_unwrap.py trojan_client_payload.bin`。脚本会打印目标地址、端口和 header 偏移，并把去掉 Trojan 头之后的内层载荷默认写到 `trojan_client_payload.bin.inner.bin`。
- 也可以直接用一体化脚本：`python scripts/decrypt_trojan_outer_pcap.py outer.pcap trojan_outer_sslkey.log`。它会调用 `tshark follow,tls` 自动枚举 TLS 流、识别包含 Trojan 请求头的首个客户端明文段，并输出每条匹配流的 `follow_tls.hex.txt`、`trojan_request_segment.bin`、`inner_first_payload.bin` 和 JSON 清单。
## DNS

- Collectors no longer pass `--dns 172.17.0.1` by default.
- Set `DOCKER_DNS` only when a collector must force a specific container DNS server.
- Otherwise, rely on Docker daemon DNS settings so custom bridge networks do not inherit a stale `172.17.0.1` assumption.
