# 仓库指南

最后更新：2026-07-15 18:54:18

## 项目概览
本仓库用于批量采集网页访问流量与页面内容，核心流程是由宿主机脚本调度 Docker 容器，在容器内驱动 Chrome、Edge、Firefox 或 Scrapy 执行任务，并输出 `pcap`、TLS 密钥日志、HTML、截图等结果。新增功能或修复问题时，优先判断改动属于“宿主机调度层”、“容器内执行层”还是“URL 收集层”，避免修改范围扩散。

## 项目结构与模块组织
`trafficIngestor/` 是源码根目录：`trafficIngestor/host_scheduler/` 和 `trafficIngestor/trafficIngestor_clash/` 保存宿主机调度脚本，`trafficIngestor/traffic_capture_single_csv/` 与 `trafficIngestor/traffic_capture_single_db/` 保存容器执行入口，`trafficIngestor/tools/browsers/` 保存浏览器实现，`trafficIngestor/tools/` 其余文件保存抓包、日志和 Action 公共模块，`trafficIngestor/url_list_collector/` 保存 Scrapy URL 收集项目。非 Clash 单 CSV 配置位于 `trafficIngestor/single_csv/`，通过 `python trafficIngestor/host_scheduler/single_csv_profiles.py <配置文件路径>` 加载；Clash 模板和节点配置位于 `configs/clash/`。输入 CSV 和数据维护脚本集中在 `scripts/`，运行工作区统一写入 `runtime/`。

## 构建、测试与开发命令
优先先做语法校验，再跑最小范围验证：

```powershell
python -m py_compile trafficIngestor\tools\base_action.py trafficIngestor\tools\browsers\firefox.py trafficIngestor\traffic_capture_single_csv\action.py
python -m py_compile trafficIngestor\tools\browsers\chrome.py trafficIngestor\host_scheduler\base_traffic_ingestor.py trafficIngestor\host_scheduler\csv_ingestor_common.py trafficIngestor\host_scheduler\single_csv_profiles.py trafficIngestor\single_csv\base.py
python trafficIngestor\host_scheduler\single_csv_profiles.py trafficIngestor\single_csv\base.py
python trafficIngestor\host_scheduler\get_url_list.py
```

`py_compile` 是最基本检查。后三个入口脚本会触发 Docker、浏览器驱动和抓包链路，仅在本机已具备 `docker`、浏览器二进制、驱动和 `tcpdump` 时运行。

## 代码风格与命名规范
使用 Python 现有风格：4 空格缩进，函数与变量使用 `snake_case`，类使用 `PascalCase`，常量使用全大写。新增逻辑优先挂到 `BaseAction` 或 `BaseTrafficIngestor` 的钩子上，不要复制已有调度流程。路径、镜像名、阈值、浏览器参数应集中为常量，避免散落硬编码。

## 测试要求
仓库当前没有正式的 `pytest` 测试体系。每次修改后，至少对改动文件运行 `python -m py_compile`，再补一个定向冒烟验证，例如使用 `scripts/test.csv` 跑一次 Firefox 单站点采集，或用缩小版 CSV 跑一次 URL 收集流程。若无法实跑，需明确说明缺失的运行条件和风险。

## 提交与合并请求规范
提交信息遵循当前历史风格，使用简短、直接的中文动宾句，例如 `优化 Firefox 抓包时序`、`新增 Edge 预热逻辑`。一次提交只处理一个明确问题。PR 需要说明目的、影响目录、验证命令、依赖环境，以及是否修改了 Docker 镜像、挂载路径、输出目录或清理逻辑。

## 安全与配置提示
不要提交真实流量数据、密钥、令牌、日志或抓包产物。合并前检查 `/netdisk/...`、浏览器路径、驱动路径和镜像标签是否仍适用于目标环境。涉及删除容器、批量清理目录或覆盖输出文件的改动，必须在说明中明确影响范围。

## 远程服务器连接
涉及 SSH、sshpass、tmux 或远程服务器运行的操作，先查阅本地文件 `REMOTE_SERVER_ACCESS.md`。该文件不进入 Git 仓库。
