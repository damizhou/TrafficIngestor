# 仓库指南

## 项目概览
本仓库用于批量采集网页访问流量与页面内容，核心流程是由宿主机脚本调度 Docker 容器，在容器内驱动 Chrome、Edge、Firefox 或 Scrapy 执行任务，并输出 `pcap`、TLS 密钥日志、HTML、截图等结果。新增功能或修复问题时，优先判断改动属于“宿主机调度层”、“容器内执行层”还是“URL 收集层”，避免修改范围扩散。

## 项目结构与模块组织
`trafficIngestor/` 保存宿主机侧调度脚本，负责管理 Docker 容器池、分发任务、汇总抓包结果。`tools/` 是公共工具层，包含浏览器驱动、`tcpdump` 抓包、日志、`BaseAction` 与 `BaseTrafficIngestor` 等复用逻辑。`traffic_capture_single_csv_firefox/`、`traffic_capture_single_csv_edge/`、`traffic_capture_single_csv/` 等目录是容器内运行目录，通常以 `action.py` 作为入口。`url_list_collector/` 是 Scrapy 子项目，用于采集候选 URL。输入 CSV 和临时辅助文件主要放在 `small_tools/`。

## 构建、测试与开发命令
优先先做语法校验，再跑最小范围验证：

```powershell
python -m py_compile tools\base_action.py tools\firefox.py traffic_capture_single_csv_firefox\action.py
python trafficIngestor\traffic_capture_single_csv_firefox.py
python trafficIngestor\get_url_list.py
```

`py_compile` 是最基本检查。后两个脚本会触发 Docker、浏览器驱动和抓包链路，仅在本机已具备 `docker`、浏览器二进制、驱动和 `tcpdump` 时运行。

## 代码风格与命名规范
使用 Python 现有风格：4 空格缩进，函数与变量使用 `snake_case`，类使用 `PascalCase`，常量使用全大写。新增逻辑优先挂到 `BaseAction` 或 `BaseTrafficIngestor` 的钩子上，不要复制已有调度流程。路径、镜像名、阈值、浏览器参数应集中为常量，避免散落硬编码。

## 测试要求
仓库当前没有正式的 `pytest` 测试体系。每次修改后，至少对改动文件运行 `python -m py_compile`，再补一个定向冒烟验证，例如使用 `small_tools/test.csv` 跑一次 Firefox 单站点采集，或用缩小版 CSV 跑一次 URL 收集流程。若无法实跑，需明确说明缺失的运行条件和风险。

## 提交与合并请求规范
提交信息遵循当前历史风格，使用简短、直接的中文动宾句，例如 `优化 Firefox 抓包时序`、`新增 Edge 预热逻辑`。一次提交只处理一个明确问题。PR 需要说明目的、影响目录、验证命令、依赖环境，以及是否修改了 Docker 镜像、挂载路径、输出目录或清理逻辑。

## 安全与配置提示
不要提交真实流量数据、密钥、令牌、日志或抓包产物。合并前检查 `/netdisk/...`、浏览器路径、驱动路径和镜像标签是否仍适用于目标环境。涉及删除容器、批量清理目录或覆盖输出文件的改动，必须在说明中明确影响范围。
