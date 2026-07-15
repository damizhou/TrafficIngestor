# 任务队列

最后更新：2026-07-15 16:42:51

- [ ] 手动删除根目录仅剩受保护 `.git` 的 `clash-for-linux/` 残留目录（低，0.1h）

- [x] 已完成并验证：收敛仓库一级目录、将核心源码迁入 src 并修正配置、数据、文档及运行路径 - 2026-07-15 16:42:51

- [x] 已完成并验证：统一非 Clash 单 CSV 用法为入口脚本加配置文件路径，并删除 profile 名兼容逻辑 - 2026-07-15 15:59:27

- [x] 已完成并验证：将非 Clash 单 CSV profile 拆分到根目录独立配置文件，每种配置一个文件 - 2026-07-15 15:51:32

- [x] 已完成并验证：简化单 CSV 入口为“文件内配置字典 + 直接运行脚本”，并保留可选 profile 参数 - 2026-07-15 15:32:20

- [x] 已完成并验证：将数据集检查、CSV 合并和类别移动脚本迁移到 small_tools/code - 2026-07-15 15:21:52

- [x] 已完成并验证：合并容器执行层重复 action.py 并保留浏览器、代理和实验差异 - 2026-07-15 15:11:25

- [x] 已完成并验证：删除 22 个兼容入口并将仓库内调用迁移到两个统一 profile CLI - 2026-07-15 14:42:33

- [x] 已完成并验证：将 22 个单 CSV 宿主入口按非 Clash 与 Clash 两组改为配置驱动 - 2026-07-15 11:19:09

- [x] 已完成并验证：审查全项目脚本并形成重复功能分析报告 - 2026-07-15 11:00:26

- [x] 已完成并验证：编写项目目录结构优化方案文档 - 2026-07-15 10:40:52

- [x] 已完成并验证：删除 Chrome ECH 与 QUIC/NoQUIC 实验链路并合并 Chrome 基类与子类，保留 ECH 数据供 Firefox 使用 - 2026-07-15 10:17:12

- [x] 已完成并验证：将统一布局下的所有任务 CSV 固定保存到 BASE_DST/data 根目录 - 2026-07-15 09:54:56

- [x] 已完成并验证：统一单 CSV 与多 CSV 入口的 BASE_DST/data、logs 和输入 CSV 落盘结构 - 2026-07-15 09:51:43

- [x] 已完成并验证：规范多 CSV 采集 BASE_DST 为 data/logs 两级结构并按域名放置输入 CSV - 2026-07-15 09:38:36

- [x] 已完成并验证：将两批任务快照中的 8 个站点 CSV 复制到远程对应产物目录 - 2026-07-15 09:29:18

- [x] 已完成并验证：补齐多 CSV 采集的源 CSV 备份、实时分站进度、全量日志和结果汇总，并在远程完成 7×500 采集 - 2026-07-14 21:42:30

- [x] 已完成并验证：将项目路径计算收敛到 BaseTrafficIngestor 并让多 CSV 入口复用 - 2026-07-14 20:36:57

- [x] 已完成并验证：将多 CSV 采集器改为全局变量配置、基类同款动态容器数和按每站成功 500 个 PCAP 滚动调度 - 2026-07-14 17:51:23

- [x] 已完成并验证：基于 Firefox 公共驱动实现强制 ECH 版本并验证配置 - 2026-07-13 17:53:03

- [x] 已完成并验证：支持 ECH patched 浏览器路径透传到容器执行环境 - 2026-07-10 17:51:02

- [x] 已完成并验证：处理 ECH keylog 缺少解密标签时的无效重试与诊断文件保留 - 2026-07-10 17:49:32

- [x] 已完成并验证：增强 ECH 采集产物的 Wireshark 可解密性校验 - 2026-07-10 17:35:35

- [x] 已完成并验证：抽取 Chrome、ECH 与 NoQUIC 驱动共享基类 - 2026-07-10 16:42:37

- [x] 已完成并验证：检查 ECH 宿主采集入口移动后的路径、导入与仓库引用 - 2026-07-10 16:21:13

- [x] 已完成并验证：将 traffic_capture_single_csv_noquic 切换到 tools/chrome_quic.py - 2026-07-07 15:39:36

- [x] 已完成并验证：将 tools/chrome_quic.py 改为禁用 QUIC 的 Chrome 启动配置 - 2026-07-07 15:36:45

- [x] 已完成并验证：在 ECH Chrome 启动参数中关闭 QUIC - 2026-07-07 15:08:36

- [x] 已完成并验证：修复最终汇总后无剩余任务仍进入 300 秒冷却的问题 - 2026-07-07 14:20:26

- [x] 已完成并验证：将额外结果文件纳入容器内 chown 后再由宿主搬运 - 2026-07-07 13:58:57

- [x] 已完成并验证：将重复容器清理逻辑收敛到 BaseTrafficIngestor 并跳过全成功等待 - 2026-07-07 12:33:37

- [x] 已完成并验证：修复 ECH 证据路径未写入 result JSON 导致宿主未搬运的问题 - 2026-07-07 12:25:12

- [x] 已完成并验证：为 ECH 成功采集保留 netlog 与 ECH 验证证据 - 2026-07-07 12:15:16

- [x] 已完成并验证：为 Chrome ECH 采集入口强制启用 ECH 并增加校验 - 2026-07-07 10:33:20

- [x] 已完成并验证：生成 TrafficIngestor 2026 年 6 月项目月度卡 - 2026-07-05 10:06:15

- [x] 已完成并验证：为 pcap 裁剪脚本补充关联产物同步删除和孤儿产物清理 - 2026-06-27 12:44:58

- [x] 已完成并验证：新增 pcap 按 id+domain 保留前 120 个的清理脚本 - 2026-06-27 12:36:36

- [x] 已完成并验证：将剩余时间预估改为剩余数量除以每分钟平均值 - 2026-06-26 17:25:40

- [x] 已完成并验证：在任务进度中增加剩余时间预估 - 2026-06-26 17:22:06

- [x] 已完成并验证：为 `small_tools/origin/x_users.csv` 填充固定 `domain` 列 - 2026-06-26 16:30:28

- [x] 已完成并验证：为 `small_tools/origin/by_12000_users_with_url.csv` 增加 `domain` 列 - 2026-06-26 16:25:36

- [x] 已完成并验证：将同任务启动节流键改为 id+domain 并清理旧动态容器池高编号残留 - 2026-06-26 15:16:52

- [x] 已完成并验证：显式启用 Firefox Kyber 对照分支并修正 disable action 文件头注释 - 2026-06-26 15:00:21

- [x] 已完成并验证：修复 worker 异常导致任务队列永久等待的问题 - 2026-06-26 09:40:23

- [x] 已完成并验证：在 BASE_DST/logs 中生成含完整度结果的单次执行任务生命周期日志 - 2026-06-25 21:16:11

- [x] 已完成并验证：任务结束后使用 BASE_DST 中的 CSV 校验五类产物完整度 - 2026-06-25 21:12:35

- [x] 已完成并验证：启动采集时将任务 CSV 复制到 BASE_DST 根目录且不覆盖已有文件 - 2026-06-25 21:08:27

- [x] 已完成并验证：为同 ID 任务增加 2 秒启动间隔 - 2026-06-25 20:46:39

- [x] 已完成并验证：修复并发重入导致 PCAP 损坏和 SSL key 孤儿产物，保持原有秒级产物命名 - 2026-06-25 20:20:44

- [x] 已完成并验证：调整 GitHub 仓库 CSV 映射为原始 id、html_url 和固定 github.com - 2026-06-25 16:17:12

- [x] 已完成并验证：修正 GitHub 仓库 CSV 转换脚本，输出 id 使用源数据 repo_id - 2026-06-25 16:06:26

- [x] 已完成并验证：批量将 5 个社交平台 URL 文本转换为 `id,url,domain` CSV - 2026-06-25 15:40:18
- [x] 已完成并验证：将 GitHub 仓库 CSV 转换为 `id,url,domain` 格式 - 2026-06-25 15:37:24

- [x] 已完成并验证：新�?`temp/servers_txt_to_sever_info.py`，将 `temp/servers.txt` 转成 `vpns_info` Python 配置格式 - 2026-03-27 14:20
- [x] 已完成并验证：参�?`temp` 中旧 VPN 方案调整 `traffic_capture_single_csv_clash.py` �?Clash 启动时机 - 2026-03-27 11:18
- [x] 已完成并验证：为 `trafficIngestor/traffic_capture_single_csv_clash.py` 增加 clash-for-linux 挂载、启动和代理采集支持 - 2026-03-27 11:12
- [x] 已完成并验证：排�?`traffic_capture_single_csv_edge/action.py` �?`EDGE_BACKGROUND_CAPTURE_EXCLUDE_HOSTS` 不生效原�?- 2026-03-20 11:17
- [x] 已完成并验证：让 Edge 背景排除主机不再出现�?pcap / SNI �?- 2026-03-20 11:25
- [x] 已完成并验证：将 Edge Linux 无头指纹伪装�?Windows 桌面�?Edge - 2026-03-20 12:10
- [x] 已完成并验证：将 Firefox Linux 无头指纹伪装�?Windows 桌面�?Firefox - 2026-03-20 12:10
- [x] 已完成并验证：将 Edge / Firefox 伪装身份�?Windows 切回 Linux 桌面�?- 2026-03-20 12:15
- [x] 已完成并验证：新�?`small_tools/x_url_txt_to_csv.py`，支持将 `x_url.txt` 转为 `id,url,domain` CSV 并控制重复次�?- 2026-03-25 10:00
- [x] 已完成并验证：调�?`small_tools/x_url_txt_to_csv.py` 的重复语义为“整表轮转重复且保持相同 id,url,domain�?- 2026-03-25 10:08
- [x] 已完成并验证：新�?`small_tools/copy_subpages_temp_csv.py`，将 `subpages_temp/<domain>/url_list.csv` 复制�?`subpages/<domain>/url_list.csv` - 2026-03-25 19:08
- [x] 已完成并验证：调�?`trafficIngestor/base_traffic_ingestor.py` 执行顺序，先读取任务，仅在有任务时再创建 Docker 和做运行时清�?- 2026-03-26 09:30
- [x] 已完成并验证：调�?`trafficIngestor/base_traffic_ingestor.py`，在获取任务后立即打印任务数�?- 2026-03-26 10:22
- [x] 已完成并验证：调�?`small_tools/code/url_txt_to_csv.py` 默认读取 `small_tools/origin/bsky_urls_1000.txt` 并输出到 `small_tools/result`，同时修正参数未生效问题 - 2026-03-26 10:31
- [x] 已完成并验证：调�?`base_traffic_ingestor_yjn.py`，让 bsky 入口先取任务并打印数量，再准备容器；同时自动准备 `HOST_CODE_PATH/action.py` - 2026-03-26 11:06
- [x] 已完成并验证：跟�?`traffic_capture_single_csv_bsky.py` 切回主基类后的执行可观测性，补充主基类容器创建前日志�?`HOST_CODE_PATH` 准备接线 - 2026-03-26 11:17
- [x] 已完成并验证：让 `trafficIngestor/traffic_capture_single_csv_clash.py` �?`config/sever_info.py` 读取节点，并按容器循环分�?VPN 节点 - 2026-03-27 15:34
- [x] 已完成并验证：仅�?`traffic_capture_single_csv_clash/action.py` 对应�?Chrome 显式注入 Clash 代理 `chrome_options` - 2026-03-27 15:34
- [x] 已完成并验证：为 `trafficIngestor/traffic_capture_single_csv_clash.py` 增加固定容器 IP 起始地址 `172.17.150.0` - 2026-03-27 15:55
- [x] 已完成并验证：让 `trafficIngestor/traffic_capture_single_csv_clash.py` 在容器准备阶段一次启�?Clash，并移除 `docker exec` 的代理环境变量注�?- 2026-03-27 16:14
- [x] 已完成并验证：对�?`trafficIngestor/traffic_capture_single_csv.py` 清理 `trafficIngestor/traffic_capture_single_csv_clash.py` 中与基类重复的空回调�?`exec_once` 覆盖 - 2026-03-27 16:29
- [x] 已完成并验证：让 `trafficIngestor/traffic_capture_single_csv_clash.py` 基于 `config/config.yaml` 模板替换节点信息生成每容�?`config.yaml`，移除手�?YAML 生成逻辑 - 2026-03-27 16:33
- [x] 已完成并验证：收�?`trafficIngestor/traffic_capture_single_csv_clash.py` 的模板替换范围，仅替�?`config/config.yaml` 中单条占位节点定义并保留固定�?`vpnnodename` - 2026-03-27 16:38
- [x] 已完成并验证：为 `trafficIngestor/base_traffic_ingestor.py` 增加固定容器 IP 自动顺延逻辑，跳过目�?Docker 网络中已占用或不可用的地址 - 2026-03-27 16:49
- [x] 已完成并验证：为固定 IP 采集器补�?Docker 自定义网络自动创建逻辑，并�?clash / europe / rsia 入口切换�?`172.18.x.x` 独立网段 - 2026-03-27 17:14
- [x] 已完成并验证：修�?`trafficIngestor_clash/traffic_capture_single_csv_clash.py` �?`pkill -f 'clash-linux-'` 误杀当前 `docker exec bash` 导致 Clash 无输出启动失败的问题，并补充返回码诊�?- 2026-03-27 17:27
- [x] 已完成并验证：按运行约束移除 `trafficIngestor_clash/traffic_capture_single_csv_clash.py` 中无必要�?Clash 旧进程清理逻辑，保留启动失败返回码诊断 - 2026-03-27 17:31
- [x] 已完成并验证：调�?`trafficIngestor_clash/traffic_capture_single_csv_clash.py`，取消共享挂�?`clash-for-linux`，改为容器创建后通过 `docker cp` 复制 Clash 目录和该容器专属 `conf/config.yaml` / `Country.mmdb`，再执行 `start.sh` 启动 Clash - 2026-03-27 17:40
- [x] 已完成并验证：修�?`trafficIngestor_clash/traffic_capture_single_csv_clash.py` 复制到容器后�?`clash-for-linux` 脚本 CRLF 问题，并在容器内重写最�?`.env`，避�?`start.sh` �?`$'\\r'` �?`CLASH_URL` 占位值启动失�?- 2026-03-27 17:57
- [x] 已完成并验证：将 `clash / fixed_ip_europe / fixed_ip_rsia` 统一到共�?Docker 网络 `traffic_ingestor_fixed_ip_net`，固定使�?`172.18.0.0/16` 和网�?`172.18.0.1`，同时让固定 IP 分配逻辑跳过网关地址 - 2026-03-27 18:10
- [x] 已完成并验证：增�?`trafficIngestor/base_traffic_ingestor.py` �?Docker 网络创建失败诊断，在共享固定 IP 网络与现有网段重叠时直接输出冲突网络名和子网 - 2026-03-27 18:26
- [x] 已完成并验证：在 `trafficIngestor/base_traffic_ingestor.py` 基类中增加宿主机 `docker0` �?offload 关闭逻辑，每次脚本运行前执行 `ethtool -K docker0 tso off gso off gro off`，失败即中止 - 2026-03-27 18:33
- [x] 已完成并验证：扩�?`trafficIngestor/base_traffic_ingestor.py` �?offload 关闭范围，除 `docker0` 外还会对目标 Docker bridge 和每个容器对应的宿主�?veth peer 执行 `tso/gso/gro off`，减少固�?IP 自定义网络下的包合并 - 2026-03-27 18:41
- [x] 已完成并验证：调�?`config/config.yaml`，移�?`url-test / fallback / load-balance` 探测类代理组及其引用，让 Clash 模板直接选择 `vpnnodename`，避免启动后主动探测节点 - 2026-03-27 18:57
- [x] 已完成并验证：将 `config/config.yaml` �?`rules:` 全部收敛�?`MATCH,🚀 节点选择`，让模板中的所有流量统一�?`vpnnodename` - 2026-03-27 19:03
- [x] 已完成并验证：进一步精简 `config/config.yaml`，仅保留 `🚀 节点选择` 单个代理组，并让全部规则统一走该节点�?- 2026-03-27 19:07

- [x] ����ɲ���֤���Ա� temp �� 3 �� clash ������־��ȷ�ϲ��췢����������� Clash ��վ�������׶ζ��Ǵ����֧��clash1 Ҳ���׵��ɹ����ڶ���ͬ�� ERR_CONNECTION_RESET - 2026-03-27 20:05
- [x] ����ɲ���֤��Ϊ trafficIngestor_clash/traffic_capture_single_csv_clash.py ���� DELETE_INVALID_FILES_ON_FAIL ���أ������������� action.py ����ʧ��ʱ�Ƿ��� pcap/html/ssl_key - 2026-03-27 20:12
- [x] ����ɲ���֤��Ϊ open_url_and_save_content �쳣��������������־����� current_url��ready_state������ҳժҪ��performance log �е� Network.loadingFailed �¼� - 2026-03-27 20:20
- [x] ����ɲ���֤����ȡ trafficIngestor_clash/traffic_capture_single_csv_clash.py �е� Clash ����ʱ�߼��� trafficIngestor/base_clash_traffic_ingestor.py����ڽ���������������ص� - 2026-03-28 19:06
- [x] ����ɲ���֤������ trafficIngestor_clash �� Edge / Firefox �� Clash �ɼ�������Ӧ action����Ϊ tools/edge.py��tools/firefox.py ����������������֧�� - 2026-03-28 19:23
- [x] ����ɲ���֤���� trafficIngestor_clash/base_clash_traffic_ingestor.py ��Ĭ�� action ����Դ��Ϊ traffic_capture_single_csv_clash/action.py������ Clash ������˵�ͨ�� Chrome action - 2026-03-28 19:29

- [x] ����ɲ���֤������ Edge / Firefox ��ͨ���� Clash �� action ��Ĭ�Ϻ�̨�ų� host ��Դ��ͳһ�� tools �㸴�� - 2026-03-28 20:18

- [x] 已完成并验证：拆分固定 IP 采集入口到独立 Docker 网络，并为容器创建/启动失败补充网络挂载数诊断，避免共享 bridge 触发 `exchange full` - 2026-03-29 09:41

- [x] Completed and verified: preserve CSV uid/gid/mode during remove_from_csv atomic replace so root runs do not replace the file with temp-file ownership - 2026-03-29 19:20
- [x] 已完成并验证：为 `trafficIngestor_clash` 基类增加按入口脚本名自动隔离的运行命名空间与 Docker 子网分配，并将容器清理收紧为精确容器名，避免复制脚本后网络冲突或误删并行任务 - 2026-03-30 15:10
- [x] 已完成并验证：将 `trafficIngestor_clash` 自动建网策略固定为 `/22` 子网，并统一使用每个子网的 `.1` 网关与 `.2` 起始容器 IP - 2026-03-30 15:22
- [x] 已完成并验证：移除 `trafficIngestor_clash` 各入口脚本中的显式网络/IP配置，并将基类自动子网分配改为在 `172.19.0.0/16` 地址池内顺序扫描 `/22` 子网 - 2026-03-30 15:35
- [x] 已完成并验证：为 `trafficIngestor/base_traffic_ingestor.py` 增加默认运行命名推导，并移除 `trafficIngestor` 各入口脚本中的显式 `BASE_NAME` / `CONTAINER_PREFIX` 配置 - 2026-03-30 15:48
- [x] 已完成并验证：移除 `github_traffic.py`、`traffic_capture_single_csv_top200000.py`、`traffic_capture_single_csv_fixed_ip_europe.py` 中遗漏的显式 `HOST_CODE_PATH` 配置，并统一改为由基类自动推导 - 2026-03-30 16:02
- [x] 已完成并验证：检查全部 `action.py` 与自动 `HOST_CODE_PATH` 下的复制逻辑，确认哪些入口会复用现有专用 `action.py`，哪些会回退复制通用主文件 - 2026-03-31 08:56
- [x] Completed and verified: remove hard-coded `--dns 172.17.0.1` from collector container creation, add optional `DOCKER_DNS` override, and default to Docker daemon DNS for custom bridge networks - 2026-04-01 10:07
- [x] Completed and verified: fix Dreamacro/clash Trojan outer TLS keylog patch by making the keylog writer a process-wide singleton and adding KeyLogWriter to the gRPC TLS path; verified with `go build ./...` - 2026-04-02 00:20
- [x] Completed and verified: update Clash outer TLS keylog snapshots to record per-task start offsets and save one task-scoped keylog file named from the pcap basename; verified with `python -m py_compile trafficIngestor_clash\\base_clash_traffic_ingestor.py` - 2026-04-02 00:32
- [x] Completed and verified: add `small_tools/decrypt_trojan_outer_pcap.py` to process `pcap + trojan_outer_sslkey.log` pairs via `tshark follow,tls`, strip Trojan headers, and emit inner payload files; verified with `python -m py_compile` and a real temp pcap/keylog pair - 2026-04-02 00:45
- [x] 已完成并验证：新增 `small_tools/build_clash_manual_commands.txt`，提供手动逐条执行的 Clash 源码拉取、编译、替换与复测命令清单 - 2026-04-01 16:45
- [x] 已完成并验证：修复 `small_tools/build_clash_from_goproxy.ps1` 中 `param(...)` 位置错误导致的 PowerShell 解析失败 - 2026-04-01 16:40
- [x] 宸插畬鎴愬苟楠岃瘉锛氫负 `trafficIngestor_clash` 澧炲姞 Trojan 澶栧眰 TLS keylog 瀵煎嚭銆佺粨鏋滃揩鐓т繚瀛樹笌绂荤嚎瑙ｅ寘璇存槑/宸ュ叿 - 2026-04-01 16:04
- [x] 已完成并验证：新增 `small_tools/build_clash_from_goproxy.ps1`，用于通过 Go 模块缓存拉取 `github.com/Dreamacro/clash v1.18.0` 源码、编译 Linux amd64 二进制并可选覆盖仓库内 `clash-linux-amd64` - 2026-04-01 16:36
- [x] 已完成并验证：为 `trafficIngestor_clash` 增加 Trojan 外层 TLS keylog 导出、结果快照保存与离线解包说明/工具 - 2026-04-01 16:04
- [x] Completed and verified: improve Clash Chrome capture success rate in `trafficIngestor_clash/traffic_capture_single_csv_clash.py` by adding limited in-task navigation retries and relaxing false low-keylog failures; verified with `python -m py_compile tools\\base_action.py traffic_capture_single_csv_clash\\action.py trafficIngestor_clash\\traffic_capture_single_csv_clash.py` - 2026-04-02 10:34
- [x] Completed and verified: add error-focused Clash Chrome diagnostics by logging richer browser failure summaries and Clash runtime log tails without adding extra success-path process logs; verified with `python -m py_compile tools\\base_action.py tools\\chrome.py traffic_capture_single_csv_clash\\action.py trafficIngestor_clash\\traffic_capture_single_csv_clash.py` - 2026-04-02 11:12
- [x] 已完成并验证：排查 `traffic_capture_single_csv_clash/logs/20260402_pcz_traffic_capture_single_csv_clash0.log` 与残留 `pcap/clash_runtime`，确认故障发生在 `la01.zlfbgac.site:443` 的 Trojan 外层 TLS 握手阶段，非本地 Clash 监听或 Chrome 代理注入问题 - 2026-04-02 12:21
- [-] 将 Clash BASE_DST 日期目录改为运行时自动生成（中，0.25h）
- [x] 已完成并验证：同步 README 中固定 IP / Clash 网段约定，并补充 Docker 网络排查命令 - 2026-04-08 10:50
 
- [x] Completed and verified: Ensure Clash CSV task output includes moved pcap path - 2026-04-17 14:49
- [x] 已完成并验证：整理 `trafficIngestor/traffic_capture_single_csv.py` 代码流程说明并写入 `temp/traffic_capture_single_csv_flow.md` - 2026-04-22 19:55
- [x] 已完成并验证：将 `temp/traffic_capture_single_csv_flow.md` 优化为“代码流程 + 学术信息图映射”版本，结合旧 benchmark 与 SiteBench / TeraWFD 重估叙事 - 2026-04-22 20:05
- [x] 已完成并验证：按 Figure 4 系统架构图要求重写 `temp/traffic_capture_single_csv_flow.md`，突出 `per-visit alignment` 与 `multimodal evidence` - 2026-04-22 20:59
- [x] 已完成并验证：将 `temp/traffic_capture_single_csv_flow.md` 重写为弱化细节、突出流程的约 800 字代码说明版本 - 2026-04-22 21:06

- [x] 已完成并验证：将 Figure 3 简化为“任务加载、环境初始化、流量采集、数据清洗与校验、数据存储”五阶段说明并写入章节文档 - 2026-04-24 00:00
- [x] 已完成并验证：分析 `news_receiver_traffic_batch.py` 批量采集实现并将 Figure 4 改图建议写入 `document/fig4_news_receiver_batch_design.md` - 2026-04-24 21:30
- [x] 已完成并验证：将 Figure 4 设计说明调整为 `traffic_capture_single_csv.py` 连续访问版本，补充 HTML、截图、正文文本与 PCAP/keylog 对齐的多模态证据链 - 2026-04-24 21:40
- [x] 已完成并验证：在 `document/fig4_news_receiver_batch_design.md` 中补充 Figure 3 五阶段系统管道设计，并整理 Figure 3 / Figure 4 分工 - 2026-04-24 21:45
- [x] 已完成并验证：检查并修正 `document/Figure 3 与 Figure 4 结构图设计建议.md` 中将 metadata/访问元信息误写为数据产物的问题，统一为 PCAP、TLS key、HTML、screenshot、text 五类数据口径 - 2026-04-24 22:00
- [x] 已完成并验证：新增 Figure 3/4 合并图绘图思路文档 - 2026-04-28 10:54
- [x] 已完成并验证：修复 CSV URL 含未转义逗号时任务读取错位 - 2026-05-09 09:03
- [x] 已完成并验证：进度日志增加总任务数量 - 2026-05-11 09:43
- [x] 已完成并验证：增强 result JSON 缺失路径时的失败诊断日志 - 2026-05-11 10:20
- [x] 已完成并验证：支持 Clash 入口指定 sever_info.py 中的 VPN 节点数组 - 2026-05-11 10:30
- [x] 已完成并验证：移除 Europe/Rsia Clash 入口显式 Docker 网络参数 - 2026-05-11 10:37
- [x] 已完成并验证：修复基础 CSV 采集器容器名冲突后仍继续启动的问题 - 2026-05-19 10:55
- [x] 已完成并验证：为 Docker 容器创建卡住增加超时与诊断 - 2026-05-19 11:03
- [x] 已完成并验证：修复 Docker 冲突容器删除后名字未释放即重试的问题 - 2026-05-19 11:09
- [x] 已完成并验证：新增 `small_tools/code/dedupe_homeonly_merged_csv.py`，用于清理 `homeonly_merged.csv` 重复行并保留首次出现记录 - 2026-05-20 09:30
- [x] 已完成并验证：优化 `small_tools/code/dedupe_homeonly_merged_csv.py` 为全局变量配置，并支持按 `MAX_OCCURRENCES_PER_RECORD` 保留指定重复次数 - 2026-05-20 09:38
- [x] 已完成并验证：收敛默认 Docker 镜像配置到基类 - 2026-05-22 15:59
- [x] 已完成并验证：新增 `small_tools/code/adjust_csv_repeat_count.py`，通过全局变量配置 CSV 路径与目标重复次数并下调重复记录保留次数 - 2026-05-27 15:14
- [x] 已完成并验证：将 `small_tools/code/adjust_csv_repeat_count.py` 改为按第一轮唯一记录扩充目标重复次数 - 2026-05-28 14:44
- [x] 已完成并验证：分析 Chrome 更新后 zh.wikipedia.org pcap 中 Google SNI 背景流量；暂不做域名阻断，仅在 `tools/chrome.py` 通过 Chrome policy、一次性 profile 和后台功能关闭减少后台联网产生 - 2026-05-28 23:40
- [x] 已完成并验证：分析 `temp/zh.wikipedia.org/20260529092553` 残余流量，并在 `tools/chrome.py` 增加默认关闭的 Chrome 启动期后台端点阻断开关，验证 `python -m py_compile` 通过 - 2026-05-29 09:55
- [ ] 复采 zh.wikipedia.org 并确认 pcap 中是否仍出现 Google 后台 SNI/DNS 与 `_ipp/_ipps._tcp.local` mDNS（高，0.5h）
- [x] 已完成并验证：新增 `tools/ungoogled_chromium.py` 与 `temp/ungoogled_chromium_container_replacement.md`，并让 `tools/chrome.py` 支持外部浏览器/driver 路径 - 2026-05-29 10:45
- [x] 已完成并验证：重新验收 ungoogled Chromium 工具脚本，并将 Git warning 原因写入 `temp/ungoogled_chromium_container_replacement.md` - 2026-05-29 10:55
- [x] 已完成并验证：在 `document` 中记录当前可见采集数据信息 - 2026-06-01 09:39
- [x] 已完成并验证：修复 `traffic_capture_single_csv_clash.py` 中 `_project_root` 类型推断过宽导致的 IDE 类型告警 - 2026-06-01 09:39
- [x] 已完成并验证：统一补齐路径 bootstrap 变量 `_current_dir` / `_project_root` 的 `str` 类型标注 - 2026-06-01 09:39
- [x] 已完成并验证：启动时检查 BASE_DST 可写并将成功产物权限改为 775/664 - 2026-06-05 09:58:32
- [x] 已完成并验证：将成功产物 chmod 收敛为只处理本次移动文件和涉及目录 - 2026-06-05 10:08:25
- [x] 已完成并验证：将 Chrome SSL keylog 改为临时源文件并在容器内复制到最终产物路径 - 2026-06-05 10:34:53
- [x] 已完成并验证：启动时检测特权/root 运行并直接退出 - 2026-06-05 10:34:53
- [x] 已完成并验证：将 Clash 外层 TLS keylog 快照改为容器内复制并 chown 后再搬运 - 2026-06-05 14:51:59
- [x] 已完成并验证：将非固定 IP 采集子类容器数改为基类按任务数动态计算 - 2026-06-05 16:44:45
- [x] 已完成并验证：将动态容器数从向下取整改为向上取整 - 2026-06-05 16:49:20
- [x] 已完成并验证：新增 `trafficIngestor/run_social_csv_captures.py` 顺序执行 bsky、mastodon、threads 三个 CSV 采集脚本 - 2026-06-05 20:44:51
- [x] 已完成并验证：批次清理 HOST_CODE_PATH 临时目录前通过运行中容器归还权限，避免 root-owned 子目录删除失败 - 2026-06-07 12:11:04
- [x] 已完成并验证：让多轮 CSV 采集入口在空任务时立即退出，并在 CSV 已空时跳过 300 秒冷却和 1200 秒等待 - 2026-06-07 12:31:08
- [x] 已完成并验证：动态 Docker 容器数改为前 50 个任务一任务一容器、之后每 10 个任务增加一个容器，并保留最大容器数上限 - 2026-06-07 15:50:10
- [x] 已完成并验证：容器启动后探测一次浏览器版本并将浏览器版本标识写入所有采集产物命名 - 2026-06-12 15:22:25
- [x] 已完成并验证：将宿主机网卡 offload 的 ethtool capability 配置说明写入 README - 2026-06-13 09:17:47
- [x] 已完成并验证：修复 Chrome 页面加载超时、整页截图过大和 CDP/Selenium 截图卡住导致内容产物缺失 - 2026-06-18 13:15:27
- [x] 已完成并验证：记录远程服务器 SSH key/sshpass 连接流程并补充本地密钥文件忽略规则 - 2026-06-18 13:56:22
- [x] 已完成并验证：恢复 Firefox HTTP/3/QUIC 与 Alt-Svc 开关 - 2026-06-24 19:58:36
- [x] 已完成并验证：新增 Docker 启动后关闭 docker0 offload 的 systemd oneshot 安装脚本并在远程服务器通过 bash 语法校验 - 2026-06-24 20:26:05
- [x] 已完成并验证：新增永久提升 nofile soft/hard 到最大值的安装脚本并同步到远程服务器通过 bash 语法校验 - 2026-06-24 20:37:51
- [x] 已完成并验证：禁用 Firefox 的 ML-KEM/TLS hybrid key share preference - 2026-06-24 20:44:49
- [x] 已完成并验证：移除采集代码中宿主机 docker0 offload 关闭逻辑，改由 systemd oneshot 处理 - 2026-06-25 09:25:00
- [x] 已完成并验证：让容器内 eth0 每次启动后都强制执行 ethtool 关闭 TSO/GSO/GRO，去掉 /tmp/.offload_disabled 跳过逻辑 - 2026-06-25 10:45:53
- [x] 已完成并验证：将 HOST_CODE_PATH 子目录清理改为默认只在启动容器池后执行，批次结束保留失败现场日志和产物 - 2026-06-25 10:54:15
- [x] 已完成并验证：放宽 Chrome 整页截图与首屏截图超时时间，降低长页面截图缺失失败 - 2026-06-25 11:05:13
- [x] 已完成并验证：按长页面采集需求将 Chrome 整页截图超时提高到 300 秒、首屏截图超时提高到 120 秒 - 2026-06-25 11:08:36
