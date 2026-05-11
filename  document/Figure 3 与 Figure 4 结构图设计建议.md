# Figure 3 与 Figure 4 结构图设计建议

## 总体定位

这份说明同时覆盖 Figure 3 和 Figure 4。两张图应形成层级关系，而不是重复表达同一条流程：

- **Figure 3** 画系统级管道，回答 `TrafficIngestor` 如何把任务从输入推进到可复验的数据产物。
- **Figure 4** 画样本级时间窗口，回答一个网站级连续访问样本如何在共享网络记录窗口中形成，并如何对齐 `PCAP`、`TLS keylog`、`HTML`、截图和正文文本五类数据。

两张图的推荐关系是：

```text
Figure 3: 任务加载 -> 环境初始化 -> 流量采集 -> 数据清洗与校验 -> 数据存储

Figure 4: 同站 URL 序列 -> 共享网络记录窗口 -> 顺序页面访问 -> 逐页面多模态证据 -> 网站级连续样本
```

## Figure 3：系统级五阶段管道

Figure 3 建议保留老师提出的五个大模块，从左到右排列：

```text
任务加载 -> 环境初始化 -> 流量采集 -> 数据清洗与校验 -> 数据存储
```

这个抽象层级比逐函数展开更适合作为正文图。它不需要画出每个 Docker 命令、每个异常分支或每个文件移动细节，而是突出 `TrafficIngestor` 的系统边界和数据生产链路。

### Figure 3 推荐主流程

```text
┌────────────┐    ┌──────────────┐    ┌────────────┐    ┌────────────────┐    ┌────────────┐
│ 任务加载   │ -> │ 环境初始化    │ -> │ 流量采集   │ -> │ 数据清洗与校验 │ -> │ 数据存储   │
└────────────┘    └──────────────┘    └────────────┘    └────────────────┘    └────────────┘
```

每个模块下方建议只保留 2-4 个关键词：

```text
任务加载
CSV / URL / domain / batch queue

环境初始化
Docker pool / browser runtime / capture dirs / network control

流量采集
browser visit / tcpdump / TLS keylog / page evidence

数据清洗与校验
completeness check / retry / temp cleanup / task dequeue

数据存储
PCAP / TLS key / HTML / screenshot / text
```

### Figure 3 推荐 caption

英文：

```text
Figure 3. TrafficIngestor system pipeline. The collector loads URL-domain tasks, initializes controlled container and browser environments, captures network and page-side evidence, validates and cleans task outputs, and stores five aligned outputs for later auditing and reuse: PCAP, TLS key, HTML, screenshot, and text.
```

中文：

```text
Figure 3. `TrafficIngestor` 系统管道。采集器加载 URL-域名任务，初始化受控容器与浏览器环境，采集网络侧和页面侧证据，完成结果校验与清理，并将五类对齐后的数据产物归档以支持后续审计与复用：`PCAP`、`TLS` key、`HTML`、截图和正文文本。
```

### Figure 3 正文建议表述

```text
`Figure 3` 给出 `TrafficIngestor` 的系统级采集管道。调度器首先从 CSV 中加载 `id,url,domain` 任务并构造批次队列；随后初始化 Docker 容器池、浏览器运行环境、抓包目录和必要的网络控制；在采集阶段，浏览器访问目标页面，同时 `tcpdump`、`TLS` keylog 与页面侧证据同步产生；采集结束后，系统检查 `PCAP`、会话密钥、`HTML`、截图和正文文本的完整性，对失败任务执行重试或清理；最终，所有有效产物按域名、批次和页面顺序归档，形成可核验、可复用的数据记录。
```

## Figure 4：连续访问窗口与多模态证据对齐

### 结论

Figure 4 更适合画成 `trafficIngestor/traffic_capture_single_csv.py` 的“同站连续访问版本”，而不是只对应当前 `news_receiver_traffic_batch.py` 的最小批量实现。这个图应同时强调两件事：

- **同站连续访问**：同一 `domain` 下的多个 URL 被组织成一个连续访问序列，在共享记录窗口内依次访问。
- **多模态证据对齐**：每个页面访问都保存 `HTML`、截图和正文文本，并与同一窗口内的 `PCAP`、`TLS keylog` 对齐。

因此，Figure 4 的主题可以从“单次页面访问生命周期”改为：

```text
Domain-Level Continuous Visit Window with Aligned Multimodal Evidence
```

中文可写为：

```text
带多模态证据对齐的网站级连续访问窗口
```

### Figure 4 要表达的核心逻辑

这个版本可以理解为把 `traffic_capture_single_csv.py` 中成熟的单 URL 采集链扩展到同站连续访问场景：每个 URL 仍然执行导航、等待、停留和页面证据保存；不同之处在于，同一 `domain` 的多个 URL 不再彼此割裂，而是被放进一个连续访问窗口中，使用共享的网络记录边界来保留跨页面时序、连接复用、后台请求和长连接延续。

图中应突出以下事实：

- 输入 CSV 仍包含 `id,url,domain`。
- 调度器按 `domain` 分组，并按 `id` 或采集顺序生成 URL 序列。
- 一个网站级任务包含 `URL_1 -> URL_2 -> ... -> URL_n`。
- 网络侧记录窗口在第一个 URL 导航前启动，在最后一个 URL 访问结束、浏览器关闭和 TCP 收尾等待后停止。
- 每个 URL 内部仍保留 `traffic_capture_single_csv.py` 的页面级证据链：`HTML`、`screenshot` 和 `text`。
- 所有页面级证据通过共同的 `domain`、批次标识、URL 序号和共享记录窗口与 `PCAP/TLS keylog` 对齐；这些对齐字段是组织索引，不作为第六类数据产物呈现。
- 最终样本粒度是网站级连续访问样本，而不是孤立单页样本。

### Figure 4 推荐主流程

建议 Figure 4 使用“横向主流程 + 页面序列展开”的结构：

```text
CSV task table
id, url, domain
        |
        v
Domain grouping
domain -> ordered URL sequence
        |
        v
Start continuous recording window
tcpdump + TLS keylog + alignment key
        |
        v
Sequential page visits
URL_1 -> URL_2 -> ... -> URL_n
        |
        v
Per-page evidence alignment
HTML_i + screenshot_i + text_i
        |
        v
Close recording window
browser quit + TCP tail wait + stop capture
        |
        v
Domain-level continuous sample
shared PCAP + shared TLS keylog + aligned page evidence
```

其中 `Sequential page visits` 可以展开成页面级小循环：

```text
URL_i navigation
      -> wait DOM complete / timeout
      -> dwell window
      -> save HTML_i
      -> save screenshot_i
      -> save text_i
      -> assign page index / alignment key
      -> inter-page gap
      -> URL_{i+1}
```

这个小循环是图的重点。它说明 Figure 4 不是简单把多个 URL 塞进同一个 pcap，而是把每个页面的可核验证据保存下来，并通过序号和时间边界与网络流量对齐。

### Figure 4 推荐四泳道版本

如果图要更清晰，建议使用四条泳道：

```text
Task lane:
CSV rows -> domain grouping -> ordered URL sequence -> website-level task

Browser lane:
create browser -> navigate URL_1 -> wait+dwell -> navigate URL_2 -> ... -> quit

Network lane:
start tcpdump/TLS keylog -------------------------------- TCP tail wait -> stop capture

Evidence lane:
URL_1: HTML_1 + screenshot_1 + text_1
URL_2: HTML_2 + screenshot_2 + text_2
...
URL_n: HTML_n + screenshot_n + text_n
        |
        v
aligned with shared PCAP and TLS keylog
```

这四条泳道能同时表达“同站连续访问”和“多模态证据链”。其中 Browser lane 和 Network lane 体现连续访问窗口，Evidence lane 体现 `pcap`、截图和页面内容之间的对齐。

### Figure 4 建议保留的关键词

建议保留这些关键词：

```text
Domain grouping
Ordered URL sequence
Continuous visit window
Shared PCAP
Shared TLS keylog
Sequential page visits
DOM complete / timeout
Dwell window
Inter-page gap
HTML snapshot
Screenshot
Text content
URL order / alignment key
Aligned evidence chain
Domain-level continuous sample
```

不建议再把下面这些内容画成独立大模块：

```text
script-triggered requests
background / heartbeat flows
resource reuse
cache reuse
third-party scripts
```

它们可以作为 `in-window network activity` 的注释出现，但不应占据主流程。Figure 4 的主线应该是“记录窗口如何跨多个页面延续，以及每个页面证据如何与共享流量对齐”。

### Figure 4 推荐图形布局

推荐画法如下：

```text
┌────────────────────────────────────────────────────────────────────┐
│ Website-level task: domain y                                       │
│ Ordered pages: URL_1, URL_2, ..., URL_n                             │
└────────────────────────────────────────────────────────────────────┘
                              |
                              v
┌────────────────────────────────────────────────────────────────────┐
│ Shared network recording window                                    │
│ tcpdump + TLS keylog                                                │
│                                                                    │
│  URL_1 visit        URL_2 visit              URL_n visit            │
│  ┌───────────┐      ┌───────────┐            ┌───────────┐          │
│  │DOM+dwell  │ ---> │DOM+dwell  │ ---> ... ->│DOM+dwell  │          │
│  │HTML/text  │      │HTML/text  │            │HTML/text  │          │
│  │screenshot │      │screenshot │            │screenshot │          │
│  │page index │      │page index │            │page index │          │
│  └───────────┘      └───────────┘            └───────────┘          │
│                                                                    │
│  cross-page timing, connection reuse, async/background traffic      │
└────────────────────────────────────────────────────────────────────┘
                              |
                              v
┌────────────────────────────────────────────────────────────────────┐
│ Domain-level continuous sample                                      │
│ PCAP + TLS keylog + {HTML_i, screenshot_i, text_i}                  │
└────────────────────────────────────────────────────────────────────┘
```

这个布局比原来的七卡片图更适合正文，因为它把“连续访问窗口”作为视觉中心，同时把页面级证据链嵌入每个 URL 访问单元。

### Figure 4 推荐 caption

英文：

```text
Figure 4. Domain-level continuous visit window in TrafficIngestor. URLs from the same domain are visited sequentially within a shared network recording window. For each page visit, TrafficIngestor waits for page readiness, keeps a dwell window, and saves HTML, screenshot, and text. These page-side outputs are aligned with the shared PCAP and TLS keylog by domain, URL order, and the common recording window to form a website-level continuous sample.
```

中文：

```text
Figure 4. `TrafficIngestor` 的网站级连续访问窗口。同一域名下的 URL 在共享网络记录窗口内顺序访问；每个页面访问都会等待页面就绪、保留停留窗口，并保存 `HTML`、截图和正文文本。这些页面侧证据通过域名、URL 顺序和共同记录窗口与共享 `PCAP`、`TLS` keylog 对齐，共同形成网站级连续访问样本。
```

### Figure 4 正文建议改写

如果采用这个 Figure 4，正文中建议这样描述：

```text
`Figure 4` 对应 website-level continuous execution chain。调度器先从 CSV 读取 `id,url,domain` 记录，并按 `domain` 生成同站 URL 序列；采集器在第一个页面导航前启动 `tcpdump` 与 `TLS` keylog，并在同一个网络记录窗口内按顺序访问该网站的多个页面。对于每个页面，浏览器完成导航后会等待 `DOM` 就绪或触发超时，随后保留固定停留窗口，并同步保存 `HTML`、截图和正文文本。相邻页面之间的时间间隔、跨页面连接复用、后台请求和长连接延续被保留在共享 `PCAP` 中。全部页面访问结束后，采集器关闭浏览器、等待 TCP 收尾、停止抓包，并将共享 `PCAP`、`TLS` keylog 与逐页面 `HTML`、截图、正文文本共同归档为一个网站级连续访问样本。
```

## 两图分工总结

Figure 3 负责解释系统管道的五个大模块：

```text
任务加载 -> 环境初始化 -> 流量采集 -> 数据清洗与校验 -> 数据存储
```

Figure 4 不应重复系统管道，而应解释一个网站级样本内部如何形成：

```text
同站 URL 序列 -> 共享网络记录窗口 -> 顺序页面访问 -> 逐页面多模态证据 -> 网站级连续样本
```

这样两张图的关系更清楚：Figure 3 是系统级流程，Figure 4 是样本级时间窗口与证据对齐。

## 建议最终图题

```text
Figure 3. TrafficIngestor System Pipeline
Figure 4. Domain-Level Continuous Visit Window with Aligned Multimodal Evidence
```

中文可写为：

```text
Figure 3. TrafficIngestor 系统级采集管道
Figure 4. 带多模态证据对齐的网站级连续访问窗口
```
