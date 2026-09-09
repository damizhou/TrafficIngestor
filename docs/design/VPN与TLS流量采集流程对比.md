# VPN 与普通 TLS 流量采集流程对比

最后更新：2026-09-08 10:25:30

## 说明与范围

本文中的“VPN 采集”特指当前仓库使用的 **Clash + Trojan 代理链路**，不是 IPsec、WireGuard 等三层 VPN。两种采集都由宿主机调度 Docker 容器、在容器内驱动浏览器并使用 `tcpdump` 保存 `pcap`；区别主要发生在浏览器访问目标网站之后的网络路径、TLS 层次和密钥日志来源。

## 普通 TLS 直连采集流程

普通 TLS 采集不设置 Clash 代理，浏览器直接连接目标网站的 HTTPS 端口。

```text
宿主机调度器
    │ 加载 CSV 任务、创建 Docker 容器
    ▼
容器内浏览器 ───────────────► 目标网站:443
    │                         │
    │                         ├─ TLS ClientHello
    │                         │    SNI = 目标网站域名
    │                         └─ TLS 加密数据
    │
    ├─ tcpdump ─────────────► pcap
    └─ 浏览器 SSL key log ──► ssl_key.log
```

具体过程如下：

1. 调度器从 CSV 读取 URL 和域名任务，准备容器、浏览器、抓包目录和结果目录。
2. 容器启动 `tcpdump`，浏览器不使用代理，直接访问目标 URL。
3. 浏览器与目标网站建立 TLS。目标域名通常出现在浏览器发出的 TLS `ClientHello` 的 SNI 扩展中。
4. 浏览器将本次 TLS 会话密钥写入 `ssl_key.log`，`tcpdump` 将网络数据写入 `pcap`。
5. Wireshark 加载 `ssl_key.log` 后，能够直接把 pcap 中的目标 TLS 流解析为 TLS 握手和应用数据。

这种模式下，pcap 中的 TLS 会话和浏览器生成的 keylog 属于同一层，通常可以直接配对分析：

```text
目标 TLS pcap + 浏览器 ssl_key.log
```

## Clash/Trojan VPN 采集流程

Clash/Trojan 采集增加了本地代理和远程 Trojan 节点。浏览器看到的是本地 Clash，Clash 再把浏览器的目标连接封装到发往远程节点的外层 TLS 中。

```text
浏览器
    │ HTTP CONNECT / 本地代理
    ▼
Clash 127.0.0.1:7890
    │
    │ 外层 TLS：Clash ─────────► 远程 Trojan 节点
    │ SNI = 节点域名             │
    │                            │ 剥离 Trojan 头并转发
    ▼                            ▼
目标网站:443                 目标网站:443
```

从数据嵌套关系看，外层 TLS 的明文载荷大致如下：

```text
外层 TLS 明文
└── Trojan 请求头
    ├── 密码摘要
    ├── 目标地址和端口
    └── 内层 TLS 字节流
        └── TLS ClientHello
            └── SNI = 真实目标网站域名
```

具体过程如下：

1. 调度器为容器准备 Clash 配置和 Trojan 节点信息，并启动 Clash。
2. Clash 监听容器内的 `127.0.0.1:7890`，同时通过 `SSLKEYLOGFILE` 指定外层 keylog 路径。
3. 浏览器配置 HTTP 代理，访问 HTTPS 网站时先向 Clash 发出 `CONNECT` 请求。
4. 浏览器仍然会产生面向目标网站的内层 TLS `ClientHello`，其中可以包含真实目标 SNI。
5. Clash 把目标连接封装为 Trojan 请求，并与远程节点建立外层 TLS。外层 `ClientHello` 的 SNI 是 Trojan 节点域名，而不是目标网站域名。
6. `tcpdump` 在外部网络接口上主要捕获 Clash 到 Trojan 节点的外层 TLS；本地浏览器到 Clash 的回环通信是否出现在 pcap 中取决于抓包接口和配置。
7. 经过补丁支持的 Clash core 将外层 TLS 会话密钥写入 `trojan_outer_sslkey.log`。任务结束时，采集器会保存与本次任务对应的 keylog 快照。
8. 远程 Trojan 节点剥离 Trojan 头，将后面的内层 TCP/TLS 字节流转发给目标网站。

因此，VPN 模式的基础配对关系是：

```text
外层 Trojan pcap + Clash trojan_outer_sslkey.log
```

## 两类 TLS 和 keylog 的对应关系

| 层次 | TLS 两端 | pcap 中的位置 | SNI 含义 | 主要 keylog |
| --- | --- | --- | --- | --- |
| 普通 TLS | 浏览器 ↔ 目标网站 | 直接位于 pcap 的 TLS 会话中 | 真实目标网站域名 | 浏览器 `ssl_key.log` |
| VPN 外层 TLS | Clash ↔ Trojan 节点 | pcap 的主要网络 TLS 会话 | Trojan 节点域名 | Clash `trojan_outer_sslkey.log` |
| VPN 内层 TLS | 浏览器 ↔ 目标网站 | 位于外层解密结果、Trojan 头之后 | 真实目标网站域名 | 浏览器 `ssl_key.log`（如果该会话记录成功） |

这里的两个 keylog 不能互相替代：

- `trojan_outer_sslkey.log` 只能解密 Clash 到 Trojan 节点的外层 TLS。
- 外层 TLS 解密后得到的是 Trojan 头和内层 TLS 字节流，不等于已经解密了内层 HTTPS。
- 如果还要解密内层 TLS 的应用数据，需要浏览器的 `ssl_key.log`，并先把内层 TLS 从外层流中提取或重构出来。
- 浏览器的 `ssl_key.log` 单独加载到原始外层 pcap 中，通常找不到匹配的 TLS 会话，因为原始 pcap 的 TCP/TLS 会话是 Clash 与节点之间的外层会话。

## SNI 在 Wireshark 中的可见性差异

### 普通 TLS

加载浏览器的 `ssl_key.log` 后，Wireshark 直接解析目标 TLS `ClientHello`，因此可以在 TLS 握手字段中查看目标 SNI。

### Clash/Trojan VPN

加载 `trojan_outer_sslkey.log` 后，Wireshark首先解析的是外层 TLS，所以显示的通常是节点 SNI，例如：

```text
la04.zlfbgac.site
la12.zlfbgac.site
```

真实目标 SNI 位于下面的位置：

```text
外层 TLS 解密
    → Trojan 请求头
        → 内层 TLS ClientHello
            → server_name
```

在当前这种嵌套结构下，Wireshark 可以通过 `Follow → TLS Stream` 查看外层解密后的原始字节，但不会自动把 Trojan 头之后的内层字节重新识别为一条独立的 TLS 会话并生成普通的 `tls.handshake.extensions_server_name` 字段。只使用 Wireshark 时，可以手工查看或导出该流；要让内层 SNI 以正常 TLS 字段出现，通常还需要先剥离 Trojan 头并重构内层流。

另外，如果目标连接启用了 ECH，真实 SNI 可能本来就没有以明文形式出现在内层 `ClientHello` 中；本文所述的“查看真实 SNI”假设目标 ClientHello 未被 ECH 加密。

## 对比总结

| 对比项 | 普通 TLS 直连 | Clash/Trojan VPN |
| --- | --- | --- |
| 浏览器的下一跳 | 目标网站 | `127.0.0.1:7890` 的 Clash |
| 远程网络连接建立者 | 浏览器 | Clash |
| TLS 层数 | 一层 | 外层 TLS + 内层 TLS |
| pcap 主要记录 | 目标网站 TLS | Trojan 节点外层 TLS |
| 外层 SNI | 目标网站域名 | Trojan 节点域名 |
| 真实目标 SNI | 直接在 TLS ClientHello 中 | 在 Trojan 头后的内层 ClientHello 中 |
| keylog 来源 | 浏览器 | Clash（外层）；浏览器（内层） |
| Wireshark加载后 | 直接解析目标 TLS | 先解析外层 TLS，内层需额外提取 |
| 是否需要剥离协议头 | 不需要 | 需要剥离 Trojan 头 |

## 结论

普通 TLS 采集得到的是“浏览器与目标网站之间的一层 TLS”，所以 `pcap`、`ssl_key.log` 和目标 SNI 可以直接对应。Clash/Trojan VPN 采集得到的主要是“Clash 与 Trojan 节点之间的外层 TLS”，`trojan_outer_sslkey.log` 只能打开这一层；真实目标 SNI 和内层 TLS 位于外层解密后的 Trojan 载荷中，需要继续定位内层 `ClientHello`，不能把外层 keylog 当作普通浏览器 TLS keylog 使用。
