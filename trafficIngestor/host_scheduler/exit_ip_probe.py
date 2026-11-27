"""容器公网出口 IPv4 探测，供采集日志与独立检测脚本共用。

只统计 IPv4 出口：端点全部使用仅解析 A 记录的 IPv4 专用域名，容器内再用
`curl -4` 限定本端地址族，并在回包不是 IPv4 时继续尝试下一个端点。避免像
`ifconfig.me`、`icanhazip.com` 这类双栈端点把出口节点的 IPv6 地址当成结果，
被宿主机侧的 IPv4 解析器判为“查询出口 IP 失败”。
"""

from __future__ import annotations

import ipaddress
import re
from typing import Optional, Tuple


EXIT_IP_ENDPOINTS = (
    # 只解析 A 记录的端点，不会返回 IPv6 地址。
    "https://api4.ipify.org",
    "https://ipv4.icanhazip.com",
    "https://ipv4.ident.me",
)
EXIT_IP_REQUEST_TIMEOUT = 15
EXIT_IP_QUERY = f"""
for url in {' '.join(EXIT_IP_ENDPOINTS)}; do
    if command -v curl >/dev/null 2>&1; then
        value=$(curl -4 -fsS --max-time {EXIT_IP_REQUEST_TIMEOUT} "$url" 2>/dev/null || true)
    elif command -v wget >/dev/null 2>&1; then
        value=$(wget -qO- --timeout={EXIT_IP_REQUEST_TIMEOUT} "$url" 2>/dev/null || true)
    else
        exit 2
    fi
    value=$(printf '%s' "$value" | tr -d '[:space:]')
    case "$value" in
        ''|*[!0-9.]*) continue ;;
    esac
    dots=$(printf '%s' "$value" | tr -cd '.' | wc -c)
    if [ "$dots" -ne 3 ]; then
        continue
    fi
    printf '%s\\n' "$value"
    exit 0
done
exit 1
""".strip()


def extract_ipv4(output: str) -> Optional[str]:
    candidates = re.findall(r"(?<!\d)(?:\d{1,3}\.){3}\d{1,3}(?!\d)", output or "")
    for candidate in candidates:
        try:
            address = ipaddress.ip_address(candidate)
        except ValueError:
            continue
        if address.version == 4:
            return str(address)
    return None


def compact_command_error(stdout: str, stderr: str) -> str:
    detail = " ".join(part.strip() for part in (stdout, stderr) if part and part.strip())
    return detail[:500] if detail else "无输出"


def query_container(
    ingestor,
    container: str,
    timeout: int,
) -> Tuple[str, Optional[str], Optional[str], str]:
    try:
        internal_result = ingestor.run_cmd(
            [
                "docker",
                "inspect",
                "-f",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}",
                container,
            ],
            timeout=timeout,
        )
    except Exception as exc:
        return container, None, None, f"读取容器地址异常: {exc}"
    internal_ip = extract_ipv4(internal_result.stdout)
    if internal_result.returncode != 0 or not internal_ip:
        error = compact_command_error(internal_result.stdout, internal_result.stderr)
        return container, None, None, f"读取容器地址失败: {error}"

    try:
        external_result = ingestor.run_cmd(
            ["docker", "exec", container, "sh", "-lc", EXIT_IP_QUERY],
            timeout=timeout,
        )
    except Exception as exc:
        return container, internal_ip, None, f"查询出口 IP 异常: {exc}"

    external_ip = extract_ipv4(external_result.stdout)
    if external_result.returncode != 0 or not external_ip:
        error = compact_command_error(external_result.stdout, external_result.stderr)
        return container, internal_ip, None, f"查询出口 IP 失败: {error}"
    return container, internal_ip, external_ip, ""

