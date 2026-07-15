"""强制使用 DoH/ECH 并禁用 HTTP/3/QUIC 的 Firefox 驱动变体。"""

import os

from tools.browsers.firefox import (
    create_firefox_driver as _create_firefox_driver,
    get_firefox_background_capture_exclude_hosts,
    kill_firefox_processes,
    open_url_and_save_content,
)


FIREFOX_ECH_DOH_URI = os.environ.get(
    "FIREFOX_ECH_DOH_URI",
    "https://cloudflare-dns.com/dns-query",
)
FIREFOX_ECH_DOH_BOOTSTRAP_ADDRESS = os.environ.get(
    "FIREFOX_ECH_DOH_BOOTSTRAP_ADDRESS",
    "1.1.1.1",
)

ECH_KEYLOG_LABELS = (
    "ECH_SECRET",
    "ECH_CONFIG",
)
TLS13_KEYLOG_LABELS_FOR_HTTP = (
    "CLIENT_HANDSHAKE_TRAFFIC_SECRET",
    "SERVER_HANDSHAKE_TRAFFIC_SECRET",
    "CLIENT_TRAFFIC_SECRET_0",
    "SERVER_TRAFFIC_SECRET_0",
)
WIRESHARK_ECH_KEYLOG_LABELS = ECH_KEYLOG_LABELS + TLS13_KEYLOG_LABELS_FOR_HTTP


def build_firefox_ech_preferences(doh_uri=FIREFOX_ECH_DOH_URI,
                                  bootstrap_address=FIREFOX_ECH_DOH_BOOTSTRAP_ADDRESS):
    """返回 Firefox ECH 采集所需的严格 DoH/ECH preferences。"""
    if not doh_uri:
        raise ValueError("Firefox ECH requires a non-empty DoH URI")

    preferences = {
        "network.trr.mode": 3,
        "network.trr.uri": doh_uri,
        "network.trr.custom_uri": doh_uri,
        "network.dns.echconfig.enabled": True,
        "network.dns.http3_echconfig.enabled": False,
        "network.dns.force_waiting_https_rr": True,
        "network.dns.use_https_rr_as_altsvc": True,
        "network.dns.native_https_query": False,
        "network.dns.echconfig.fallback_to_origin_when_all_failed": False,
        "security.tls.ech.grease_probability": 0,
        "network.http.http3.enabled": False,
        "network.http.http3.enable_kyber": False,
        "network.http.altsvc.enabled": False,
    }
    if bootstrap_address:
        preferences["network.trr.bootstrapAddress"] = bootstrap_address
    return preferences


def create_firefox_driver(task_name, formatted_time, parsers, data_base_dir=None,
                          proxy_server=None, proxy_bypass_list=None, artifact_label=None,
                          doh_uri=FIREFOX_ECH_DOH_URI,
                          bootstrap_address=FIREFOX_ECH_DOH_BOOTSTRAP_ADDRESS):
    """创建强制 TRR-only、禁用非 ECH 回退的 Firefox WebDriver。"""
    browser, ssl_key_file_path = _create_firefox_driver(
        task_name,
        formatted_time,
        parsers,
        data_base_dir=data_base_dir,
        proxy_server=proxy_server,
        proxy_bypass_list=proxy_bypass_list,
        artifact_label=artifact_label,
        preference_overrides=build_firefox_ech_preferences(
            doh_uri=doh_uri,
            bootstrap_address=bootstrap_address,
        ),
    )
    browser._traffic_ingestor_force_ech = True
    browser._traffic_ingestor_ech_target = task_name or ""
    browser._traffic_ingestor_ech_doh_uri = doh_uri
    return browser, ssl_key_file_path


def summarize_firefox_ech_key_log(ssl_key_path):
    """统计 RFC 9850 ECH 与后续 TLS 1.3 解密标签。"""
    counts = {label: 0 for label in WIRESHARK_ECH_KEYLOG_LABELS}
    with open(ssl_key_path, "r", encoding="utf-8", errors="replace") as key_log:
        for line in key_log:
            label = line.partition(" ")[0]
            if label in counts:
                counts[label] += 1
    return counts


def validate_firefox_ech_key_log(ssl_key_path):
    """确认密钥日志足以供 Wireshark 解密 ECH 与 HTTP TLS 流量。"""
    if not ssl_key_path or not os.path.isfile(ssl_key_path):
        return False, f"ech_keylog_missing={ssl_key_path or ''}"

    counts = summarize_firefox_ech_key_log(ssl_key_path)
    missing = [label for label, count in counts.items() if count == 0]
    if missing:
        return False, f"ech_keylog_missing_labels={','.join(missing)}"
    return True, "ech_keylog_wireshark_decryptable=true"


__all__ = [
    "ECH_KEYLOG_LABELS",
    "FIREFOX_ECH_DOH_BOOTSTRAP_ADDRESS",
    "FIREFOX_ECH_DOH_URI",
    "WIRESHARK_ECH_KEYLOG_LABELS",
    "build_firefox_ech_preferences",
    "create_firefox_driver",
    "get_firefox_background_capture_exclude_hosts",
    "kill_firefox_processes",
    "open_url_and_save_content",
    "summarize_firefox_ech_key_log",
    "validate_firefox_ech_key_log",
]
