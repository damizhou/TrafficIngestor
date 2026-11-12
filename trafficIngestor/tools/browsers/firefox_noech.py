"""Firefox 驱动变体：显式关闭 ECH，使用普通 DNS/TLS 采集。"""

from tools.browsers.firefox import (
    create_firefox_driver as _create_firefox_driver,
    get_firefox_background_capture_exclude_hosts,
    kill_firefox_processes,
    open_url_and_save_content,
)


def create_firefox_driver(task_name, formatted_time, parsers, data_base_dir=None,
                          proxy_server=None, proxy_bypass_list=None, artifact_label=None):
    """创建禁用 DoH/ECH 的 Firefox WebDriver。"""
    browser, ssl_key_file_path = _create_firefox_driver(
        task_name,
        formatted_time,
        parsers,
        data_base_dir=data_base_dir,
        proxy_server=proxy_server,
        proxy_bypass_list=proxy_bypass_list,
        artifact_label=artifact_label,
        preference_overrides={
            "network.trr.mode": 5,
            "network.trr.uri": "",
            "network.trr.custom_uri": "",
            "network.dns.echconfig.enabled": False,
            "network.dns.http3_echconfig.enabled": False,
            "network.dns.force_waiting_https_rr": False,
            "network.dns.use_https_rr_as_altsvc": False,
            "network.dns.native_https_query": False,
            "network.dns.echconfig.fallback_to_origin_when_all_failed": True,
            "security.tls.ech.grease_probability": 0,
            "network.http.http3.enabled": False,
            "network.http.http3.enable_kyber": False,
            "network.http.altsvc.enabled": False,
        },
    )
    return browser, ssl_key_file_path


__all__ = [
    "create_firefox_driver",
    "get_firefox_background_capture_exclude_hosts",
    "kill_firefox_processes",
    "open_url_and_save_content",
]
