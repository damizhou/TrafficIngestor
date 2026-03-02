#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
容器内 URL 抓取 Action：
- 访问站点
- 递归提取同站链接
- 输出可访问页面的重定向后 URL 列表到 /app/meta/{container}_last.json
"""

import json
import os
import re
import socket
import sys
import time
from collections import deque
from typing import Dict, List, Set, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait

# 添加当前目录到路径（容器内 /app 包含 tools/ 子目录）
_current_dir = os.path.dirname(os.path.abspath(__file__))
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)

from tools.chrome import create_chrome_driver, kill_chrome_processes
from tools.logger import setup_logging


SKIP_SCHEMES = ("javascript:", "mailto:", "tel:", "data:")
TRACKING_QUERY_PREFIX = "utm_"
TRACKING_QUERY_KEYS = {
    "fbclid", "gclid", "yclid", "mc_cid", "mc_eid", "igshid",
    "spm", "ref", "source", "from", "_openstat", "msclkid",
}
SKIP_EXTENSIONS = (
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico",
    ".pdf", ".zip", ".rar", ".7z", ".tar", ".gz",
    ".mp4", ".m4v", ".mov", ".avi", ".mp3", ".wav", ".flac",
    ".css", ".js", ".xml", ".txt", ".csv", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
)
COMMON_PATHS = [
    "/", "/about", "/news", "/blog", "/products", "/product", "/docs", "/help",
    "/support", "/contact", "/careers", "/jobs", "/privacy", "/terms", "/sitemap",
]
PAGE_INTERVAL_SECONDS = int(os.environ.get("URL_LIST_PAGE_INTERVAL_SECONDS", "60"))

JS_EXTRACT_LINKS = r"""
const result = { anchors: [], canonical: "", ogUrl: "", location: "" };
try {
  result.location = String(window.location.href || "");
  const canonical = document.querySelector('link[rel="canonical"]');
  if (canonical && canonical.href) result.canonical = String(canonical.href);
  const og = document.querySelector('meta[property="og:url"]');
  if (og && og.content) result.ogUrl = String(og.content);
  const list = [];
  const nodes = document.querySelectorAll("a[href]");
  for (let i = 0; i < nodes.length && i < 3000; i++) {
    const raw = nodes[i].getAttribute("href") || nodes[i].href || "";
    if (raw) list.push(String(raw));
  }
  result.anchors = list;
} catch (e) {}
return result;
"""


def normalize_domain(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    if "://" not in text:
        text = "https://" + text
    parts = urlsplit(text)
    return (parts.hostname or "").strip().lower().strip(".")


def host_variants(host: str) -> List[str]:
    normalized = normalize_domain(host)
    if not normalized:
        return []

    variants = [normalized]
    if normalized.startswith("www."):
        plain = normalized[4:]
        if plain:
            variants.append(plain)
    else:
        variants.append(f"www.{normalized}")

    unique = []
    seen = set()
    for item in variants:
        if item not in seen:
            seen.add(item)
            unique.append(item)
    return unique


def normalize_url(raw_url: str, base_url: str = "") -> str:
    raw = (raw_url or "").strip()
    if not raw:
        return ""
    if raw.startswith("#"):
        return ""
    low = raw.lower()
    if low.startswith(SKIP_SCHEMES):
        return ""

    joined = urljoin(base_url, raw) if base_url else raw
    parts = urlsplit(joined)
    scheme = (parts.scheme or "").lower()
    if scheme not in ("http", "https"):
        return ""

    host = (parts.hostname or "").strip().lower().strip(".")
    if not host:
        return ""

    port = parts.port
    if (scheme == "http" and (port is None or port == 80)) or (scheme == "https" and (port is None or port == 443)):
        netloc = host
    elif port is not None:
        netloc = f"{host}:{port}"
    else:
        netloc = host

    path = parts.path or "/"
    if path != "/":
        path = re.sub(r"/{2,}", "/", path)
        if path.endswith("/"):
            path = path.rstrip("/")

    lower_path = path.lower()
    for ext in SKIP_EXTENSIONS:
        if lower_path.endswith(ext):
            return ""

    filtered_query = []
    for k, v in parse_qsl(parts.query, keep_blank_values=True):
        lk = k.strip().lower()
        if not lk:
            continue
        if lk.startswith(TRACKING_QUERY_PREFIX):
            continue
        if lk in TRACKING_QUERY_KEYS:
            continue
        filtered_query.append((k, v))
    query = urlencode(filtered_query, doseq=True)

    return urlunsplit((scheme, netloc, path, query, ""))


def build_seed_candidates(seed_url: str, seed_domain: str) -> List[str]:
    candidates: List[str] = []

    normalized_seed = normalize_url(seed_url)
    if normalized_seed:
        candidates.append(normalized_seed)

    parts = urlsplit(normalized_seed) if normalized_seed else None
    scheme = (parts.scheme or "").lower() if parts else "https"
    if scheme not in ("http", "https"):
        scheme = "https"
    path = (parts.path or "/") if parts else "/"
    query = (parts.query or "") if parts else ""
    port = parts.port if parts else None

    seed_host = normalize_domain(seed_domain) or (normalize_domain(normalized_seed) if normalized_seed else "")
    if seed_host:
        for host in host_variants(seed_host):
            if (scheme == "http" and (port is None or port == 80)) or (scheme == "https" and (port is None or port == 443)):
                netloc = host
            elif port is not None:
                netloc = f"{host}:{port}"
            else:
                netloc = host
            raw = urlunsplit((scheme, netloc, path, query, ""))
            nurl = normalize_url(raw)
            if nurl:
                candidates.append(nurl)

    unique = []
    seen = set()
    for item in candidates:
        if item not in seen:
            seen.add(item)
            unique.append(item)
    return unique


def is_dns_resolve_error(exc: Exception) -> bool:
    text = str(exc).lower()
    markers = (
        "err_name_not_resolved",
        "name_not_resolved",
        "dns_probe_finished_nxdomain",
    )
    return any(m in text for m in markers)


def short_error(prefix: str, exc: Exception, max_len: int = 260) -> str:
    msg = re.sub(r"\s+", " ", str(exc)).strip()
    if len(msg) > max_len:
        msg = msg[:max_len] + "..."
    return f"{prefix}: {msg}"


def can_resolve_host(url: str) -> bool:
    host = normalize_domain(url)
    if not host:
        return False
    try:
        socket.getaddrinfo(host, None)
        return True
    except socket.gaierror:
        return False


def enqueue_alternate_hosts(current_url: str, queue: deque, queued: Set[str], visited: Set[str], domain_suffixes: Set[str]) -> int:
    parts = urlsplit(current_url)
    host = normalize_domain(parts.hostname or "")
    if not host:
        return 0

    scheme = (parts.scheme or "").lower()
    if scheme not in ("http", "https"):
        scheme = "https"
    path = parts.path or "/"
    query = parts.query or ""
    port = parts.port

    added = 0
    for alt in host_variants(host):
        domain_suffixes.add(alt)
        if alt == host:
            continue
        if (scheme == "http" and (port is None or port == 80)) or (scheme == "https" and (port is None or port == 443)):
            netloc = alt
        elif port is not None:
            netloc = f"{alt}:{port}"
        else:
            netloc = alt
        candidate = normalize_url(urlunsplit((scheme, netloc, path, query, "")))
        if not candidate:
            continue
        if candidate in visited or candidate in queued:
            continue
        queue.appendleft(candidate)
        queued.add(candidate)
        added += 1
    return added


def host_matches(url: str, domain_suffixes: Set[str]) -> bool:
    host = (urlsplit(url).hostname or "").strip().lower()
    if not host:
        return False
    for suffix in domain_suffixes:
        s = (suffix or "").strip().lower().strip(".")
        if not s:
            continue
        if host == s or host.endswith("." + s):
            return True
    return False


def extract_links(driver, current_url: str, logger) -> List[str]:
    try:
        raw = driver.execute_script(JS_EXTRACT_LINKS)
    except Exception as e:
        logger.warning(f"提取链接失败: {e}")
        return []

    candidates: List[str] = []
    if isinstance(raw, dict):
        for key in ("canonical", "ogUrl", "location"):
            value = raw.get(key)
            if isinstance(value, str) and value.strip():
                candidates.append(value.strip())

        anchors = raw.get("anchors", [])
        if isinstance(anchors, list):
            for href in anchors:
                if isinstance(href, str) and href.strip():
                    candidates.append(href.strip())

    unique = []
    seen = set()
    for item in candidates:
        nurl = normalize_url(item, current_url)
        if not nurl or nurl in seen:
            continue
        seen.add(nurl)
        unique.append(nurl)
    return unique


def enqueue_fallback_urls(domain_suffixes: Set[str], queue: deque, queued: Set[str], visited: Set[str]) -> None:
    # 当页面上提取不到足够链接时，尝试一些常见路径
    for host in list(domain_suffixes)[:8]:
        for path in COMMON_PATHS:
            candidate = normalize_url(f"https://{host}{path}")
            if not candidate:
                continue
            if candidate in visited or candidate in queued:
                continue
            queue.append(candidate)
            queued.add(candidate)


def crawl_urls(seed_url: str, seed_domain: str, target_count: int, logger) -> Tuple[List[str], int, List[str], str]:
    browser = None
    queue: deque = deque()
    queued: Set[str] = set()
    visited: Set[str] = set()
    collected: List[str] = []
    collected_set: Set[str] = set()
    domain_suffixes: Set[str] = set()
    last_error = ""

    seed_suffix = normalize_domain(seed_domain) or normalize_domain(seed_url)
    if seed_suffix:
        for host in host_variants(seed_suffix):
            domain_suffixes.add(host)

    for candidate in build_seed_candidates(seed_url, seed_domain):
        if candidate in queued:
            continue
        queue.append(candidate)
        queued.add(candidate)
        candidate_host = normalize_domain(candidate)
        if candidate_host:
            for host in host_variants(candidate_host):
                domain_suffixes.add(host)

    max_visits = max(120, target_count * 10)

    try:
        browser = create_chrome_driver(enable_ssl_key_log=False, data_base_dir=_current_dir)
        browser.set_page_load_timeout(35)
        browser.set_script_timeout(30)

        while queue and len(collected) < target_count and len(visited) < max_visits:
            current = queue.popleft()
            queued.discard(current)
            if current in visited:
                continue
            visited.add(current)

            final_url = ""
            try:
                if not can_resolve_host(current):
                    last_error = f"dns_unresolved: {current}"
                    logger.warning(f"DNS预检查失败: {current}")
                    added = enqueue_alternate_hosts(current, queue, queued, visited, domain_suffixes)
                    if added > 0:
                        logger.info(f"DNS预检查失败，已加入 {added} 个候选域名变体: {current}")
                    continue

                browser.get(current)
                WebDriverWait(browser, 12).until(
                    lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
                )
                time.sleep(1.0)
                final_url = normalize_url(browser.current_url or current, current)
            except TimeoutException as e:
                last_error = short_error("timeout", e)
                try:
                    browser.execute_script("window.stop();")
                except Exception:
                    pass
                final_url = normalize_url(browser.current_url or current, current)
            except WebDriverException as e:
                if is_dns_resolve_error(e):
                    last_error = f"dns_unresolved: {current}"
                    logger.warning(f"DNS解析失败: {current} -> {e}")
                    added = enqueue_alternate_hosts(current, queue, queued, visited, domain_suffixes)
                    if added > 0:
                        logger.info(f"已加入 {added} 个候选域名变体: {current}")
                    continue
                last_error = short_error("webdriver", e)
                logger.warning(f"访问失败: {current} -> {e}")
                continue
            except Exception as e:
                last_error = short_error("error", e)
                logger.warning(f"访问异常: {current} -> {e}")
                continue

            if not final_url:
                continue

            final_host = normalize_domain(final_url)
            if final_host:
                domain_suffixes.add(final_host)

            if host_matches(final_url, domain_suffixes) and final_url not in collected_set:
                collected_set.add(final_url)
                collected.append(final_url)

            links = extract_links(browser, final_url, logger)
            for link in links:
                if len(queue) > 3000:
                    break
                if not host_matches(link, domain_suffixes):
                    continue
                if link in visited or link in queued or link in collected_set:
                    continue
                queue.append(link)
                queued.add(link)

            if not queue and len(collected) < target_count:
                enqueue_fallback_urls(domain_suffixes, queue, queued, visited)

            # 控制访问频率：每个页面处理完成后，等待固定间隔再访问下一个页面
            if queue and len(collected) < target_count and PAGE_INTERVAL_SECONDS > 0:
                time.sleep(PAGE_INTERVAL_SECONDS)

        return collected[:target_count], len(visited), sorted(domain_suffixes), last_error
    finally:
        if browser is not None:
            try:
                browser.quit()
            except Exception:
                pass


def write_result(container: str, result: Dict) -> None:
    meta_path = os.path.join(_current_dir, "meta", f"{container}_last.json")
    os.makedirs(os.path.dirname(meta_path), exist_ok=True)
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python action.py '<json_payload>'")
        sys.exit(1)

    payload = json.loads(sys.argv[1])
    container = str(payload.get("container", "unknown")).strip() or "unknown"
    logger = setup_logging(container_name=container, logs_dir=os.path.join(_current_dir, "logs"))

    seed_url = (payload.get("url", "") or "").strip()
    domain = normalize_domain(payload.get("domain", "")) or normalize_domain(seed_url)
    target_count = int(payload.get("target_count", 40) or 40)
    target_count = max(target_count, 1)

    if not seed_url and domain:
        seed_url = f"https://{domain}"

    result = {
        "seed_url": seed_url,
        "domain": domain,
        "target_count": target_count,
        "collected_urls": [],
        "visited_count": 0,
        "allowed_domains": [],
        "error": "",
    }

    kill_chrome_processes()
    time.sleep(0.5)

    try:
        if not domain:
            raise ValueError("invalid domain")
        urls, visited_count, allowed_domains, last_error = crawl_urls(seed_url, domain, target_count, logger)
        result["collected_urls"] = urls
        result["visited_count"] = visited_count
        result["allowed_domains"] = allowed_domains
        result["error"] = last_error
        logger.info(
            f"抓取完成 domain={domain} collected={len(urls)}/{target_count} visited={visited_count}"
        )
    except Exception as e:
        result["error"] = f"{type(e).__name__}: {e}"
        logger.exception(f"抓取异常 domain={domain}: {e}")
    finally:
        kill_chrome_processes()
        write_result(container, result)

    # 始终返回 0：由上层通过结果内容判断是否需要重试。
    sys.exit(0)


if __name__ == "__main__":
    main()
