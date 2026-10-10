"""Validate the rendered page against its top-level Chrome network response."""

import json
import re
from urllib.parse import unquote, urlsplit


PAGE_SETTLE_SECONDS = 15
HTML_MIME_TYPES = {"text/html", "application/xhtml+xml"}
RESOURCE_SUFFIXES = {
    ".js", ".mjs", ".css", ".map", ".json", ".xml", ".txt", ".csv",
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".svg", ".ico",
    ".bmp", ".tif", ".tiff", ".pdf", ".zip", ".gz", ".rar", ".7z",
    ".tar", ".exe", ".dmg", ".apk", ".bin", ".wasm", ".woff", ".woff2",
    ".ttf", ".otf", ".mp3", ".mp4", ".wav", ".ogg", ".webm", ".mov",
    ".avi", ".m3u8", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
}
BLOCKED_HEADING = re.compile(
    r"access denied|request blocked|forbidden|verify (?:that )?you are human|"
    r"checking your browser|just a moment|security check|captcha|"
    r"page not found|404(?:\s|$)|site can.t be reached|"
    r"拒绝访问|访问被拒绝|访问受限|人机验证|安全验证|验证码|页面不存在|无法访问此网站",
    re.IGNORECASE,
)
LOGIN_HEADING = re.compile(r"\blog\s*in\b|\bsign\s*in\b|登录|登入", re.IGNORECASE)
LOGIN_PATH = re.compile(r"/(?:login|signin|sign-in|log-in)(?:/|$)", re.IGNORECASE)

# Snapshot all fields in one JS execution so the URL and DOM refer to the same page.
PAGE_SNAPSHOT_SCRIPT = r"""
const visible = el => {
    const style = getComputedStyle(el);
    return el.getClientRects().length > 0 && style.visibility !== 'hidden'
        && style.display !== 'none' && Number(style.opacity) !== 0;
};
const anyVisible = selector => [...document.querySelectorAll(selector)].some(visible);
return {
    url: location.href,
    ready_state: document.readyState,
    content_type: document.contentType,
    html: document.documentElement ? document.documentElement.outerHTML : '',
    title: document.title,
    headings: [...document.querySelectorAll('h1,[role="heading"][aria-level="1"]')]
        .filter(visible).map(el => el.innerText).join('\n'),
    text: document.body && visible(document.body) ? document.body.innerText.trim() : '',
    password: anyVisible('input[type="password"]'),
    challenge: anyVisible('iframe[src*="recaptcha"][src*="bframe"],'
        + 'iframe[src*="hcaptcha"],iframe[src*="challenges.cloudflare.com"],'
        + '#challenge-running,#challenge-stage,form#challenge-form'),
    media: [...document.querySelectorAll('img,video,canvas,svg')].some(el => {
        const rect = el.getBoundingClientRect();
        return visible(el) && rect.width >= 100 && rect.height >= 100
            && (el.tagName !== 'IMG' || (el.complete && el.naturalWidth > 0));
    })
};
"""


class PageRejected(ValueError):
    """The page cannot be accepted as a successfully loaded HTML page."""


def is_page_url(url: str) -> bool:
    parsed = urlsplit(url)
    path = unquote(parsed.path).lower().rstrip("/")
    return (parsed.scheme in {"http", "https"} and bool(parsed.hostname)
            and not any(path.endswith(suffix) for suffix in RESOURCE_SUFFIXES))


def main_document_response(entries: list, frame: dict) -> dict:
    """Ignore subresources, iframes and earlier documents in a redirect chain."""
    response = None
    for entry in entries:
        event = json.loads(entry["message"])["message"]
        if event["method"] != "Network.responseReceived":
            continue
        params = event["params"]
        if (params.get("type") == "Document"
                and params.get("frameId") == frame["id"]
                and params.get("loaderId") == frame["loaderId"]):
            response = params["response"]
    if response is None:
        raise PageRejected("missing_main_document_response")
    return response


def validate_page(snapshot: dict, response: dict) -> int:
    raw_status = response["status"]
    status = int(raw_status)
    if raw_status != status or not 200 <= status < 300:
        raise PageRejected(f"http_status:{raw_status}")
    headers = {name.lower(): str(value) for name, value in response.get("headers", {}).items()}
    if headers.get("content-disposition", "").split(";", 1)[0].strip().lower() == "attachment":
        raise PageRejected("download_attachment")
    content_type = headers.get("content-type", response.get("mimeType", ""))
    mime = content_type.split(";", 1)[0].strip().lower()
    if mime not in HTML_MIME_TYPES:
        raise PageRejected(f"non_html:{mime or 'unknown'}")
    browser_mime = response.get("mimeType", "").lower()
    if browser_mime not in HTML_MIME_TYPES or snapshot["content_type"].lower() not in HTML_MIME_TYPES:
        raise PageRejected(f"non_html_document:{browser_mime or 'unknown'}")
    if not is_page_url(snapshot["url"]):
        raise PageRejected("resource_or_non_http_url")
    if snapshot["ready_state"] != "complete":
        raise PageRejected("document_not_ready")
    headings = snapshot["title"] + "\n" + snapshot["headings"]
    if BLOCKED_HEADING.search(headings):
        raise PageRejected("challenge_or_error_page")
    if BLOCKED_HEADING.fullmatch(snapshot["text"].strip()):
        raise PageRejected("challenge_or_error_page")
    if snapshot["challenge"]:
        raise PageRejected("visible_challenge")
    if (snapshot["password"] or LOGIN_HEADING.search(headings)
            or LOGIN_PATH.search(unquote(urlsplit(snapshot["url"]).path))):
        raise PageRejected("login_page")
    if not snapshot["html"].strip() or (not snapshot["text"].strip() and not snapshot["media"]):
        raise PageRejected("blank_page")
    return status


def read_validated_page(browser) -> tuple[dict, int]:
    frame = browser.execute_cdp_cmd("Page.getFrameTree", {})["frameTree"]["frame"]
    snapshot = browser.execute_script(PAGE_SNAPSHOT_SCRIPT)
    entries = browser.get_log("performance")
    final_frame = browser.execute_cdp_cmd("Page.getFrameTree", {})["frameTree"]["frame"]
    final_url = final_frame["url"] + final_frame.get("urlFragment", "")
    initial_url = frame["url"] + frame.get("urlFragment", "")
    if (frame["id"] != final_frame["id"] or frame["loaderId"] != final_frame["loaderId"]
            or initial_url != final_url or snapshot["url"] != final_url):
        raise PageRejected("navigation_changed_during_validation")
    response = main_document_response(entries, final_frame)
    return snapshot, validate_page(snapshot, response)
