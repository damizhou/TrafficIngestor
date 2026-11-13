"""Offline regression checks; run with python tests/test_url_find_page_validation.py."""

import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parents[1]
PAGE_VALIDATION = ROOT / "trafficIngestor" / "urlFind" / "page_validation.py"


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


validation = load_module("page_validation", PAGE_VALIDATION)


def snapshot(**changes):
    result = dict(
        url="https://example.com/news", ready_state="complete", content_type="text/html",
        html="<html><body><h1>News</h1><p>Latest news</p></body></html>",
        title="News", headings="News", text="Latest news", password=False,
        challenge=False, media=False,
    )
    result.update(changes)
    return result


def response(**changes):
    result = dict(status=200, mimeType="text/html", headers={})
    result.update(changes)
    return result


def event(frame="main", loader="final", kind="Document", **changes):
    return {"message": json.dumps({"message": {
        "method": "Network.responseReceived", "params": {
            "type": kind, "frameId": frame, "loaderId": loader,
            "response": response(**changes),
        },
    }})}


class ValidationTests(unittest.TestCase):
    def test_success_and_xhtml(self):
        self.assertEqual(validation.validate_page(snapshot(), response()), 200)
        self.assertEqual(validation.validate_page(snapshot(), response(mimeType="application/xhtml+xml")), 200)

    def test_http_errors(self):
        for status in [301, 304, 403, 404, 429, 500]:
            with self.subTest(status=status), self.assertRaisesRegex(validation.PageRejected, "http_status"):
                validation.validate_page(snapshot(), response(status=status))

    def test_resources_with_extensionless_urls(self):
        for mime in ["image/png", "application/pdf", "text/css", "application/javascript", "application/json", ""]:
            with self.subTest(mime=mime), self.assertRaisesRegex(validation.PageRejected, "non_html"):
                validation.validate_page(snapshot(), response(mimeType=mime))

    def test_resource_suffixes(self):
        for url in ["https://example.com/a.JS?v=1", "https://example.com/a%2Epdf", "https://example.com/a.png#x", "data:text/html,hello"]:
            with self.subTest(url=url):
                self.assertFalse(validation.is_page_url(url))
        self.assertTrue(validation.is_page_url("https://example.com/news.php?id=1"))

    def test_attachment_and_header_mime(self):
        for headers, reason in [
            ({"Content-Disposition": 'attachment; filename="page.html"'}, "download_attachment"),
            ({"Content-Type": "image/jpeg"}, "non_html"),
        ]:
            with self.subTest(headers=headers), self.assertRaisesRegex(validation.PageRejected, reason):
                validation.validate_page(snapshot(), response(headers=headers))

    def test_html_header_does_not_override_browser_resource_type(self):
        with self.assertRaisesRegex(validation.PageRejected, "non_html_document"):
            validation.validate_page(snapshot(), response(mimeType="application/pdf", headers={"Content-Type": "text/html"}))
        with self.assertRaisesRegex(validation.PageRejected, "non_html_document"):
            validation.validate_page(snapshot(content_type="image/png"), response())

    def test_challenges_login_and_error_pages(self):
        cases = [
            ({"title": "Just a moment..."}, "challenge_or_error_page"),
            ({"headings": "Access Denied"}, "challenge_or_error_page"),
            ({"text": "Access Denied", "headings": "", "title": ""}, "challenge_or_error_page"),
            ({"challenge": True}, "visible_challenge"),
            ({"title": "Sign in to continue"}, "login_page"),
            ({"password": True}, "login_page"),
            ({"url": "https://example.com/login"}, "login_page"),
        ]
        for changes, reason in cases:
            with self.subTest(changes=changes), self.assertRaisesRegex(validation.PageRejected, reason):
                validation.validate_page(snapshot(**changes), response())

    def test_article_mentioning_login_is_not_login_page(self):
        page = snapshot(text="This article explains login, captcha and access denied errors.")
        self.assertEqual(validation.validate_page(page, response()), 200)

    def test_blank_and_loading(self):
        for changes, reason in [({"text": "  "}, "blank_page"), ({"ready_state": "loading"}, "document_not_ready")]:
            with self.assertRaisesRegex(validation.PageRejected, reason):
                validation.validate_page(snapshot(**changes), response())
        self.assertEqual(validation.validate_page(snapshot(text="", media=True), response()), 200)

    def test_final_main_document_not_iframe_or_resource(self):
        entries = [event(loader="old", status=200), event(status=403),
                   event(frame="child", status=200), event(kind="Script", status=200)]
        result = validation.main_document_response(entries, {"id": "main", "loaderId": "final"})
        self.assertEqual(result["status"], 403)

    def test_missing_evidence_rejected(self):
        with self.assertRaisesRegex(validation.PageRejected, "missing_main_document_response"):
            validation.main_document_response([event(loader="old")], {"id": "main", "loaderId": "final"})

    def test_snapshot_after_redirect_and_fragment(self):
        browser = Mock()
        frame = dict(id="main", loaderId="final", url="https://example.com/news", urlFragment="#section")
        browser.execute_cdp_cmd.return_value = {"frameTree": {"frame": frame}}
        browser.execute_script.return_value = snapshot(url=frame["url"] + frame["urlFragment"])
        browser.get_log.return_value = [event(loader="old", status=302), event()]
        page, status = validation.read_validated_page(browser)
        self.assertEqual(page["url"], "https://example.com/news#section")
        self.assertEqual(status, 200)

    def test_navigation_race_rejected(self):
        browser = Mock()
        browser.execute_cdp_cmd.side_effect = [
            {"frameTree": {"frame": dict(id="main", loaderId=loader, url="https://example.com/news")}}
            for loader in ["old", "final"]
        ]
        browser.execute_script.return_value = snapshot()
        browser.get_log.return_value = [event()]
        with self.assertRaisesRegex(validation.PageRejected, "navigation_changed"):
            validation.read_validated_page(browser)


if __name__ == "__main__":
    unittest.main()
