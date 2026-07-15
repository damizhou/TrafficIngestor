"""Shared artifact filename helpers."""

import re


def normalize_artifact_token(value) -> str:
    token = re.sub(r"[^0-9A-Za-z]+", "_", str(value or "").strip().lower())
    return token.strip("_")


def extract_major_version(version_text) -> str:
    matched = re.search(r"\d+", str(version_text or ""))
    return matched.group(0) if matched else ""


def build_browser_artifact_label(browser_name, version_text) -> str:
    browser = normalize_artifact_token(browser_name)
    major = extract_major_version(version_text)
    if not browser or not major:
        raise ValueError(f"invalid browser artifact label: browser={browser_name!r}, version={version_text!r}")
    return f"{browser}{major}"


def build_artifact_filename_stem(parsers, formatted_time, task_name, artifact_label=None) -> str:
    filename_prefix = f"{parsers}_" if parsers else ""
    label = normalize_artifact_token(artifact_label)
    label_part = f"_{label}" if label else ""
    return f"{filename_prefix}{formatted_time}{label_part}_{task_name}"
