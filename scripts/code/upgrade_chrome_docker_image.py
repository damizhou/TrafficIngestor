#!/usr/bin/env python3
"""升级采集镜像中的 Chrome 与 ChromeDriver，校验后推送到 Docker Hub。

默认生成的镜像格式为：
    chuanzhoupan/trace_spider_chrome:<Chrome主版本>_<构建日期YYMMDD>

默认基础镜像取自 ``BaseTrafficIngestor.DOCKER_IMAGE``。
仅当新 Chrome 主版本高于基础镜像时才推送正式镜像。

使用前需通过 ``docker login`` 登录目标 Docker Hub 账号。
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


# Allow running this script by absolute path from outside the repository.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from trafficIngestor.host_scheduler.base_traffic_ingestor import BaseTrafficIngestor


DEFAULT_SOURCE_IMAGE = BaseTrafficIngestor.DOCKER_IMAGE
DEFAULT_TARGET_NAMESPACE = "chuanzhoupan"
TARGET_REPOSITORY_NAME = "trace_spider_chrome"
NAMESPACE_PATTERN = re.compile(r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$")
TAG_PATTERN = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$")
FULL_VERSION_PATTERN = re.compile(r"\d+(?:\.\d+){3}")

DOCKERFILE = r"""ARG SOURCE_IMAGE
FROM ${SOURCE_IMAGE}

USER root

RUN set -eux; \
    rm -f \
        /etc/apt/sources.list.d/google-chrome.list \
        /etc/apt/sources.list.d/google-chrome.sources; \
    printf '%s\n' \
        'Acquire::Retries "5";' \
        'Acquire::http::Timeout "60";' \
        'Acquire::https::Timeout "60";' \
        > /etc/apt/apt.conf.d/80-traffic-ingestor-retries; \
    apt-get update; \
    apt-get install -y --no-upgrade --no-install-recommends ca-certificates curl gnupg unzip; \
    install -d -m 0755 /etc/apt/keyrings; \
    curl -fsSL https://dl.google.com/linux/linux_signing_key.pub \
        | gpg --dearmor --yes -o /etc/apt/keyrings/google-chrome.gpg; \
    echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/google-chrome.gpg] https://dl.google.com/linux/chrome/deb/ stable main" \
        > /etc/apt/sources.list.d/google-chrome.list; \
    apt-get update; \
    apt-cache policy google-chrome-stable; \
    apt-get install -y --no-install-recommends google-chrome-stable; \
    chrome_version="$(google-chrome --version | grep -Eo '[0-9]+(\.[0-9]+){3}')"; \
    test -n "$chrome_version"; \
    chrome_build="${chrome_version%.*}"; \
    driver_version="$(curl -fsSL --retry 3 \
        "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_${chrome_build}")"; \
    test "${driver_version%.*}" = "$chrome_build"; \
    driver_url="https://storage.googleapis.com/chrome-for-testing-public/${driver_version}/linux64/chromedriver-linux64.zip"; \
    curl -fL --retry 3 -o /tmp/chromedriver.zip "$driver_url"; \
    unzip -q /tmp/chromedriver.zip -d /tmp/chromedriver; \
    install -m 0755 /tmp/chromedriver/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver; \
    installed_driver_version="$(chromedriver --version | grep -Eo '[0-9]+(\.[0-9]+){3}' | head -n 1)"; \
    test "$installed_driver_version" = "$driver_version"; \
    rm -rf /tmp/chromedriver /tmp/chromedriver.zip; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "升级基础镜像中的 Chrome 与 ChromeDriver；仅在主版本升级时推送新镜像。"
        )
    )
    parser.add_argument(
        "source_image",
        nargs="?",
        default=DEFAULT_SOURCE_IMAGE,
        help=(
            "基础镜像，默认取 BaseTrafficIngestor.DOCKER_IMAGE："
            f"{DEFAULT_SOURCE_IMAGE}"
        ),
    )
    parser.add_argument(
        "target_namespace",
        nargs="?",
        default=DEFAULT_TARGET_NAMESPACE,
        help=f"Docker Hub 命名空间，默认：{DEFAULT_TARGET_NAMESPACE}",
    )
    parser.add_argument(
        "tag",
        nargs="?",
        help="自定义标签；默认使用 <Chrome主版本>_<构建日期YYMMDD>",
    )
    return parser.parse_args()


def run(command: list[str], *, capture_output: bool = False) -> subprocess.CompletedProcess[str]:
    print(f"+ {subprocess.list2cmdline(command)}", flush=True)
    return subprocess.run(
        command,
        check=True,
        capture_output=capture_output,
        text=True,
    )


def check_prerequisites(namespace: str, requested_tag: str | None) -> None:
    if shutil.which("docker") is None:
        raise RuntimeError("缺少 docker 命令")
    if not NAMESPACE_PATTERN.fullmatch(namespace):
        raise ValueError(f"Docker Hub 命名空间格式无效：{namespace}")
    if requested_tag is not None and not TAG_PATTERN.fullmatch(requested_tag):
        raise ValueError(f"镜像标签格式无效：{requested_tag}")

    result = subprocess.run(
        ["docker", "info"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError("Docker daemon 不可用，或当前用户无权访问 Docker")


def read_component_version(image: str, executable: str) -> tuple[str, str]:
    result = run(
        ["docker", "run", "--rm", "--entrypoint", executable, image, "--version"],
        capture_output=True,
    )
    output = "\n".join(part.strip() for part in (result.stdout, result.stderr) if part.strip())
    match = FULL_VERSION_PATTERN.search(output)
    if match is None:
        raise RuntimeError(f"无法从 {executable} 输出解析版本：{output!r}")
    return match.group(0), output


def get_major_version(version: str) -> int:
    return int(version.split(".", maxsplit=1)[0])


def is_major_upgrade(current_version: str, candidate_version: str) -> bool:
    return get_major_version(candidate_version) > get_major_version(current_version)


def remove_temporary_image(image: str) -> None:
    inspect_result = subprocess.run(
        ["docker", "image", "inspect", image],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if inspect_result.returncode != 0:
        return

    remove_result = subprocess.run(
        ["docker", "image", "rm", "--force", image],
        capture_output=True,
        text=True,
        check=False,
    )
    if remove_result.returncode != 0:
        detail = (remove_result.stderr or remove_result.stdout).strip()
        print(f"警告：临时镜像清理失败：{detail}", file=sys.stderr)


def main() -> int:
    args = parse_args()
    check_prerequisites(args.target_namespace, args.tag)

    now = dt.datetime.now()
    build_date = now.strftime("%y%m%d")
    build_id = now.strftime("%Y%m%d%H%M%S")
    target_repository = f"{args.target_namespace}/{TARGET_REPOSITORY_NAME}"
    temporary_image = f"{target_repository}:build-{build_id}"

    try:
        with tempfile.TemporaryDirectory(prefix="chrome-image-build-") as build_context:
            dockerfile_path = Path(build_context, "Dockerfile")
            dockerfile_path.write_text(DOCKERFILE, encoding="utf-8", newline="\n")

            print(f"拉取基础镜像：{args.source_image}")
            run(["docker", "pull", args.source_image])

            current_chrome_version, current_chrome_output = read_component_version(
                args.source_image, "google-chrome"
            )
            print(
                f"当前基础镜像版本：Chrome={current_chrome_version}"
                f"（{current_chrome_output}）"
            )

            print(f"构建升级镜像：{temporary_image}")
            run(
                [
                    "docker",
                    "build",
                    "--pull",
                    "--no-cache",
                    "--build-arg",
                    f"SOURCE_IMAGE={args.source_image}",
                    "--tag",
                    temporary_image,
                    build_context,
                ]
            )

        chrome_version, chrome_output = read_component_version(
            temporary_image, "google-chrome"
        )
        driver_version, driver_output = read_component_version(
            temporary_image, "chromedriver"
        )
        chrome_build = chrome_version.rsplit(".", maxsplit=1)[0]
        driver_build = driver_version.rsplit(".", maxsplit=1)[0]
        if chrome_build != driver_build:
            raise RuntimeError(
                "版本不兼容："
                f"Chrome={chrome_version}（{chrome_output}），"
                f"ChromeDriver={driver_version}（{driver_output}）"
            )

        chrome_major = get_major_version(chrome_version)
        if not is_major_upgrade(current_chrome_version, chrome_version):
            print(
                "无需更新："
                f"当前 Chrome={current_chrome_version}，"
                f"最新 Chrome={chrome_version}；主版本未升级。"
            )
            return 0

        target_tag = args.tag or f"{chrome_major}_{build_date}"
        target_image = f"{target_repository}:{target_tag}"

        print(
            f"版本校验通过：Chrome={chrome_version}，"
            f"ChromeDriver={driver_version}"
        )
        print(f"生成正式标签：{target_image}")
        run(["docker", "tag", temporary_image, target_image])

        print(f"推送镜像：{target_image}")
        run(["docker", "push", target_image])
        print(f"完成：{target_image}")
        print("如需切换项目默认镜像，请更新 BaseTrafficIngestor.DOCKER_IMAGE。")
        return 0
    finally:
        remove_temporary_image(temporary_image)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        print(f"错误：{error}", file=sys.stderr)
        raise SystemExit(1) from error
