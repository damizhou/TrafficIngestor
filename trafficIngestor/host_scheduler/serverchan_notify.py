"""Server酱通知封装。SendKey 只从环境变量读取。"""

from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Any, Mapping


def send_notification(
    title: str,
    desp: str,
    tags: str,
    *,
    sendkey_env: str = "SERVERCHAN_SENDKEY",
) -> str | None:
    """发送通知；未配置 SendKey 时返回 None，不影响采集流程。"""
    sendkey = os.environ.get(sendkey_env, "").strip()
    if not sendkey:
        default_file = Path(__file__).resolve().parents[2] / ".serverchan_sendkey"
        sendkey_file = Path(
            os.environ.get("SERVERCHAN_SENDKEY_FILE", str(default_file))
        ).expanduser()
        try:
            raw_value = sendkey_file.read_text(encoding="utf-8").strip()
            if raw_value.startswith("{"):
                payload = json.loads(raw_value)
                sendkey = str(payload.get("sendkey", "")).strip()
            else:
                sendkey = "\n".join(
                    line.strip()
                    for line in raw_value.splitlines()
                    if line.strip() and not line.lstrip().startswith("#")
                ).strip()
        except FileNotFoundError:
            sendkey = ""
        except (json.JSONDecodeError, AttributeError, TypeError) as exc:
            raise RuntimeError(f"Server酱密钥文件格式错误: {sendkey_file}") from exc
    if not sendkey:
        return None
    try:
        from serverchan_sdk import sc_send
    except ImportError as exc:
        raise RuntimeError(
            "已配置 Server酱 SendKey，但未安装 serverchan-sdk，请执行 pip install serverchan-sdk"
        ) from exc
    response: Mapping[str, Any] | Any = sc_send(
        sendkey,
        title,
        desp,
        {"tags": tags},
    )
    return str(response)
