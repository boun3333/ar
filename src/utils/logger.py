from __future__ import annotations
import os
import logging
import logging.config
from pathlib import Path
from typing import Literal, Optional
import yaml
from .config import LOG_CONFIG_PATH, LOG_DIR

# kind에 대한 타입 힌트 (원하면 Enum으로 바꿔도 OK)
LogKind = Literal["system", "stdout", "access", "error"]

# 공용 매핑
_BASE_LOGGERS = {
    "system": "app.system",
    "stdout": "app.stdout",
    "access": "uvicorn.access",
    "error":  "uvicorn.error",
}

def get_logger(kind: LogKind = "system", name: Optional[str] = None) -> logging.Logger:
    """
    팀 표준 로거 팩토리.
    - kind: 'system' | 'stdout' | 'access' | 'error'
    - name: 서브 로거 이름(보통 __name__), 없으면 베이스 그대로 반환
    """
    base = _BASE_LOGGERS[kind]
    full_name = f"{base}.{name}" if name else base
    return logging.getLogger(full_name)


def load_logging():
    log_dir = LOG_DIR
    logging_config_path = LOG_CONFIG_PATH
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    with open(logging_config_path, "r", encoding="utf-8") as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)

    def replace(obj):
        if isinstance(obj, dict):
            return {k: replace(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [replace(v) for v in obj]
        if isinstance(obj, str):
            return obj.replace("{LOG_DIR}", log_dir)
        return obj

    cfg = replace(cfg)
    logging.config.dictConfig(cfg)

load_logging()
