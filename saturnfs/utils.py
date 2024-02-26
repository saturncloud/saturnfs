from enum import Enum
from typing import Dict, Optional

from requests import Session
from requests.adapters import HTTPAdapter, Retry


class Units(int, Enum):
    KiB = 1 << 10
    MiB = 1 << 20
    GiB = 1 << 30
    TiB = 1 << 40


def human_readable_format(size: float):
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if size < 1024.0 or unit == "TiB":
            break
        size /= 1024.0
    formatted = f"{size:.2f}"
    formatted = formatted.rstrip("0").rstrip(".")
    return f"{formatted} {unit}"


def byte_range_header(start: int, end: int) -> Dict[str, str]:
    """
    HTTP byte range header with non-inclusive end
    """
    return {"Range": f"bytes={start}-{end - 1}"}


def requests_session(
    retries: int = 10,
    backoff_factor: float = 0.1,
    headers: Optional[Dict[str, str]] = None,
    **kwargs,
) -> Session:
    retry = Retry(total=retries, backoff_factor=backoff_factor, **kwargs)
    session = Session()
    session.mount("http", HTTPAdapter(max_retries=retry))
    if headers:
        session.headers.update(headers)
    return session
