import json
import logging
import urllib.request
from collections.abc import Iterable
from typing import Any, cast


def configure_logging() -> None:
    log_fmt = '%(asctime)s %(name)s %(levelname)s  %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=log_fmt)
    # Disable some third-party noise
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def dump_records(it: Iterable[dict[str, Any]]) -> None:
    for record in it:
        print(json.dumps(record, default=str))


def fetch(url: str, timeout: float = 10) -> bytes:
    with urllib.request.urlopen(url, timeout=timeout) as f:
        return cast(bytes, f.read())
