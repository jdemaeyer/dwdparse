import logging
import pathlib
import tempfile
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlparse

from dwdparse.parsers import Parser, StrPath, get_parser
from dwdparse.utils import fetch


logger = logging.getLogger(__name__)


def _force_parser(filename: str) -> Parser:
    parser_cls = get_parser(filename)
    if parser_cls is None:
        raise ValueError(f"No parser found for {filename}")
    return parser_cls()


def parse(
    path: StrPath,
    parser: Parser | None = None,
    **extra: Any,
) -> Iterator[dict[str, Any]]:
    parser = parser or _force_parser(pathlib.Path(path).name)
    return parser.parse(path, **extra)


def parse_url(
    url: str,
    parser: Parser | None = None,
) -> Iterator[dict[str, Any]]:
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = _download(url, tmpdir)
        parser = parser or _force_parser(file_path.name)
        extra = {
            kwarg: _download(extra_url, tmpdir)
            for kwarg, extra_url in parser.get_extra_urls(file_path).items()
        }
        yield from parser.parse(file_path, **extra)


def _download(url: str, tmpdir: str) -> pathlib.Path:
    dir_path = pathlib.Path(tmpdir)
    filename = pathlib.Path(urlparse(url).path).name
    file_path = dir_path / filename
    logger.info("Downloading %s to %s", url, file_path)
    with file_path.open('wb') as f:
        f.write(fetch(url))
    return file_path
