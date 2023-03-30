import json
from enum import Enum
from glob import has_magic
from typing import Any, Dict, List, Optional, Union

import click
from saturnfs.errors import SaturnError
from saturnfs.schemas.list import ObjectStorageInfo
from saturnfs.schemas.upload import ObjectStorageUploadInfo
from saturnfs.utils import human_readable_format


class OutputFormats(str, Enum):
    TABLE = "table"
    JSON = "json"

    def __str__(self) -> str:
        return str(self.value)

    @classmethod
    def values(cls) -> List[str]:
        return [str(value) for value in list(cls)]  # type: ignore

    @classmethod
    def validate(cls, format: str):
        if format not in cls.values():
            raise SaturnError(f'Unknown output format "{format}"')


def print_json(data: Union[List, Dict]):
    click.echo(json.dumps(data, indent=2))


def print_file_table(
    results: List[ObjectStorageInfo],
    human_readable: bool = False,
    relative_prefix: Optional[str] = None,
):
    headers = ["LastModified", "Size", "RelativePath" if relative_prefix else "Path"]
    data: List[List[str]] = []
    for info in results:
        last_modified = ""
        size = ""
        if info.updated_at:
            last_modified = info.updated_at.astimezone().strftime("%Y-%m-%d %H:%M:%S")
        if not info.is_dir:
            size = human_readable_format(info.size) if human_readable else str(info.size)
        if relative_prefix and info.name.startswith(relative_prefix):
            if info.name == relative_prefix:
                path = info.name.rsplit("/", 1)[-1]
            else:
                path = info.name[len(relative_prefix) :].lstrip("/")
        else:
            path = info.name
        data.append([last_modified, size, path])
    tabulate(data, headers, justify={"Size": ">"}, minwidth={"LastModified": 20, "Size": 10})


def print_upload_table(
    uploads: List[ObjectStorageUploadInfo], is_not_copy: bool = False, human_readable: bool = False
):
    headers = ["ID", "Expiration", "Size", "Name"]
    if not is_not_copy:
        headers.append("CopySource")

    data: List[List[str]] = []
    for upload in uploads:
        expires = upload.expires_at.astimezone().strftime("%Y-%m-%d %H:%M:%S")
        size = ""
        if upload.size is not None:
            size = human_readable_format(upload.size) if human_readable else str(upload.size)
        row = [upload.id, expires, size, upload.name]
        if not is_not_copy:
            row.append(upload.copy_source.name if upload.copy_source else "")
        data.append(row)

    tabulate(
        data, headers, justify={"Size": ">"}, minwidth={"ID": 32, "Expiration": 20, "Size": 10}
    )


def tabulate(
    data: List[List[Any]],
    headers: List[str],
    rpadding: int = 4,
    justify: Optional[Dict[str, str]] = None,
    minwidth: Optional[Dict[str, int]] = None,
):
    justify = justify if justify else {}
    minwidth = minwidth if minwidth else {}
    widths: List[int] = [0] * len(headers)
    for i, header in enumerate(headers):
        widths[i] = max(len(header), minwidth.get(header, 0))

    for row in data:
        for i, value in enumerate(row):
            widths[i] = max(widths[i], len(str(value)))

    header_format_str = ""
    format_str = ""
    for i, width in enumerate(widths):
        justify_char = justify.get(headers[i], "<")
        format_str += f"{{:{justify_char}{width}}}"
        header_format_str += f"{{:<{width}}}"
        if i < len(widths) - 1:
            format_str += " " * rpadding
            header_format_str += " " * rpadding

    click.echo(header_format_str.format(*headers))
    click.echo("-" * (sum(widths) + rpadding * (len(headers) - 1)))
    for row in data:
        click.echo(format_str.format(*row))


def strip_glob(path: str) -> str:
    """
    Return the longest full directory prefix of path with no glob characters

    e.g. org/identity/my/path*/tofiles?.txt -> org/identity/my/
    """
    if not has_magic(path):
        return path

    indstar = path.find("*") if path.find("*") >= 0 else len(path)
    indques = path.find("?") if path.find("?") >= 0 else len(path)
    indbrace = path.find("[") if path.find("[") >= 0 else len(path)
    first = min(indstar, indques, indbrace)

    path = path[:first]
    if "/" in path:
        return path.rsplit("/", 1)[0]
    return ""
