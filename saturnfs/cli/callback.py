from __future__ import annotations

from typing import Dict, Optional

import click
from fsspec.callbacks import Callback
from saturnfs import settings


class FileOpCallback(Callback):
    """
    Simple callback to print file upload/download/copy/delete operations
    at the outer layer, followed by a "." for each chunk that is transferred.
    """

    def __init__(self, operation: str, inner: bool = False, first_branch: bool = False, **kwargs):
        self.operation = operation
        self.inner = inner
        self.first_branch = first_branch
        super().__init__(**kwargs)

    def branch(self, path_1: str, path_2: str, kwargs: Dict):
        if not self.first_branch:
            self.first_branch = True
        else:
            click.echo()

        description = self._description(path_1, path_2)
        click.echo(f"{self.operation}: {description} ", nl=False)
        callback = FileOpCallback(self.operation, inner=True)
        kwargs["callback"] = callback

    def call(self, hook_name: Optional[str] = None, **kwargs):
        if hook_name:
            super().call(hook_name=hook_name, **kwargs)
        elif self.inner:
            click.echo(".", nl=False)

    def set_size(self, size: int):
        self.size = size

    def _description(self, path_1: str, path_2: str) -> str:
        if self.operation in {"download", "copy"}:
            path_1 = self._remote_format(path_1)
        if self.operation in {"upload", "copy"}:
            path_2 = self._remote_format(path_2)
        if path_2:
            return f"{path_1} to {path_2}"
        return path_1

    def _remote_format(self, path: str) -> str:
        if path and not path.startswith(settings.SATURNFS_FILE_PREFIX):
            path = path.lstrip("/")
            path = f"{settings.SATURNFS_FILE_PREFIX}{path}"
        return path


def file_op(src_is_local: bool, dst_is_local: bool) -> str:
    if src_is_local:
        return "upload"
    elif dst_is_local:
        return "download"
    return "copy"
