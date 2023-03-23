from typing import Optional

import click
from fsspec.callbacks import Callback


class FileOpCallback(Callback):
    """
    Simple callback to print file upload/download/copy operations
    at the outer layer, followed by a "." for each chunk that is transferred.
    """

    def __init__(self, operation: str, inner: bool = False, **kwargs):
        self.operation = operation
        self.inner = inner
        super().__init__(**kwargs)

    def branch(self, path_1, path_2, kwargs):
        click.echo(f"\n{self.operation}: {path_1} to {path_2} ", nl=False)
        kwargs["callback"] = FileOpCallback(self.operation, inner=True)

    def call(self, hook_name: Optional[str] = None, **kwargs):
        if hook_name:
            super().call(hook_name=hook_name, **kwargs)
        elif self.inner:
            click.echo(".", nl=False)

    def set_size(self, size):
        self.size = size
