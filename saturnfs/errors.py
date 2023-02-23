


from typing import Any, Dict


class SaturnError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class PathErrors:
    INVALID_REMOTE_PATH = "Invalid remote path. Expected format is sfs://<org_name>/<owner_name>/..."
    AT_LEAST_ONE_REMOTE_PATH = "Either source or destination must be a remote path"
