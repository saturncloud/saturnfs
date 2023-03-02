class SaturnError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class ExpiredSignature(SaturnError):
    def __init__(self) -> None:
        self.message = "Presigned URL has expired"
        super().__init__(self.message)


class PathErrors:
    INVALID_REMOTE_PATH = (
        "Invalid remote path. Expected format is sfs://<org_name>/<owner_name>/..."
    )
    INVALID_REMOTE_FILE = (
        "Invalid remote file path. Expected format is sfs://<org_name>/<owner_name>/<file_path>"
    )
    AT_LEAST_ONE_REMOTE_PATH = "Either source or destination must be a remote path"
