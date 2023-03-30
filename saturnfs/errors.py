class SaturnError(Exception):
    def __init__(self, message: str, status: int = 400) -> None:
        self.message = message
        self.status = status
        super().__init__(message)


class ExpiredSignature(SaturnError):
    def __init__(self) -> None:
        self.message = "Presigned URL has expired"
        super().__init__(self.message, status=401)


class PathErrors:
    INVALID_REMOTE_PATH = "Invalid remote path. Expected format is sfs://<org>/<identity>/..."
    INVALID_REMOTE_FILE = (
        "Invalid remote file path. Expected format is sfs://<org>/<identity>/<file_path>"
    )
    AT_LEAST_ONE_REMOTE_PATH = "Either source or destination must be a remote path"
