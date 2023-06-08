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
    EXPECTED_REMOTE_PATH = "Expected format is sfs://<org>/<identity>/..."
    EXPECTED_REMOTE_FILE = "Expected format is sfs://<org>/<identity>/<file_path>"

    INVALID_REMOTE_PATH = f"Invalid remote path. {EXPECTED_REMOTE_PATH}"
    INVALID_REMOTE_FILE = f"Invalid remote file path. {EXPECTED_REMOTE_FILE}"
    AT_LEAST_ONE_REMOTE_PATH = (
        f"Either source or destination must be a remote path. {EXPECTED_REMOTE_PATH}"
    )
