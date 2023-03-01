import os
from datetime import datetime
from typing import BinaryIO, List, Tuple

from saturnfs.client.aws import AWSPresignedClient
from saturnfs.errors import ExpiredSignature
from saturnfs.schemas import (
    ObjectStorageCompletePart,
    ObjectStoragePresignedCopy,
    ObjectStoragePresignedDownload,
    ObjectStoragePresignedUpload,
)


class FileTransferClient:
    """
    Translates copy commands to specific upload/download/copy operations
    """

    def __init__(self):
        self.aws = AWSPresignedClient()

    def upload(
        self, local_path: str, presigned_upload: ObjectStoragePresignedUpload, file_offset: int = 0
    ) -> Tuple[List[ObjectStorageCompletePart], bool]:
        completed_parts: List[ObjectStorageCompletePart] = []
        with open(local_path, "rb") as f:
            f.seek(file_offset)
            for part in presigned_upload.parts:
                chunk = FileLimiter(f, part.size)
                try:
                    response = self.aws.put(
                        part.url,
                        chunk,
                        headers={
                            "Content-Type": "application/octet-stream",
                            "Content-Length": str(part.size),
                        }
                    )
                except ExpiredSignature:
                    return completed_parts, False

                etag = self.aws.parse_etag(response)
                completed_parts.append(
                    ObjectStorageCompletePart(part_number=part.part_number, etag=etag)
                )
        return completed_parts, True

    def copy(self, presigned_copy: ObjectStoragePresignedCopy):
        completed_parts: List[ObjectStorageCompletePart] = []
        for part in presigned_copy.parts:
            try:
                response = self.aws.put(part.url, headers=part.headers)
            except ExpiredSignature:
                return completed_parts, False

            etag = self.aws.parse_etag(response)
            completed_parts.append(
                ObjectStorageCompletePart(part_number=part.part_number, etag=etag)
            )

        return completed_parts, True

    def download(self, presigned_download: ObjectStoragePresignedDownload, local_path: str):
        dirname = os.path.dirname(local_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)

        response = self.aws.get(presigned_download.url, stream=True)
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)

        set_last_modified(local_path, presigned_download.updated_at)


class FileLimiter:
    """
    File-like object that limits the max number of bytes to be
    read from the current position of an open file
    """

    def __init__(self, file: BinaryIO, max_bytes: int):
        self.file = file
        self.max_bytes = max_bytes
        self.bytes_read = 0

        # Ensures requests will stream this instead of attempting
        # to treat it as iterable chunks
        self.len = max_bytes

    def read(self, amount: int = -1) -> bytes:
        if self.bytes_read >= self.max_bytes:
            return b""

        bytes_remaining = self.max_bytes - self.bytes_read
        data = self.file.read(min(amount, bytes_remaining))
        self.bytes_read += len(data)
        return data


def set_last_modified(local_path: str, last_modified: datetime):
    timestamp = last_modified.timestamp()
    os.utime(local_path, (timestamp, timestamp))
