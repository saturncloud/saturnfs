import os
from datetime import datetime
from typing import Any, BinaryIO, List, Optional, Tuple

from fsspec import Callback
from saturnfs.client.aws import AWSPresignedClient
from saturnfs.errors import ExpiredSignature
from saturnfs.schemas import (
    ObjectStorageCompletePart,
    ObjectStoragePresignedDownload,
    ObjectStoragePresignedUpload,
)
from saturnfs.schemas.upload import ObjectStoragePresignedPart


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
                    completed_parts.append(self.upload_part(chunk, part))
                except ExpiredSignature:
                    return completed_parts, False
        return completed_parts, True

    def upload_part(self, data: Any, part: ObjectStoragePresignedPart) -> ObjectStorageCompletePart:
        response = self.aws.put(
            part.url,
            data,
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": str(part.size),
                **part.headers,
            },
        )
        etag = self.aws.parse_etag(response)
        return ObjectStorageCompletePart(part_number=part.part_number, etag=etag)

    def copy(
        self, presigned_copy: ObjectStoragePresignedUpload
    ) -> Tuple[List[ObjectStorageCompletePart], bool]:
        completed_parts: List[ObjectStorageCompletePart] = []
        for part in presigned_copy.parts:
            try:
                completed_part = self.copy_part(part)
                completed_parts.append(completed_part)
            except ExpiredSignature:
                return completed_parts, False
        return completed_parts, True

    def copy_part(self, copy_part: ObjectStoragePresignedPart) -> ObjectStorageCompletePart:
        response = self.aws.put(copy_part.url, headers=copy_part.headers)
        etag = self.aws.parse_etag(response)
        return ObjectStorageCompletePart(part_number=copy_part.part_number, etag=etag)

    def download(
        self,
        presigned_download: ObjectStoragePresignedDownload,
        local_path: str,
        callback: Optional[Callback] = None,
    ):
        dirname = os.path.dirname(local_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)

        response = self.aws.get(presigned_download.url, stream=True)
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(None):
                chunk_size = f.write(chunk)
                if callback is not None:
                    callback.relative_update(chunk_size)

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
