import os
from datetime import datetime
from typing import BinaryIO, Iterable, List, Optional, Tuple
from xml.etree import ElementTree

from saturnfs.client.aws import AWSPresignedClient
from saturnfs.client.object_storage import ObjectStorageClient
from saturnfs.errors import ExpiredSignature, SaturnError
from saturnfs.schemas import (
    ObjectStorage,
    ObjectStorageCompletedCopy,
    ObjectStorageCompletedUpload,
    ObjectStorageCompletePart,
    ObjectStoragePrefix,
    ObjectStoragePresignedCopy,
    ObjectStoragePresignedDownload,
    ObjectStoragePresignedUpload,
)
from saturnfs.schemas.reference import BulkObjectStorage


class FileTransferClient:
    """
    Translates copy commands to specific upload/download/copy operations
    """

    def __init__(self, object_storage_client: Optional[ObjectStorageClient] = None):
        self.object_storage_client = object_storage_client or ObjectStorageClient()
        self.aws = AWSPresignedClient()

    def upload_file(self, local_path: str, remote_path: str, part_size: Optional[int] = None):
        remote = ObjectStorage.parse(remote_path)
        self._upload(local_path, remote, part_size)

    def upload_dir(self, local_dir: str, remote_prefix: str, part_size: Optional[int] = None):
        remote_dir = ObjectStoragePrefix.parse(remote_prefix)

        for local_path in walk_dir(local_dir):
            local_relative_path = relative_path(local_dir, local_path)
            remote_file = ObjectStorage(
                file_path=os.path.join(remote_dir.prefix or "", local_relative_path),
                org_name=remote_dir.org_name,
                owner_name=remote_dir.owner_name,
            )
            self._upload(local_path, remote_file, part_size)

    def _upload(self, local_path: str, remote: ObjectStorage, part_size: Optional[int] = None):
        size = os.path.getsize(local_path)
        if part_size is not None and part_size > size:
            part_size = size

        upload = self.object_storage_client.start_upload(remote, size, part_size)
        done = False
        completed_parts: List[ObjectStorageCompletePart] = []
        file_offset: int = 0
        while not done:
            parts, done = self._presigned_upload(local_path, upload, file_offset)
            completed_parts.extend(parts)
            if not done:
                # Presigned URL(s) expired during upload
                # TODO: May want to set rate limit/max retries
                upload = self.object_storage_client.resume_upload(upload.object_storage_upload_id)
                file_offset = sum(part.size for part in upload.parts[: len(completed_parts)])
                upload.parts = upload.parts[len(completed_parts) :]

        self.object_storage_client.complete_upload(
            upload.object_storage_upload_id, ObjectStorageCompletedUpload(parts=completed_parts)
        )

    def _presigned_upload(
        self, local_path: str, upload: ObjectStoragePresignedUpload, file_offset: int = 0
    ) -> Tuple[List[ObjectStorageCompletePart], bool]:
        completed_parts: List[ObjectStorageCompletePart] = []
        with open(local_path, "rb") as f:
            f.seek(file_offset)
            for part in upload.parts:
                chunk = FileLimiter(f, part.size)
                try:
                    response = self.aws.put(
                        part.url, chunk, headers={"Content-Type": "application/octet-stream"}
                    )
                except ExpiredSignature:
                    return completed_parts, False

                etag = response.headers["ETag"]
                completed_parts.append(
                    ObjectStorageCompletePart(part_number=part.part_number, etag=etag)
                )

        return completed_parts, True

    def download_file(self, remote_path: str, local_path: str):
        remote = ObjectStorage.parse(remote_path)
        download = self.object_storage_client.download_file(remote)
        self._presigned_download(download, local_path)

    def download_dir(self, remote_prefix: str, local_dir: str):
        remote_dir = ObjectStoragePrefix.parse(remote_prefix)

        for files in self.object_storage_client.list_iter(remote_dir):
            bulk = BulkObjectStorage(
                file_paths=[file.file_path for file in files],
                org_name=remote_dir.org_name,
                owner_name=remote_dir.owner_name,
            )
            bulk_download = self.object_storage_client.download_bulk(bulk)

            for download in bulk_download.files:
                local_path = os.path.join(
                    local_dir, relative_path(remote_dir.prefix, download.file_path)
                )
                self._presigned_download(download, local_path)

    def _presigned_download(self, download: ObjectStoragePresignedDownload, local_path: str):
        dirname = os.path.dirname(local_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)

        response = self.aws.get(download.url, stream=True)
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)

        set_last_modified(local_path, download.updated_at)

    def copy_file(self, source_path: str, destination_path: str, part_size: Optional[int] = None):
        source = ObjectStorage.parse(source_path)
        destination = ObjectStorage.parse(destination_path)
        self._copy_file(source, destination, part_size)

    def copy_dir(
        self, source_prefix: str, destination_prefix: str, part_size: Optional[int] = None
    ):
        source = ObjectStoragePrefix.parse(source_prefix)
        destination = ObjectStoragePrefix.parse(destination_prefix)
        for files in self.object_storage_client.list_iter(source):
            for file in files:
                file_source = ObjectStorage(
                    file_path=file.file_path, org_name=source.org_name, owner_name=source.owner_name
                )
                file_destination = ObjectStorage(
                    file_path=os.path.join(
                        destination.prefix or "", relative_path(source.prefix, file.file_path)
                    ),
                    org_name=destination.org_name,
                    owner_name=destination.owner_name,
                )
                self._copy_file(file_source, file_destination, part_size)

    def _copy_file(
        self, source: ObjectStorage, destination: ObjectStorage, part_size: Optional[int] = None
    ):
        copy = self.object_storage_client.start_copy(source, destination, part_size)
        done = False
        completed_parts: List[ObjectStorageCompletePart] = []
        while not done:
            parts, done = self._presigned_copy(copy)
            completed_parts.extend(parts)
            if not done:
                # Presigned URL(s) expired during copy
                copy = self.object_storage_client.resume_copy(copy.object_storage_copy_id)
                copy.parts = copy.parts[len(completed_parts) :]

        self.object_storage_client.complete_copy(
            copy.object_storage_copy_id, ObjectStorageCompletedCopy(parts=completed_parts)
        )

    def _presigned_copy(
        self, copy: ObjectStoragePresignedCopy
    ) -> Tuple[List[ObjectStorageCompletePart], bool]:
        completed_parts: List[ObjectStorageCompletePart] = []
        for part in copy.parts:
            try:
                response = self.aws.put(part.url, headers=part.headers)
            except ExpiredSignature:
                return completed_parts, False

            etag: Optional[str] = None
            if "Etag" in response.headers:
                # Copying zero-byte object uses upload_part rather than upload_part_copy
                etag = response.headers["ETag"]
            else:
                root = ElementTree.fromstring(response.text)
                namespace = root.tag.split("}")[0].lstrip("{")
                etag = root.findtext(f"./{{{namespace}}}ETag")

            if not etag:
                raise SaturnError("Failed to parse etag from response")

            completed_parts.append(
                ObjectStorageCompletePart(part_number=part.part_number, etag=etag)
            )

        return completed_parts, True


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


def relative_path(prefix: Optional[str], file_path: str) -> str:
    if prefix:
        dirname = f"{os.path.dirname(prefix)}/"
        if file_path.startswith(dirname):
            return file_path[len(dirname) :]
    return file_path

def walk_dir(local_dir: str) -> Iterable[str]:
    for root, _, files in os.walk(local_dir):
        for file in files:
            yield os.path.join(root, file)
