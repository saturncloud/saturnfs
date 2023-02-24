from datetime import datetime
import os
from typing import List, Tuple
from xml.etree import ElementTree
import requests

from saturnfs import settings
from saturnfs.api.object_storage import ObjectStorageAPI
from saturnfs.client.aws import AWSPresignedClient
from saturnfs.errors import ExpiredSignature, PathErrors, SaturnError
from saturnfs.schemas import (
    ObjectStorageCompletedCopy,
    ObjectStoragePresignedCopy,
    ObjectStoragePresignedDownload,
    ObjectStorage,
    ObjectStoragePrefix,
    ObjectStorageCompletePart,
    ObjectStorageCompletedUpload,
    ObjectStoragePresignedUpload,
)


class FileTransferClient:
    def __init__(self):
        self.api = ObjectStorageAPI()
        self.aws = AWSPresignedClient()

    def copy(self, source_path: str, destination_path: str, recursive: bool = False):
        source_is_local = not source_path.startswith(settings.SATURNFS_FILE_PREFIX)
        destination_is_local = not destination_path.startswith(settings.SATURNFS_FILE_PREFIX)
        if source_is_local and destination_is_local:
            raise SaturnError(PathErrors.AT_LEAST_ONE_REMOTE_PATH)

        if recursive:
            if source_is_local:
                self.upload_dir(source_path, destination_path)
            elif destination_is_local:
                self.download_dir(source_path, destination_path)
            else:
                self.copy_dir(source_path, destination_path)
        else:
            if source_is_local:
                self.upload_file(source_path, destination_path)
            elif destination_is_local:
                self.download_file(source_path, destination_path)
            else:
                self.copy_file(source_path, destination_path)

    def upload_file(self, local_path: str, remote_path: str):
        remote = ObjectStorage.parse(remote_path)
        self._upload(local_path, remote)

    def upload_dir(self, local_dir: str, remote_prefix: str):
        remote_dir = ObjectStoragePrefix.parse(remote_prefix)

        for root, _, files in os.walk(local_dir):
            for file in files:
                local_path = os.path.join(root, file)
                local_relative_path = relative_path(local_dir, local_path)
                remote_file = ObjectStorage(
                    file_path=os.path.join(remote_dir.prefix, local_relative_path),
                    org_name=remote_dir.org_name,
                    owner_name=remote_dir.owner_name,
                )
                self._upload(local_path, remote_file)

    def _upload(self, local_path: str, remote: ObjectStorage):
        size = os.path.getsize(local_path)
        if size > settings.S3_MAX_PART_SIZE:
            part_size = settings.S3_MIN_PART_SIZE
        else:
            part_size = size

        upload = self.api.Upload.start(remote, size, part_size)
        done = False
        completed_parts: List[ObjectStorageCompletePart] = []
        file_offset: int = 0
        while not done:
            parts, done = self._presigned_upload(local_path, upload, file_offset)
            completed_parts.extend(parts)
            if not done:
                # Presigned URL(s) expired during upload
                # TODO: May want to set rate limit/max retries
                upload = self.api.Upload.resume(upload.object_storage_upload_id)
                file_offset = sum(part.size for part in upload.parts[:len(completed_parts)])
                upload.parts = upload.parts[len(completed_parts):]

        self.api.Upload.complete(
            upload.object_storage_upload_id, ObjectStorageCompletedUpload(parts=completed_parts)
        )

    def _presigned_upload(
        self, local_path: str, upload: ObjectStoragePresignedUpload, file_offset: int = 0
    ) -> Tuple[List[ObjectStorageCompletePart], bool]:
        completed_parts: List[ObjectStorageCompletePart] = []
        with open(local_path, "r") as f:
            f.seek(file_offset)
            for part in upload.parts:
                chunk = f.read(part.size)
                try:
                    response = self.aws.put(part.url, chunk)
                except ExpiredSignature:
                    return completed_parts, False

                etag = response.headers["ETag"]
                completed_parts.append(
                    ObjectStorageCompletePart(part_number=part.part_number, etag=etag)
                )

        return completed_parts, True

    def download_file(self, remote_path: str, local_path: str):
        remote = ObjectStorage.parse(remote_path)
        download = self.api.Download.get(remote)
        self._presigned_download(download, local_path)

    def download_dir(self, remote_prefix: str, local_dir: str):
        remote_dir = ObjectStoragePrefix.parse(remote_prefix)

        for files in self.api.List.recurse(remote_dir):
            bulk_download = self.api.BulkDownload.get(
                [file.file_path for file in files], remote_dir.org_name, remote_dir.owner_name
            )

            for download in bulk_download.files:
                local_path = os.path.join(
                    local_dir, relative_path(remote_dir.prefix, download.file_path)
                )
                self._presigned_download(download, local_path)

    def _presigned_download(self, download: ObjectStoragePresignedDownload, local_path: str):
        dirname = os.path.dirname(local_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)

        response = self.aws.get(download.url)
        with open(local_path, "wb") as f:
            f.write(response.content)

        set_last_modified(local_path, download.updated_at)

    def copy_file(self, source_path: str, destination_path: str):
        source = ObjectStorage.parse(source_path)
        destination = ObjectStorage.parse(destination_path)

        copy = self.api.Copy.start(source, destination)
        self._presigned_copy(copy)

    def copy_dir(self, source: ObjectStoragePrefix, destination: ObjectStoragePrefix):
        for files in self.api.List.recurse(source):
            for file in files:
                file_source = ObjectStorage(
                    file_path=file.file_path,
                    org_name=source.org_name,
                    owner_name=source.owner_name
                )
                file_destination = ObjectStorage(
                    file_path=os.path.join(
                        destination.prefix, relative_path(source.prefix, file.file_path)
                    ),
                    org_name=destination.org_name,
                    owner_name=destination.owner_name
                )
                copy = self.api.Copy.start(file_source, file_destination)
                self._presigned_copy(copy)

    def _copy_file(self, source: ObjectStorage, destination: ObjectStorage):
        copy = self.api.Copy.start(source, destination)
        done = False
        completed_parts: List[ObjectStorageCompletePart] = []
        while not done:
            parts, done = self._presigned_copy(copy)
            completed_parts.extend(parts)
            if not done:
                # Presigned URL(s) expired during copy
                copy = self.api.Copy.resume(copy.object_storage_copy_id)
                copy.parts = copy.parts[len(completed_parts):]

        self.api.Copy.complete(
            copy.object_storage_copy_id, ObjectStorageCompletedCopy(parts=completed_parts)
        )

    def _presigned_copy(self, copy: ObjectStoragePresignedCopy) -> Tuple[List[ObjectStorageCompletePart], bool]:
        completed_parts: List[ObjectStorageCompletePart] = []
        for part in copy.parts:
            try:
                response = self.aws.put(part.url, headers=part.headers)
            except ExpiredSignature:
                return completed_parts, False

            if "Etag" in response.headers:
                # Copying zero-byte object uses upload_part rather than upload_part_copy
                etag = response.headers["ETag"]
            else:
                root = ElementTree.fromstring(response.text)
                namespace = root.tag.split("}")[0].lstrip("{")
                etag = root.findtext(f"./{{{namespace}}}ETag")
            completed_parts.append(
                ObjectStorageCompletePart(part_number=part.part_number, etag=etag)
            )

        return completed_parts, True


def set_last_modified(local_path: str, last_modified: datetime):
    timestamp = last_modified.timestamp()
    os.utime(local_path, (timestamp, timestamp))


def relative_path(prefix: str, file_path: str) -> str:
    dirname = f"{os.path.dirname(prefix)}/"
    if file_path.startswith(dirname):
        return file_path[len(dirname):]
    return file_path
