from datetime import datetime
import os
from typing import List
from xml.etree import ElementTree
import requests

from saturnfs import settings
from saturnfs.api import CopyAPI, DownloadAPI, UploadAPI
from saturnfs.api import download
from saturnfs.api.download import BulkDownloadAPI
from saturnfs.api.list import ListAPI
from saturnfs.errors import PathErrors, SaturnError
from saturnfs.schemas.copy import ObjectStorageCompletedCopy, ObjectStoragePresignedCopy
from saturnfs.schemas.download import ObjectStoragePresignedDownload
from saturnfs.schemas.reference import FileReference, PrefixReference
from saturnfs.schemas.upload import ObjectStorageCompletePart, ObjectStorageCompletedUpload


class FileTransfer:
    def __init__(self):
        self.aws_session = requests.Session()

    def copy(self, source_path: str, destination_path: str, recursive: bool = False):
        if recursive:
            return self.copy_recursive(source_path, destination_path)

        source_is_local = not source_path.startswith(settings.SATURNFS_FILE_PREFIX)
        destination_is_local = not destination_path.startswith(settings.SATURNFS_FILE_PREFIX)
        if source_is_local and destination_is_local:
            raise SaturnError(PathErrors.AT_LEAST_ONE_REMOTE_PATH)

        if source_is_local:
            self.upload_local(source_path, FileReference.parse(destination_path))
        elif destination_is_local:
            self.download_remote(FileReference.parse(source_path), destination_path)
        else:
            self.copy_remote(
                FileReference.parse(source_path), FileReference.parse(destination_path)
            )

    def copy_recursive(self, source_path: str, destination_path: str):
        source_is_local = not source_path.startswith(settings.SATURNFS_FILE_PREFIX)
        destination_is_local = not destination_path.startswith(settings.SATURNFS_FILE_PREFIX)
        if source_is_local and destination_is_local:
            raise SaturnError(PathErrors.AT_LEAST_ONE_REMOTE_PATH)

        if source_is_local:
            self.upload_local_recursive(source_path, PrefixReference.parse(destination_path))
        elif destination_is_local:
            self.download_remote_recursive(PrefixReference.parse(source_path), destination_path)
        else:
            self.copy_remote_recursive(
                PrefixReference.parse(source_path), PrefixReference.parse(destination_path)
            )


    def upload_local(self, local_path: str, remote: FileReference):
        size = os.path.getsize(local_path)
        if size > settings.S3_MAX_PART_SIZE:
            part_size = settings.S3_MIN_PART_SIZE
        else:
            part_size = size

        upload_api = UploadAPI()
        upload = upload_api.create(remote, size, part_size)

        completed_parts: List[ObjectStorageCompletePart] = []
        with open(local_path, "r") as f:
            for part in upload.parts:
                chunk = f.read(part.size)
                response = self.aws_session.put(part.url, chunk)
                if not response.ok:
                    raise SaturnError(response.text)

                etag = response.headers["ETag"]
                completed_parts.append(
                    ObjectStorageCompletePart(part_number=part.part_number, etag=etag)
                )

        upload_api.complete(
            upload.object_storage_upload_id, ObjectStorageCompletedUpload(parts=completed_parts)
        )

    def upload_local_recursive(self, local_dir: str, remote: PrefixReference):
        for root, _, files in os.walk(local_dir):
            for file in files:
                local_path = os.path.join(root, file)
                local_relative_path = relative_path(local_dir, local_path)
                remote_path = FileReference(
                    file_path=os.path.join(remote.prefix, local_relative_path),
                    org_name=remote.org_name,
                    owner_name=remote.owner_name,
                )
                self.upload_local(local_path, remote_path)

    def download_remote(self, remote: FileReference, local_path: str):
        download_api = DownloadAPI()
        download = download_api.get(remote)
        self._download_file(download, local_path)

    def download_remote_recursive(self, remote: PrefixReference, local_dir: str):
        bulk_download_api = BulkDownloadAPI()
        list_api = ListAPI()

        for files in list_api.recurse(remote):
            bulk_download = bulk_download_api.get(
                [file.file_path for file in files], remote.org_name, remote.owner_name
            )

            for download in bulk_download.files:
                local_path = os.path.join(
                    local_dir, relative_path(remote.prefix, download.file_path)
                )
                self._download_file(download, local_path)

    def _download_file(self, download: ObjectStoragePresignedDownload, local_path: str):
        dirname = os.path.dirname(local_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)

        with open(local_path, "wb") as f:
            response = self.aws_session.get(download.url)
            if not response.ok:
                raise SaturnError(response.text)
            f.write(response.content)

        set_last_modified(local_path, download.updated_at)

    def copy_remote(self, source: FileReference, destination: FileReference):
        copy_api = CopyAPI()
        copy = copy_api.create(source, destination)
        self._copy_file(copy_api, copy)

    def copy_remote_recursive(self, source: PrefixReference, destination: PrefixReference):
        copy_api = CopyAPI()
        list_api = ListAPI()

        for files in list_api.recurse(source):
            for file in files:
                file_source = FileReference(
                    file_path=file.file_path,
                    org_name=source.org_name,
                    owner_name=source.owner_name
                )
                file_destination = FileReference(
                    file_path=os.path.join(
                        destination.prefix, relative_path(source.prefix, file.file_path)
                    ),
                    org_name=destination.org_name,
                    owner_name=destination.owner_name
                )
                copy = copy_api.create(file_source, file_destination)
                self._copy_file(copy_api, copy)

    def _copy_file(self, copy_api: CopyAPI, copy: ObjectStoragePresignedCopy):
        completed_parts: List[ObjectStorageCompletePart] = []
        for part in copy.parts:
            response = self.aws_session.put(
                part.url, headers=part.headers
            )
            if not response.ok:
                raise SaturnError(response.text)

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

        copy_api.complete(
            copy.object_storage_copy_id, ObjectStorageCompletedCopy(parts=completed_parts)
        )



def set_last_modified(local_path: str, last_modified: datetime):
    timestamp = last_modified.timestamp()
    os.utime(local_path, (timestamp, timestamp))


def relative_path(prefix: str, file_path: str) -> str:
    dirname = f"{os.path.dirname(prefix)}/"
    if file_path.startswith(dirname):
        return file_path[len(dirname):]
    return file_path
