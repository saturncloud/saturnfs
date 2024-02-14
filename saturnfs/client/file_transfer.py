from __future__ import annotations

import heapq
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from io import BufferedWriter
from math import ceil
from queue import Empty, Full, PriorityQueue, Queue
from threading import Event, Thread
from typing import Any, BinaryIO, List, Optional, Tuple

from fsspec import Callback
from requests import Session
from saturnfs import settings
from saturnfs.api import upload
from saturnfs.client.aws import AWSPresignedClient
from saturnfs.errors import ExpiredSignature
from saturnfs.schemas import (
    ObjectStorageCompletePart,
    ObjectStoragePresignedDownload,
    ObjectStoragePresignedUpload,
)
from saturnfs.schemas.upload import ObjectStoragePresignedPart
from saturnfs.utils import byte_range_header


class FileTransferClient:
    """
    Translates copy commands to specific upload/download/copy operations
    """

    def __init__(self):
        self.aws = AWSPresignedClient()

    def upload(
        self, local_path: str, presigned_upload: ObjectStoragePresignedUpload, file_offset: int = 0, max_workers: int = 10
    ) -> Tuple[List[ObjectStorageCompletePart], bool]:
        if max_workers > 1 and len(presigned_upload.parts) > 1:
            return self._parallel_upload(local_path, presigned_upload, file_offset=file_offset, max_workers=max_workers)

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

    def upload_part(self, data: Any, part: ObjectStoragePresignedPart, **kwargs) -> ObjectStorageCompletePart:
        response = self.aws.put(
            part.url,
            data,
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": str(part.size),
                **part.headers,
            },
            **kwargs,
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
        block_size: int = settings.S3_MIN_PART_SIZE,
        max_workers: int = 10,
    ):
        dirname = os.path.dirname(local_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)

        if max_workers > 1 and presigned_download.size >= 3 * block_size:
            self._parallel_download(
                presigned_download,
                local_path,
                block_size,
                callback=callback,
                max_workers=max_workers,
            )
        else:
            with open(local_path, "wb") as f:
                self.download_to_writer(
                    presigned_download, f, callback=callback, block_size=block_size
                )
        set_last_modified(local_path, presigned_download.updated_at)

    def download_to_writer(
        self,
        presigned_download: ObjectStoragePresignedDownload,
        outfile: BufferedWriter,
        callback: Optional[Callback] = None,
        block_size: int = settings.S3_MIN_PART_SIZE,
        stream: bool = True,
        **kwargs,
    ):
        response = self.aws.get(presigned_download.url, stream=stream, **kwargs)
        if callback is not None:
            content_length = response.headers.get("Content-Length")
            callback.set_size(int(content_length) if content_length else None)

        for chunk in response.iter_content(block_size):
            bytes_written = outfile.write(chunk)
            if callback is not None:
                callback.relative_update(bytes_written)

        if callback is not None and callback.size == 0:
            callback.relative_update(0)
        response.close()

    def _parallel_download(
        self,
        presigned_download: ObjectStoragePresignedDownload,
        local_path: str,
        block_size: int,
        max_workers: int = 10,
        callback: Optional[Callback] = None,
    ):
        filename = os.path.basename(local_path)
        parent_dir = os.path.dirname(local_path)
        num_chunks = ceil(presigned_download.size / block_size)
        last_chunk_size = presigned_download.size % block_size
        num_workers = min(num_chunks, max_workers)

        download_queue: Queue[Optional[DownloadChunk]] = Queue(2 * num_workers)
        completed_queue: PriorityQueue[DownloadChunk] = PriorityQueue()

        # Downloads are non-resumable for now
        with tempfile.TemporaryDirectory(
            prefix=f".saturnfs_{filename}_", dir=parent_dir
        ) as tmp_dir:
            producer_kwargs = {
                "download_queue": download_queue,
                "tmp_dir": tmp_dir,
                "block_size": block_size,
                "num_chunks": num_chunks,
                "last_chunk_size": last_chunk_size,
                "num_workers": num_workers,
            }
            worker_kwargs = {
                "presigned_download": presigned_download,
                "download_queue": download_queue,
                "completed_queue": completed_queue,
                "block_size": block_size,
            }

            Thread(target=self._download_producer, kwargs=producer_kwargs, daemon=True).start()
            for _ in range(num_workers):
                Thread(target=self._download_worker, kwargs=worker_kwargs, daemon=True).start()
            self._reconstruct(completed_queue, local_path, num_chunks, callback=callback)

    def _download_producer(
        self,
        download_queue: Queue[Optional[DownloadChunk]],
        tmp_dir: str,
        block_size: int,
        num_chunks: int,
        last_chunk_size: int,
        num_workers: int,
    ):
        """
        Generate chunks on download queue to be downloaded
        Waits for work to be completed, and signals worker shutdown at the end.
        """
        for i in range(num_chunks):
            if i == num_chunks - 1 and last_chunk_size:
                chunk_size = last_chunk_size
            else:
                chunk_size = block_size

            part_number = i + 1
            tmp_path = os.path.join(tmp_dir, f"part_{part_number}")
            chunk = DownloadChunk(part_number=part_number, chunk_size=chunk_size, tmp_path=tmp_path)
            download_queue.put(chunk)
        # Wait for workers to finish processing all chunks
        download_queue.join()

        # Signal shutdown to download workers
        for i in range(num_workers):
            download_queue.put(None)

    def _download_worker(
        self,
        presigned_download: ObjectStoragePresignedDownload,
        download_queue: Queue[DownloadChunk],
        completed_queue: PriorityQueue[DownloadChunk],
        block_size: int,
    ):
        """
        Pull chunks from the download queue, and write the associated byte range to a temp file.
        Push completed chunk onto the completed queue to be reconstructed.
        """
        session = Session()
        while True:
            chunk = download_queue.get()
            if chunk is None:
                # No more download tasks. Put another sentinal
                # on the queue just in case, then break.
                try:
                    download_queue.put_nowait(None)
                except Full:
                    pass
                break

            start = (chunk.part_number - 1) * block_size
            end = start + chunk.chunk_size
            if end > presigned_download.size:
                end = presigned_download.size
            headers = byte_range_header(start, end)

            with open(chunk.tmp_path, "wb") as f:
                self.download_to_writer(presigned_download, f, headers=headers, session=session)

            download_queue.task_done()
            completed_queue.put(chunk)
        session.close()

    def _reconstruct(
        self,
        completed_queue: PriorityQueue[DownloadChunk],
        local_path: str,
        num_chunks: int,
        callback: Optional[Callback] = None,
    ):
        """
        Pulls chunks from the completed queue and appends them in order to the given file path
        """
        done = False
        next_part: int = 1
        heap: List[DownloadChunk] = []

        def _mv(tmp_path: str, f: BufferedWriter):
            with open(tmp_path, "rb") as tmp:
                f.write(tmp.read())
            os.remove(tmp_path)

        with open(local_path, "wb") as f:
            done = False
            while not done:
                # Get the next completed chunk
                chunk = completed_queue.get()
                if callback is not None:
                    callback.relative_update(chunk.chunk_size)

                if chunk.part_number == next_part:
                    # Append chunk to the final file
                    _mv(chunk.tmp_path, f)
                    if next_part == num_chunks:
                        done = True
                    else:
                        # Check if the next part(s) are already in the heap
                        next_part += 1
                        while len(heap) > 0 and heap[0].part_number == next_part:
                            chunk = heapq.heappop(heap)
                            _mv(chunk.tmp_path, f)
                            if next_part == num_chunks:
                                done = True
                                break
                            next_part += 1
                else:
                    # Push chunk onto the heap to wait for all previous chunks to complete
                    heapq.heappush(heap, chunk)

    def _parallel_upload(
        self,
        local_path: str,
        presigned_upload: ObjectStoragePresignedUpload,
        file_offset: int = 0,
        max_workers: int = 10,
    ):
        num_workers = min(len(presigned_upload.parts), max_workers)
        upload_queue: Queue[Optional[UploadChunk]] = Queue(2 * num_workers)
        completed_queue: Queue[Optional[ObjectStorageCompletePart]] = Queue()
        cancelled = Event()

        worker_kwargs = {
            "upload_queue": upload_queue,
            "completed_queue": completed_queue,
            "cancelled": cancelled,
        }
        for _ in range(num_workers):
            Thread(target=self._upload_worker, kwargs=worker_kwargs, daemon=True).start()

        return self._upload_producer(presigned_upload, upload_queue, completed_queue, cancelled, local_path, file_offset=file_offset, num_workers=num_workers)

    def _upload_producer(
        self,
        presigned_upload: ObjectStoragePresignedUpload,
        upload_queue: Queue[Optional[UploadChunk]],
        completed_queue: Queue[Optional[ObjectStorageCompletePart]],
        cancelled: Event,
        local_path: str,
        file_offset: int,
        num_workers: int,
    ):
        """
        Generate chunks on upload queue to be uploaded
        Waits for work to be completed, and signals worker shutdown at the end.
        """
        with open(local_path, "rb") as f:
            f.seek(file_offset)
            for part in presigned_upload.parts:
                if cancelled.is_set():
                    break

                chunk = UploadChunk(part=part, data=f.read(part.size))
                upload_queue.put(chunk)

        # Wait for workers to finish processing all chunks, or exit due to expired signatures
        uploads_finished = False
        def _queue_complete():
            nonlocal uploads_finished
            upload_queue.join()
            uploads_finished = True
            cancelled.set()

        Thread(target=_queue_complete, daemon=True).start()
        cancelled.wait()

        if not uploads_finished:
            # Consume any remaining chunks
            while True:
                try:
                    upload_queue.get_nowait()
                    upload_queue.task_done()
                except Empty:
                    break

            # Wait for any remaining worker tasks putting completed parts on the completed_queue
            upload_queue.join()

        # Signal shutdown to upload workers
        for _ in range(num_workers):
            upload_queue.put(None)

        # Collect completed parts
        completed_parts: List[ObjectStorageCompletePart] = []
        try:
            for _ in range(len(presigned_upload.parts)):
                completed_part = completed_queue.get_nowait()
                completed_parts.append(completed_part)
                completed_queue.task_done()
        except Empty:
            uploads_finished = False

        completed_parts.sort(key=lambda p: p.part_number)
        completed_len = len(completed_parts)
        if not uploads_finished and completed_len > 0:
            # Throw out non-sequential completed parts
            last_seen = presigned_upload.parts[0].part_number - 1
            for i, part in enumerate(completed_parts):
                if part.part_number != last_seen + 1:
                    completed_len = i
                    break
                last_seen = part.part_number
            completed_parts = completed_parts[:completed_len]

        return completed_parts, uploads_finished

    def _upload_worker(self, upload_queue: Queue[Optional[UploadChunk]], completed_queue: Queue[Optional[ObjectStorageCompletePart]], cancelled: Event):
        with Session() as session:
            while True:
                chunk = upload_queue.get()
                if chunk is None:
                    # No more upload tasks. Put another sentinal
                    # on the queue just in case, then break.
                    try:
                        upload_queue.put_nowait(None)
                    except Full:
                        pass
                    break

                try:
                    completed_part = self.upload_part(chunk.data, chunk.part, session=session)
                except ExpiredSignature:
                    # Signal that an error has occurred
                    cancelled.set()
                    upload_queue.task_done()
                    return

                upload_queue.task_done()
                completed_queue.put(completed_part)

    def close(self):
        self.aws.close()


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


@dataclass
class DownloadChunk:
    """
    Stores information for parallel chunk download
    """

    part_number: int
    chunk_size: int
    tmp_path: str

    def __lt__(self, other: DownloadChunk):
        return self.part_number < other.part_number


@dataclass
class UploadChunk:
    """
    Stores information for parrallel chunk upload
    """

    part: ObjectStoragePresignedPart
    data: Any


def set_last_modified(local_path: str, last_modified: datetime):
    timestamp = last_modified.timestamp()
    os.utime(local_path, (timestamp, timestamp))
