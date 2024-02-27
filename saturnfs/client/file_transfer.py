from __future__ import annotations

import heapq
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime
from io import BufferedReader, BytesIO
from math import ceil
from queue import Empty, Full, PriorityQueue, Queue
from threading import Event, Thread
from typing import Any, BinaryIO, Dict, Iterable, List, Optional, Tuple, Union

from fsspec import Callback
from saturnfs import settings
from saturnfs.client.aws import AWSPresignedClient
from saturnfs.errors import ExpiredSignature
from saturnfs.schemas import (
    ObjectStorageCompletePart,
    ObjectStoragePresignedDownload,
    ObjectStoragePresignedUpload,
)
from saturnfs.schemas.upload import ObjectStoragePresignedPart
from saturnfs.utils import byte_range_header, requests_session


class FileTransferClient:
    """
    Translates copy commands to specific upload/download/copy operations
    """

    def __init__(self):
        self.aws = AWSPresignedClient()

    def upload(
        self,
        local_path: str,
        presigned_upload: ObjectStoragePresignedUpload,
        file_offset: int = 0,
        max_workers: int = settings.SATURNFS_DEFAULT_MAX_WORKERS,
        callback: Optional[Callback] = None,
    ) -> Tuple[List[ObjectStorageCompletePart], bool]:
        if max_workers > 1 and len(presigned_upload.parts) > 1:
            return self._parallel_upload(
                local_path,
                presigned_upload,
                file_offset=file_offset,
                max_workers=max_workers,
                callback=callback,
            )

        completed_parts: List[ObjectStorageCompletePart] = []
        with open(local_path, "rb") as f:
            f.seek(file_offset)
            for part in presigned_upload.parts:
                chunk = FileLimiter(f, part.size)
                try:
                    completed_parts.append(self.upload_part(chunk, part))
                    if callback is not None:
                        callback.relative_update(part.size)
                except ExpiredSignature:
                    return completed_parts, False
        return completed_parts, True

    def upload_part(
        self, data: Any, part: ObjectStoragePresignedPart, **kwargs
    ) -> ObjectStorageCompletePart:
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
        return ObjectStorageCompletePart(part_number=part.part_number, etag=etag, size=part.size)

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
        return ObjectStorageCompletePart(
            part_number=copy_part.part_number, etag=etag, size=copy_part.size
        )

    def download(
        self,
        presigned_download: ObjectStoragePresignedDownload,
        destination: Union[str, BinaryIO],
        callback: Optional[Callback] = None,
        block_size: int = settings.S3_MIN_PART_SIZE,
        offset: int = 0,
        max_workers: int = settings.SATURNFS_DEFAULT_MAX_WORKERS,
    ) -> int:
        local_path: Optional[str] = None
        outfile: BinaryIO
        if isinstance(destination, str):
            local_path = destination
            dirname = os.path.dirname(local_path)
            if dirname:
                os.makedirs(dirname, exist_ok=True)

            if offset:
                outfile = open(local_path, "ab")  # pylint: disable=consider-using-with
                outfile.seek(offset)
            else:
                outfile = open(local_path, "wb")  # pylint: disable=consider-using-with
        else:
            outfile = destination

        bytes_written: int = 0
        try:
            if max_workers > 1 and presigned_download.size >= 3 * block_size:
                bytes_written = self._parallel_download(
                    presigned_download,
                    outfile,
                    block_size,
                    offset=offset,
                    callback=callback,
                    max_workers=max_workers,
                )
            else:
                headers: Optional[Dict[str, str]] = None
                if offset:
                    headers = byte_range_header(offset, presigned_download.size)
                bytes_written = self._download_to_writer(
                    presigned_download.url,
                    outfile,
                    callback=callback,
                    block_size=block_size,
                    headers=headers,
                )
        except ExpiredSignature:
            # If caught here, then no data has been downloaded yet
            return 0
        finally:
            if local_path:
                outfile.close()

        if local_path:
            set_last_modified(local_path, presigned_download.updated_at)
        return bytes_written

    def _download_to_writer(
        self,
        url: str,
        outfile: BinaryIO,
        callback: Optional[Callback] = None,
        block_size: int = settings.S3_MIN_PART_SIZE,
        stream: bool = True,
        **kwargs,
    ) -> int:
        response = self.aws.get(url, stream=stream, **kwargs)
        if callback is not None:
            content_length = response.headers.get("Content-Length")
            callback.set_size(int(content_length) if content_length else None)

        total_bytes: int = 0
        for chunk in response.iter_content(block_size):
            bytes_written = outfile.write(chunk)
            total_bytes += bytes_written
            if callback is not None:
                callback.relative_update(bytes_written)

        if callback is not None and callback.size == 0:
            callback.relative_update(0)
        response.close()
        return total_bytes

    def _parallel_download(
        self,
        presigned_download: ObjectStoragePresignedDownload,
        outfile: BinaryIO,
        block_size: int,
        offset: int = 0,
        max_workers: int = settings.SATURNFS_DEFAULT_MAX_WORKERS,
        callback: Optional[Callback] = None,
    ) -> int:
        size = presigned_download.size - offset
        num_chunks = ceil(float(size) / block_size)
        last_chunk_size = size % block_size
        num_workers = min(num_chunks, max_workers)

        downloader = ParallelDownloader(self, num_workers)

        try:
            chunk_iterator = self._iter_download_parts(
                presigned_download, block_size, offset, num_chunks, last_chunk_size
            )
            downloader.download_chunks(outfile, chunk_iterator, callback=callback)
            return outfile.tell() - offset
        finally:
            downloader.close()

    def _iter_download_parts(
        self,
        presigned_download: ObjectStoragePresignedDownload,
        block_size: int,
        offset: int,
        num_chunks: int,
        last_chunk_size: int,
    ) -> Iterable[DownloadPart]:
        """
        Generate chunks to be downloaded
        """
        for i in range(num_chunks):
            if i == num_chunks - 1 and last_chunk_size:
                chunk_size = last_chunk_size
            else:
                chunk_size = block_size

            part_number = i + 1
            start = offset + (part_number - 1) * block_size
            end = start + chunk_size
            headers = byte_range_header(start, end)

            yield DownloadPart(
                part_number=part_number,
                size=chunk_size,
                url=presigned_download.url,
                headers=headers,
            )

    def _parallel_upload(
        self,
        local_path: str,
        presigned_upload: ObjectStoragePresignedUpload,
        file_offset: int = 0,
        max_workers: int = settings.SATURNFS_DEFAULT_MAX_WORKERS,
        callback: Optional[Callback] = None,
    ):
        num_workers = min(len(presigned_upload.parts), max_workers)
        uploader = ParallelUploader(self, num_workers)
        try:
            chunk_iterator = self._iter_upload_parts(presigned_upload, local_path, file_offset)
            return uploader.upload_chunks(chunk_iterator, callback=callback)
        finally:
            uploader.close()

    def _iter_upload_parts(
        self,
        presigned_upload: ObjectStoragePresignedUpload,
        local_path: str,
        file_offset: int,
    ) -> Iterable[UploadChunk]:
        """
        Generate chunks on upload queue to be uploaded
        Waits for work to be completed, and signals worker shutdown at the end.
        """
        with open(local_path, "rb") as f:
            f.seek(file_offset)
            for part in presigned_upload.parts:
                yield UploadChunk(part=part, data=f.read(part.size))

    def close(self):
        self.aws.close()


class ParallelDownloader:
    """
    Helper class that manages worker threads for parallel chunk downloading
    """

    def __init__(
        self,
        file_tranfer: FileTransferClient,
        num_workers: int,
        disk_buffer: bool = True,
        exit_on_timeout: bool = True,
    ) -> None:
        self.file_transfer = file_tranfer
        self.num_workers = num_workers
        self.disk_buffer = disk_buffer
        self.exit_on_timeout = exit_on_timeout
        self.download_queue: Queue[DownloadPart] = Queue(2 * num_workers)
        self.completed_queue: PriorityQueue[Union[DownloadChunk, DownloadStop]] = PriorityQueue()
        self.stop = Event()

        for _ in range(num_workers):
            Thread(target=self._worker, daemon=True).start()

    def download_chunks(
        self, f: BinaryIO, parts: Iterable[DownloadPart], callback: Optional[Callback] = None
    ):
        collector_thread = Thread(
            target=self._collector, kwargs={"f": f, "callback": callback}, daemon=True
        )
        collector_thread.start()

        all_parts_read: bool = False
        downloads_finished: bool = False
        for part in parts:
            if not self._put_part(part):
                break
        else:
            all_parts_read = True

        def _downloads_finished():
            nonlocal downloads_finished
            self.download_queue.join()
            if not self.stop.is_set():
                downloads_finished = all_parts_read
                self.stop.set()

        Thread(target=_downloads_finished, daemon=True).start()
        self.stop.wait()

        if not downloads_finished:
            # An error has occurred. Consume remaining download parts on queue
            self._clear_downloads()

        # Signal end of download
        self.completed_queue.put(DownloadStop())
        # Wait for collector to finish and clear any remaining state
        collector_thread.join()
        self._clear_completed()
        self.stop.clear()

    def close(self):
        # Signal shutdown to download workers
        for _ in range(self.num_workers):
            self.download_queue.put(None)

    def _put_part(self, part: DownloadPart, poll_interval: int = 5) -> bool:
        while True:
            try:
                self.download_queue.put(part, timeout=poll_interval)
                return True
            except Full:
                if self.stop.is_set():
                    # Error occurred during download, likely an expired signature
                    return False

    def _clear_downloads(self):
        while True:
            try:
                self.download_queue.get_nowait()
                self.download_queue.task_done()
            except Empty:
                break

    def _clear_completed(self):
        while True:
            try:
                self.completed_queue.get_nowait()
                self.completed_queue.task_done()
            except Empty:
                break

    def _worker(self):
        """
        Pull chunks from the download queue, and write the associated byte range to a temp file.
        Push completed chunk onto the completed queue to be reconstructed.
        """
        with requests_session() as session:
            while True:
                part = self.download_queue.get()
                if part is None:
                    # No more download tasks. Put another sentinal
                    # on the queue just in case, then break.
                    try:
                        self.download_queue.put_nowait(None)
                    except Full:
                        pass
                    break

                try:
                    if self.disk_buffer:
                        buffer = tempfile.TemporaryFile(mode="w+b")
                    else:
                        buffer = BytesIO()

                    self.file_transfer._download_to_writer(
                        part.url, buffer, headers=part.headers, session=session
                    )
                except Exception as e:
                    # Signal that an error has occurred
                    self.stop.set()
                    self.download_queue.task_done()
                    buffer.close()
                    if isinstance(e, ExpiredSignature) and not self.exit_on_timeout:
                        continue
                    return

                buffer.seek(0)
                self.completed_queue.put(DownloadChunk(part.part_number, part.size, buffer))
                self.download_queue.task_done()

    def _collector(
        self,
        f: BinaryIO,
        callback: Optional[Callback] = None,
    ):
        """
        Pulls chunks from the completed queue and appends them in order to the given file path
        """
        done = False
        next_part: int = 1
        heap: List[DownloadChunk] = []

        done = False
        while not done:
            # Get the next completed chunk
            chunk = self.completed_queue.get()
            if isinstance(chunk, DownloadStop):
                done = True
                return
            if callback is not None:
                callback.relative_update(chunk.size)

            if chunk.part_number == next_part:
                # Append chunk to the final file
                shutil.copyfileobj(chunk.data, f)
                chunk.data.close()

                # Check if the next part(s) are already in the heap
                next_part += 1
                while len(heap) > 0 and heap[0].part_number == next_part:
                    chunk = heapq.heappop(heap)
                    shutil.copyfileobj(chunk.data, f)
                    chunk.data.close()
                    next_part += 1
            else:
                # Push chunk onto the heap to wait for all previous chunks to complete
                heapq.heappush(heap, chunk)


class ParallelUploader:
    """
    Helper class that manages worker threads for parallel chunk uploading
    """

    def __init__(
        self, file_transfer: FileTransferClient, num_workers: int, exit_on_timeout: bool = True
    ) -> None:
        self.file_transfer = file_transfer
        self.num_workers = num_workers
        self.exit_on_timeout = exit_on_timeout
        self.upload_queue: Queue[Optional[UploadChunk]] = Queue(2 * self.num_workers)
        self.completed_queue: Queue[Union[ObjectStorageCompletePart, UploadStop]] = Queue()
        self.stop = Event()

        for _ in range(self.num_workers):
            Thread(target=self._worker, daemon=True).start()

    def upload_chunks(
        self, chunks: Iterable[UploadChunk], callback: Optional[Callback] = None
    ) -> Tuple[List[ObjectStorageCompletePart], bool]:
        first_part_number = self._producer_init(chunks)
        if first_part_number == -1:
            # No chunks given
            return [], True

        completed_parts, uploads_finished = self._collect(first_part_number, callback=callback)
        self.stop.clear()
        return completed_parts, uploads_finished

    def close(self):
        # Signal shutdown to upload workers
        for _ in range(self.num_workers):
            self.upload_queue.put(None)

    def _producer_init(self, chunks: Iterable[UploadChunk]) -> int:
        # Grab first chunk from iterable to determine the starting part_number
        first_chunk = next(iter(chunks), None)
        if first_chunk is None:
            return -1
        self.upload_queue.put(first_chunk)

        # Start producer thread
        Thread(target=self._producer, kwargs={"chunks": chunks}, daemon=True).start()
        return first_chunk.part.part_number

    def _producer(self, chunks: Iterable[UploadChunk]):
        # Iterate chunks onto the upload_queue until completed or error detected
        all_chunks_read: bool = False
        for chunk in chunks:
            if not self._put_chunk(chunk):
                break
        else:
            all_chunks_read = True

        # Wait for workers to finish processing the queue
        uploads_finished = self._wait()

        # Signal end of upload to the collector
        self.completed_queue.put(UploadStop(error=not (uploads_finished and all_chunks_read)))

    def _put_chunk(self, chunk: UploadChunk, poll_interval: int = 5) -> bool:
        while True:
            try:
                self.upload_queue.put(chunk, timeout=poll_interval)
                return True
            except Full:
                if self.stop.is_set():
                    # Error occurred during upload, likely an expired signature
                    return False

    def _worker(self):
        with requests_session() as session:
            while True:
                chunk = self.upload_queue.get()
                if chunk is None:
                    # No more upload tasks. Put another sentinal
                    # on the queue just in case, then break.
                    try:
                        self.upload_queue.put_nowait(None)
                    except Full:
                        pass
                    break

                try:
                    completed_part = self.file_transfer.upload_part(
                        chunk.data, chunk.part, session=session
                    )
                except Exception as e:
                    # Signal that an error has occurred
                    self.stop.set()
                    self.upload_queue.task_done()
                    if isinstance(e, ExpiredSignature) and not self.exit_on_timeout:
                        continue
                    return
                self.upload_queue.task_done()
                self.completed_queue.put(completed_part)

    def _wait(self) -> bool:
        # Wait for workers to finish processing all chunks, or exit due to expired signatures
        uploads_finished = False

        def _uploads_finished():
            nonlocal uploads_finished
            self.upload_queue.join()
            if not self.stop.is_set():
                uploads_finished = True
                self.stop.set()

        uploads_finished_thread = Thread(target=_uploads_finished, daemon=True)
        uploads_finished_thread.start()
        self.stop.wait()

        if not uploads_finished:
            # Consume any remaining chunks
            while True:
                try:
                    self.upload_queue.get_nowait()
                    self.upload_queue.task_done()
                except Empty:
                    break

            # Wait for any remaining worker tasks putting completed parts on the completed_queue
            # Join the thread instead of queue to ensure there is no race-condition when
            # worker threads are signaled to shutdown (otherwise uploads_finished_thread could leak)
            uploads_finished_thread.join()
        return uploads_finished

    def _collect(
        self,
        first_part: int,
        callback: Optional[Callback] = None,
    ):
        # Collect completed parts
        completed_parts: List[ObjectStorageCompletePart] = []
        uploads_finished: bool = False
        while True:
            completed_part = self.completed_queue.get()
            if isinstance(completed_part, UploadStop):
                # End of upload detected
                uploads_finished = not completed_part.error
                self.completed_queue.task_done()
                break

            completed_parts.append(completed_part)
            self.completed_queue.task_done()

            if callback is not None:
                callback.relative_update(completed_part.size)

        completed_parts.sort(key=lambda p: p.part_number)
        completed_len = len(completed_parts)
        if not uploads_finished and completed_len > 0:
            # Throw out non-sequential completed parts for simpler retry logic
            last_seen = first_part - 1
            for i, part in enumerate(completed_parts):
                if part.part_number != last_seen + 1:
                    completed_len = i
                    break
                last_seen = part.part_number
            completed_parts = completed_parts[:completed_len]

        return completed_parts, uploads_finished


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
class DownloadPart:
    part_number: int
    size: int
    url: str
    headers: Optional[Dict[str, str]] = None


@dataclass
class DownloadChunk:
    part_number: int
    size: int
    data: BufferedReader

    def __lt__(self, other: Union[DownloadChunk, DownloadStop]):
        if isinstance(other, DownloadStop):
            return True
        return self.part_number < other.part_number


@dataclass
class DownloadStop:
    """
    Sentinal type to mark end of download
    """

    def __lt__(self, other):
        return False


@dataclass
class UploadChunk:
    """
    Stores information for parrallel chunk upload
    """

    part: ObjectStoragePresignedPart
    data: Any


@dataclass
class UploadStop:
    """
    Sentinel type to mark end of upload
    """

    error: bool = False


def set_last_modified(local_path: str, last_modified: datetime):
    timestamp = last_modified.timestamp()
    os.utime(local_path, (timestamp, timestamp))
