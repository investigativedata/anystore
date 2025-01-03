import sys
import threading
import time
from collections import Counter
from datetime import datetime, timedelta
from functools import cached_property
from io import BytesIO, StringIO
from multiprocessing import cpu_count
from queue import Queue
from typing import Any, Callable, Generator, Type

from pydantic import BaseModel

from anystore.io import Uri, smart_open, smart_write
from anystore.logging import get_logger
from anystore.settings import Settings
from anystore.util import ensure_uri

log = get_logger(__name__)
settings = Settings()


class RaisingThread(threading.Thread):
    def run(self):
        self._exc = None
        try:
            super().run()
        except Exception as e:
            self._exc = e

    def join(self, timeout=None):
        super().join(timeout=timeout)
        if self._exc:
            raise self._exc


class WorkerStatus(BaseModel):
    started: datetime | None = None
    stopped: datetime | None = None
    last_updated: datetime | None = None
    pending: int = 0
    done: int = 0
    errors: int = 0
    running: bool = False
    exc: str | None = None
    took: timedelta = timedelta()

    def touch(self) -> None:
        self.last_updated = datetime.now()

    def start(self) -> None:
        self.running = True
        self.started = datetime.now()

    def stop(self, exc: Exception | None = None) -> None:
        self.running = False
        self.stopped = datetime.now()
        self.exc = str(exc)
        if self.started and self.stopped:
            self.took = self.stopped - self.started


class Worker:
    def __init__(
        self,
        threads: int | None = settings.worker_threads,
        tasks: Generator[Any, None, None] | None = None,
        handle: Callable | None = None,
        handle_error: Callable | None = None,
        job_id: str | None = None,
        heartbeat: int | None = settings.worker_heartbeat,
        status_model: Type[WorkerStatus] | None = WorkerStatus,
    ) -> None:
        self.consumer_threads = max(2, threads or cpu_count()) - 1
        self.queue = Queue()
        self.consumers = []
        self.tasks = tasks
        self.handle = handle
        self.handle_error = handle_error
        self.lock = threading.Lock()
        self.counter = Counter()
        self.status_model = status_model or WorkerStatus
        self.status = status_model() if status_model else WorkerStatus()
        self.job_id = job_id or f"{self.__class__.__name__}-{time.time()}"
        self.heartbeat = heartbeat or settings.worker_heartbeat

    def get_tasks(self) -> Generator[Any, None, None]:
        if self.tasks is None:
            raise NotImplementedError
        yield from self.tasks

    def handle_task(self, task: Any) -> Any:
        if self.handle is None:
            raise NotImplementedError
        self.handle(task)

    def exception(self, task: Any, e: Exception) -> None:
        if self.handle_error is None:
            if task is not None:
                raise Exception(task) from e
            raise e
        self.handle_error(task, e)

    def produce(self) -> None:
        for task in self.get_tasks():
            self.queue_task(task)
        self.queue.put(None)

    def queue_task(self, task: Any) -> None:
        self.count(pending=1)
        self.queue.put(task)

    def consume(self) -> None:
        while True:
            task = self.queue.get()
            if task is None:
                self.queue.put(task)  # notify other consumers
                if self.counter["pending"] < 1:
                    break
            else:
                try:
                    self.handle_task(task)
                    self.count(pending=-1)
                    self.count(done=1)
                except Exception as e:
                    self.count(pending=-1)
                    self.count(errors=1)
                    self.exception(task, e)

    def count(self, **kwargs) -> None:
        with self.lock:
            self.status.touch()
            self.counter.update(**kwargs)

    def beat(self) -> None:
        last_beat = time.time() - self.heartbeat
        while self.status.running:
            if time.time() - last_beat > self.heartbeat:
                self.log_status()
                last_beat = time.time()
                time.sleep(1)

    def log_status(self) -> None:
        status = self.get_status()
        log.info(f"[{self.job_id}] 💚 ", **status.model_dump())

    def get_status(self) -> WorkerStatus:
        return self.status_model(**{**self.status.model_dump(), **self.counter})

    def exit(self, exc: Exception | None = None, status: int | None = 0):
        if exc is not None:
            log.error(f"{exc.__class__.__name__}: `{exc}`", exception=exc)
            if settings.debug:
                raise exc
        sys.exit(status)

    def done(self) -> None:
        pass

    def run(self) -> WorkerStatus:
        try:
            log.info(f"Using `{self.consumer_threads}` consumer threads.")
            self.status.start()
            if self.heartbeat > 0:
                heartbeat = RaisingThread(target=self.beat)
                heartbeat.start()
            producer = RaisingThread(target=self.produce)
            for _ in range(self.consumer_threads):
                consumer = RaisingThread(target=self.consume)
                consumer.start()
                self.consumers.append(consumer)
            producer.start()
            for consumer in self.consumers:
                try:
                    consumer.join()
                except Exception as e:
                    self.exception(None, e)
            producer.join()
        except KeyboardInterrupt:
            self.status.stop()
            self.log_status()
            self.exit()
        except Exception as e:
            self.status.stop(exc=e)
            self.log_status()
            raise e
        finally:
            self.status.stop()
            self.log_status()
            self.done()
        return self.get_status()


class Writer:
    """
    Use a generic writer for any out uri

    This is used by parallel workers:
    - if output is stdout or a local path, write in parallel
    - if output is any other uri, buffer results and write at the end
    """

    def __init__(self, uri: Uri) -> None:
        self.uri = uri
        self.buffer = []

    @cached_property
    def can_write_parallel(self) -> bool:
        if isinstance(self.uri, (BytesIO, StringIO)):
            return True
        uri = ensure_uri(self.uri)
        return uri == "-" or uri.startswith("file://")

    def write(self, data: Any) -> None:
        if self.can_write_parallel:
            self._write_parallel(data)
        else:
            self.buffer.append(data)

    def flush(self) -> None:
        if not self.can_write_parallel and self.buffer:
            self._write_flush()

    def _write_parallel(self, data: Any) -> None:
        smart_write(self.uri, data, "ab")

    def _write_flush(self) -> None:
        with smart_open(ensure_uri(self.uri)) as f:
            f.writelines(self.buffer)


class WriteWorker(Worker):
    def __init__(self, writer: Writer, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.writer = writer

    def write(self, value: Any) -> None:
        with self.lock:
            self.writer.write(value)

    def done(self) -> None:
        self.writer.flush()
        log.info("Write results.", uri=str(self.writer.uri))
