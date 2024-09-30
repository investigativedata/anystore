from typing import Any, Callable, Generator
import threading
from multiprocessing import cpu_count
from queue import Queue

from anystore.logging import get_logger


log = get_logger(__name__)


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


class Worker:
    def __init__(
        self,
        threads: int | None = None,
        tasks: Generator[Any, None, None] | None = None,
        handle: Callable | None = None,
        handle_error: Callable | None = None,
    ) -> None:
        self.consumer_threads = max(1, threads or cpu_count())
        self.producer = RaisingThread(target=self.produce)
        self.queue = Queue()
        self.consumers = []
        self.tasks = tasks
        self.handle = handle
        self.handle_error = handle_error

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
            raise Exception(task) from e
        self.handle_error(task, e)

    def produce(self) -> None:
        for task in self.get_tasks():
            self.queue.put(task)
        self.queue.put(None)

    def consume(self) -> None:
        while True:
            task = self.queue.get()
            if task is None:
                self.queue.put(task)  # notify other consumers
                break
            try:
                self.handle_task(task)
            except Exception as e:
                self.exception(task, e)

    def exit(self):
        raise

    def done(self) -> None:
        pass

    def run(self):
        try:
            log.info(f"Using `{self.consumer_threads}` consumer threads.")
            for _ in range(self.consumer_threads):
                consumer = RaisingThread(target=self.consume)
                consumer.start()
                self.consumers.append(consumer)
            self.producer.start()
            self.producer.join()
            for consumer in self.consumers:
                try:
                    consumer.join()
                except Exception as e:
                    self.exception(None, e)
        except KeyboardInterrupt:
            self.exit()
        except Exception as e:
            raise e
        finally:
            self.done()
