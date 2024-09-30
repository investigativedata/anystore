import random
import threading
import time
from typing import Generator

from anystore.worker import Worker
from anystore.io import smart_write, smart_read


def test_worker(tmp_path):
    class TestWorker(Worker):
        def get_tasks(self) -> Generator[int, None, None]:
            yield from range(5)

        def handle_task(self, task: int) -> None:
            print(f"Task: `{task}`, Thread: `{threading.get_native_id()}`")
            time.sleep(1)

        def done(self):
            smart_write(tmp_path / "done", b"yes")

    # parallel
    worker = TestWorker()
    start = time.time()
    worker.run()
    assert time.time() - start < 5
    assert smart_read(tmp_path / "done") == b"yes"

    # only 1 worker
    worker = TestWorker(threads=1)
    start = time.time()
    worker.run()
    assert time.time() - start > 5


def test_worker_simple():
    # simple worker
    tasks = (i for i in range(5))
    worker = Worker(tasks=tasks, handle=lambda x: print(x))
    start = time.time()
    worker.run()
    assert time.time() - start < 5


def test_worker_errors():

    class TestWorker(Worker):
        results = 0
        errors = 0

        def handle_task(self, i):
            v = random.random()
            time.sleep(v / 100)
            assert v < 0.5
            self.results += 1

        def exception(self, task, e):
            self.errors += 1

    worker = TestWorker(tasks=range(1000))
    worker.run()
    assert worker.results + worker.errors == 1000
