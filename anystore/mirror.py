"""
Mirror one store to another
"""

from typing import Generator

from anystore.logging import get_logger
from anystore.store.base import BaseStore
from anystore.worker import Worker

log = get_logger(__name__)


class MirrorWorker(Worker):
    def __init__(
        self,
        source: BaseStore,
        target: BaseStore,
        glob: str | None = None,
        prefix: str | None = None,
        exclude_prefix: str | None = None,
        overwrite: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.glob = glob
        self.prefix = prefix
        self.exclude_prefix = exclude_prefix
        self.overwrite = overwrite
        self.source = source
        self.target = target
        self.skipped: int = 0
        self.mirrored: int = 0

    def get_tasks(self) -> Generator[str, None, None]:
        log.info("Start mirroring ...", source=self.source.uri, target=self.target.uri)
        yield from self.source.iterate_keys(glob=self.glob, prefix=self.prefix)

    def handle_task(self, task: str) -> None:
        if not self.overwrite and self.target.exists(task):
            log.info(
                f"Skipping already existing key `{task}` ...",
                source=self.source.uri,
                target=self.target.uri,
            )
            self.skipped += 1
            return

        log.info(
            f"Mirroring key `{task}` ...",
            source=self.source.uri,
            target=self.target.uri,
        )
        with self.source.open(task, "rb") as i:
            with self.target.open(task, "wb") as o:
                o.write(i.read())
        self.mirrored += 1

    def done(self) -> None:
        log.info(
            "Done mirroring.",
            source=self.source.uri,
            target=self.target.uri,
            mirrored=self.mirrored,
            skipped=self.skipped,
        )


def mirror(
    source: BaseStore,
    target: BaseStore,
    glob: str | None = None,
    prefix: str | None = None,
    exclude_prefix: str | None = None,
    overwrite: bool = False,
    **kwargs,
) -> tuple[int, int]:
    worker = MirrorWorker(
        source=source,
        target=target,
        glob=glob,
        prefix=prefix,
        exclude_prefix=exclude_prefix,
        overwrite=overwrite,
        **kwargs,
    )
    worker.run()
    return worker.mirrored, worker.skipped
