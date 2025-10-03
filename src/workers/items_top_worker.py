#!/usr/bin/env python3

"""Entrypoint script for the top items worker."""

from workers.top.items import TopItemsWorker

try:
    from workers.worker_utils import run_main  # type: ignore
except ImportError:  # pragma: no cover - fallback
    from worker_utils import run_main  # type: ignore


def main() -> None:
    run_main(TopItemsWorker)


if __name__ == '__main__':
    main()
