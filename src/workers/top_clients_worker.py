#!/usr/bin/env python3

"""Entrypoint script for the top clients worker."""

from workers.top.clients import TopClientsWorker

try:
    from workers.worker_utils import run_main  # type: ignore
except ImportError:  # pragma: no cover - fallback
    from worker_utils import run_main  # type: ignore


def main() -> None:
    run_main(TopClientsWorker)


if __name__ == '__main__':
    main()
