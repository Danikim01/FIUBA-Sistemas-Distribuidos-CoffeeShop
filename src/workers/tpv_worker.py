#!/usr/bin/env python3

"""Entrypoint script for the TPV worker."""

from workers.top.tpv import TPVWorker

try:
    from workers.worker_utils import run_main  # type: ignore
except ImportError:  # pragma: no cover - fallback
    from worker_utils import run_main  # type: ignore


def main() -> None:
    run_main(TPVWorker)


if __name__ == '__main__':
    main()
