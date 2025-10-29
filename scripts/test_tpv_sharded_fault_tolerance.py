#!/usr/bin/env python3
"""Chaos test runner for the sharded TPV worker using Docker-in-Docker.

The script spins up a docker:dind engine, boots the project stack inside it,
and applies a simple chaos monkey strategy (inspired by the student examples)
to randomly kill the sharded TPV worker containers. It captures restart counts
and verifies that TPV summaries are produced even under failures.
"""

from __future__ import annotations

import argparse
import logging
import random
import shlex
import subprocess
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

DEFAULT_TARGET_PATTERNS = [
    "tpv-worker-sharded-0",
    "tpv-worker-sharded-1",
]

DEFAULT_DIND_IMAGE = "docker:24-dind"


def _log_command(prefix: str, cmd: Sequence[str]) -> None:
    logging.debug("%s%s", prefix, " ".join(shlex.quote(part) for part in cmd))


def run_host_command(cmd: Sequence[str], *, capture: bool = False, check: bool = True) -> subprocess.CompletedProcess:
    """Run a command on the host and optionally capture output."""
    _log_command("[host] ", cmd)
    return subprocess.run(cmd, check=check, text=True, capture_output=capture)


def ensure_image_available(image: str) -> None:
    """Ensure the Docker image exists locally, pulling it if necessary."""
    result = run_host_command(
        ["docker", "images", "-q", image],
        capture=True,
        check=False,
    )
    if result.returncode == 0 and result.stdout.strip():
        logging.info("Using cached image %s", image)
        return

    logging.info("Image %s not found locally; pulling", image)
    run_host_command(["docker", "pull", image])


def exec_in_dind(
    container: str,
    command: str,
    *,
    workdir: str = "/workspace",
    capture: bool = False,
    check: bool = True,
) -> subprocess.CompletedProcess:
    """Execute a shell command inside the DinD container."""
    exec_cmd = ["docker", "exec", container, "sh", "-lc", f"cd {shlex.quote(workdir)} && {command}"]
    _log_command(f"[{container}] ", exec_cmd)
    return subprocess.run(exec_cmd, check=check, text=True, capture_output=capture)


def ensure_no_container(name: str) -> None:
    """Remove any leftover container with the given name."""
    result = run_host_command(["docker", "ps", "-aq", "-f", f"name=^{name}$"], capture=True, check=True)
    container_id = result.stdout.strip()
    if container_id:
        logging.info("Removing existing container %s", name)
        run_host_command(["docker", "rm", "-f", container_id])


def start_dind(
    name: str,
    repo_root: Path,
    port: int,
    *,
    image: str = DEFAULT_DIND_IMAGE,
    pull_image: bool = True,
) -> None:
    """Start docker:dind with the repository mounted."""
    if pull_image:
        ensure_image_available(image)
    else:
        logging.info("Skipping image pull for %s; relying on local availability", image)
    cmd = [
        "docker",
        "run",
        "--rm",
        "-d",
        "--privileged",
        "--name",
        name,
        "-e",
        "DOCKER_TLS_CERTDIR=",
        "-p",
        f"{port}:2375",
        "-v",
        f"{repo_root}:{'/workspace'}",
        "-w",
        "/workspace",
        image,
    ]
    run_host_command(cmd)


def wait_for_dind(name: str, timeout: float = 60.0) -> None:
    """Wait until the DinD engine is ready."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            exec_in_dind(name, "docker info", capture=True)
            return
        except subprocess.CalledProcessError:
            time.sleep(1)
    raise RuntimeError("Timed out waiting for docker:dind to become ready")


def ensure_compose_plugin(name: str) -> None:
    """Make sure docker compose is present inside DinD."""
    try:
        exec_in_dind(name, "docker compose version", capture=True)
    except subprocess.CalledProcessError:
        logging.info("Installing docker compose plugin inside %s", name)
        exec_in_dind(name, "apk add --no-cache docker-cli-compose")


def compose_base_command(compose_file: str, project_name: str) -> str:
    parts = [
        "docker",
        "compose",
        "-f",
        compose_file,
        "-p",
        project_name,
    ]
    return " ".join(shlex.quote(part) for part in parts)


def list_project_containers(
    dind_name: str,
    project_name: str,
    *,
    include: Iterable[str] | None = None,
    exclude: Iterable[str] | None = None,
) -> list[str]:
    command = "docker ps --filter " \
        + shlex.quote(f"label=com.docker.compose.project={project_name}") \
        + " --format '" + "{{.Names}}" + "'"
    result = exec_in_dind(dind_name, command, capture=True, check=True)
    names = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if include:
        include = list(include)
        names = [name for name in names if any(pattern in name for pattern in include)]
    if exclude:
        exclude = list(exclude)
        names = [name for name in names if not any(pattern in name for pattern in exclude)]
    return names


def wait_for_targets(
    dind_name: str,
    project_name: str,
    patterns: Sequence[str],
    *,
    timeout: float = 120.0,
) -> list[str]:
    wanted = set(patterns)
    deadline = time.time() + timeout
    while time.time() < deadline:
        found = list_project_containers(dind_name, project_name, include=wanted)
        hit = {pattern for pattern in wanted for name in found if pattern in name}
        if hit == wanted:
            return found
        time.sleep(2)
    raise RuntimeError(f"Target containers {patterns} not ready within timeout")


def list_running_clients(dind_name: str, project_name: str) -> list[str]:
    return list_project_containers(dind_name, project_name, include=["client-"])


def kill_container(dind_name: str, container_name: str) -> None:
    # Mimic a crash by killing PID 1 inside the container so Docker restart
    # policies treat it as an unexpected failure instead of a manual stop.
    kill_inner = f"docker exec {shlex.quote(container_name)} kill -9 1"
    result = exec_in_dind(dind_name, kill_inner, capture=True, check=False)
    if result.returncode == 0:
        return

    logging.debug(
        "Failed to kill PID 1 in %s via docker exec (rc=%s, stderr=%s); falling back to docker kill",
        container_name,
        result.returncode,
        result.stderr.strip() if result.stderr else "",
    )
    exec_in_dind(dind_name, f"docker kill {shlex.quote(container_name)}", check=True)


def inspect_restart_count(dind_name: str, container_name: str) -> tuple[int, str]:
    fmt = "'{{(.RestartCount)}} {{(.State.Status)}}'".replace("(", "").replace(")", "")
    command = f"docker inspect -f {fmt} {shlex.quote(container_name)}"
    result = exec_in_dind(dind_name, command, capture=True, check=True)
    parts = result.stdout.strip().split()
    if not parts:
        return 0, "unknown"
    count = int(parts[0])
    status = parts[1] if len(parts) > 1 else "unknown"
    return count, status


@dataclass
class ChaosStats:
    attempts: int = 0
    kills: int = 0
    skips: int = 0
    empty_cycles: int = 0
    errors: int = 0


class ChaosThread(threading.Thread):
    def __init__(
        self,
        dind_name: str,
        project_name: str,
        patterns: Sequence[str],
        *,
        interval: float,
        failure_rate: float,
        stop_event: threading.Event,
        dry_run: bool,
    ) -> None:
        super().__init__(name="chaos-thread", daemon=True)
        self.dind_name = dind_name
        self.project_name = project_name
        self.patterns = patterns
        self.interval = interval
        self.failure_rate = failure_rate
        self.stop_event = stop_event
        self.dry_run = dry_run
        self.stats = ChaosStats()
        self.killed_containers: Counter[str] = Counter()
        self.kill_log: list[tuple[float, str]] = []

    def run(self) -> None:
        while not self.stop_event.is_set():
            self.stats.attempts += 1
            try:
                containers = list_project_containers(self.dind_name, self.project_name, include=self.patterns)
            except subprocess.CalledProcessError as exc:
                self.stats.errors += 1
                logging.debug("Failed to list containers: %s", exc)
                if self.stop_event.wait(self.interval):
                    return
                continue

            if not containers:
                self.stats.empty_cycles += 1
                logging.debug("No target containers found in this cycle")
                if self.stop_event.wait(self.interval):
                    return
                continue

            candidate = random.choice(containers)
            if random.random() > self.failure_rate:
                self.stats.skips += 1
                if self.stop_event.wait(self.interval):
                    return
                continue

            if self.dry_run:
                logging.info("[dry-run] Would kill %s", candidate)
                self.stats.kills += 1
                self.killed_containers[candidate] += 1
                self.kill_log.append((time.time(), candidate))
            else:
                try:
                    logging.info("Killing container %s", candidate)
                    kill_container(self.dind_name, candidate)
                    self.stats.kills += 1
                    self.killed_containers[candidate] += 1
                    self.kill_log.append((time.time(), candidate))
                except subprocess.CalledProcessError as exc:
                    self.stats.errors += 1
                    logging.warning("Failed to kill %s: %s", candidate, exc)

            if self.stop_event.wait(self.interval):
                return


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Chaos test for sharded TPV worker")
    parser.add_argument("--compose-file", default="docker-compose.yml", help="Compose file to use inside DinD")
    parser.add_argument("--project-name", default="tpvchaos", help="Docker Compose project name")
    parser.add_argument("--dind-name", default="tpv-chaos-dind", help="Name for the docker:dind container")
    parser.add_argument("--dind-port", type=int, default=23755, help="Host port bound to DinD dockerd")
    parser.add_argument("--dind-image", default=DEFAULT_DIND_IMAGE, help="docker:dind image to use")
    parser.add_argument("--skip-image-pull", action="store_true", help="Do not pull the DinD image before running")
    parser.add_argument("--force-recreate", action="store_true", help="Pass --force-recreate to docker compose up")
    parser.add_argument("--duration", type=float, default=90.0, help="Duration of the chaos phase in seconds")
    parser.add_argument("--interval", type=float, default=12.0, help="Seconds between chaos attempts")
    parser.add_argument("--failure-rate", type=float, default=0.6, help="Probability of killing a target per cycle")
    parser.add_argument("--warmup", type=float, default=60.0, help="Seconds to wait after boot before chaos starts")
    parser.add_argument("--cooldown", type=float, default=60.0, help="Seconds to wait after chaos for the pipeline to settle")
    parser.add_argument(
        "--client-timeout",
        type=float,
        default=900.0,
        help="Max seconds to wait for clients to finish (increase if DinD runs slower)",
    )
    parser.add_argument("--target", action="append", dest="targets", help="Target pattern to include (can be repeated)")
    parser.add_argument("--service", nargs="*", dest="services", help="Services to start (default: all defined in compose)")
    parser.add_argument("--build", action="store_true", help="Force docker compose to build images")
    parser.add_argument("--dry-run", action="store_true", help="Do not actually kill containers")
    parser.add_argument("--keep-env", action="store_true", help="Do not tear down the compose stack or DinD container")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ...)")
    return parser.parse_args(argv)


def find_repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def ensure_results_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def find_new_result_dirs(results_dir: Path, before: set[str]) -> list[Path]:
    after = {entry.name for entry in results_dir.iterdir() if entry.is_dir()}
    new_entries = sorted(after - before)
    return [results_dir / name for name in new_entries]


def read_summary_lines(summary_path: Path, limit: int = 5) -> list[str]:
    if not summary_path.exists():
        return []
    lines = summary_path.read_text(encoding="utf-8").splitlines()
    return lines[:limit]


def wait_for_clients_completion(
    dind_name: str,
    project_name: str,
    *,
    timeout: float,
    poll_interval: float = 5.0,
) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        running = list_running_clients(dind_name, project_name)
        if not running:
            return True
        logging.debug("Clients still running: %s", ", ".join(running))
        time.sleep(poll_interval)
    return False


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        run_host_command(["docker", "info"], capture=True)
    except subprocess.CalledProcessError as exc:
        logging.error("Docker must be available on the host: %s", exc)
        return 1

    repo_root = find_repo_root()
    compose_file = (repo_root / args.compose_file).resolve()
    if not compose_file.exists():
        logging.error("Compose file %s not found", compose_file)
        return 1

    try:
        relative_compose = compose_file.relative_to(repo_root)
    except ValueError:
        logging.error("Compose file must live inside the repository root %s", repo_root)
        return 1

    compose_inside = f"/workspace/{relative_compose.as_posix()}"
    results_dir = repo_root / "results"
    ensure_results_directory(results_dir)
    preexisting_results = {entry.name for entry in results_dir.iterdir() if entry.is_dir()}

    targets = args.targets if args.targets else DEFAULT_TARGET_PATTERNS
    should_keep_env = args.keep_env
    auto_preserve_env = False
    ensure_no_container(args.dind_name)

    logging.info("Starting docker:dind %s", args.dind_name)
    start_dind(
        args.dind_name,
        repo_root,
        args.dind_port,
        image=args.dind_image,
        pull_image=not args.skip_image_pull,
    )
    dind_started = True

    try:
        wait_for_dind(args.dind_name)
        ensure_compose_plugin(args.dind_name)

        compose_cmd = compose_base_command(compose_inside, args.project_name)
        up_parts = [compose_cmd, "up"]
        if args.build:
            up_parts.append("--build")
        if args.force_recreate:
            up_parts.append("--force-recreate")
        up_parts.extend(["-d", "--remove-orphans"])
        if args.services:
            up_parts.extend(shlex.quote(service) for service in args.services)
        up_command = " ".join(up_parts)

        logging.info("Booting compose stack")
        exec_in_dind(args.dind_name, up_command)

        logging.info("Waiting for target containers: %s", ", ".join(targets))
        wait_for_targets(args.dind_name, args.project_name, targets)

        logging.info("Warmup for %.1f seconds", args.warmup)
        if args.warmup > 0:
            time.sleep(args.warmup)

        stop_event = threading.Event()
        chaos_thread = ChaosThread(
            args.dind_name,
            args.project_name,
            targets,
            interval=args.interval,
            failure_rate=args.failure_rate,
            stop_event=stop_event,
            dry_run=args.dry_run,
        )

        logging.info("Starting chaos phase for %.1f seconds", args.duration)
        random.seed()
        chaos_thread.start()

        end_time = time.time() + args.duration
        interrupted = False
        try:
            while time.time() < end_time:
                remaining = end_time - time.time()
                if remaining <= 0:
                    break
                if stop_event.wait(min(5.0, remaining)):
                    break
        except KeyboardInterrupt:
            interrupted = True
            logging.warning("Interrupted by user; stopping chaos loop")
        finally:
            stop_event.set()
            chaos_thread.join()

        logging.info(
            "Chaos stats: attempts=%d kills=%d skips=%d empty=%d errors=%d",
            chaos_thread.stats.attempts,
            chaos_thread.stats.kills,
            chaos_thread.stats.skips,
            chaos_thread.stats.empty_cycles,
            chaos_thread.stats.errors,
        )
        if chaos_thread.killed_containers:
            logging.info(
                "Kill counts per container: %s",
                ", ".join(f"{name}:{count}" for name, count in chaos_thread.killed_containers.items()),
            )

        clients_completed = False
        if interrupted:
            logging.info("Skipping client wait due to interruption")
        else:
            clients_completed = wait_for_clients_completion(
                args.dind_name,
                args.project_name,
                timeout=args.client_timeout,
            )
            if clients_completed:
                logging.info("Clients finished within timeout")
            else:
                logging.warning(
                    "Clients did not finish within %.0f seconds; keeping environment for manual inspection",
                    args.client_timeout,
                )
                should_keep_env = True
                auto_preserve_env = True

        logging.info("Cooldown for %.1f seconds", args.cooldown)
        if args.cooldown > 0:
            time.sleep(args.cooldown)

        current_targets = list_project_containers(args.dind_name, args.project_name, include=targets)
        restart_info = {
            name: inspect_restart_count(args.dind_name, name)
            for name in current_targets
        }
        if restart_info:
            formatted = ", ".join(
                f"{name} -> restarts:{count} status:{status}" for name, (count, status) in restart_info.items()
            )
            logging.info("Target container status: %s", formatted)

        down_parts = [compose_cmd, "down", "--remove-orphans"]
        down_command = " ".join(down_parts)
        if should_keep_env:
            if auto_preserve_env and not args.keep_env:
                logging.info(
                    "Keeping compose stack and DinD running so pending clients can finish; rerun with --client-timeout to adjust the wait"
                )
            else:
                logging.info("Keeping compose stack and DinD running as requested")
        else:
            logging.info("Tearing down compose stack")
            exec_in_dind(args.dind_name, down_command)

    finally:
        if dind_started and not should_keep_env:
            logging.info("Stopping docker:dind %s", args.dind_name)
            run_host_command(["docker", "stop", args.dind_name])

    new_dirs = find_new_result_dirs(results_dir, preexisting_results)
    if new_dirs:
        logging.info("Detected %d new result directories", len(new_dirs))
        for path in new_dirs:
            summary_path = path / "tpv_summary.txt"
            if summary_path.exists():
                preview = read_summary_lines(summary_path, limit=30)
                logging.info("Summary created at %s", summary_path)
                if preview:
                    logging.info("Summary preview:\n%s", "\n".join(preview))
            else:
                logging.warning("No tpv_summary.txt found in %s", path)
    else:
        logging.warning("No new result directories found after the test")

    logging.info("Chaos test finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())
