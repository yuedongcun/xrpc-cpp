#!/usr/bin/env python3

import argparse
import json
import os
import random
import re
import selectors
import signal
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


TOTAL_PATTERN = re.compile(r"total_calls=(\d+) success=(\d+) failed=(\d+)")
LATENCY_PATTERN = re.compile(
    r"qps=([0-9.]+) avg_us=([0-9.]+) p50_us=([0-9.]+) "
    r"p95_us=([0-9.]+) p99_us=([0-9.]+)"
)
READY_PORT_PATTERN = re.compile(r"\bport=(\d+)\b")
BENCHMARK_HOST = "127.0.0.1"
PROCESS_TIMEOUT_MARGIN = 30


@dataclass(frozen=True)
class Config:
    benchmark_type: str
    duration: int
    payload_size: int
    server_worker_threads: int
    server_connection_io_threads: int
    server_max_inflight_per_connection: int = 0
    warmup_duration: int = 0
    repetitions: int = 1
    threads: int | list[int] = 0
    connections: int | list[int] = 0
    inflight: int | list[int] = 0
    inflight_per_connection: int = 0
    io_threads: int = 0


@dataclass(frozen=True)
class Case:
    name: str
    threads: int = 0
    connections: int = 0
    inflight: int = 0


def require_int(data, name):
    value = data[name]
    if type(value) is not int:
        raise RuntimeError(f"{name} must be an integer")
    return value


def optional_int(data, name, default):
    value = data.get(name, default)
    if type(value) is not int:
        raise RuntimeError(f"{name} must be an integer")
    return value


def values(value, name):
    if type(value) is int:
        if value <= 0:
            raise RuntimeError(f"{name} must be greater than zero")
        return [value]
    if not isinstance(value, list) or not value:
        raise RuntimeError(f"{name} must be a positive integer or a non-empty list")
    if any(type(item) is not int or item <= 0 for item in value):
        raise RuntimeError(f"{name} values must be positive integers")
    return value


def load_config(path):
    data = json.loads(path.read_text(encoding="utf-8"))
    common = {
        "type",
        "duration",
        "warmup_duration",
        "repetitions",
        "payload_size",
        "server_worker_threads",
        "server_connection_io_threads",
    }
    benchmark_type = data.get("type")
    if benchmark_type == "client":
        allowed = common | {"threads"}
    elif benchmark_type == "firehose":
        allowed = common | {
            "connections",
            "inflight",
            "inflight_per_connection",
            "io_threads",
            "server_max_inflight_per_connection",
        }
    else:
        raise RuntimeError("type must be firehose or client")

    unknown = sorted(set(data) - allowed)
    if unknown:
        raise RuntimeError("unknown config keys: " + ", ".join(unknown))

    if benchmark_type == "firehose":
        has_inflight = "inflight" in data
        has_inflight_per_connection = "inflight_per_connection" in data
        if has_inflight == has_inflight_per_connection:
            raise RuntimeError("firehose config must set exactly one of inflight or inflight_per_connection")

    config = Config(
        benchmark_type=benchmark_type,
        duration=require_int(data, "duration"),
        warmup_duration=optional_int(data, "warmup_duration", 0),
        repetitions=optional_int(data, "repetitions", 1),
        payload_size=require_int(data, "payload_size"),
        server_worker_threads=require_int(data, "server_worker_threads"),
        server_connection_io_threads=require_int(data, "server_connection_io_threads"),
        server_max_inflight_per_connection=(
            require_int(data, "server_max_inflight_per_connection") if benchmark_type == "firehose" else 0
        ),
        threads=data.get("threads", 0),
        connections=data.get("connections", 0),
        inflight=data.get("inflight", 0),
        inflight_per_connection=optional_int(data, "inflight_per_connection", 0),
        io_threads=require_int(data, "io_threads") if benchmark_type == "firehose" else 0,
    )
    return config


def cases(config):
    if config.benchmark_type == "client":
        return [Case(name=f"threads={threads}", threads=threads) for threads in values(config.threads, "threads")]

    result = []
    for connections in values(config.connections, "connections"):
        inflights = (
            [connections * config.inflight_per_connection]
            if config.inflight_per_connection > 0
            else values(config.inflight, "inflight")
        )
        for inflight in inflights:
            if inflight < connections:
                raise RuntimeError("inflight must be greater than or equal to connections")
            result.append(Case(name=f"connections={connections} inflight={inflight}", connections=connections, inflight=inflight))
    return result


def stop(process):
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)


def start_server(repo_root, server_bin, config):
    command = [
        str(server_bin),
        "--port=0",
        f"--worker_threads={config.server_worker_threads}",
        f"--io_threads={config.server_connection_io_threads}",
    ]
    if config.server_max_inflight_per_connection > 0:
        command.append(f"--max_inflight_per_connection={config.server_max_inflight_per_connection}")
    process = subprocess.Popen(
        command,
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        preexec_fn=os.setsid,
    )
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    deadline = time.monotonic() + 10
    try:
        while time.monotonic() < deadline:
            for key, _ in selector.select(timeout=0.1):
                line = key.fileobj.readline()
                if line:
                    print("server:", line, end="")
                if line.startswith("ready "):
                    match = READY_PORT_PATTERN.search(line)
                    if match is None:
                        raise RuntimeError("server ready line did not include port")
                    return process, int(match.group(1))
            if process.poll() is not None:
                raise RuntimeError("benchmark server exited before ready")
        raise RuntimeError("benchmark server did not become ready within 10s")
    except Exception:
        stop(process)
        raise
    finally:
        selector.close()


def client_command(client_bin, config, case, port, duration):
    command = [
        str(client_bin),
        f"--host={BENCHMARK_HOST}",
        f"--port={port}",
        f"--duration_s={duration}",
        f"--payload_size={config.payload_size}",
    ]
    if config.benchmark_type == "client":
        command.append(f"--threads={case.threads}")
    else:
        command += [
            f"--connections={case.connections}",
            f"--inflight={case.inflight}",
            f"--io_threads={config.io_threads}",
        ]
    return command


def parse_stats(output):
    total = TOTAL_PATTERN.search(output)
    latency = LATENCY_PATTERN.search(output)
    if total is None or latency is None:
        raise RuntimeError("benchmark client output did not contain final statistics")
    return {
        "total": int(total.group(1)),
        "success": int(total.group(2)),
        "failed": int(total.group(3)),
        "qps": float(latency.group(1)),
        "p99_us": float(latency.group(5)),
    }


def run_client(command, timeout):
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=timeout)
    print(result.stdout, end="")
    if result.returncode != 0:
        raise RuntimeError(f"benchmark client failed with exit code {result.returncode}")
    return parse_stats(result.stdout)


def run_case(repo_root, build_dir, config, case):
    server_bin = build_dir / "tools" / "benchmark" / "xrpc_benchmark_server"
    client_bin = build_dir / "tools" / "benchmark" / (
        "xrpc_benchmark_firehose" if config.benchmark_type == "firehose" else "xrpc_benchmark_client"
    )
    server, port = start_server(repo_root, server_bin, config)
    try:
        if config.warmup_duration > 0:
            print(f"warmup: {case.name}")
            run_client(
                client_command(client_bin, config, case, port, config.warmup_duration),
                config.warmup_duration + PROCESS_TIMEOUT_MARGIN,
            )
        return run_client(
            client_command(client_bin, config, case, port, config.duration),
            config.duration + PROCESS_TIMEOUT_MARGIN,
        )
    finally:
        stop(server)


def summarize(rows):
    groups = {}
    for row in rows:
        groups.setdefault(row["case"], []).append(row)
    print("\nsummary:")
    for name, group in groups.items():
        qps = statistics.median(row["qps"] for row in group)
        p99 = statistics.median(row["p99_us"] for row in group)
        qps_min = min(row["qps"] for row in group)
        qps_max = max(row["qps"] for row in group)
        p99_min = min(row["p99_us"] for row in group)
        p99_max = max(row["p99_us"] for row in group)
        failed = sum(row["failed"] for row in group)
        print(
            f"{name}: runs={len(group)} "
            f"qps_median={qps:.2f} qps_range=[{qps_min:.2f},{qps_max:.2f}] "
            f"p99_us_median={p99:.2f} p99_us_range=[{p99_min:.2f},{p99_max:.2f}] failed={failed}"
        )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run XRPC benchmark cases.")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--build-dir", type=Path, default=Path("build-release"))
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    repo_root = Path(__file__).resolve().parents[3]
    build_dir = args.build_dir if args.build_dir.is_absolute() else repo_root / args.build_dir
    config = load_config(args.config)
    case_list = cases(config)

    rows = []
    total = len(case_list) * config.repetitions
    index = 0
    for repetition in range(1, config.repetitions + 1):
        repetition_cases = list(case_list)
        random.Random(repetition).shuffle(repetition_cases)
        for case in repetition_cases:
            index += 1
            print(f"\n[{index}/{total}] repetition={repetition} {case.name}")
            stats = run_case(repo_root, build_dir, config, case)
            stats["case"] = case.name
            rows.append(stats)
    summarize(rows)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as error:
        print(error, file=sys.stderr)
        raise SystemExit(1)
