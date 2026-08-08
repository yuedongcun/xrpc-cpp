#!/usr/bin/env python3

import argparse
import json
import os
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


@dataclass(frozen=True)
class Config:
    type: str
    duration: int
    payload_size: int
    server_worker_threads: int
    server_connection_io_threads: int
    run_timeout: int
    warmup_duration: int = 0
    repetitions: int = 1
    host: str = "127.0.0.1"
    port: int = 0
    server_delay_us: int = 0
    server_listen_backlog: int = 128
    threads: int | list[int] = 1
    connections: int | list[int] = 1
    inflight: int | list[int] = 1
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
    allowed = {
        "type",
        "duration",
        "warmup_duration",
        "repetitions",
        "payload_size",
        "server_worker_threads",
        "server_connection_io_threads",
        "run_timeout",
        "host",
        "port",
        "server_delay_us",
        "server_listen_backlog",
        "threads",
        "connections",
        "inflight",
        "io_threads",
    }
    unknown = sorted(set(data) - allowed)
    if unknown:
        raise RuntimeError("unknown config keys: " + ", ".join(unknown))

    benchmark_type = data.get("type")
    if benchmark_type not in ("firehose", "client"):
        raise RuntimeError("type must be firehose or client")

    config = Config(
        type=benchmark_type,
        duration=require_int(data, "duration"),
        warmup_duration=optional_int(data, "warmup_duration", 0),
        repetitions=optional_int(data, "repetitions", 1),
        payload_size=require_int(data, "payload_size"),
        server_worker_threads=require_int(data, "server_worker_threads"),
        server_connection_io_threads=require_int(data, "server_connection_io_threads"),
        run_timeout=require_int(data, "run_timeout"),
        host=data.get("host", "127.0.0.1"),
        port=optional_int(data, "port", 0),
        server_delay_us=optional_int(data, "server_delay_us", 0),
        server_listen_backlog=optional_int(data, "server_listen_backlog", 128),
        threads=data.get("threads", 1),
        connections=data.get("connections", 1),
        inflight=data.get("inflight", 1),
        io_threads=optional_int(data, "io_threads", 0),
    )
    return config


def cases(config):
    if config.type == "client":
        return [Case(name=f"threads={threads}", threads=threads) for threads in values(config.threads, "threads")]

    result = []
    for connections in values(config.connections, "connections"):
        for inflight in values(config.inflight, "inflight"):
            if inflight < connections:
                raise RuntimeError("inflight must be greater than or equal to connections")
            result.append(Case(name=f"connections={connections} inflight={inflight}", connections=connections, inflight=inflight))
    return result


def build(repo_root, build_dir, benchmark_type):
    client_target = "xrpc_benchmark_firehose" if benchmark_type == "firehose" else "xrpc_benchmark_client"
    subprocess.run(
        ["cmake", "--build", str(build_dir), "--target", "xrpc_benchmark_server", client_target, "--parallel"],
        cwd=repo_root,
        check=True,
    )


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
        f"--host={config.host}",
        f"--port={config.port}",
        f"--delay_us={config.server_delay_us}",
        f"--worker_threads={config.server_worker_threads}",
        f"--io_threads={config.server_connection_io_threads}",
        f"--listen_backlog={config.server_listen_backlog}",
    ]
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
        f"--host={config.host}",
        f"--port={port}",
        f"--duration_s={duration}",
        f"--payload_size={config.payload_size}",
    ]
    if config.type == "client":
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
        "xrpc_benchmark_firehose" if config.type == "firehose" else "xrpc_benchmark_client"
    )
    server, port = start_server(repo_root, server_bin, config)
    try:
        if config.warmup_duration > 0:
            print(f"warmup: {case.name}")
            run_client(client_command(client_bin, config, case, port, config.warmup_duration), config.warmup_duration + 30)
        return run_client(client_command(client_bin, config, case, port, config.duration), config.run_timeout)
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
        failed = sum(row["failed"] for row in group)
        print(f"{name}: runs={len(group)} qps_median={qps:.2f} p99_us_median={p99:.2f} failed={failed}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run XRPC benchmark cases.")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--build-dir", type=Path, default=Path("build"))
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    repo_root = Path(__file__).resolve().parents[3]
    build_dir = args.build_dir if args.build_dir.is_absolute() else repo_root / args.build_dir
    config = load_config(args.config)
    case_list = cases(config)

    if args.build:
        build(repo_root, build_dir, config.type)

    rows = []
    total = len(case_list) * config.repetitions
    index = 0
    for repetition in range(1, config.repetitions + 1):
        for case in case_list:
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
