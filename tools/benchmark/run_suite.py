#!/usr/bin/env python3

import argparse
import csv
import datetime
import json
import os
import platform
import re
import resource
import selectors
import signal
import shlex
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path


TOTAL_PATTERN = re.compile(r"total_calls=(\d+) success=(\d+) failed=(\d+)")
LATENCY_PATTERN = re.compile(
    r"qps=([0-9.]+) avg_us=([0-9.]+) p50_us=([0-9.]+) "
    r"p95_us=([0-9.]+) p99_us=([0-9.]+)"
)
READY_PORT_PATTERN = re.compile(r"\bport=(\d+)\b")
BACKPRESSURE_PATTERN = re.compile(
    r"backpressure rejected_inflight=(\d+) rejected_global_pending=(\d+) "
    r"closed_write_queue=(\d+) max_inflight=(\d+) max_write_queue_bytes=(\d+)"
)
WORKER_QUEUE_PATTERN = re.compile(r"worker_queue rejected=(\d+) max_depth=(\d+)")
CPU_TICKS_PER_SECOND = os.sysconf("SC_CLK_TCK")
OPTIONAL_CONFIG_KEYS = {
    "client_mode",
    "client_threads",
    "description",
    "firehose_cases",
    "firehose_connections",
    "firehose_inflight",
    "firehose_io_threads",
    "repetitions",
    "server_lifecycle",
    "warmup_duration",
    "workload",
}
REQUIRED_CONFIG_KEYS = {
    "duration",
    "payload_size",
    "server_delay_us",
    "server_worker_threads",
    "server_connection_io_threads",
    "server_listen_backlog",
    "server_cpus",
    "client_cpus",
    "host",
    "port",
    "run_timeout",
}


@dataclass(frozen=True)
class BenchmarkConfig:
    client_mode: str
    workload: str
    duration: int
    warmup_duration: int
    repetitions: int
    payload_size: int
    server_delay_us: int
    server_worker_threads: int
    server_connection_io_threads: int
    server_listen_backlog: int
    server_lifecycle: str
    server_cpus: str | None
    client_cpus: str | None
    host: str
    port: int
    run_timeout: int
    client_threads: int | list[int]
    firehose_cases: list[dict[str, int]] | None
    firehose_connections: int | list[int]
    firehose_inflight: int | list[int]
    firehose_io_threads: int
    description: str = ""


@dataclass(frozen=True)
class BenchmarkRunSpec:
    client_threads: int = 0
    firehose_connections: int = 0
    firehose_inflight: int = 0


def parse_cpu_list(value):
    cpus = set()
    for item in value.split(","):
        item = item.strip()
        if not item:
            raise ValueError("CPU list contains an empty item")
        if "-" in item:
            start_text, end_text = item.split("-", 1)
            start = int(start_text)
            end = int(end_text)
            if start < 0 or end < start:
                raise ValueError("CPU ranges must be non-negative and ordered")
            cpus.update(range(start, end + 1))
        else:
            cpu = int(item)
            if cpu < 0:
                raise ValueError("CPU numbers must be non-negative")
            cpus.add(cpu)
    return cpus


def format_cpu_list(cpus):
    ordered = sorted(cpus)
    if not ordered:
        return ""

    ranges = []
    start = ordered[0]
    end = ordered[0]
    for cpu in ordered[1:]:
        if cpu == end + 1:
            end = cpu
            continue
        ranges.append(str(start) if start == end else f"{start}-{end}")
        start = cpu
        end = cpu
    ranges.append(str(start) if start == end else f"{start}-{end}")
    return ",".join(ranges)


def apply_cpu_affinity(command, cpu_list):
    if cpu_list is None:
        return command
    if shutil.which("taskset") is None:
        raise RuntimeError("taskset is required when CPU affinity is configured")
    return ["taskset", "--cpu-list", cpu_list, *command]


def positive_int_list(value, field_name):
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list of positive integers")

    values = []
    for item in value:
        if type(item) is not int or item <= 0:
            raise ValueError(f"{field_name} values must be positive integers")
        values.append(item)
    if not values:
        raise ValueError(f"{field_name} must not be empty")
    return values


def positive_int_or_list(value, field_name):
    if type(value) is int:
        if value <= 0:
            raise ValueError(f"{field_name} must be greater than zero")
        return value
    return positive_int_list(value, field_name)


def validate_firehose_cases(value):
    if value is None:
        return
    if not isinstance(value, list) or not value:
        raise ValueError("firehose_cases must be a non-empty list")

    expected_keys = {"firehose_connections", "firehose_inflight"}
    for index, case in enumerate(value):
        if not isinstance(case, dict) or set(case) != expected_keys:
            raise ValueError(
                f"firehose_cases[{index}] must contain exactly "
                "firehose_connections and firehose_inflight"
            )
        connections = case["firehose_connections"]
        inflight = case["firehose_inflight"]
        if type(connections) is not int or connections <= 0:
            raise ValueError(
                f"firehose_cases[{index}].firehose_connections must be a positive integer"
            )
        if type(inflight) is not int or inflight < connections:
            raise ValueError(
                f"firehose_cases[{index}].firehose_inflight must be an integer "
                "greater than or equal to firehose_connections"
            )


def require_int(config, name):
    value = config[name]
    if type(value) is not int:
        raise ValueError(f"{name} must be an integer")
    return value


def require_str_or_none(config, name):
    value = config[name]
    if value is not None and not isinstance(value, str):
        raise ValueError(f"{name} must be a string or null")
    return value


def load_benchmark_config(config_path):
    try:
        with config_path.open("r", encoding="utf-8") as config_file:
            raw_config = json.load(config_file)
    except OSError as error:
        raise RuntimeError(f"failed to read config file {config_path}: {error}") from error
    except json.JSONDecodeError as error:
        raise RuntimeError(f"invalid JSON config file {config_path}: {error}") from error

    if not isinstance(raw_config, dict):
        raise RuntimeError("benchmark config must be a JSON object")

    allowed_keys = REQUIRED_CONFIG_KEYS | OPTIONAL_CONFIG_KEYS
    unknown_keys = sorted(set(raw_config) - allowed_keys)
    missing_keys = sorted(REQUIRED_CONFIG_KEYS - set(raw_config))
    if unknown_keys:
        raise RuntimeError("unknown benchmark config keys: " + ", ".join(unknown_keys))
    if missing_keys:
        raise RuntimeError("missing benchmark config keys: " + ", ".join(missing_keys))

    try:
        description = raw_config.get("description", "")
        if not isinstance(description, str):
            raise ValueError("description must be a string")

        workload = raw_config.get("workload", "raw")
        if workload not in ("protobuf", "raw"):
            raise ValueError("workload must be one of: protobuf, raw")

        client_mode = raw_config.get("client_mode", "firehose")
        if client_mode not in ("firehose", "rpc_client"):
            raise ValueError("client_mode must be one of: firehose, rpc_client")

        server_lifecycle = raw_config.get("server_lifecycle", "per_case")
        if server_lifecycle not in ("per_case", "per_suite"):
            raise ValueError("server_lifecycle must be one of: per_case, per_suite")

        normalized = BenchmarkConfig(
            client_mode=client_mode,
            workload=workload,
            duration=require_int(raw_config, "duration"),
            warmup_duration=raw_config.get("warmup_duration", 0),
            repetitions=raw_config.get("repetitions", 1),
            payload_size=require_int(raw_config, "payload_size"),
            server_delay_us=require_int(raw_config, "server_delay_us"),
            server_worker_threads=require_int(raw_config, "server_worker_threads"),
            server_connection_io_threads=require_int(raw_config, "server_connection_io_threads"),
            server_listen_backlog=require_int(raw_config, "server_listen_backlog"),
            server_lifecycle=server_lifecycle,
            server_cpus=require_str_or_none(raw_config, "server_cpus"),
            client_cpus=require_str_or_none(raw_config, "client_cpus"),
            host=raw_config["host"],
            port=require_int(raw_config, "port"),
            run_timeout=require_int(raw_config, "run_timeout"),
            client_threads=raw_config.get("client_threads", [1]),
            firehose_cases=raw_config.get("firehose_cases"),
            firehose_connections=raw_config.get("firehose_connections", [1]),
            firehose_inflight=raw_config.get("firehose_inflight", [1]),
            firehose_io_threads=raw_config.get("firehose_io_threads", 0),
            description=description,
        )
        validate_benchmark_config(normalized)
        return normalized
    except (TypeError, ValueError) as error:
        raise RuntimeError(f"invalid benchmark config {config_path}: {error}") from error


def validate_benchmark_config(config):
    if not isinstance(config.host, str):
        raise ValueError("host must be a string")

    for name in ("duration", "payload_size", "repetitions"):
        if getattr(config, name) <= 0:
            raise ValueError(f"{name} must be greater than zero")
    if type(config.warmup_duration) is not int or config.warmup_duration < 0:
        raise ValueError("warmup_duration must be a non-negative integer")
    if config.port < 0:
        raise ValueError("port must not be negative")
    if config.server_delay_us < 0:
        raise ValueError("server_delay_us must not be negative")
    if config.server_worker_threads < 0:
        raise ValueError("server_worker_threads must not be negative")
    if config.server_connection_io_threads <= 0:
        raise ValueError("server_connection_io_threads must be greater than zero")
    if config.server_listen_backlog <= 0:
        raise ValueError("server_listen_backlog must be greater than zero")
    if config.run_timeout < 0:
        raise ValueError("run_timeout must not be negative")
    if type(config.firehose_io_threads) is not int or config.firehose_io_threads < 0:
        raise ValueError("firehose_io_threads must be a non-negative integer")
    if config.client_mode == "firehose":
        validate_firehose_cases(config.firehose_cases)
        if config.firehose_cases is None:
            connection_values = positive_int_or_list(
                config.firehose_connections, "firehose_connections"
            )
            max_connections = (
                max(connection_values)
                if isinstance(connection_values, list)
                else connection_values
            )
            inflight_values = positive_int_or_list(
                config.firehose_inflight, "firehose_inflight"
            )
            min_inflight = (
                min(inflight_values)
                if isinstance(inflight_values, list)
                else inflight_values
            )
            if min_inflight < max_connections:
                raise ValueError(
                    "firehose_inflight must be greater than or equal to firehose_connections"
                )
    else:
        if config.firehose_cases is not None:
            raise ValueError("firehose_cases is only valid in firehose mode")
        if config.workload != "protobuf":
            raise ValueError("rpc_client mode requires workload=protobuf")
        positive_int_or_list(config.client_threads, "client_threads")

    if (config.server_cpus is None) != (config.client_cpus is None):
        raise ValueError("server_cpus and client_cpus must both be set or both be null")
    if config.server_cpus is None:
        return

    try:
        server_cpus = parse_cpu_list(config.server_cpus)
        client_cpus = parse_cpu_list(config.client_cpus)
    except ValueError as error:
        raise ValueError(f"invalid CPU list: {error}") from error
    overlap = server_cpus & client_cpus
    if overlap:
        raise ValueError(
            "server_cpus and client_cpus overlap: " + format_cpu_list(overlap)
        )


def resolve_config_affinity(config):
    if config.server_cpus is None:
        return {
            "mode": "off",
            "server_cpus": None,
            "client_cpus": None,
            "server_cpu_count": None,
            "client_cpu_count": None,
        }

    return {
        "mode": "config",
        "server_cpus": config.server_cpus,
        "client_cpus": config.client_cpus,
        "server_cpu_count": len(parse_cpu_list(config.server_cpus)),
        "client_cpu_count": len(parse_cpu_list(config.client_cpus)),
    }


def run_text(command, cwd):
    result = subprocess.run(
        command,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else "unavailable"


def collect_environment(repo_root):
    return {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "hostname": platform.node(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "git_commit": run_text(["git", "rev-parse", "HEAD"], repo_root),
        "git_branch": run_text(["git", "branch", "--show-current"], repo_root),
        "git_status": run_text(["git", "status", "--short"], repo_root),
        "compiler": run_text(["clang++-20", "--version"], repo_root).splitlines()[0],
        "cpu": run_text(["lscpu"], repo_root),
        "process_cpu_affinity": format_cpu_list(set(os.sched_getaffinity(0))),
        "kernel": run_text(["uname", "-a"], repo_root),
    }


def read_thread_cpu_snapshot(process_id):
    snapshot = {}
    task_dir = Path("/proc") / str(process_id) / "task"
    try:
        task_paths = list(task_dir.iterdir())
    except (FileNotFoundError, PermissionError):
        return snapshot

    for task_path in task_paths:
        try:
            stat = (task_path / "stat").read_text(encoding="utf-8")
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue

        name_end = stat.rfind(")")
        if name_end < 0:
            continue
        fields = stat[name_end + 2 :].split()
        if len(fields) <= 12:
            continue

        # After stripping the "(comm)" field, indices 11 and 12 are utime and
        # stime from proc_pid_stat(5).
        thread_id = int(task_path.name)
        snapshot[thread_id] = {
            "name": stat[stat.find("(") + 1 : name_end],
            "ticks": int(fields[11]) + int(fields[12]),
        }
    return snapshot


def read_child_process_ids(process_id):
    child_process_ids = set()
    task_dir = Path("/proc") / str(process_id) / "task"
    try:
        task_paths = list(task_dir.iterdir())
    except (FileNotFoundError, PermissionError):
        return child_process_ids

    for task_path in task_paths:
        try:
            children = (task_path / "children").read_text(encoding="utf-8")
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        for raw_child in children.split():
            try:
                child_process_ids.add(int(raw_child))
            except ValueError:
                continue
    return child_process_ids


def read_descendant_process_ids(process_id):
    descendants = set()
    pending = list(read_child_process_ids(process_id))
    while pending:
        child_process_id = pending.pop()
        if child_process_id in descendants:
            continue
        descendants.add(child_process_id)
        pending.extend(read_child_process_ids(child_process_id))
    return descendants


def read_process_tree_thread_cpu_snapshot(process_id, include_root):
    process_ids = read_descendant_process_ids(process_id)
    if include_root:
        process_ids.add(process_id)

    snapshot = {}
    for current_process_id in process_ids:
        snapshot.update(read_thread_cpu_snapshot(current_process_id))
    return snapshot


def calculate_thread_cpu_usage(start_snapshot, end_snapshot, elapsed_seconds):
    if elapsed_seconds <= 0:
        return []

    usage = []
    for thread_id, end in end_snapshot.items():
        start = start_snapshot.get(thread_id)
        if start is None:
            continue
        elapsed_ticks = max(0, end["ticks"] - start["ticks"])
        cpu_percent = elapsed_ticks * 100.0 / CPU_TICKS_PER_SECOND / elapsed_seconds
        usage.append(
            {
                "thread_id": thread_id,
                "name": end["name"],
                "cpu_percent": cpu_percent,
            }
        )
    return sorted(usage, key=lambda item: item["cpu_percent"], reverse=True)


def update_thread_cpu_tracking(first_snapshot, last_snapshot, snapshot):
    for thread_id, current in snapshot.items():
        if thread_id not in first_snapshot:
            first_snapshot[thread_id] = current
        last_snapshot[thread_id] = current


def format_thread_cpu_usage(usage):
    return ";".join(
        f"{item['thread_id']}:{item['name']}:{item['cpu_percent']:.2f}"
        for item in usage
    )


def configure_and_build(repo_root, build_dir):
    subprocess.run(
        [
            "cmake",
            "-S",
            str(repo_root),
            "-B",
            str(build_dir),
            "-DCMAKE_CXX_COMPILER=clang++-20",
            "-DCMAKE_BUILD_TYPE=Release",
            "-DXRPC_BUILD_TOOLS=ON",
            "-DXRPC_BUILD_TESTS=OFF",
            "-DXRPC_BUILD_EXAMPLES=OFF",
        ],
        check=True,
    )
    subprocess.run(
        [
            "cmake",
            "--build",
            str(build_dir),
            "--parallel",
            "--target",
            "xrpc_benchmark_server",
            "xrpc_benchmark_client",
        ],
        check=True,
    )


def firehose_inflight_values(config):
    if isinstance(config.firehose_inflight, list):
        return config.firehose_inflight
    return [config.firehose_inflight]


def firehose_connection_values(config):
    if isinstance(config.firehose_connections, list):
        return config.firehose_connections
    return [config.firehose_connections]


def client_thread_values(config):
    if isinstance(config.client_threads, list):
        return config.client_threads
    return [config.client_threads]


def build_cases(config):
    if config.client_mode == "rpc_client":
        return [
            BenchmarkRunSpec(client_threads=client_threads)
            for client_threads in client_thread_values(config)
        ]
    if config.firehose_cases is not None:
        return [
            BenchmarkRunSpec(
                firehose_connections=case["firehose_connections"],
                firehose_inflight=case["firehose_inflight"],
            )
            for case in config.firehose_cases
        ]
    return [
        BenchmarkRunSpec(
            firehose_connections=firehose_connections,
            firehose_inflight=firehose_inflight,
        )
        for firehose_connections in firehose_connection_values(config)
        for firehose_inflight in firehose_inflight_values(config)
    ]


def resolve_firehose_io_threads(config, run_spec, affinity):
    if config.client_mode != "firehose":
        return 0
    if config.firehose_io_threads != 0:
        return config.firehose_io_threads
    if affinity["client_cpu_count"] is None:
        return 0
    return min(affinity["client_cpu_count"], run_spec.firehose_connections)


def benchmark_command(
    binary,
    config,
    run_spec,
    port,
    client_cpus,
    firehose_io_threads,
    duration=None,
):
    command = [
        str(binary),
        f"--client_mode={config.client_mode}",
        f"--workload={config.workload}",
        f"--duration_s={duration if duration is not None else config.duration}",
        f"--payload_size={config.payload_size}",
        f"--host={config.host}",
        f"--port={port}",
    ]
    if config.client_mode == "firehose":
        command.extend(
            [
                f"--firehose_connections={run_spec.firehose_connections}",
                f"--firehose_inflight={run_spec.firehose_inflight}",
                f"--firehose_io_threads={firehose_io_threads}",
            ]
        )
    else:
        command.append(f"--client_threads={run_spec.client_threads}")
    return apply_cpu_affinity(command, client_cpus)


def client_profile_command(command, profile_path):
    return [
        "perf",
        "record",
        "-F",
        "199",
        "--call-graph",
        "dwarf",
        "-o",
        str(profile_path),
        "--",
        *command,
    ]


def is_client_profile_command(command):
    return bool(command) and Path(command[0]).name == "perf" and "record" in command


def profile_artifact_paths(profile_path):
    return {
        "data": profile_path,
        "report": profile_path.with_suffix(".report.txt"),
        "script": profile_path.with_suffix(".script.txt"),
        "folded": profile_path.with_suffix(".folded"),
        "flamegraph": profile_path.with_suffix(".flamegraph.svg"),
        "log": profile_path.with_suffix(".artifacts.log"),
    }


def run_profile_artifact_command(command, repo_root, output_path, log_file):
    log_file.write("$ " + shlex.join(str(item) for item in command) + "\n")
    with output_path.open("w", encoding="utf-8") as output_file:
        result = subprocess.run(
            command,
            cwd=repo_root,
            check=False,
            stdout=output_file,
            stderr=subprocess.PIPE,
            text=True,
        )
    if result.stderr:
        log_file.write(result.stderr)
    if result.returncode != 0:
        log_file.write(f"command exited with code {result.returncode}\n")
        return False
    return True


def generate_profile_artifacts(profile_path, repo_root, title):
    paths = profile_artifact_paths(profile_path)
    generated = {"data": paths["data"]}
    if not profile_path.is_file() or profile_path.stat().st_size == 0:
        return generated

    with paths["log"].open("w", encoding="utf-8") as log_file:
        generated["log"] = paths["log"]
        if run_profile_artifact_command(
            [
                "perf",
                "report",
                "--stdio",
                "--children",
                "--percent-limit",
                "0",
                "-g",
                "graph,0.1,caller,function,percent",
                "--show-total-period",
                "-i",
                str(profile_path),
            ],
            repo_root,
            paths["report"],
            log_file,
        ):
            generated["report"] = paths["report"]

        script_ok = run_profile_artifact_command(
            ["perf", "script", "-i", str(profile_path)],
            repo_root,
            paths["script"],
            log_file,
        )
        if script_ok:
            generated["script"] = paths["script"]

        stackcollapse = shutil.which("stackcollapse-perf.pl")
        flamegraph = shutil.which("flamegraph.pl")
        if not stackcollapse or not flamegraph:
            log_file.write("FlameGraph tools not found; skipped SVG generation\n")
            return generated

        folded_ok = run_profile_artifact_command(
            [stackcollapse, str(paths["script"])],
            repo_root,
            paths["folded"],
            log_file,
        )
        if not folded_ok:
            return generated
        generated["folded"] = paths["folded"]

        flamegraph_ok = run_profile_artifact_command(
            [
                flamegraph,
                "--title",
                title,
                "--countname",
                "samples",
                str(paths["folded"]),
            ],
            repo_root,
            paths["flamegraph"],
            log_file,
        )
        if flamegraph_ok:
            generated["flamegraph"] = paths["flamegraph"]

    return generated


def format_artifact_paths(artifacts, output_dir):
    return ";".join(
        f"{name}={path.relative_to(output_dir)}"
        for name, path in artifacts.items()
        if path.is_file()
    )


def parse_run_case_selector(value, option_name):
    if value is None:
        return None

    selector = {}
    for item in value.split(","):
        item = item.strip()
        if not item:
            raise ValueError(f"{option_name} contains an empty item")
        key, separator, raw_value = item.partition("=")
        if separator != "=":
            raise ValueError(
                f"{option_name} must use key=value pairs such as firehose_connections=3,firehose_inflight=288"
            )
        key = key.strip()
        raw_value = raw_value.strip()
        if key not in (
            "client_threads",
            "ct",
            "firehose_connections",
            "fc",
            "firehose_inflight",
            "fh",
        ):
            raise ValueError(
                f"{option_name} only supports client_threads/ct, "
                "firehose_connections/fc, and firehose_inflight/fh"
            )
        parsed = int(raw_value)
        if parsed <= 0:
            raise ValueError(f"{option_name} values must be greater than zero")
        if key == "fc":
            key = "firehose_connections"
        if key == "fh":
            key = "firehose_inflight"
        if key == "ct":
            key = "client_threads"
        selector[key] = parsed

    return selector


def run_case_matches(selector, run_spec):
    if selector is None:
        return True
    if (
        selector.get("client_threads") is not None
        and selector["client_threads"] != run_spec.client_threads
    ):
        return False
    if (
        selector.get("firehose_connections") is not None
        and selector["firehose_connections"] != run_spec.firehose_connections
    ):
        return False
    if (
        selector.get("firehose_inflight") is not None
        and selector["firehose_inflight"] != run_spec.firehose_inflight
    ):
        return False
    return True


def start_server_profile(server_process_id, profile_path):
    log_file = profile_path.with_suffix(".log").open("w", encoding="utf-8")
    process = subprocess.Popen(
        [
            "perf",
            "record",
            "-F",
            "199",
            "--call-graph",
            "dwarf",
            "-o",
            str(profile_path),
            "-p",
            str(server_process_id),
        ],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    time.sleep(0.1)
    if process.poll() is not None:
        log_file.close()
        raise RuntimeError(
            f"server perf exited before benchmark run; inspect {profile_path.with_suffix('.log')}"
        )
    return process, log_file


def stop_server_profile(process, log_file):
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            stop_process_group(process)
    log_file.close()


def server_command(binary, config, worker_threads, server_cpus):
    command = [
        str(binary),
        f"--host={config.host}",
        f"--port={config.port}",
        f"--server_delay_us={config.server_delay_us}",
        f"--workload={config.workload}",
        f"--worker_threads={worker_threads}",
        f"--connection_io_threads={config.server_connection_io_threads}",
        f"--listen_backlog={config.server_listen_backlog}",
    ]
    return apply_cpu_affinity(command, server_cpus)


def stop_process_group(process):
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()


def read_available_chunks(process, selector, log_file, label, output_chunks):
    for key, _ in selector.select(timeout=0.2):
        try:
            chunk = os.read(key.fileobj.fileno(), 65536)
        except BlockingIOError:
            continue
        if chunk:
            text = chunk.decode("utf-8", errors="replace")
            output_chunks.append(text)
            log_file.write(text)
            log_file.flush()
            print(f"[{label}] {text}", end="", flush=True)


def drain_process_output(process, log_file, label, output_chunks):
    while True:
        try:
            chunk = os.read(process.stdout.fileno(), 65536)
        except BlockingIOError:
            break
        if not chunk:
            break
        text = chunk.decode("utf-8", errors="replace")
        output_chunks.append(text)
        log_file.write(text)
        print(f"[{label}] {text}", end="", flush=True)


def start_server(command, repo_root, log_path, ready_timeout):
    output_chunks = []
    log_file = log_path.open("w", encoding="utf-8")
    log_file.write("command: " + " ".join(command) + "\n\n")
    process = subprocess.Popen(
        command,
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    assert process.stdout is not None
    os.set_blocking(process.stdout.fileno(), False)
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    deadline = time.monotonic() + ready_timeout
    ready_line = None

    while process.poll() is None:
        read_available_chunks(process, selector, log_file, "server", output_chunks)
        joined = "".join(output_chunks)
        for line in joined.splitlines():
            if line.startswith("ready "):
                ready_line = line
                break
        if ready_line is not None:
            break
        if time.monotonic() >= deadline:
            stop_process_group(process)
            drain_process_output(process, log_file, "server", output_chunks)
            selector.close()
            log_file.close()
            raise RuntimeError(f"benchmark server did not become ready within {ready_timeout}s")

    if process.poll() is not None and ready_line is None:
        drain_process_output(process, log_file, "server", output_chunks)
        selector.close()
        log_file.close()
        joined = "".join(output_chunks).strip()
        raise RuntimeError(
            "benchmark server exited before becoming ready"
            + (f": {joined}" if joined else "")
        )

    drain_process_output(process, log_file, "server", output_chunks)
    selector.close()
    return process, log_file, output_chunks, ready_line


def stop_server(process, log_file, output_chunks):
    stop_process_group(process)
    drain_process_output(process, log_file, "server", output_chunks)
    log_file.close()


def parse_ready_port(ready_line):
    match = READY_PORT_PATTERN.search(ready_line)
    if match is None:
        raise RuntimeError(f"benchmark server ready line did not include port: {ready_line}")
    return int(match.group(1))


def case_suffix(config, run_spec, repetition):
    if config.client_mode == "rpc_client":
        base = f"rpc-threads-{run_spec.client_threads}"
    else:
        base = (
            f"fhconn-{run_spec.firehose_connections}_fh-{run_spec.firehose_inflight}"
        )
    return f"{base}_r-{repetition}"


def empty_server_stats():
    return {
        "server_rejected_inflight": "",
        "server_rejected_global_pending": "",
        "server_closed_write_queue": "",
        "server_max_inflight": "",
        "server_max_write_queue_bytes": "",
        "server_worker_rejected": "",
        "server_worker_max_depth": "",
    }


def parse_server_stats(log_path):
    stats = empty_server_stats()
    try:
        output = log_path.read_text(encoding="utf-8")
    except OSError:
        return stats

    backpressure_match = BACKPRESSURE_PATTERN.search(output)
    if backpressure_match is not None:
        (
            stats["server_rejected_inflight"],
            stats["server_rejected_global_pending"],
            stats["server_closed_write_queue"],
            stats["server_max_inflight"],
            stats["server_max_write_queue_bytes"],
        ) = map(int, backpressure_match.groups())

    worker_match = WORKER_QUEUE_PATTERN.search(output)
    if worker_match is not None:
        (
            stats["server_worker_rejected"],
            stats["server_worker_max_depth"],
        ) = map(int, worker_match.groups())
    return stats


def run_benchmark(
    command,
    repo_root,
    log_path,
    label,
    timeout_seconds,
    server_process_id,
):
    output_chunks = []
    server_cpu_start = read_thread_cpu_snapshot(server_process_id)
    client_cpu_first = {}
    client_cpu_last = {}
    # With client perf enabled the root process is perf itself; measure the
    # benchmark child process tree instead of the profiler wrapper.
    include_client_root_process = not is_client_profile_command(command)
    children_cpu_start = resource.getrusage(resource.RUSAGE_CHILDREN)
    measurement_start = time.monotonic()
    with log_path.open("w", encoding="utf-8") as log_file:
        log_file.write("command: " + " ".join(command) + "\n\n")
        process = subprocess.Popen(
            command,
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        assert process.stdout is not None
        update_thread_cpu_tracking(
            client_cpu_first,
            client_cpu_last,
            read_process_tree_thread_cpu_snapshot(
                process.pid,
                include_client_root_process,
            ),
        )
        os.set_blocking(process.stdout.fileno(), False)
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        deadline = time.monotonic() + timeout_seconds
        timed_out = False
        while process.poll() is None:
            if time.monotonic() >= deadline:
                timed_out = True
                update_thread_cpu_tracking(
                    client_cpu_first,
                    client_cpu_last,
                    read_process_tree_thread_cpu_snapshot(
                        process.pid,
                        include_client_root_process,
                    ),
                )
                stop_process_group(process)
                break
            read_available_chunks(process, selector, log_file, label, output_chunks)
            update_thread_cpu_tracking(
                client_cpu_first,
                client_cpu_last,
                read_process_tree_thread_cpu_snapshot(
                    process.pid,
                    include_client_root_process,
                ),
            )

        update_thread_cpu_tracking(
            client_cpu_first,
            client_cpu_last,
            read_process_tree_thread_cpu_snapshot(
                process.pid,
                include_client_root_process,
            ),
        )
        drain_process_output(process, log_file, label, output_chunks)

        selector.close()
        return_code = process.wait()
        if timed_out:
            timeout_message = f"benchmark timed out after {timeout_seconds}s\n"
            output_chunks.append(timeout_message)
            log_file.write(timeout_message)
            print(f"[{label}] {timeout_message}", end="", flush=True)

    measurement_seconds = time.monotonic() - measurement_start
    cpu_interval_seconds = measurement_seconds
    children_cpu_end = resource.getrusage(resource.RUSAGE_CHILDREN)
    client_cpu_seconds = (
        children_cpu_end.ru_utime
        + children_cpu_end.ru_stime
        - children_cpu_start.ru_utime
        - children_cpu_start.ru_stime
    )
    client_cpu_percent = client_cpu_seconds * 100.0 / cpu_interval_seconds
    client_cpu_usage = calculate_thread_cpu_usage(
        client_cpu_first,
        client_cpu_last,
        cpu_interval_seconds,
    )
    client_max_thread_cpu_percent = max(
        (item["cpu_percent"] for item in client_cpu_usage), default=0.0
    )
    server_cpu_usage = calculate_thread_cpu_usage(
        server_cpu_start,
        read_thread_cpu_snapshot(server_process_id),
        cpu_interval_seconds,
    )
    server_cpu_percent = sum(item["cpu_percent"] for item in server_cpu_usage)
    server_max_thread_cpu_percent = max(
        (item["cpu_percent"] for item in server_cpu_usage), default=0.0
    )

    output = "".join(output_chunks)
    total_match = TOTAL_PATTERN.search(output)
    latency_match = LATENCY_PATTERN.search(output)
    if timed_out or return_code != 0 or total_match is None or latency_match is None:
        return {
            "status": "failed",
            "return_code": return_code,
            "error": (
                f"benchmark timed out after {timeout_seconds}s"
                if timed_out
                else "benchmark failed or final statistics were not found"
            ),
            "measurement_seconds": measurement_seconds,
            "client_cpu_percent": client_cpu_percent,
            "client_max_thread_cpu_percent": client_max_thread_cpu_percent,
            "client_thread_cpu": format_thread_cpu_usage(client_cpu_usage),
            "server_cpu_percent": server_cpu_percent,
            "server_max_thread_cpu_percent": server_max_thread_cpu_percent,
            "server_thread_cpu": format_thread_cpu_usage(server_cpu_usage),
        }

    total_calls, success_calls, failed_calls = map(int, total_match.groups())
    qps, avg_us, p50_us, p95_us, p99_us = map(float, latency_match.groups())
    success_qps = 0.0 if total_calls == 0 else qps * success_calls / total_calls
    failure_percent = 0.0 if total_calls == 0 else failed_calls * 100.0 / total_calls
    return {
        "status": "ok",
        "return_code": return_code,
        "total_calls": total_calls,
        "success_calls": success_calls,
        "failed_calls": failed_calls,
        "reported_qps": qps,
        "success_qps": success_qps,
        "failure_percent": failure_percent,
        "avg_us": avg_us,
        "p50_us": p50_us,
        "p95_us": p95_us,
        "p99_us": p99_us,
        "measurement_seconds": measurement_seconds,
        "client_cpu_percent": client_cpu_percent,
        "client_max_thread_cpu_percent": client_max_thread_cpu_percent,
        "client_thread_cpu": format_thread_cpu_usage(client_cpu_usage),
        "server_cpu_percent": server_cpu_percent,
        "server_max_thread_cpu_percent": server_max_thread_cpu_percent,
        "server_thread_cpu": format_thread_cpu_usage(server_cpu_usage),
    }


def write_runs_csv(output_dir, rows):
    fieldnames = [
        "client_mode",
        "workload",
        "repetition",
        "client_threads",
        "firehose_io_threads",
        "firehose_connections",
        "firehose_inflight",
        "status",
        "return_code",
        "total_calls",
        "success_calls",
        "failed_calls",
        "reported_qps",
        "success_qps",
        "failure_percent",
        "avg_us",
        "p50_us",
        "p95_us",
        "p99_us",
        "measurement_seconds",
        "client_cpu_percent",
        "client_max_thread_cpu_percent",
        "client_thread_cpu",
        "server_cpu_percent",
        "server_max_thread_cpu_percent",
        "server_thread_cpu",
        "server_rejected_inflight",
        "server_rejected_global_pending",
        "server_closed_write_queue",
        "server_max_inflight",
        "server_max_write_queue_bytes",
        "server_worker_rejected",
        "server_worker_max_depth",
        "client_profile",
        "server_profile",
        "client_profile_artifacts",
        "server_profile_artifacts",
        "log",
        "server_log",
        "error",
    ]
    with (output_dir / "runs.csv").open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def median(values):
    return statistics.median(values) if values else 0.0


def write_summary(output_dir, config, rows, affinity, server_worker_threads):
    groups = {}
    for row in rows:
        if config.client_mode == "rpc_client":
            key = (row["client_threads"],)
        else:
            key = (
                row["firehose_io_threads"],
                row["firehose_connections"],
                row["firehose_inflight"],
            )
        groups.setdefault(key, []).append(row)

    firehose_io_threads_label = "auto" if config.firehose_io_threads == 0 else config.firehose_io_threads

    lines = [
        "# XRPC Benchmark Suite",
        "",
        f"- client mode: `{config.client_mode}`",
        f"- workload: `{config.workload}`",
        f"- warmup: `{config.warmup_duration}s`",
        f"- duration: `{config.duration}s`",
        f"- repetitions: `{config.repetitions}`",
        f"- payload: `{config.payload_size} bytes`",
        f"- CPU affinity: `{affinity['mode']}`",
        f"- server CPUs: `{affinity['server_cpus'] or 'unbound'}`",
        f"- client CPUs: `{affinity['client_cpus'] or 'unbound'}`",
        f"- server worker threads: `{server_worker_threads}`",
        f"- server connection I/O threads: `{config.server_connection_io_threads}`",
        f"- server lifecycle: `{config.server_lifecycle}`",
    ]
    if config.client_mode == "rpc_client":
        lines.extend(
            [
                f"- client threads: `{config.client_threads}`",
                "",
                "| Client Threads | Valid Runs | Median Success QPS | Min Success QPS | Max Success QPS | Total Failed | Median Avg us | Median p50 us | Median p95 us | Median p99 us | Median Client CPU | Median Server CPU |",
                "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
    else:
        if config.firehose_cases is None:
            firehose_case_lines = [
                f"- firehose connections: `{config.firehose_connections}`",
                f"- firehose inflight: `{config.firehose_inflight}`",
            ]
        else:
            case_pairs = [
                (
                    case["firehose_connections"],
                    case["firehose_inflight"],
                )
                for case in config.firehose_cases
            ]
            firehose_case_lines = [f"- firehose cases: `{case_pairs}`"]
        lines.extend(
            [
                f"- firehose I/O threads: `{firehose_io_threads_label}`",
                *firehose_case_lines,
                "",
                "| Firehose I/O Threads | Firehose Connections | Firehose Inflight | Valid Runs | Median Success QPS | Min Success QPS | Max Success QPS | Total Failed | Median Avg us | Median p50 us | Median p95 us | Median p99 us | Median Client CPU | Median Server CPU |",
                "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )

    for key, group_rows in sorted(groups.items()):
        valid = [row for row in group_rows if row["status"] == "ok"]
        qps_values = [row["success_qps"] for row in valid]
        avg_values = [row["avg_us"] for row in valid]
        p50_values = [row["p50_us"] for row in valid]
        p95_values = [row["p95_us"] for row in valid]
        p99_values = [row["p99_us"] for row in valid]
        client_cpu_values = [row["client_cpu_percent"] for row in valid]
        server_cpu_values = [row["server_cpu_percent"] for row in valid]
        total_failed = sum(row.get("failed_calls", 0) for row in valid)
        result_columns = (
            f"{len(valid)}/{len(group_rows)} | {median(qps_values):.2f} | "
            f"{min(qps_values, default=0.0):.2f} | "
            f"{max(qps_values, default=0.0):.2f} | {total_failed} | "
            f"{median(avg_values):.2f} | {median(p50_values):.2f} | "
            f"{median(p95_values):.2f} | {median(p99_values):.2f} | "
            f"{median(client_cpu_values):.2f}% | {median(server_cpu_values):.2f}% |"
        )
        if config.client_mode == "rpc_client":
            lines.append(f"| {key[0]} | {result_columns}")
        else:
            lines.append(f"| {key[0]} | {key[1]} | {key[2]} | {result_columns}")

    lines.extend(
        [
            "",
            "## Audit Notes",
            "",
            "- `Success QPS` excludes failed calls from the benchmark-reported total QPS.",
            "- Payload size means raw payload bytes for `raw` and Echo message bytes before Protobuf encoding for `protobuf`.",
            "- Inspect `runs.csv` and `logs/` before accepting a performance conclusion.",
            "- This suite starts external benchmark server process(es) according to `server_lifecycle`.",
            "- Perf runs generate `.data`, `.report.txt`, `.script.txt`, `.folded`, `.flamegraph.svg`, and `.artifacts.log` when the local tools are available.",
            "",
        ]
    )
    (output_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Run an auditable XRPC benchmark matrix."
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="benchmark JSON profile; all benchmark parameters come from this file",
    )
    parser.add_argument("--build-dir", type=Path, default=Path("build"))
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--build", action="store_true")
    parser.add_argument(
        "--client-perf",
        action="store_true",
        help="record a perf profile for each benchmark client run",
    )
    parser.add_argument(
        "--server-perf",
        action="store_true",
        help="record an attached perf profile for the server during each client run",
    )
    parser.add_argument(
        "--case",
        help=(
            "run only the matching case, such as "
            "firehose_connections=3,firehose_inflight=288 or client_threads=3"
        ),
    )
    parser.add_argument(
        "--validate-config",
        action="store_true",
        help="validate the benchmark config and exit without building or running benchmarks",
    )
    return parser.parse_args(argv)


def run_warmup(
    repo_root,
    logs_dir,
    client_binary,
    config,
    affinity,
    run_spec,
    server_port,
    server_process,
    repetition,
):
    if config.warmup_duration == 0:
        return

    suffix = case_suffix(config, run_spec, repetition)
    log_path = logs_dir / f"warmup_{suffix}.log"
    firehose_io_threads = resolve_firehose_io_threads(config, run_spec, affinity)
    command = benchmark_command(
        client_binary,
        config,
        run_spec,
        server_port,
        affinity["client_cpus"],
        firehose_io_threads,
        duration=config.warmup_duration,
    )
    result = run_benchmark(
        command,
        repo_root,
        log_path,
        f"warmup {suffix}",
        config.warmup_duration + 30,
        server_process.pid,
    )
    if result["status"] != "ok" or result.get("failed_calls", 0) != 0:
        raise RuntimeError(f"benchmark warmup failed; inspect {log_path}")


def run_client_benchmark_case(
    repo_root,
    output_dir,
    logs_dir,
    profiles_dir,
    client_binary,
    config,
    affinity,
    run_spec,
    server_port,
    server_process,
    server_log_path,
    label,
    repetition,
    run_timeout,
    enable_client_perf,
    enable_server_perf,
):
    suffix = case_suffix(config, run_spec, repetition)
    log_name = f"{suffix}.log"
    log_path = logs_dir / log_name
    firehose_io_threads = resolve_firehose_io_threads(config, run_spec, affinity)
    command = benchmark_command(
        client_binary,
        config,
        run_spec,
        server_port,
        affinity["client_cpus"],
        firehose_io_threads,
    )
    client_profile_path = None
    if enable_client_perf:
        client_profile_path = profiles_dir / f"client_{suffix}.data"
        command = client_profile_command(command, client_profile_path)

    server_profile_path = None
    server_profiler = None
    server_profile_log_file = None
    if enable_server_perf:
        server_profile_path = profiles_dir / f"server_{suffix}.data"
        server_profiler, server_profile_log_file = start_server_profile(
            server_process.pid, server_profile_path
        )
    try:
        result = run_benchmark(
            command,
            repo_root,
            log_path,
            label,
            run_timeout,
            server_process.pid,
        )
    finally:
        if server_profiler is not None and server_profile_log_file is not None:
            stop_server_profile(server_profiler, server_profile_log_file)

    client_profile_artifacts = {}
    if client_profile_path is not None:
        client_profile_artifacts = generate_profile_artifacts(
            client_profile_path,
            repo_root,
            f"XRPC {config.client_mode} client {suffix}",
        )

    server_profile_artifacts = {}
    if server_profile_path is not None:
        server_profile_artifacts = generate_profile_artifacts(
            server_profile_path,
            repo_root,
            f"XRPC {config.workload} server {suffix}",
        )

    return {
        "client_mode": config.client_mode,
        "workload": config.workload,
        "repetition": repetition,
        "client_threads": run_spec.client_threads,
        "firehose_io_threads": firehose_io_threads,
        "firehose_connections": run_spec.firehose_connections,
        "firehose_inflight": run_spec.firehose_inflight,
        **result,
        "client_profile": (
            str(client_profile_path.relative_to(output_dir))
            if client_profile_path is not None
            else ""
        ),
        "server_profile": (
            str(server_profile_path.relative_to(output_dir))
            if server_profile_path is not None
            else ""
        ),
        "client_profile_artifacts": format_artifact_paths(
            client_profile_artifacts,
            output_dir,
        ),
        "server_profile_artifacts": format_artifact_paths(
            server_profile_artifacts,
            output_dir,
        ),
        "log": str(log_path.relative_to(output_dir)),
        "server_log": str(server_log_path.relative_to(output_dir)),
        "error": result.get("error", ""),
    }


def main():
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    build_dir = (repo_root / args.build_dir).resolve()

    try:
        run_case = parse_run_case_selector(args.case, "--case")
    except ValueError as error:
        print(f"benchmark case selector failed: {error}", file=sys.stderr)
        return 2

    try:
        config = load_benchmark_config(args.config)
    except RuntimeError as error:
        print(f"benchmark config failed: {error}", file=sys.stderr)
        return 2

    affinity = resolve_config_affinity(config)
    cases = build_cases(config)
    run_specs = []
    for run_spec in cases:
        if run_case_matches(run_case, run_spec):
            run_specs.append(run_spec)
    if run_case is not None and len(run_specs) != 1:
        print(
            f"benchmark case selector must match exactly one run; matched {len(run_specs)}",
            file=sys.stderr,
        )
        return 2
    if run_case is None:
        run_specs = cases

    print(
        f"client_mode={config.client_mode} "
        "CPU affinity: "
        f"mode={affinity['mode']} "
        f"server={affinity['server_cpus'] or 'unbound'} "
        f"client={affinity['client_cpus'] or 'unbound'} "
        f"firehose_io_threads={config.firehose_io_threads} "
        f"server_workers={config.server_worker_threads} "
        f"server_connection_io_threads={config.server_connection_io_threads} "
        f"server_lifecycle={config.server_lifecycle}"
    )

    if args.validate_config:
        print(
            "benchmark config valid: "
            f"path={args.config} "
            f"cases={len(run_specs)} "
            f"repetitions={config.repetitions} "
            f"client_mode={config.client_mode} "
            f"workload={config.workload}"
        )
        return 0

    if args.build:
        configure_and_build(repo_root, build_dir)

    client_binary = build_dir / "tools" / "benchmark" / "xrpc_benchmark_client"
    server_binary = build_dir / "tools" / "benchmark" / "xrpc_benchmark_server"
    if not client_binary.is_file():
        print(f"benchmark binary not found: {client_binary}", file=sys.stderr)
        print("rerun with --build or build the benchmark targets first", file=sys.stderr)
        return 2
    if not server_binary.is_file():
        print(f"benchmark server binary not found: {server_binary}", file=sys.stderr)
        print("rerun with --build or build the benchmark targets first", file=sys.stderr)
        return 2

    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = (
        (repo_root / args.output_dir).resolve()
        if args.output_dir is not None
        else build_dir / "benchmark-results" / timestamp
    )
    logs_dir = output_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=False)
    profiles_dir = output_dir / "profiles"
    if args.client_perf or args.server_perf:
        profiles_dir.mkdir()

    environment = collect_environment(repo_root)
    (output_dir / "environment.json").write_text(
        json.dumps(environment, indent=2), encoding="utf-8"
    )
    written_config = asdict(config)
    written_config["config_path"] = str(args.config)
    written_config["resolved_cpu_affinity"] = affinity
    written_config["run_options"] = {
        "build": args.build,
        "build_dir": str(args.build_dir),
        "output_dir": str(args.output_dir) if args.output_dir is not None else None,
        "client_perf": args.client_perf,
        "server_perf": args.server_perf,
        "case": args.case,
    }
    (output_dir / "config.json").write_text(
        json.dumps(written_config, indent=2), encoding="utf-8"
    )

    rows = []
    total_runs = len(run_specs) * config.repetitions
    run_timeout = config.run_timeout or config.duration + 30
    suite_server_process = None
    suite_server_log_file = None
    suite_server_output_chunks = None
    suite_server_port = None
    suite_server_log_path = logs_dir / "server.log"
    try:
        if config.server_lifecycle == "per_suite":
            suite_server_process, suite_server_log_file, suite_server_output_chunks, ready_line = start_server(
                server_command(
                    server_binary,
                    config,
                    config.server_worker_threads,
                    affinity["server_cpus"],
                ),
                repo_root,
                suite_server_log_path,
                30,
            )
            print(f"server ready: {ready_line}")
            suite_server_port = parse_ready_port(ready_line)

        run_number = 0
        for repetition in range(1, config.repetitions + 1):
            for run_spec in run_specs:
                run_number += 1
                suffix = case_suffix(config, run_spec, repetition)
                if config.client_mode == "rpc_client":
                    case_description = f"threads={run_spec.client_threads}"
                else:
                    case_description = (
                        f"fhconn={run_spec.firehose_connections} "
                        f"fh={run_spec.firehose_inflight}"
                    )
                label = (
                    f"{run_number}/{total_runs} {case_description} "
                    f"repetition={repetition}"
                )
                print(f"\n=== {label} ===")
                if config.server_lifecycle == "per_case":
                    server_log_path = logs_dir / f"server_{suffix}.log"
                    (
                        server_process,
                        server_log_file,
                        server_output_chunks,
                        ready_line,
                    ) = start_server(
                        server_command(
                            server_binary,
                            config,
                            config.server_worker_threads,
                            affinity["server_cpus"],
                        ),
                        repo_root,
                        server_log_path,
                        30,
                    )
                    print(f"server ready: {ready_line}")
                    server_port = parse_ready_port(ready_line)
                else:
                    if suite_server_process is None or suite_server_port is None:
                        raise RuntimeError("suite server was not started")
                    server_process = suite_server_process
                    server_log_file = None
                    server_output_chunks = None
                    server_log_path = suite_server_log_path
                    server_port = suite_server_port

                row = None
                try:
                    run_warmup(
                        repo_root,
                        logs_dir,
                        client_binary,
                        config,
                        affinity,
                        run_spec,
                        server_port,
                        server_process,
                        repetition,
                    )
                    row = run_client_benchmark_case(
                        repo_root,
                        output_dir,
                        logs_dir,
                        profiles_dir,
                        client_binary,
                        config,
                        affinity,
                        run_spec,
                        server_port,
                        server_process,
                        server_log_path,
                        label,
                        repetition,
                        run_timeout,
                        args.client_perf,
                        args.server_perf,
                    )
                finally:
                    if (
                        config.server_lifecycle == "per_case"
                        and server_log_file is not None
                        and server_output_chunks is not None
                    ):
                        stop_server(
                            server_process, server_log_file, server_output_chunks
                        )

                if row is not None:
                    if config.server_lifecycle == "per_case":
                        row.update(parse_server_stats(server_log_path))
                    else:
                        row.update(empty_server_stats())
                    rows.append(row)
                    write_runs_csv(output_dir, rows)
                    write_summary(
                        output_dir,
                        config,
                        rows,
                        affinity,
                        config.server_worker_threads,
                    )

        write_runs_csv(output_dir, rows)
        write_summary(output_dir, config, rows, affinity, config.server_worker_threads)
        print(f"\nResults: {output_dir}")
        print(f"Summary: {output_dir / 'summary.md'}")
        return 1 if any(row["status"] != "ok" for row in rows) else 0
    finally:
        if (
            suite_server_process is not None
            and suite_server_log_file is not None
            and suite_server_output_chunks is not None
        ):
            stop_server(suite_server_process, suite_server_log_file, suite_server_output_chunks)


if __name__ == "__main__":
    sys.exit(main())
