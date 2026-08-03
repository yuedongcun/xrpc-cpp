#!/usr/bin/env python3

import argparse
import json
import re
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


READY_PORT_PATTERN = re.compile(r"\bport=(\d+)\b")
TOTAL_PATTERN = re.compile(r"total_calls=(\d+) success=(\d+) failed=(\d+)")
METRIC_PATTERN = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+0-9.eE]+)$")


def repo_root():
    return Path(__file__).resolve().parents[2]


def fetch_text(url, timeout_s=2.0, method="GET"):
    request = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(request, timeout=timeout_s) as response:
        return response.read().decode("utf-8")


def wait_until(description, timeout_s, predicate):
    deadline = time.monotonic() + timeout_s
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except Exception as error:
            last_error = error
        time.sleep(0.2)
    if last_error is not None:
        raise RuntimeError(f"timed out waiting for {description}: {last_error}") from last_error
    raise RuntimeError(f"timed out waiting for {description}")


def consul_healthy_instances(consul_address, service_name):
    body = fetch_text(f"http://{consul_address}/v1/health/service/{service_name}?passing=true")
    instances = json.loads(body)
    result = []
    for item in instances:
        service = item.get("Service", {})
        result.append(
            {
                "id": service.get("ID", ""),
                "address": service.get("Address", ""),
                "port": int(service.get("Port", 0)),
            }
        )
    return result


def wait_consul_instance_count(consul_address, service_name, expected_count, timeout_s):
    return wait_until(
        f"Consul service {service_name} to have {expected_count} healthy instance(s)",
        timeout_s,
        lambda: len(consul_healthy_instances(consul_address, service_name)) == expected_count,
    )


def reload_prometheus(prometheus_address):
    try:
        fetch_text(f"http://{prometheus_address}/-/reload", method="POST")
    except urllib.error.URLError:
        pass


def wait_http_ready(url, timeout_s):
    return wait_until(f"{url} ready", timeout_s, lambda: fetch_text(url).strip())


def command_text(command):
    return " ".join(command)


def validate_port(name, value):
    if value <= 0 or value > 65535:
        raise ValueError(f"{name} must be in range 1..65535")


def validate_non_negative(name, value):
    if value < 0:
        raise ValueError(f"{name} must not be negative")


def validate_positive(name, value):
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")


def validate_args(args):
    if not args.consul_address:
        raise ValueError("--consul-address must not be empty")
    if not args.prometheus_address:
        raise ValueError("--prometheus-address must not be empty")
    if not args.rpc_service_name:
        raise ValueError("--rpc-service-name must not be empty")
    if not args.metrics_host:
        raise ValueError("--metrics-host must not be empty")

    validate_port("--client-metrics-port", args.client_metrics_port)
    validate_port("--server-a-metrics-port", args.server_a_metrics_port)
    validate_port("--server-b-metrics-port", args.server_b_metrics_port)
    metrics_ports = {
        args.client_metrics_port,
        args.server_a_metrics_port,
        args.server_b_metrics_port,
    }
    if len(metrics_ports) != 3:
        raise ValueError("client/server metrics ports must be distinct")

    validate_positive("--consul-timeout-ms", args.consul_timeout_ms)
    validate_positive("--discovery-refresh-interval-ms", args.discovery_refresh_interval_ms)
    validate_positive("--server-worker-threads", args.server_worker_threads)
    validate_positive("--server-connection-io-threads", args.server_connection_io_threads)
    validate_positive("--client-threads", args.client_threads)
    validate_positive("--client-timeout-ms", args.client_timeout_ms)
    validate_positive("--duration-s", args.duration_s)
    validate_positive("--payload-size", args.payload_size)
    validate_positive("--ready-timeout-s", args.ready_timeout_s)
    validate_non_negative("--before-add-s", args.before_add_s)
    validate_non_negative("--before-stop-s", args.before_stop_s)
    validate_non_negative("--after-stop-s", args.after_stop_s)
    validate_non_negative("--max-client-failures", args.max_client_failures)


def benchmark_binaries(root, build_dir):
    build_root = Path(build_dir)
    if not build_root.is_absolute():
        build_root = root / build_root
    return (
        build_root / "tools" / "benchmark" / "xrpc_benchmark_server",
        build_root / "tools" / "benchmark" / "xrpc_benchmark_client",
    )


def require_benchmark_binaries(root, build_dir):
    server_binary, client_binary = benchmark_binaries(root, build_dir)
    if not server_binary.exists() or not client_binary.exists():
        raise RuntimeError("benchmark binaries are missing; build xrpc_benchmark_server and xrpc_benchmark_client first")
    return server_binary, client_binary


def print_dry_run(args):
    server_binary, client_binary = benchmark_binaries(repo_root(), args.benchmark_build_dir)
    print("dry_run=1")
    print(f"consul_address={args.consul_address}")
    print(f"prometheus_address={args.prometheus_address}")
    print(f"discovery_service_name={args.discovery_service_name or '<generated>'}")
    print(f"rpc_service_name={args.rpc_service_name}")
    print(
        "metrics_ports="
        f"client:{args.client_metrics_port},"
        f"server_a:{args.server_a_metrics_port},"
        f"server_b:{args.server_b_metrics_port}"
    )
    print(
        "server="
        f"worker_threads:{args.server_worker_threads},"
        f"connection_io_threads:{args.server_connection_io_threads}"
    )
    print(
        "client="
        f"threads:{args.client_threads},"
        f"duration_s:{args.duration_s},"
        f"timeout_ms:{args.client_timeout_ms},"
        f"payload_size:{args.payload_size}"
    )
    print(f"server_binary={'present' if server_binary.exists() else 'missing'} path={server_binary}")
    print(f"client_binary={'present' if client_binary.exists() else 'missing'} path={client_binary}")


class ManagedProcess:
    def __init__(self, name, command):
        self.name = name
        self.command = command
        self.process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self.output_lines = []

    def read_ready_port(self, timeout_s):
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            line = self.process.stdout.readline()
            if line:
                self.output_lines.append(line.rstrip("\n"))
                match = READY_PORT_PATTERN.search(line)
                if match is not None:
                    return int(match.group(1))
                continue
            if self.process.poll() is not None:
                raise RuntimeError(f"{self.name} exited before ready:\n{self.output()}")
            time.sleep(0.05)
        raise RuntimeError(f"timed out waiting for {self.name} ready line")

    def communicate(self, timeout_s):
        try:
            output, _ = self.process.communicate(timeout=timeout_s)
        except subprocess.TimeoutExpired as error:
            self.stop()
            raise RuntimeError(f"{self.name} timed out") from error
        if output:
            self.output_lines.extend(output.splitlines())
        return self.process.returncode

    def stop(self):
        if self.process.poll() is not None:
            return
        self.process.send_signal(signal.SIGTERM)
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
        if self.process.stdout is not None:
            remaining = self.process.stdout.read()
            if remaining:
                self.output_lines.extend(remaining.splitlines())

    def output(self):
        return "\n".join(self.output_lines)


def start_server(args, server_binary, label, metrics_port):
    service_id = f"{args.discovery_service_name}-{label}-{int(time.time() * 1000)}"
    command = [
        str(server_binary),
        "--host=127.0.0.1",
        "--port=0",
        "--workload=protobuf",
        f"--metrics_host={args.metrics_host}",
        f"--metrics_port={metrics_port}",
        f"--service_name={args.discovery_service_name}",
        f"--service_id={service_id}",
        "--service_address=127.0.0.1",
        f"--consul_address={args.consul_address}",
        f"--consul_timeout_ms={args.consul_timeout_ms}",
        f"--worker_threads={args.server_worker_threads}",
        f"--connection_io_threads={args.server_connection_io_threads}",
    ]
    process = ManagedProcess(f"server-{label}", command)
    port = process.read_ready_port(args.ready_timeout_s)
    print(f"server_{label}_started port={port} metrics_port={metrics_port} service_id={service_id}")
    return process


def parse_client_totals(output):
    for line in output.splitlines():
        match = TOTAL_PATTERN.search(line)
        if match is not None:
            return {
                "total": int(match.group(1)),
                "success": int(match.group(2)),
                "failed": int(match.group(3)),
            }
    raise RuntimeError("client output did not contain total_calls line")


def metric_value(metrics_text, metric_name, required_labels):
    total = 0.0
    for line in metrics_text.splitlines():
        match = METRIC_PATTERN.match(line)
        if match is None or match.group(1) != metric_name:
            continue
        labels_text = match.group(2) or ""
        if all(f'{name}="{value}"' in labels_text for name, value in required_labels.items()):
            total += float(match.group(3))
    return total


def start_client(args, client_binary):
    command = [
        str(client_binary),
        f"--target=consul://{args.discovery_service_name}",
        f"--consul_address={args.consul_address}",
        f"--discovery_refresh_interval_ms={args.discovery_refresh_interval_ms}",
        f"--metrics_host={args.metrics_host}",
        f"--metrics_port={args.client_metrics_port}",
        f"--threads={args.client_threads}",
        f"--duration_s={args.duration_s}",
        f"--payload_size={args.payload_size}",
        f"--timeout_ms={args.client_timeout_ms}",
        "--workload=protobuf",
    ]
    print("client_start command=" + command_text(command))
    return ManagedProcess("client", command)


def run_smoke(args):
    root = repo_root()
    server_binary, client_binary = require_benchmark_binaries(root, args.benchmark_build_dir)

    wait_http_ready(f"http://{args.consul_address}/v1/status/leader", args.ready_timeout_s)
    wait_http_ready(f"http://{args.prometheus_address}/-/ready", args.ready_timeout_s)
    reload_prometheus(args.prometheus_address)
    if not args.discovery_service_name:
        args.discovery_service_name = f"xrpc-ha-smoke-{int(time.time() * 1000)}"
    print(f"discovery_service_name={args.discovery_service_name} rpc_service_name={args.rpc_service_name}")

    server_a = None
    server_b = None
    client = None
    try:
        server_a = start_server(args, server_binary, "a", args.server_a_metrics_port)
        wait_consul_instance_count(args.consul_address, args.discovery_service_name, 1, args.ready_timeout_s)
        wait_http_ready(f"http://127.0.0.1:{args.server_a_metrics_port}/-/ready", args.ready_timeout_s)

        client = start_client(args, client_binary)
        wait_http_ready(f"http://127.0.0.1:{args.client_metrics_port}/-/ready", args.ready_timeout_s)

        time.sleep(args.before_add_s)
        server_b = start_server(args, server_binary, "b", args.server_b_metrics_port)
        wait_consul_instance_count(args.consul_address, args.discovery_service_name, 2, args.ready_timeout_s)
        wait_http_ready(f"http://127.0.0.1:{args.server_b_metrics_port}/-/ready", args.ready_timeout_s)

        time.sleep(args.before_stop_s)
        server_a_metrics = fetch_text(f"http://127.0.0.1:{args.server_a_metrics_port}/metrics")
        server_a.stop()
        print("server_a_stopped")
        wait_consul_instance_count(args.consul_address, args.discovery_service_name, 1, args.ready_timeout_s)

        time.sleep(args.after_stop_s)
        client_metrics = fetch_text(f"http://127.0.0.1:{args.client_metrics_port}/metrics")
        server_b_metrics = fetch_text(f"http://127.0.0.1:{args.server_b_metrics_port}/metrics")

        client_return = client.communicate(args.duration_s + args.ready_timeout_s)
        if client_return != 0:
            raise RuntimeError(f"client exited with {client_return}:\n{client.output()}")

        totals = parse_client_totals(client.output())
        server_a_completed = metric_value(
            server_a_metrics,
            "xrpc_server_rpc_completed_total",
            {"service": args.rpc_service_name, "method": "Echo", "status_code": "ok"},
        )
        server_b_completed = metric_value(
            server_b_metrics,
            "xrpc_server_rpc_completed_total",
            {"service": args.rpc_service_name, "method": "Echo", "status_code": "ok"},
        )
        client_completed = metric_value(
            client_metrics,
            "xrpc_client_rpc_completed_total",
            {"service": args.rpc_service_name, "method": "Echo", "status_code": "ok"},
        )

        print(f"client_total={totals['total']} client_success={totals['success']} client_failed={totals['failed']}")
        print(f"client_completed_metric={client_completed:.0f}")
        print(f"server_a_completed_metric={server_a_completed:.0f}")
        print(f"server_b_completed_metric={server_b_completed:.0f}")

        if totals["success"] == 0:
            raise RuntimeError("client did not complete any successful RPC")
        if totals["failed"] > args.max_client_failures:
            raise RuntimeError(f"client failures exceeded threshold: {totals['failed']} > {args.max_client_failures}")
        if client_completed <= 0:
            raise RuntimeError("client metrics did not record successful RPCs")
        if server_a_completed <= 0:
            raise RuntimeError("server A metrics did not record successful RPCs")
        if server_b_completed <= 0:
            raise RuntimeError("server B metrics did not record successful RPCs after failover")

        print("result=PASS")
    finally:
        if client is not None and client.process.poll() is None:
            client.stop()
        if server_a is not None and server_a.process.poll() is None:
            server_a.stop()
        if server_b is not None and server_b.process.poll() is None:
            server_b.stop()


def parse_args():
    parser = argparse.ArgumentParser(description="Run a local Consul failover smoke test for XRPC.")
    parser.add_argument("--consul-address", default="127.0.0.1:8500")
    parser.add_argument("--prometheus-address", default="127.0.0.1:9090")
    parser.add_argument("--benchmark-build-dir", default="build")
    parser.add_argument("--discovery-service-name", default="")
    parser.add_argument("--rpc-service-name", default="BenchmarkService")
    parser.add_argument("--metrics-host", default="0.0.0.0")
    parser.add_argument("--client-metrics-port", type=int, default=19100)
    parser.add_argument("--server-a-metrics-port", type=int, default=19101)
    parser.add_argument("--server-b-metrics-port", type=int, default=19102)
    parser.add_argument("--consul-timeout-ms", type=int, default=1000)
    parser.add_argument("--discovery-refresh-interval-ms", type=int, default=300)
    parser.add_argument("--server-worker-threads", type=int, default=1)
    parser.add_argument("--server-connection-io-threads", type=int, default=1)
    parser.add_argument("--client-threads", type=int, default=2)
    parser.add_argument("--client-timeout-ms", type=int, default=1000)
    parser.add_argument("--duration-s", type=int, default=12)
    parser.add_argument("--payload-size", type=int, default=64)
    parser.add_argument("--before-add-s", type=float, default=2.0)
    parser.add_argument("--before-stop-s", type=float, default=2.0)
    parser.add_argument("--after-stop-s", type=float, default=2.0)
    parser.add_argument("--ready-timeout-s", type=float, default=10.0)
    parser.add_argument("--max-client-failures", type=int, default=20)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate arguments and print the smoke plan without starting processes",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    try:
        validate_args(args)
        if args.dry_run:
            print_dry_run(args)
            return 0
        run_smoke(args)
        return 0
    except ValueError as error:
        print(f"result=INVALID reason={error}", file=sys.stderr)
        return 2
    except Exception as error:
        print(f"result=FAIL reason={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
