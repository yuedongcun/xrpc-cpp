#!/usr/bin/env python3

import argparse
import json
import re
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path


READY_PORT_PATTERN = re.compile(r"\bport=(\d+)\b")
TOTAL_PATTERN = re.compile(r"total_calls=(\d+) success=(\d+) failed=(\d+)")

SCENARIOS = {
    "redundant-server-kill",
    "resolver-empty",
    "delayed-registration",
}


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


def consul_deregister(consul_address, service_id):
    if not service_id:
        return
    fetch_text(f"http://{consul_address}/v1/agent/service/deregister/{service_id}", method="PUT")


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
    if args.scenario not in SCENARIOS:
        raise ValueError(f"--scenario must be one of: {', '.join(sorted(SCENARIOS))}")
    if not args.consul_address:
        raise ValueError("--consul-address must not be empty")
    if not args.discovery_service_name and args.no_unique_service_name:
        raise ValueError("--discovery-service-name is required when --no-unique-service-name is set")

    validate_positive("--consul-timeout-ms", args.consul_timeout_ms)
    validate_positive("--discovery-refresh-interval-ms", args.discovery_refresh_interval_ms)
    validate_positive("--server-worker-threads", args.server_worker_threads)
    validate_positive("--server-connection-io-threads", args.server_connection_io_threads)
    validate_positive("--client-threads", args.client_threads)
    validate_positive("--client-timeout-ms", args.client_timeout_ms)
    validate_positive("--duration-s", args.duration_s)
    validate_positive("--payload-size", args.payload_size)
    validate_positive("--ready-timeout-s", args.ready_timeout_s)
    validate_non_negative("--fault-after-s", args.fault_after_s)
    validate_non_negative("--replacement-delay-s", args.replacement_delay_s)
    validate_non_negative("--max-client-failures", args.max_client_failures)
    validate_non_negative("--min-client-failures", args.min_client_failures)

    if args.duration_s <= args.fault_after_s + 1.0:
        raise ValueError("--duration-s must leave at least one second after --fault-after-s")
    if args.scenario == "delayed-registration" and args.duration_s <= args.fault_after_s + args.replacement_delay_s + 1.0:
        raise ValueError("--duration-s must leave at least one second after replacement registration")


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
    service_name = args.discovery_service_name or "<generated>"
    print("dry_run=1")
    print(f"scenario={args.scenario}")
    print(f"consul_address={args.consul_address}")
    print(f"discovery_service_name={service_name}")
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
    print(
        "fault="
        f"after_s:{args.fault_after_s},"
        f"replacement_delay_s:{args.replacement_delay_s},"
        f"max_client_failures:{args.max_client_failures},"
        f"min_client_failures:{args.min_client_failures}"
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
            self.kill()
        self._read_remaining_output()

    def kill(self):
        if self.process.poll() is not None:
            return
        self.process.kill()
        self.process.wait(timeout=5)
        self._read_remaining_output()

    def _read_remaining_output(self):
        if self.process.stdout is not None:
            remaining = self.process.stdout.read()
            if remaining:
                self.output_lines.extend(remaining.splitlines())

    def output(self):
        return "\n".join(self.output_lines)


class StartedServer:
    def __init__(self, label, service_id, process):
        self.label = label
        self.service_id = service_id
        self.process = process


def start_server(args, server_binary, label):
    service_id = f"{args.discovery_service_name}-{label}-{int(time.time() * 1000)}"
    command = [
        str(server_binary),
        "--host=127.0.0.1",
        "--port=0",
        "--workload=protobuf",
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
    print(f"server_{label}_started port={port} service_id={service_id}")
    return StartedServer(label, service_id, process)


def start_client(args, client_binary):
    command = [
        str(client_binary),
        f"--target=consul://{args.discovery_service_name}",
        f"--consul_address={args.consul_address}",
        f"--discovery_refresh_interval_ms={args.discovery_refresh_interval_ms}",
        f"--threads={args.client_threads}",
        f"--duration_s={args.duration_s}",
        f"--payload_size={args.payload_size}",
        f"--timeout_ms={args.client_timeout_ms}",
        "--workload=protobuf",
    ]
    print("client_start command=" + command_text(command))
    return ManagedProcess("client", command)


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


def validate_client_totals(args, totals):
    print(f"client_total={totals['total']} client_success={totals['success']} client_failed={totals['failed']}")
    if totals["total"] == 0:
        raise RuntimeError("client did not run any RPC")
    if totals["success"] == 0:
        raise RuntimeError("client did not complete any successful RPC")
    if totals["failed"] > args.max_client_failures:
        raise RuntimeError(f"client failures exceeded threshold: {totals['failed']} > {args.max_client_failures}")
    if totals["failed"] < args.min_client_failures:
        raise RuntimeError(f"client failures below expected threshold: {totals['failed']} < {args.min_client_failures}")


def cleanup_servers(args, servers):
    for server in reversed(servers):
        if server.process.process.poll() is None:
            server.process.stop()
        try:
            consul_deregister(args.consul_address, server.service_id)
        except Exception as error:
            print(f"cleanup_warning service_id={server.service_id} error={error}", file=sys.stderr)


def finish_client(args, client):
    client_return = client.communicate(args.duration_s + args.ready_timeout_s)
    if client_return != 0:
        raise RuntimeError(f"client exited with {client_return}:\n{client.output()}")
    totals = parse_client_totals(client.output())
    validate_client_totals(args, totals)


def run_redundant_server_kill(args, server_binary, client_binary):
    servers = []
    client = None
    try:
        server_a = start_server(args, server_binary, "a")
        servers.append(server_a)
        wait_consul_instance_count(args.consul_address, args.discovery_service_name, 1, args.ready_timeout_s)

        server_b = start_server(args, server_binary, "b")
        servers.append(server_b)
        wait_consul_instance_count(args.consul_address, args.discovery_service_name, 2, args.ready_timeout_s)

        client = start_client(args, client_binary)
        time.sleep(args.fault_after_s)
        print(f"inject_fault scenario={args.scenario} action=kill target=server_a service_id={server_a.service_id}")
        server_a.process.kill()

        finish_client(args, client)
        print("result=PASS")
    finally:
        if client is not None and client.process.poll() is None:
            client.stop()
        cleanup_servers(args, servers)


def run_resolver_empty(args, server_binary, client_binary):
    servers = []
    client = None
    try:
        server_a = start_server(args, server_binary, "a")
        servers.append(server_a)
        wait_consul_instance_count(args.consul_address, args.discovery_service_name, 1, args.ready_timeout_s)

        client = start_client(args, client_binary)
        time.sleep(args.fault_after_s)
        print(f"inject_fault scenario={args.scenario} action=stop target=server_a service_id={server_a.service_id}")
        server_a.process.stop()
        consul_deregister(args.consul_address, server_a.service_id)
        wait_consul_instance_count(args.consul_address, args.discovery_service_name, 0, args.ready_timeout_s)

        finish_client(args, client)
        print("result=PASS")
    finally:
        if client is not None and client.process.poll() is None:
            client.stop()
        cleanup_servers(args, servers)


def run_delayed_registration(args, server_binary, client_binary):
    servers = []
    client = None
    try:
        server_a = start_server(args, server_binary, "a")
        servers.append(server_a)
        wait_consul_instance_count(args.consul_address, args.discovery_service_name, 1, args.ready_timeout_s)

        client = start_client(args, client_binary)
        time.sleep(args.fault_after_s)
        print(f"inject_fault scenario={args.scenario} action=stop target=server_a service_id={server_a.service_id}")
        server_a.process.stop()
        consul_deregister(args.consul_address, server_a.service_id)
        wait_consul_instance_count(args.consul_address, args.discovery_service_name, 0, args.ready_timeout_s)

        print(f"replacement_wait_s={args.replacement_delay_s}")
        time.sleep(args.replacement_delay_s)
        server_b = start_server(args, server_binary, "b")
        servers.append(server_b)
        wait_consul_instance_count(args.consul_address, args.discovery_service_name, 1, args.ready_timeout_s)

        finish_client(args, client)
        print("result=PASS")
    finally:
        if client is not None and client.process.poll() is None:
            client.stop()
        cleanup_servers(args, servers)


def run_scenario(args):
    root = repo_root()
    server_binary, client_binary = require_benchmark_binaries(root, args.benchmark_build_dir)
    fetch_text(f"http://{args.consul_address}/v1/status/leader", args.ready_timeout_s)
    if not args.discovery_service_name:
        args.discovery_service_name = f"xrpc-fault-{args.scenario}-{int(time.time() * 1000)}"
    print(f"scenario={args.scenario} discovery_service_name={args.discovery_service_name}")

    if args.scenario == "redundant-server-kill":
        run_redundant_server_kill(args, server_binary, client_binary)
    elif args.scenario == "resolver-empty":
        run_resolver_empty(args, server_binary, client_binary)
    elif args.scenario == "delayed-registration":
        run_delayed_registration(args, server_binary, client_binary)
    else:
        raise ValueError(f"unsupported scenario: {args.scenario}")


def parse_args():
    parser = argparse.ArgumentParser(description="Run local Consul fault injection scenarios for XRPC.")
    parser.add_argument("--scenario", default="redundant-server-kill", choices=sorted(SCENARIOS))
    parser.add_argument("--consul-address", default="127.0.0.1:8500")
    parser.add_argument("--benchmark-build-dir", default="build")
    parser.add_argument("--discovery-service-name", default="")
    parser.add_argument(
        "--no-unique-service-name",
        action="store_true",
        help="require and reuse --discovery-service-name instead of generating a unique test service",
    )
    parser.add_argument("--consul-timeout-ms", type=int, default=1000)
    parser.add_argument("--discovery-refresh-interval-ms", type=int, default=300)
    parser.add_argument("--server-worker-threads", type=int, default=1)
    parser.add_argument("--server-connection-io-threads", type=int, default=1)
    parser.add_argument("--client-threads", type=int, default=2)
    parser.add_argument("--client-timeout-ms", type=int, default=1000)
    parser.add_argument("--duration-s", type=int, default=10)
    parser.add_argument("--payload-size", type=int, default=64)
    parser.add_argument("--fault-after-s", type=float, default=2.0)
    parser.add_argument("--replacement-delay-s", type=float, default=2.0)
    parser.add_argument("--ready-timeout-s", type=float, default=10.0)
    parser.add_argument("--max-client-failures", type=int, default=200)
    parser.add_argument("--min-client-failures", type=int, default=0)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate arguments and print the fault injection plan without starting processes",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    try:
        validate_args(args)
        if args.dry_run:
            print_dry_run(args)
            return 0
        run_scenario(args)
        return 0
    except ValueError as error:
        print(f"result=INVALID reason={error}", file=sys.stderr)
        return 2
    except Exception as error:
        print(f"result=FAIL reason={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
