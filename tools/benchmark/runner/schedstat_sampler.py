#!/usr/bin/env python3

import csv
import threading
import time
from pathlib import Path


FIELDS = [
    "timestamp_ns",
    "role",
    "pid",
    "tid",
    "comm",
    "runtime_ns",
    "runqueue_wait_ns",
    "timeslices",
    "voluntary_context_switches",
    "nonvoluntary_context_switches",
]


def read_status_switches(path):
    voluntary = 0
    nonvoluntary = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("voluntary_ctxt_switches:"):
            voluntary = int(line.split(":", 1)[1])
        elif line.startswith("nonvoluntary_ctxt_switches:"):
            nonvoluntary = int(line.split(":", 1)[1])
    return voluntary, nonvoluntary


class SchedstatSampler:
    def __init__(self, output_path, interval_ms=50):
        if interval_ms < 10:
            raise ValueError("schedstat sampling interval must be at least 10 ms")
        self.output_path = Path(output_path)
        self.interval_seconds = interval_ms / 1000.0
        self.processes = {}
        self.rows = []
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="xrpc-schedstat", daemon=True)
        self.thread.start()

    def add_process(self, role, pid):
        with self.lock:
            self.processes[int(pid)] = str(role)
        self.sample_once()

    def sample_once(self):
        timestamp_ns = time.monotonic_ns()
        with self.lock:
            processes = list(self.processes.items())
        sampled = []
        for pid, role in processes:
            task_dir = Path("/proc") / str(pid) / "task"
            try:
                tids = list(task_dir.iterdir())
            except (FileNotFoundError, PermissionError):
                continue
            for tid_dir in tids:
                try:
                    runtime_ns, runqueue_wait_ns, timeslices = map(
                        int, (tid_dir / "schedstat").read_text(encoding="utf-8").split()[:3]
                    )
                    comm = (tid_dir / "comm").read_text(encoding="utf-8").strip()
                    voluntary, nonvoluntary = read_status_switches(tid_dir / "status")
                except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError):
                    continue
                sampled.append(
                    {
                        "timestamp_ns": timestamp_ns,
                        "role": role,
                        "pid": pid,
                        "tid": int(tid_dir.name),
                        "comm": comm,
                        "runtime_ns": runtime_ns,
                        "runqueue_wait_ns": runqueue_wait_ns,
                        "timeslices": timeslices,
                        "voluntary_context_switches": voluntary,
                        "nonvoluntary_context_switches": nonvoluntary,
                    }
                )
        with self.lock:
            self.rows.extend(sampled)

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=max(2.0, self.interval_seconds * 4))
        self.sample_once()
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with self.output_path.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(self.rows)

    def _run(self):
        while not self.stop_event.wait(self.interval_seconds):
            self.sample_once()
