"""Benchmark-only snapshot transport and interval aggregation."""

import json
import os
import signal
import time

def collect_io_snapshot(server, path, start_window=False):
    path.unlink(missing_ok=True)
    os.kill(server.pid, signal.SIGUSR2 if start_window else signal.SIGUSR1)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if path.exists():
            snapshot = json.loads(path.read_text())
            if "error" in snapshot:
                raise RuntimeError("I/O statistics: " + snapshot["error"])
            if snapshot.get("schema_version") != 3 or snapshot.get("scope") != "server_runtime":
                raise RuntimeError("unsupported I/O statistics schema")
            return snapshot
        if server.poll() is not None:
            raise RuntimeError("server exited while collecting I/O statistics")
        time.sleep(0.01)
    raise RuntimeError("I/O statistics request timed out")


def io_stats_interval(before, after, success):
    start = {loop["loop_id"]: loop for loop in before["loops"]}
    end = {loop["loop_id"]: loop for loop in after["loops"]}
    if not start or start.keys() != end.keys():
        raise RuntimeError("I/O loop set changed during measurement")
    loops = []
    totals = {}
    for loop_id in sorted(start):
        first, last = start[loop_id], end[loop_id]
        if first["window_id"] != last["window_id"]:
            raise RuntimeError("I/O peak window changed during measurement")
        if first["counters"].keys() != last["counters"].keys():
            raise RuntimeError("I/O counter set changed during measurement")
        delta = {key: value - first["counters"][key] for key, value in last["counters"].items()}
        if any(value < 0 for value in delta.values()):
            raise RuntimeError("I/O counters decreased during measurement")
        loops.append({"loop_id": loop_id, "counters": delta,
                      "window_id": last["window_id"], "peaks": last["peaks"],
                      "gauges_before": first["gauges"], "gauges_after": last["gauges"]})
        for key, value in delta.items():
            totals[key] = totals.get(key, 0) + value
    def ratio(numerator, denominator):
        return numerator / denominator if denominator else None
    return {"scope": "connection_io_loops", "before": before, "after": after,
            "worker_pool": worker_stats_interval(before["worker_pool"], after["worker_pool"]),
            "loops": loops, "counters": totals,
            "ratios": {
                "sqes_per_submit_call": ratio(totals["submitted_sqes"], totals["submit_calls"]),
                "cqes_per_prepared_recv": ratio(totals["recv_cqes"], totals["prepared_recv_sqes"]),
                "prepared_recv_sqes_per_1000_success": ratio(totals["prepared_recv_sqes"] * 1000, success),
                "submit_calls_per_1000_success": ratio(totals["submit_calls"] * 1000, success)}}


def worker_stats_interval(before, after):
    if before["window_id"] != after["window_id"]:
        raise RuntimeError("worker peak window changed during measurement")
    start = {queue["worker_id"]: queue for queue in before["queues"]}
    end = {queue["worker_id"]: queue for queue in after["queues"]}
    if not start or start.keys() != end.keys():
        raise RuntimeError("worker set changed during measurement")
    return {"window_id": after["window_id"],
            "pending_logical_jobs_before": before["pending_logical_jobs"],
            "pending_logical_jobs_after": after["pending_logical_jobs"],
            "queues": [{"worker_id": worker_id,
                        "gauges_before": start[worker_id]["gauges"],
                        "gauges_after": end[worker_id]["gauges"],
                        "peaks": end[worker_id]["peaks"]}
                       for worker_id in sorted(start)]}
