#!/usr/bin/env python3

import argparse
import json
import math
import statistics
import struct
from collections import Counter, defaultdict
from pathlib import Path


RECORD = struct.Struct("<QQIHH")

STAGES = {
    1: "client_created",
    2: "client_sent",
    3: "client_recv",
    4: "client_complete",
    10: "server_recv",
    11: "server_decoded",
    12: "worker_enqueue",
    13: "worker_start",
    14: "request_start",
    15: "dispatch_end",
    16: "encode_end",
    17: "mailbox_submit",
    18: "mailbox_drain",
    19: "write_enqueue",
    20: "server_send",
}

INTERVALS = [
    ("client_prepare_send", 1, 2),
    ("request_transport_server_wakeup", 2, 10),
    ("server_decode", 10, 11),
    ("server_submit", 11, 12),
    ("worker_queue", 12, 13),
    ("batch_hol_before_request", 13, 14),
    ("handler_dispatch", 14, 15),
    ("response_frame_encode", 15, 16),
    ("batch_completion_hold", 16, 17),
    ("mailbox_return", 17, 18),
    ("write_enqueue", 18, 19),
    ("server_write_queue", 19, 20),
    ("response_transport_client_wakeup", 20, 3),
    ("client_decode_complete", 3, 4),
]

REQUIRED_STAGES = {1, 2, 3, 4, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}


def percentile(values, fraction):
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[int((len(ordered) - 1) * fraction)]


def distribution(values):
    if not values:
        return {"count": 0, "mean_us": 0.0, "p50_us": 0.0, "p95_us": 0.0, "p99_us": 0.0}
    micros = [value / 1000.0 for value in values]
    return {
        "count": len(values),
        "mean_us": statistics.fmean(micros),
        "p50_us": percentile(micros, 0.50),
        "p95_us": percentile(micros, 0.95),
        "p99_us": percentile(micros, 0.99),
    }


def value_distribution(values):
    if not values:
        return {"count": 0, "mean": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    return {
        "count": len(values),
        "mean": statistics.fmean(values),
        "p50": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "max": max(values),
    }


def pearson(xs, ys):
    if len(xs) < 2 or len(xs) != len(ys):
        return 0.0
    mean_x = statistics.fmean(xs)
    mean_y = statistics.fmean(ys)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    denominator_x = sum((x - mean_x) ** 2 for x in xs)
    denominator_y = sum((y - mean_y) ** 2 for y in ys)
    denominator = math.sqrt(denominator_x * denominator_y)
    return numerator / denominator if denominator else 0.0


def load_records(prefix):
    paths = sorted(prefix.parent.glob(prefix.name + ".pid*.tid*.bin"))
    traces = defaultdict(dict)
    values = defaultdict(dict)
    stage_counts = Counter()
    total_records = 0
    for path in paths:
        data = path.read_bytes()
        if len(data) % RECORD.size != 0:
            raise RuntimeError(f"trace file has truncated record: {path}")
        for offset in range(0, len(data), RECORD.size):
            request_id, timestamp_ns, value, stage, _ = RECORD.unpack_from(data, offset)
            stage_counts[stage] += 1
            total_records += 1
            existing = traces[request_id].get(stage)
            if existing is None or timestamp_ns < existing:
                traces[request_id][stage] = timestamp_ns
                values[request_id][stage] = value
    return paths, traces, values, stage_counts, total_records


def analyze(prefix):
    paths, traces, values, stage_counts, total_records = load_records(prefix)
    if not paths:
        raise RuntimeError(f"no trace files found for prefix: {prefix}")

    complete_ids = [request_id for request_id, stages in traces.items() if REQUIRED_STAGES <= set(stages)]
    complete_id_set = set(complete_ids)
    incomplete_ids = [request_id for request_id in traces if request_id not in complete_id_set]

    durations = {}
    e2e_ns = {}
    invalid_timeline_ids = []
    for request_id in complete_ids:
        stages = traces[request_id]
        request_durations = {}
        valid = True
        for name, begin, end in INTERVALS:
            elapsed = stages[end] - stages[begin]
            if elapsed < 0:
                valid = False
                break
            request_durations[name] = elapsed
        if not valid:
            invalid_timeline_ids.append(request_id)
            continue
        request_durations["e2e"] = stages[4] - stages[1]
        durations[request_id] = request_durations
        e2e_ns[request_id] = request_durations["e2e"]

    ordered_ids = sorted(durations, key=e2e_ns.get)
    slow_count = max(1, math.ceil(len(ordered_ids) * 0.01)) if ordered_ids else 0
    slow_ids = ordered_ids[-slow_count:] if slow_count else []

    all_distributions = {}
    slow_distributions = {}
    slow_mean_e2e = statistics.fmean(e2e_ns[request_id] for request_id in slow_ids) if slow_ids else 0.0
    for name, _, _ in INTERVALS:
        all_values = [durations[request_id][name] for request_id in ordered_ids]
        slow_values = [durations[request_id][name] for request_id in slow_ids]
        all_distributions[name] = distribution(all_values)
        slow_distribution = distribution(slow_values)
        slow_distribution["share_of_slow_e2e_pct"] = (
            statistics.fmean(slow_values) / slow_mean_e2e * 100.0 if slow_values and slow_mean_e2e else 0.0
        )
        slow_distributions[name] = slow_distribution

    batch_positions = []
    batch_hol = []
    total_e2e = []
    batch_sizes = []
    worker_depths = []
    mailbox_drain_sizes = []
    for request_id in ordered_ids:
        packed = values[request_id][14]
        batch_size = packed >> 16
        batch_position = packed & 0xFFFF
        batch_positions.append(batch_position)
        batch_sizes.append(batch_size)
        batch_hol.append(durations[request_id]["batch_hol_before_request"] / 1000.0)
        total_e2e.append(durations[request_id]["e2e"] / 1000.0)
        worker_depths.append(values[request_id][12])
        mailbox_drain_sizes.append(values[request_id][18])

    return {
        "trace_prefix": str(prefix),
        "trace_files": [str(path) for path in paths],
        "trace_file_count": len(paths),
        "record_count": total_records,
        "request_ids_seen": len(traces),
        "complete_request_count": len(ordered_ids),
        "incomplete_request_count": len(incomplete_ids),
        "invalid_timeline_count": len(invalid_timeline_ids),
        "stage_counts": {STAGES.get(stage, str(stage)): count for stage, count in sorted(stage_counts.items())},
        "e2e": distribution(list(e2e_ns.values())),
        "all_requests": all_distributions,
        "slowest_1pct": {
            "request_count": len(slow_ids),
            "e2e": distribution([e2e_ns[request_id] for request_id in slow_ids]),
            "stages": slow_distributions,
        },
        "batch": {
            "size": value_distribution(batch_sizes),
            "position": value_distribution(batch_positions),
            "position_vs_batch_hol_pearson": pearson(batch_positions, batch_hol),
            "position_vs_e2e_pearson": pearson(batch_positions, total_e2e),
        },
        "queues": {
            "worker_pending_jobs": value_distribution(worker_depths),
            "mailbox_drain_completions": value_distribution(mailbox_drain_sizes),
        },
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Analyze xRPC fixed-size sampled latency trace records.")
    parser.add_argument("--trace-prefix", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    result = analyze(args.trace_prefix)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(
        f"trace_files={result['trace_file_count']} records={result['record_count']} "
        f"complete_requests={result['complete_request_count']} "
        f"incomplete_requests={result['incomplete_request_count']}"
    )
    print(
        f"e2e_mean_us={result['e2e']['mean_us']:.2f} e2e_p99_us={result['e2e']['p99_us']:.2f} "
        f"batch_position_hol_r={result['batch']['position_vs_batch_hol_pearson']:.3f}"
    )


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as error:
        raise SystemExit(str(error))
