#!/usr/bin/env python3

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path


def analyze(path):
    samples = defaultdict(list)
    with path.open(newline="", encoding="utf-8") as file:
        for row in csv.DictReader(file):
            key = (row["role"], int(row["pid"]), int(row["tid"]))
            samples[key].append(
                {
                    "timestamp_ns": int(row["timestamp_ns"]),
                    "comm": row["comm"],
                    "runtime_ns": int(row["runtime_ns"]),
                    "runqueue_wait_ns": int(row["runqueue_wait_ns"]),
                    "timeslices": int(row["timeslices"]),
                    "voluntary_context_switches": int(row["voluntary_context_switches"]),
                    "nonvoluntary_context_switches": int(row["nonvoluntary_context_switches"]),
                }
            )

    threads = []
    for (role, pid, tid), thread_samples in samples.items():
        thread_samples.sort(key=lambda item: item["timestamp_ns"])
        first = thread_samples[0]
        last = thread_samples[-1]
        elapsed_ns = last["timestamp_ns"] - first["timestamp_ns"]
        if elapsed_ns <= 0:
            continue
        runtime_ns = max(0, last["runtime_ns"] - first["runtime_ns"])
        wait_ns = max(0, last["runqueue_wait_ns"] - first["runqueue_wait_ns"])
        threads.append(
            {
                "role": role,
                "pid": pid,
                "tid": tid,
                "comm": last["comm"],
                "samples": len(thread_samples),
                "elapsed_ms": elapsed_ns / 1_000_000,
                "runtime_ms": runtime_ns / 1_000_000,
                "runqueue_wait_ms": wait_ns / 1_000_000,
                "cpu_pct_observed_lifetime": runtime_ns / elapsed_ns * 100,
                "runqueue_wait_pct_observed_lifetime": wait_ns / elapsed_ns * 100,
                "wait_to_runtime_pct": wait_ns / runtime_ns * 100 if runtime_ns else 0.0,
                "timeslices": max(0, last["timeslices"] - first["timeslices"]),
                "voluntary_context_switches": max(
                    0, last["voluntary_context_switches"] - first["voluntary_context_switches"]
                ),
                "nonvoluntary_context_switches": max(
                    0, last["nonvoluntary_context_switches"] - first["nonvoluntary_context_switches"]
                ),
            }
        )

    if not threads:
        raise RuntimeError(f"no complete thread samples in {path}")

    all_timestamps = [sample["timestamp_ns"] for thread_samples in samples.values() for sample in thread_samples]
    wall_ns = max(all_timestamps) - min(all_timestamps)
    groups = defaultdict(list)
    for thread in threads:
        groups[(thread["role"], thread["comm"])].append(thread)

    summaries = []
    for (role, comm), group in sorted(groups.items()):
        runtime_ms = sum(thread["runtime_ms"] for thread in group)
        wait_ms = sum(thread["runqueue_wait_ms"] for thread in group)
        summaries.append(
            {
                "role": role,
                "comm": comm,
                "thread_count": len(group),
                "runtime_ms": runtime_ms,
                "runqueue_wait_ms": wait_ms,
                "average_cpu_cores": runtime_ms * 1_000_000 / wall_ns if wall_ns else 0.0,
                "average_runnable_wait_cores": wait_ms * 1_000_000 / wall_ns if wall_ns else 0.0,
                "wait_to_runtime_pct": wait_ms / runtime_ms * 100 if runtime_ms else 0.0,
                "max_thread_cpu_pct": max(thread["cpu_pct_observed_lifetime"] for thread in group),
                "max_thread_runqueue_wait_pct": max(
                    thread["runqueue_wait_pct_observed_lifetime"] for thread in group
                ),
                "voluntary_context_switches": sum(thread["voluntary_context_switches"] for thread in group),
                "nonvoluntary_context_switches": sum(
                    thread["nonvoluntary_context_switches"] for thread in group
                ),
            }
        )

    return {
        "source": str(path),
        "wall_ms": wall_ns / 1_000_000,
        "threads": sorted(threads, key=lambda item: (item["role"], item["comm"], item["tid"])),
        "groups": summaries,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Analyze per-thread Linux schedstat samples.")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    result = analyze(args.input)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(f"wall_ms={result['wall_ms']:.2f} threads={len(result['threads'])}")
    for group in result["groups"]:
        print(
            f"{group['role']}/{group['comm']}: threads={group['thread_count']} "
            f"cpu_cores={group['average_cpu_cores']:.3f} "
            f"runqueue_wait_cores={group['average_runnable_wait_cores']:.3f} "
            f"max_thread_cpu_pct={group['max_thread_cpu_pct']:.1f}"
        )


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as error:
        raise SystemExit(str(error))
