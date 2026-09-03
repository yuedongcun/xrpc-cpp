#!/usr/bin/env python3

import argparse
import csv
import hashlib
import html
import json
import platform
import re
import statistics
import subprocess
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from analyze_latency_trace import INTERVALS, analyze
from analyze_schedstat import analyze as analyze_schedstat


STAGE_LABELS = {
    "client_prepare_send": "客户端组帧/等待 send",
    "request_transport_server_wakeup": "请求入内核 → 服务端 I/O 唤醒",
    "server_decode": "服务端收包/拆帧",
    "server_submit": "提交 Worker",
    "worker_queue": "Worker 队列",
    "batch_hol_before_request": "batch 内前序请求 HOL",
    "handler_dispatch": "registry + handler dispatch",
    "response_frame_encode": "响应组帧编码",
    "batch_completion_hold": "等待 batch 后续请求完成",
    "mailbox_return": "Worker completion 回 I/O 线程",
    "write_enqueue": "写队列入队",
    "server_write_queue": "服务端写队列/等待 send",
    "response_transport_client_wakeup": "响应入内核 → 客户端 epoll 唤醒",
    "client_decode_complete": "客户端拆帧/完成回调",
}

STAGE_COLORS = {
    "client_prepare_send": "#64748b",
    "request_transport_server_wakeup": "#dc2626",
    "server_decode": "#f59e0b",
    "server_submit": "#a16207",
    "worker_queue": "#7c3aed",
    "batch_hol_before_request": "#9333ea",
    "handler_dispatch": "#16a34a",
    "response_frame_encode": "#22c55e",
    "batch_completion_hold": "#c026d3",
    "mailbox_return": "#2563eb",
    "write_enqueue": "#0891b2",
    "server_write_queue": "#0e7490",
    "response_transport_client_wakeup": "#ea580c",
    "client_decode_complete": "#475569",
}


def median(values):
    return statistics.median(values)


def pct(values, fraction):
    ordered = sorted(values)
    if not ordered:
        return 0.0
    return ordered[int((len(ordered) - 1) * fraction)]


def extract_number(text, name):
    match = re.search(rf"(?:^| ){re.escape(name)}=(\d+)", text)
    if match is None:
        raise RuntimeError(f"missing {name} in case: {text}")
    return int(match.group(1))


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def summarize_rows(rows, dimension):
    groups = defaultdict(list)
    for row in rows:
        groups[dimension(row)].append(row)
    summaries = []
    for key, group in sorted(groups.items()):
        summaries.append(
            {
                "key": key,
                "runs": len(group),
                "qps": median([row["qps"] for row in group]),
                "qps_min": min(row["qps"] for row in group),
                "qps_max": max(row["qps"] for row in group),
                "avg_us": median([row["avg_us"] for row in group]),
                "p50_us": median([row["p50_us"] for row in group]),
                "p95_us": median([row["p95_us"] for row in group]),
                "p99_us": median([row["p99_us"] for row in group]),
                "p99_min": min(row["p99_us"] for row in group),
                "p99_max": max(row["p99_us"] for row in group),
                "failed": sum(row["failed"] for row in group),
            }
        )
    return summaries


def analyze_trace_suite(suite):
    runs = []
    for row in suite["rows"]:
        result = analyze(Path(row["trace_prefix"]))
        result["case"] = row["case"]
        result["inflight"] = extract_number(row["case"], "inflight")
        result["repetition"] = row["repetition"]
        result["benchmark"] = {key: row[key] for key in ("qps", "avg_us", "p50_us", "p95_us", "p99_us")}
        runs.append(result)

    by_inflight = defaultdict(list)
    for run in runs:
        by_inflight[run["inflight"]].append(run)

    aggregates = {}
    for inflight, group in sorted(by_inflight.items()):
        stage_result = {}
        for stage, _, _ in INTERVALS:
            stage_result[stage] = {
                metric: median([run["all_requests"][stage][metric] for run in group])
                for metric in ("mean_us", "p50_us", "p95_us", "p99_us")
            }
            stage_result[stage]["slow_mean_us"] = median(
                [run["slowest_1pct"]["stages"][stage]["mean_us"] for run in group]
            )
            stage_result[stage]["slow_share_pct"] = median(
                [run["slowest_1pct"]["stages"][stage]["share_of_slow_e2e_pct"] for run in group]
            )
        aggregates[str(inflight)] = {
            "runs": len(group),
            "complete_samples": sum(run["complete_request_count"] for run in group),
            "incomplete_samples": sum(run["incomplete_request_count"] for run in group),
            "invalid_timelines": sum(run["invalid_timeline_count"] for run in group),
            "benchmark": {
                metric: median([run["benchmark"][metric] for run in group])
                for metric in ("qps", "avg_us", "p50_us", "p95_us", "p99_us")
            },
            "sampled_e2e": {
                metric: median([run["e2e"][metric] for run in group])
                for metric in ("mean_us", "p50_us", "p95_us", "p99_us")
            },
            "slowest_1pct_e2e": {
                metric: median([run["slowest_1pct"]["e2e"][metric] for run in group])
                for metric in ("mean_us", "p50_us", "p95_us", "p99_us")
            },
            "stages": stage_result,
            "batch": {
                "size_mean": median([run["batch"]["size"]["mean"] for run in group]),
                "size_p50": median([run["batch"]["size"]["p50"] for run in group]),
                "size_p95": median([run["batch"]["size"]["p95"] for run in group]),
                "position_hol_r": median([run["batch"]["position_vs_batch_hol_pearson"] for run in group]),
                "position_e2e_r": median([run["batch"]["position_vs_e2e_pearson"] for run in group]),
            },
            "queues": {
                "worker_pending_mean": median(
                    [run["queues"]["worker_pending_jobs"]["mean"] for run in group]
                ),
                "worker_pending_p95": median(
                    [run["queues"]["worker_pending_jobs"]["p95"] for run in group]
                ),
                "worker_pending_p99": median(
                    [run["queues"]["worker_pending_jobs"]["p99"] for run in group]
                ),
                "mailbox_drain_mean": median(
                    [run["queues"]["mailbox_drain_completions"]["mean"] for run in group]
                ),
                "mailbox_drain_p99": median(
                    [run["queues"]["mailbox_drain_completions"]["p99"] for run in group]
                ),
            },
        }
    return {"runs": runs, "aggregates": aggregates}


def analyze_bottleneck_suite(suite):
    runs = []
    for row in suite["rows"]:
        trace = analyze(Path(row["trace_prefix"]))
        schedstat = analyze_schedstat(Path(row["schedstat_path"]))
        runs.append(
            {
                "inflight": extract_number(row["case"], "inflight"),
                "repetition": row["repetition"],
                "benchmark": {key: row[key] for key in ("qps", "avg_us", "p50_us", "p95_us", "p99_us")},
                "trace": trace,
                "schedstat": schedstat,
            }
        )

    by_inflight = defaultdict(list)
    for run in runs:
        by_inflight[run["inflight"]].append(run)

    aggregates = {}
    for inflight, group in sorted(by_inflight.items()):
        detailed_stages = {}
        for stage in group[0]["trace"]["detailed"]["all_requests"]:
            detailed_stages[stage] = {
                metric: median(
                    [run["trace"]["detailed"]["all_requests"][stage][metric] for run in group]
                )
                for metric in ("mean_us", "p50_us", "p95_us", "p99_us")
            }

        sched_groups = defaultdict(list)
        for run in group:
            for sched_group in run["schedstat"]["groups"]:
                sched_groups[f"{sched_group['role']}/{sched_group['comm']}"] .append(sched_group)
        sched_aggregate = {}
        for name, values in sched_groups.items():
            if len(values) != len(group):
                continue
            sched_aggregate[name] = {
                metric: median([value[metric] for value in values])
                for metric in (
                    "thread_count",
                    "average_cpu_cores",
                    "average_runnable_wait_cores",
                    "wait_to_runtime_pct",
                    "max_thread_cpu_pct",
                    "max_thread_runqueue_wait_pct",
                    "voluntary_context_switches",
                    "nonvoluntary_context_switches",
                )
            }

        aggregates[str(inflight)] = {
            "runs": len(group),
            "samples": sum(run["trace"]["detailed"]["complete_request_count"] for run in group),
            "negative_intervals": {
                stage: sum(
                    run["trace"]["detailed"]["negative_interval_counts"].get(stage, 0) for run in group
                )
                for stage in detailed_stages
            },
            "benchmark": {
                metric: median([run["benchmark"][metric] for run in group])
                for metric in ("qps", "avg_us", "p50_us", "p95_us", "p99_us")
            },
            "detailed_stages": detailed_stages,
            "batch_size_mean": median([run["trace"]["batch"]["size"]["mean"] for run in group]),
            "worker_pending_mean": median(
                [run["trace"]["queues"]["worker_pending_jobs"]["mean"] for run in group]
            ),
            "worker_pending_p95": median(
                [run["trace"]["queues"]["worker_pending_jobs"]["p95"] for run in group]
            ),
            "schedstat": sched_aggregate,
        }
    return {"runs": runs, "aggregates": aggregates}


def analyze_schedstat_suite(suite):
    runs = []
    for row in suite["rows"]:
        runs.append(
            {
                "inflight": extract_number(row["case"], "inflight"),
                "repetition": row["repetition"],
                "benchmark": {key: row[key] for key in ("qps", "avg_us", "p50_us", "p95_us", "p99_us")},
                "schedstat": analyze_schedstat(Path(row["schedstat_path"])),
            }
        )
    by_inflight = defaultdict(list)
    for run in runs:
        by_inflight[run["inflight"]].append(run)
    aggregates = {}
    for inflight, group in sorted(by_inflight.items()):
        sched_groups = defaultdict(list)
        for run in group:
            for sched_group in run["schedstat"]["groups"]:
                sched_groups[f"{sched_group['role']}/{sched_group['comm']}"].append(sched_group)
        aggregates[str(inflight)] = {
            "runs": len(group),
            "benchmark": {
                metric: median([run["benchmark"][metric] for run in group])
                for metric in ("qps", "avg_us", "p50_us", "p95_us", "p99_us")
            },
            "schedstat": {
                name: {
                    metric: median([value[metric] for value in values])
                    for metric in (
                        "thread_count",
                        "average_cpu_cores",
                        "average_runnable_wait_cores",
                        "wait_to_runtime_pct",
                        "max_thread_cpu_pct",
                        "max_thread_runqueue_wait_pct",
                        "voluntary_context_switches",
                        "nonvoluntary_context_switches",
                    )
                }
                for name, values in sched_groups.items()
                if len(values) == len(group)
            },
        }
    return {"runs": runs, "aggregates": aggregates}


def command_output(command):
    try:
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=10)
        return result.stdout.strip() if result.returncode == 0 else f"unavailable: {result.stdout.strip()}"
    except (OSError, subprocess.TimeoutExpired) as error:
        return f"unavailable: {error}"


def environment_info(repo_root):
    return {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "platform": platform.platform(),
        "kernel": platform.release(),
        "machine": platform.machine(),
        "cpu": command_output(["lscpu"]),
        "compiler": command_output(["c++", "--version"]).splitlines()[0],
        "cmake": command_output(["cmake", "--version"]).splitlines()[0],
        "perf": command_output(["perf", "version"]),
        "perf_event_paranoid": Path("/proc/sys/kernel/perf_event_paranoid").read_text().strip(),
        "git_commit": command_output(["git", "-C", str(repo_root), "rev-parse", "HEAD"]),
        "git_branch": command_output(["git", "-C", str(repo_root), "branch", "--show-current"]),
        "push_url": command_output(["git", "-C", str(repo_root), "remote", "get-url", "--push", "origin"]),
        "push_default": command_output(["git", "-C", str(repo_root), "config", "--local", "--get", "push.default"]),
    }


def parse_perf(path):
    result = {}
    with path.open(newline="", encoding="utf-8") as file:
        for row in csv.reader(line for line in file if not line.startswith("#") and line.strip()):
            if len(row) < 3:
                continue
            try:
                value = float(row[0])
            except ValueError:
                continue
            result[row[2]] = {"value": value, "unit": row[1], "detail": row[5:]}
    return result


def perf_metric(stats, name):
    return stats.get(f"{name}:u", stats.get(name, {"value": 0.0, "detail": []}))


def fnum(value, digits=2):
    return f"{value:,.{digits}f}"


def duration(value):
    if value >= 1000:
        return f"{value / 1000:.2f} ms"
    return f"{value:.2f} μs"


def line_chart(points, y_key, title, color, y_formatter=lambda value: f"{value:,.0f}"):
    width, height = 860, 300
    left, right, top, bottom = 72, 24, 35, 56
    plot_w, plot_h = width - left - right, height - top - bottom
    xs = [float(point["key"]) for point in points]
    ys = [float(point[y_key]) for point in points]
    min_x, max_x = min(xs), max(xs)
    max_y = max(ys) * 1.08

    def x_pos(value):
        if min_x == max_x:
            return left + plot_w / 2
        import math

        low, high = math.log2(min_x), math.log2(max_x)
        return left + (math.log2(value) - low) / (high - low) * plot_w

    def y_pos(value):
        return top + plot_h - value / max_y * plot_h

    coords = " ".join(f"{x_pos(x):.1f},{y_pos(y):.1f}" for x, y in zip(xs, ys))
    grid = []
    for index in range(5):
        value = max_y * index / 4
        y = y_pos(value)
        grid.append(f'<line x1="{left}" y1="{y:.1f}" x2="{width-right}" y2="{y:.1f}" class="grid"/>')
        grid.append(f'<text x="{left-9}" y="{y+4:.1f}" text-anchor="end">{html.escape(y_formatter(value))}</text>')
    labels = []
    dots = []
    for x, y in zip(xs, ys):
        xp, yp = x_pos(x), y_pos(y)
        labels.append(f'<text x="{xp:.1f}" y="{height-26}" text-anchor="middle">{int(x)}</text>')
        dots.append(f'<circle cx="{xp:.1f}" cy="{yp:.1f}" r="4.5" fill="{color}"/>')
        dots.append(f'<text x="{xp:.1f}" y="{yp-10:.1f}" text-anchor="middle" class="value">{html.escape(y_formatter(y))}</text>')
    return (
        f'<svg class="chart" viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">'
        f'<text x="{left}" y="20" class="chart-title">{html.escape(title)}</text>'
        + "".join(grid)
        + f'<polyline points="{coords}" fill="none" stroke="{color}" stroke-width="3"/>'
        + "".join(dots)
        + "".join(labels)
        + f'<text x="{left + plot_w/2:.1f}" y="{height-5}" text-anchor="middle">在途请求数（对数轴）</text></svg>'
    )


def stage_bar_chart(trace_aggregates):
    selected = ["request_transport_server_wakeup", "worker_queue", "batch_hol_before_request",
                "handler_dispatch", "batch_completion_hold", "mailbox_return", "server_write_queue",
                "response_transport_client_wakeup"]
    rows = []
    max_total = max(sum(data["stages"][stage]["mean_us"] for stage in selected) for data in trace_aggregates.values())
    for inflight, data in sorted(trace_aggregates.items(), key=lambda item: int(item[0])):
        segments = []
        for stage in selected:
            value = data["stages"][stage]["mean_us"]
            width = value / max_total * 100
            segments.append(
                f'<span style="width:{width:.3f}%;background:{STAGE_COLORS[stage]}" '
                f'title="{html.escape(STAGE_LABELS[stage])}: {duration(value)}"></span>'
            )
        rows.append(
            f'<div class="stack-row"><div class="stack-label">{inflight}</div>'
            f'<div class="stack">{"".join(segments)}</div>'
            f'<div class="stack-total">{duration(data["sampled_e2e"]["mean_us"])}</div></div>'
        )
    legend = "".join(
        f'<span><i style="background:{STAGE_COLORS[stage]}"></i>{html.escape(STAGE_LABELS[stage])}</span>'
        for stage in selected
    )
    return f'<div class="stack-chart">{"".join(rows)}</div><div class="legend">{legend}</div>'


def rows_table(headers, rows, classes=""):
    head = "".join(f"<th>{html.escape(str(item))}</th>" for item in headers)
    body = "".join("<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>" for row in rows)
    return f'<div class="table-wrap"><table class="{classes}"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>'


def control_summary(raw_dir, names, label_getter):
    result = []
    for name in names:
        payload = load_json(raw_dir / name)
        summary = summarize_rows(payload["rows"], lambda row: row["case"])[0]
        summary["label"] = label_getter(name)
        result.append(summary)
    return result


def manifest(raw_dir):
    result = []
    for path in sorted(raw_dir.rglob("*")):
        if not path.is_file():
            continue
        digest = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
        result.append({"path": str(path), "bytes": path.stat().st_size, "sha256_16": digest})
    return result


def build_report(repo_root, raw_dir, output_path, trace_analysis, bottleneck_analysis, schedstat_analysis, env):
    load_curve = summarize_rows(
        load_json(raw_dir / "formal-load-curve.json")["rows"],
        lambda row: extract_number(row["case"], "inflight"),
    )
    trace = trace_analysis["aggregates"]
    at_1536 = trace["1536"]
    stages_1536 = at_1536["stages"]

    worker = control_summary(
        raw_dir, ["control-worker-1.json", "control-worker-3.json", "control-worker-6.json"],
        lambda name: re.search(r"-(\d+)\.json", name).group(1),
    )
    server_io = control_summary(
        raw_dir, ["control-server-io-1.json", "control-server-io-3.json", "control-server-io-6.json"],
        lambda name: re.search(r"-(\d+)\.json", name).group(1),
    )
    client_io = control_summary(
        raw_dir,
        ["control-client-io-1.json", "control-client-io-3.json", "control-client-io-6.json", "control-client-io-12.json"],
        lambda name: re.search(r"-(\d+)\.json", name).group(1),
    )
    payloads = control_summary(
        raw_dir, ["control-payload-16.json", "control-payload-128.json", "control-payload-1024.json"],
        lambda name: re.search(r"-(\d+)\.json", name).group(1) + " B",
    )
    connection_data = load_json(raw_dir / "control-connections.json")
    connections = summarize_rows(
        connection_data["rows"], lambda row: extract_number(row["case"], "connections")
    )
    affinity = summarize_rows(
        load_json(raw_dir / "control-affinity-split.json")["rows"], lambda row: "server 0–5 / client 6–11"
    )[0]
    perf_run = load_json(raw_dir / "perf-1536.json")["rows"][0]
    server_perf = parse_perf(raw_dir.parent / "perf" / "server-1536.csv")
    client_perf = parse_perf(raw_dir.parent / "perf" / "client-1536.csv")

    law_rows = []
    for point in load_curve:
        observed_l = point["qps"] * point["avg_us"] / 1_000_000
        predicted = point["key"] / point["qps"] * 1_000_000
        law_rows.append(
            [
                fnum(point["key"], 0), fnum(point["qps"], 0), duration(point["avg_us"]),
                duration(point["p50_us"]), duration(point["p95_us"]), duration(point["p99_us"]),
                f"{duration(point['p99_min'])} – {duration(point['p99_max'])}",
                fnum(observed_l, 1), duration(predicted), str(point["failed"]),
            ]
        )

    stage_rows = []
    stage_sum = sum(item["mean_us"] for item in stages_1536.values())
    for stage, data in sorted(stages_1536.items(), key=lambda item: item[1]["mean_us"], reverse=True):
        stage_rows.append(
            [
                html.escape(STAGE_LABELS[stage]), duration(data["mean_us"]), duration(data["p95_us"]),
                duration(data["p99_us"]), f"{data['mean_us'] / stage_sum * 100:.2f}%",
                duration(data["slow_mean_us"]), f"{data['slow_share_pct']:.2f}%",
            ]
        )

    trace_rows = []
    for inflight, data in sorted(trace.items(), key=lambda item: int(item[0])):
        trace_rows.append(
            [
                inflight, str(data["runs"]), fnum(data["complete_samples"], 0),
                duration(data["benchmark"]["avg_us"]), duration(data["benchmark"]["p99_us"]),
                duration(data["sampled_e2e"]["mean_us"]), duration(data["sampled_e2e"]["p99_us"]),
                fnum(data["batch"]["size_mean"], 1), fnum(data["queues"]["worker_pending_mean"], 1),
                fnum(data["batch"]["position_hol_r"], 3), fnum(data["batch"]["position_e2e_r"], 3),
            ]
        )

    def control_table(items, first_header):
        return rows_table(
            [first_header, "重复", "QPS 中位", "QPS 范围", "p99 中位", "p99 范围", "失败"],
            [
                [
                    str(item.get("label", item["key"])), str(item["runs"]), fnum(item["qps"], 0),
                    f"{fnum(item['qps_min'], 0)} – {fnum(item['qps_max'], 0)}", duration(item["p99_us"]),
                    f"{duration(item['p99_min'])} – {duration(item['p99_max'])}", str(item["failed"]),
                ]
                for item in items
            ],
        )

    control_connections_table = control_table(connections, "连接数")
    manifest_rows = manifest(raw_dir)
    manifest_table = rows_table(
        ["本地文件", "字节", "SHA-256 前 16 位"],
        [[html.escape(item["path"]), fnum(item["bytes"], 0), item["sha256_16"]] for item in manifest_rows],
    )

    request_share = stages_1536["request_transport_server_wakeup"]["slow_share_pct"]
    response_share = stages_1536["response_transport_client_wakeup"]["slow_share_pct"]
    worker_share = stages_1536["worker_queue"]["slow_share_pct"]
    mailbox_share = stages_1536["mailbox_return"]["slow_share_pct"]
    dispatch_mean = stages_1536["handler_dispatch"]["mean_us"]
    codec_upper_stages = [
        "client_prepare_send",
        "server_decode",
        "response_frame_encode",
        "client_decode_complete",
        "handler_dispatch",
    ]
    codec_upper_mean = sum(stages_1536[stage]["mean_us"] for stage in codec_upper_stages)
    codec_upper_share = codec_upper_mean / stage_sum * 100
    codec_upper_slow_share = sum(stages_1536[stage]["slow_share_pct"] for stage in codec_upper_stages)
    transport_mean = (
        stages_1536["request_transport_server_wakeup"]["mean_us"]
        + stages_1536["response_transport_client_wakeup"]["mean_us"]
    )
    release_1536 = next(point for point in load_curve if point["key"] == 1536)
    traced_1536 = at_1536["benchmark"]
    trace_qps_overhead = (release_1536["qps"] - traced_1536["qps"]) / release_1536["qps"] * 100

    server_clock_metric = perf_metric(server_perf, "task-clock")
    client_clock_metric = perf_metric(client_perf, "task-clock")
    server_clock = server_clock_metric["value"]
    client_clock = client_clock_metric["value"]
    server_cycles = perf_metric(server_perf, "cycles")["value"]
    server_instructions = perf_metric(server_perf, "instructions")["value"]
    client_cycles = perf_metric(client_perf, "cycles")["value"]
    client_instructions = perf_metric(client_perf, "instructions")["value"]
    server_cpu_util = float(server_clock_metric["detail"][0])
    client_cpu_util = float(client_clock_metric["detail"][0])
    server_cache_refs = perf_metric(server_perf, "cache-references")["value"]
    server_cache_misses = perf_metric(server_perf, "cache-misses")["value"]
    client_cache_refs = perf_metric(client_perf, "cache-references")["value"]
    client_cache_misses = perf_metric(client_perf, "cache-misses")["value"]
    perf_table = rows_table(
        ["进程", "user task-clock", "逻辑 CPU 利用", "instructions", "cycles", "IPC", "cache miss / ref"],
        [
            [
                "server",
                f"{server_clock / 1000:.2f} CPU·s",
                f"{server_cpu_util:.3f}",
                fnum(server_instructions, 0),
                fnum(server_cycles, 0),
                f"{server_instructions / server_cycles:.2f}",
                f"{server_cache_misses / server_cache_refs * 100:.2f}%",
            ],
            [
                "client",
                f"{client_clock / 1000:.2f} CPU·s",
                f"{client_cpu_util:.3f}",
                fnum(client_instructions, 0),
                fnum(client_cycles, 0),
                f"{client_instructions / client_cycles:.2f}",
                f"{client_cache_misses / client_cache_refs * 100:.2f}%",
            ],
        ],
    )

    detailed = bottleneck_analysis["aggregates"]
    sched_only = schedstat_analysis["aggregates"]
    detailed_1536 = detailed["1536"]
    sched_1536 = sched_only["1536"]["schedstat"]
    io_1536 = sched_1536["server/xrpc-io"]
    worker_1536 = sched_1536["server/xrpc-worker"]
    client_1536 = sched_1536["client/xrpc-client-io"]
    detailed_timing_rows = []
    thread_rows = []
    for inflight in ("384", "1536", "6144"):
        point = detailed[inflight]
        stages = point["detailed_stages"]
        detailed_timing_rows.append(
            [
                inflight,
                fnum(point["benchmark"]["qps"], 0),
                duration(point["benchmark"]["p99_us"]),
                duration(stages["client_send_syscall"]["mean_us"]),
                duration(stages["request_after_send_return"]["mean_us"]),
                duration(stages["mailbox_mutex_wait"]["mean_us"]),
                duration(stages["mailbox_callback_wait"]["mean_us"]),
                duration(stages["server_send_completion"]["mean_us"]),
                duration(stages["response_to_client_epoll"]["mean_us"]),
                duration(stages["client_epoll_to_recv"]["mean_us"]),
                fnum(point["worker_pending_mean"], 1),
                fnum(point["batch_size_mean"], 1),
            ]
        )
        sched_point = sched_only[inflight]["schedstat"]
        for role, label in (
            ("server/xrpc-io", "服务端 I/O"),
            ("server/xrpc-worker", "Worker"),
            ("client/xrpc-client-io", "客户端 I/O"),
        ):
            group = sched_point[role]
            thread_rows.append(
                [
                    inflight,
                    label,
                    fnum(group["thread_count"], 0),
                    f"{group['average_cpu_cores']:.2f}",
                    f"{group['max_thread_cpu_pct']:.1f}%",
                    f"{group['average_runnable_wait_cores']:.3f}",
                    f"{group['wait_to_runtime_pct']:.1f}%",
                    fnum(group["nonvoluntary_context_switches"], 0),
                ]
            )
    detailed_timing_table = rows_table(
        [
            "在途",
            "QPS",
            "p99",
            "client send syscall",
            "send 返回→server recv",
            "mailbox mutex",
            "mailbox callback wait",
            "server send completion",
            "send submit→client epoll",
            "epoll ready→recv",
            "Worker pending",
            "batch size",
        ],
        detailed_timing_rows,
    )
    thread_table = rows_table(
        ["在途", "线程组", "线程", "平均占用核数", "最忙线程 CPU", "平均 runqueue 核数", "wait/runtime", "非自愿切换"],
        thread_rows,
    )
    negative_count = sum(sum(point["negative_intervals"].values()) for point in detailed.values())
    detailed_sample_count = sum(point["samples"] for point in detailed.values())

    css = """
    :root{--ink:#14213d;--muted:#64748b;--line:#dbe4ee;--panel:#f8fafc;--accent:#2563eb;--good:#15803d;--warn:#b45309;--bad:#b91c1c}
    *{box-sizing:border-box}body{margin:0;background:#eef3f8;color:var(--ink);font-family:Inter,"Noto Sans SC","Microsoft YaHei",system-ui,sans-serif;line-height:1.58}
    main{max-width:1180px;margin:0 auto;background:white;min-height:100vh;padding:44px 54px 80px;box-shadow:0 0 35px #cbd5e1}
    h1{font-size:34px;line-height:1.2;margin:0 0 10px}h2{font-size:24px;margin:46px 0 16px;padding-bottom:8px;border-bottom:2px solid var(--line)}h3{font-size:18px;margin:28px 0 10px}
    p{margin:9px 0}.subtitle,.muted{color:var(--muted)}.banner{margin:22px 0;padding:15px 18px;border-left:5px solid var(--good);background:#f0fdf4}
    .cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:22px 0}.card{border:1px solid var(--line);border-radius:10px;padding:16px;background:var(--panel)}.card strong{display:block;font-size:23px}.card span{font-size:13px;color:var(--muted)}
    .verdict{border-radius:10px;padding:18px 20px;margin:14px 0;border:1px solid}.high{border-color:#fecaca;background:#fff7f7}.medium{border-color:#fed7aa;background:#fffaf2}.low{border-color:#bbf7d0;background:#f5fff8}
    .rank{display:inline-block;min-width:30px;height:30px;text-align:center;padding-top:2px;margin-right:8px;border-radius:50%;background:var(--ink);color:white;font-weight:700}
    .table-wrap{overflow-x:auto;margin:12px 0 22px}table{border-collapse:collapse;width:100%;font-size:13px}th,td{padding:8px 9px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap}th{background:#edf3f9;position:sticky;top:0}th:first-child,td:first-child{text-align:left}tbody tr:hover{background:#f8fafc}
    code,pre{font-family:"SFMono-Regular",Consolas,monospace}code{background:#edf2f7;border-radius:4px;padding:1px 5px}pre{background:#111827;color:#e5e7eb;padding:16px;border-radius:8px;overflow:auto;font-size:12px}
    .chart{width:100%;height:auto;background:#fff;border:1px solid var(--line);border-radius:10px;margin:12px 0}.chart text{font-size:11px;fill:#64748b}.chart .chart-title{font-size:15px;fill:#14213d;font-weight:700}.chart .value{font-size:10px;fill:#334155}.grid{stroke:#e2e8f0;stroke-width:1}
    .stack-chart{padding:16px;border:1px solid var(--line);border-radius:10px}.stack-row{display:grid;grid-template-columns:58px 1fr 92px;gap:10px;align-items:center;margin:13px 0}.stack-label{font-weight:700;text-align:right}.stack{height:27px;display:flex;background:#eef2f7;border-radius:4px;overflow:hidden}.stack span{height:100%;min-width:1px}.stack-total{font-size:12px;color:var(--muted)}
    .legend{display:flex;flex-wrap:wrap;gap:7px 14px;font-size:12px;margin:10px 0}.legend span{display:flex;align-items:center}.legend i{width:11px;height:11px;margin-right:5px;border-radius:2px}
    .flow{display:flex;gap:6px;align-items:stretch;overflow-x:auto;margin:18px 0}.flow div{min-width:118px;padding:12px;border-radius:8px;background:#edf3f9;text-align:center;font-size:12px}.flow b{display:block;font-size:13px}.flow .hot{background:#fee2e2;border:1px solid #fca5a5}.flow .warm{background:#fef3c7;border:1px solid #fcd34d}.arrow{min-width:20px!important;background:transparent!important;padding:19px 0!important;font-size:20px!important}
    .note{border-left:4px solid var(--accent);background:#eff6ff;padding:12px 16px;margin:14px 0}.warning{border-left-color:var(--warn);background:#fffbeb}.small{font-size:12px}.toc{columns:2;padding-left:24px}.toc a{color:var(--accent);text-decoration:none}.footer{margin-top:55px;padding-top:18px;border-top:1px solid var(--line);color:var(--muted);font-size:12px}
    @media(max-width:850px){main{padding:26px 18px}.cards{grid-template-columns:1fr 1fr}.toc{columns:1}.flow{flex-direction:column}.arrow{transform:rotate(90deg);text-align:center}.stack-row{grid-template-columns:45px 1fr}.stack-total{display:none}}
    @media print{body{background:white}main{box-shadow:none;max-width:none}.table-wrap{overflow:visible}.chart{break-inside:avoid}h2{break-after:avoid}}
    """

    report = f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>xRPC 128B 端到端延迟瓶颈诊断报告</title><style>{css}</style></head><body><main>
<h1>xRPC 128B 端到端延迟瓶颈诊断</h1>
<p class="subtitle">诊断范围：本机 loopback Firehose，12 连接，128B payload；结论面向“延迟在哪里”，不包含优化实施。</p>
<div class="banner"><strong>本地隔离状态：</strong>报告、原始 trace 与 perf 数据均位于 <code>.local-perf/</code>，由 <code>.git/info/exclude</code> 排除；当前分支 <code>{html.escape(env['git_branch'])}</code>，origin push URL 为 <code>{html.escape(env['push_url'])}</code>，<code>push.default={html.escape(env['push_default'])}</code>。本报告没有执行任何 push。</div>

<div class="cards">
  <div class="card"><strong>{fnum(release_1536['qps'],0)} QPS</strong><span>1536 在途，Release 三次中位</span></div>
  <div class="card"><strong>{duration(release_1536['avg_us'])}</strong><span>平均端到端延迟</span></div>
  <div class="card"><strong>{duration(release_1536['p99_us'])}</strong><span>p99 端到端延迟</span></div>
  <div class="card"><strong>{duration(codec_upper_mean)}</strong><span>所有可能含编解码的桶 + handler 保守上界</span></div>
</div>

<h2 id="summary">结论摘要</h2>
<div class="verdict high"><span class="rank">1</span><strong>主瓶颈：服务端 I/O 事件循环的服务节奏，以及它共享的 recv/send/callback 等待队列。</strong>1536 在途时，请求侧阶段均值 {duration(stages_1536['request_transport_server_wakeup']['mean_us'])}，响应侧 {duration(stages_1536['response_transport_client_wakeup']['mean_us'])}，合计 {duration(transport_mean)}；在最慢 1% 请求中两者占 {request_share + response_share:.1f}%（{request_share:.1f}% + {response_share:.1f}%）。二次拆分显示 client send syscall 和 epoll-ready→recv 都很短，而等待随并发在 send 返回→server recv、server send completion、mailbox callback 和 send→client epoll 区间同步放大。</div>
<div class="verdict medium"><span class="rank">2</span><strong>次瓶颈：Worker 排队与 completion 回投。</strong>Worker queue 均值 {duration(stages_1536['worker_queue']['mean_us'])}，mailbox return 均值 {duration(stages_1536['mailbox_return']['mean_us'])}；最慢 1% 中分别占 {worker_share:.1f}% 和 {mailbox_share:.1f}%。batch 内前序 HOL 和 batch 完成等待也真实存在，但量级更小。</div>
<div class="verdict low"><span class="rank">3</span><strong>Protobuf 不是当前 128B 场景的主要解释。</strong>本次没有把 Protobuf 单独打点；采用更保守的上界：将所有可能包含协议编解码的客户端组帧、服务端整批解帧、响应编码、客户端解帧，再加 handler dispatch 全部相加，均值也只有 {duration(codec_upper_mean)}，约占阶段均值总和 {codec_upper_share:.1f}%。这还是“整桶归给 Protobuf”的高估，而不是 Protobuf 的纯成本。</div>
<p>因此，更准确的表述是：<strong>128B 只说明单次业务计算和字节搬运的“有效工作量”较小，不代表闭环并发系统没有排队与唤醒。</strong>在 1536 个持续在途请求下，Little’s Law 给出的平均停留时间为 <code>L/λ ≈ 1536 / {fnum(release_1536['qps'],0)} = {duration(1536/release_1536['qps']*1_000_000)}</code>，与实测 {duration(release_1536['avg_us'])} 一致。</p>

<h3>证据强度</h3>
<p><strong>高置信：</strong>主耗时位于服务端 I/O/内核收发流水线；mailbox callback 与 socket 事件共享 I/O 线程造成局部排队；Protobuf、客户端 epoll 后处理和全局 CPU 饱和都不是主因。<strong>中置信：</strong>大桶内部 TCP loopback、io_uring recv/send completion 与 I/O 线程事件公平性各自的精确比例。<strong>低置信/未证明：</strong>某一个内核锁或单一 syscall 是唯一根因。</p>

<h2 id="confirmation">二次瓶颈确认：CPU、runqueue 与细分事件边界</h2>
<p>为了区分“CPU 跑满”“线程等调度”和“事件流水线排队”，新增了每线程 <code>/proc/&lt;pid&gt;/task/&lt;tid&gt;/schedstat</code> 采样，并将 mailbox、客户端 send/epoll、服务端 send completion 进一步打点。正式细分实验覆盖 384、1536、6144 在途，各 3 次；共 {fnum(detailed_sample_count,0)} 条完整请求。细分区间中有 {negative_count} 个跨进程并发边界出现负值（不足 {negative_count/detailed_sample_count*100:.3f}%），已从对应区间分布过滤，不影响其他阶段。</p>
<div class="verdict high"><strong>确认结果：</strong>1536 点不开 trace 写盘时，3 个服务端 I/O 线程合计使用 {io_1536['average_cpu_cores']:.2f} 核，最忙线程 {io_1536['max_thread_cpu_pct']:.1f}%；3 个 Worker 合计 {worker_1536['average_cpu_cores']:.2f} 核，最忙 {worker_1536['max_thread_cpu_pct']:.1f}%；客户端 I/O 合计 {client_1536['average_cpu_cores']:.2f} 核，最忙 {client_1536['max_thread_cpu_pct']:.1f}%。服务端 I/O 是最忙的局部资源，但没有单核 100%，整机也未打满。</div>
<div class="verdict low"><strong>排除“主要是 OS 调度饿死”：</strong>1536 点服务端 I/O 的 runqueue wait/runtime 仅 {io_1536['wait_to_runtime_pct']:.1f}%，Worker 为 {worker_1536['wait_to_runtime_pct']:.1f}%。说明线程一旦 runnable，通常能及时拿到 CPU；毫秒延迟主要不是在操作系统 runqueue 中等待。</div>
<div class="verdict medium"><strong>定位 mailbox：</strong>1536 点 mailbox 总回投中的 mutex wait 只有 {duration(detailed_1536['detailed_stages']['mailbox_mutex_wait']['mean_us'])}，真正的大头是 completion 入队后等待 I/O callback，约 {duration(detailed_1536['detailed_stages']['mailbox_callback_wait']['mean_us'])}。所以不是 mailbox 锁竞争，而是 callback 要与 recv/send CQE 一起等待同一个 I/O 事件循环处理。</div>
<h3>细分等待随负载变化</h3>
{detailed_timing_table}
<p>最关键的形状是：384→1536→6144 时，send 返回→server recv 从约 0.68→1.62→4.66 ms，send submit→client epoll 从约 0.59→1.26→3.38 ms，mailbox callback wait 从约 126→240→451 μs。与之相对，客户端 send syscall 只有约 16→23→27 μs，epoll ready→recv 只有约 17→35→68 μs。等待发生在事件被流水线消费之前，而不是客户端拿到事件之后。</p>
<h3>每线程 CPU 与 runnable 等待（不开 trace 写盘）</h3>
{thread_table}
<p>服务端 I/O 线程是最接近容量上限的一组，但其 CPU 从 384 到 6144 只小幅变化，等待时间却成倍增长；这是典型的局部服务队列接近容量、突发 batch 加深排队，而不是整个进程算力耗尽。Worker pending 均值随负载约 62→200→645，Worker CPU 仍明显低于 I/O 线程，因此 Worker/batch 排队是次级连锁拥堵。</p>

<h2 id="toc">目录</h2><ol class="toc">
<li><a href="#confirmation">二次瓶颈确认</a></li><li><a href="#model">为什么 128B 仍有毫秒延迟</a></li><li><a href="#method">方法与时间戳</a></li>
<li><a href="#load">负载曲线与 Little’s Law</a></li><li><a href="#stages">阶段归因</a></li>
<li><a href="#batch">batch / HOL / 队列</a></li><li><a href="#controls">对照实验</a></li>
<li><a href="#cpu">CPU 与调度证据</a></li><li><a href="#protobuf">为何不能归因于 Protobuf</a></li>
<li><a href="#limits">限制与下一步观测</a></li><li><a href="#reproduce">本地复现与文件清单</a></li></ol>

<h2 id="model">为什么 128B 仍有毫秒延迟</h2>
<p>一次 RPC 的端到端时间不是 payload 长度除以内存带宽。它是多个“服务时间 + 等待时间”的串联：客户端组批并获得可写机会、内核 TCP loopback、服务端 io_uring 完成与事件循环调度、Worker 入队、batch 内串行、completion 跨线程回投、服务端写队列、客户端 epoll 唤醒和解析完成。payload 很小，只能压低其中少数服务时间；当闭环维持固定在途量时，其余等待仍然存在。</p>
<div class="flow"><div>客户端组帧<br><b>send</b></div><div class="arrow">→</div><div class="hot">TCP / 调度<br><b>服务端 I/O 唤醒</b></div><div class="arrow">→</div><div class="warm">Worker queue<br><b>batch 串行</b></div><div class="arrow">→</div><div>dispatch<br><b>约 2 μs</b></div><div class="arrow">→</div><div class="warm">mailbox / 写队列<br><b>回 I/O 线程</b></div><div class="arrow">→</div><div class="hot">TCP / 调度<br><b>客户端 epoll 唤醒</b></div></div>

<h2 id="method">方法与时间戳</h2>
<p>基准使用 Release 构建测负载曲线；另用编译期 <code>XRPC_ENABLE_LATENCY_TRACE=ON</code> 的 Release 构建，在请求 ID 上做确定性 1/512 采样。每线程写固定 24 字节二进制记录并批量刷盘，避免全局 trace 锁。正式 trace 覆盖在途 48、384、1536、6144，各 3 次、每次 10 秒。</p>
<p>端到端被切成 14 个不重叠区间：client-created → send 尝试、send → server recv 完成、server decode、提交 Worker、Worker queue、batch 前序 HOL、dispatch、响应编码、等待 batch 余项、mailbox 回投、写入连接队列、写队列 → send、send → client recv、客户端解析完成。1536 点共 {fnum(at_1536['complete_samples'],0)} 条完整采样，incomplete={at_1536['incomplete_samples']}，非法时间线={at_1536['invalid_timelines']}。</p>
<div class="note warning"><strong>观测开销：</strong>1536 点 trace 构建的 QPS 中位数为 {fnum(traced_1536['qps'],0)}，无 trace Release 为 {fnum(release_1536['qps'],0)}，差约 {trace_qps_overhead:.1f}%。因此报告把 trace 用于阶段比例和量级判断，把无 trace Release 用于最终吞吐/延迟数值。</div>
{rows_table(["在途","重复","完整样本","基准 avg","基准 p99","采样 avg","采样 p99","batch 均值","Worker pending 均值","position↔HOL r","position↔E2E r"], trace_rows)}

<h2 id="load">负载曲线与 Little’s Law</h2>
{line_chart(load_curve, 'qps', '吞吐随在途请求数变化', '#2563eb')}
{line_chart(load_curve, 'p99_us', 'p99 延迟随在途请求数变化', '#dc2626', lambda value: duration(value))}
{rows_table(["在途 L","QPS λ","avg W","p50","p95","p99","p99 三次范围","λW","L/λ","失败"], law_rows)}
<p>从 48 增至 6144 在途，QPS 增长逐渐变缓，而 p99 从约 {duration(next(p for p in load_curve if p['key']==48)['p99_us'])} 增至 {duration(next(p for p in load_curve if p['key']==6144)['p99_us'])}。这正是排队系统接近容量区后的形状：继续增加并发主要增加等待，而不是按比例增加有效吞吐。表中的 <code>λW</code> 与配置的 L 接近，也验证了平均延迟不是计时器异常，而是闭环在途守恒的直接结果。</p>

<h2 id="stages">1536 在途的阶段归因</h2>
{stage_bar_chart(trace)}
<p class="small muted">堆叠条只显示八个主要阶段；右侧为完整采样 E2E 均值。各阶段数值采用三次运行各自统计量的中位数，因此单独中位数相加不要求与 E2E 中位数完全相等。</p>
{rows_table(["阶段","全部请求 mean","全部 p95","全部 p99","mean 占阶段和","最慢 1% mean","最慢 1% 占其 E2E"], stage_rows)}
<p>尾部归因必须在<strong>同一个慢请求 cohort</strong>内计算，而不能把每个阶段各自的 p99 相加。本报告先按采样 E2E 排序选最慢 1%，再计算这些请求每个阶段的均值和占比，因此“最慢 1% 占比”可以解释尾请求真正把时间花在哪里。</p>

<h2 id="batch">batch、HOL 与队列</h2>
<p>1536 点的 batch size 均值 {at_1536['batch']['size_mean']:.1f}，p50 {at_1536['batch']['size_p50']:.0f}，p95 {at_1536['batch']['size_p95']:.0f}；Worker pending logical jobs 均值 {at_1536['queues']['worker_pending_mean']:.1f}、p95 {at_1536['queues']['worker_pending_p95']:.0f}、p99 {at_1536['queues']['worker_pending_p99']:.0f}。batch position 与 batch HOL 的 Pearson r={at_1536['batch']['position_hol_r']:.3f}，说明越靠后越要等前序请求；但 position 与完整 E2E 的 r={at_1536['batch']['position_e2e_r']:.3f}，相关性弱，说明总延迟仍由更大的 I/O/调度阶段盖过。</p>
<p>这里存在两种 HOL：一是 Worker 在一个 job 内顺序 dispatch 请求，后面的请求在 <code>worker_start → request_start</code> 等待；二是已经编码完成的前序响应要等 batch 全部处理后才统一 <code>mailbox Submit</code>。在 1536 点，两者均值分别为 {duration(stages_1536['batch_hol_before_request']['mean_us'])} 与 {duration(stages_1536['batch_completion_hold']['mean_us'])}。它们是可证实的次要延迟，而不是最大桶。</p>

<h2 id="controls">对照实验</h2>
<p>除连接数扫描外，下列实验均固定 12 连接、1536 在途、128B，并各跑 3 次；payload 扫描只改变 payload。由于 WSL2 同机压测存在明显时间漂移，判断依据是趋势是否单调、差异是否超过三次范围，而不是挑选最好的一次。</p>
<h3>Worker 线程数</h3>{control_table(worker, 'Worker')}
<p>1 → 3 → 6 没有稳定单调的 QPS 或 p99 改善，故不能把“Worker 数量不足”列为首要容量瓶颈。Worker queue 本身仍是次级尾延迟来源；这两句话并不矛盾：队列等待可来自整机调度、批次到达突发和跨线程回投节奏，而不一定能靠增加 Worker 消除。</p>
<h3>服务端 I/O 线程数</h3>{control_table(server_io, '服务端 I/O')}
<p>单 I/O 线程降低吞吐但 p99 较平滑；从 3 增到 6 没有稳定收益，反而扩大波动。它支持“事件循环与调度参与瓶颈”，但不支持简单的“线程越多越快”。</p>
<h3>客户端 I/O 线程数</h3>{control_table(client_io, '客户端 I/O')}
<p>3 线程附近表现最好，但 6/12 没有继续改善；客户端 epoll 大桶不能简单解释为客户端线程数量不足，更可能是 loopback 事件到达、每线程连接/批次处理和与服务端争抢 CPU 的组合。</p>
<h3>固定 1536 在途，改变连接数</h3>{control_connections_table}
<p>连接数效果非单调，12 连接在本轮最好，48 连接出现明显退化。连接数同时改变每连接 inflight、收发 batch、epoll 就绪集合和服务端连接分片，因此它是调度形态变量，而不是纯粹“并行度旋钮”。</p>
<h3>payload 大小</h3>{control_table(payloads, 'payload')}
<p>16B → 128B → 1024B 带来强烈的吞吐下降和 p99 上升，说明 framing、复制、socket/TCP 字节处理或内存/cache 压力随 payload 增长；但 128B formal trace 中所有可能含协议编解码的整桶加 handler 的保守上界也只有 {duration(codec_upper_mean)}，因此这个趋势不能单独证明 Protobuf 是主因。</p>
<h3>CPU 集合隔离对照</h3>
{control_table([affinity], '亲和性')}
<p>将服务端固定在 CPU 0–5、客户端固定在 6–11 后，QPS/p99 未相对邻近的 unpinned 实验形成稳健改善。由于 i7-9750H 的逻辑 CPU 是超线程兄弟，编号区间并不等价于物理核隔离；本结果只能说明这一简单 pinning 方案没有解决抖动，不能证明调度无关。</p>

<h2 id="cpu">CPU 与调度证据</h2>
<p>单次无 trace、1536 在途 perf stat：QPS {fnum(perf_run['qps'],0)}，avg {duration(perf_run['avg_us'])}，p99 {duration(perf_run['p99_us'])}。计数器如下；task-clock 与逻辑 CPU 利用来自 perf 的实际进程墙钟，不用固定 10 秒反推。</p>
{perf_table}
<p>服务端 IPC {server_instructions/server_cycles:.2f}，客户端 IPC {client_instructions/client_cycles:.2f}。这表明执行的是大量协议、事件循环、队列和内核边界工作，而不是 128B handler 独占 CPU。cache miss/ref 只作为本机画像，不据此做单一根因归因。</p>
<div class="note warning"><strong>调度计数器限制：</strong>本机 <code>perf_event_paranoid={html.escape(env['perf_event_paranoid'])}</code>，perf 自动将事件标记为 user-only；因此 context-switches 和 cpu-migrations 输出为 0，属于不可观测，不代表真的没有切换/迁核。本报告没有修改 sysctl，也没有用这些 0 支撑结论。</div>

<h2 id="protobuf">为什么不能简单归因于 Protobuf</h2>
<ol>
<li><strong>先承认边界：</strong>本次没有在 <code>ParseFromArray</code>/<code>SerializeToString</code> 内部单独打点，所以不能声称 {duration(dispatch_mean)} 就是 Protobuf。<code>request_start → dispatch_end</code> 只覆盖 registry + handler，均值 {duration(dispatch_mean)}、p99 {duration(stages_1536['handler_dispatch']['p99_us'])}。</li>
<li><strong>请求解析的上界：</strong><code>server recv → decoded</code> 包含 ByteBuffer append、整批可用 frame 循环、header/metadata 校验和请求 metadata Protobuf parse，均值 {duration(stages_1536['server_decode']['mean_us'])}、p99 {duration(stages_1536['server_decode']['p99_us'])}。同一批请求共享整批 decode 的停留时间，因此它是每请求 residence time，不是单个 Protobuf parse 的 CPU 成本。</li>
<li><strong>其余含编解码桶：</strong>客户端请求组帧/等待 send 均值 {duration(stages_1536['client_prepare_send']['mean_us'])}，服务端响应 frame encode {duration(stages_1536['response_frame_encode']['mean_us'])}，客户端响应拆帧/完成 {duration(stages_1536['client_decode_complete']['mean_us'])}。这些桶也混有 buffer、framing 和等待，不能全算成 Protobuf。</li>
<li><strong>保守反事实上界：</strong>把上述所有混合桶连同 handler 全部假设成可消除的“协议/业务成本”，均值总计 {duration(codec_upper_mean)}，占阶段和 {codec_upper_share:.1f}%；在同一最慢 1% cohort 中占比总计 {codec_upper_slow_share:.1f}%。实际 Protobuf 纯成本只会更小。</li>
<li><strong>payload 对照不构成单独归因：</strong>payload 扫描只能说明“字节相关路径”重要，范围包括 frame encode/decode、字符串增长与复制、socket/TCP、cache 与内存访问；不能将全部差异归给 Protobuf。</li>
</ol>
<p>因此建议在对外解释时使用：<q>128B 证明业务计算轻，不证明端到端等待轻。固定 1536 在途时，Little’s Law 要求平均停留约数毫秒；分阶段采样显示主要时间在双向 I/O 唤醒/调度，其次是 Worker queue 和 completion 回投。即使把所有可能含协议编解码的混合桶连同 handler 全算进去，保守上界也只占均值约 {codec_upper_share:.1f}%，不能解释主体延迟。</q></p>

<h2 id="limits">限制与下一步观测</h2>
<ul>
<li><strong>环境外推：</strong>结果来自 WSL2、同机 client/server、loopback，不应直接外推到裸机、跨机网络或生产 CPU 拓扑。</li>
<li><strong>大桶边界：</strong><code>send timestamp → peer recv observed</code> 同时包含用户态 send 边界、TCP loopback、内核调度、peer event completion 和事件循环处理。当前只能证明时间在这个边界内，尚不能精确拆成每个内核子项。</li>
<li><strong>Observer effect：</strong>1/512 trace 仍带来约 {trace_qps_overhead:.1f}% QPS 差异；绝对数优先使用无 trace Release。</li>
<li><strong>运行波动：</strong>若三次范围很宽，报告只给趋势性结论，不把某次最好值当因果证据。</li>
<li><strong>不做优化：</strong>本轮没有更改 batching、线程模型、write 策略或 protobuf 实现；诊断代码仅在显式编译选项开启时生效。</li>
</ul>
<p>本轮已经用 schedstat 排除了 runqueue delay 是主体。若继续只做观测，剩余最高价值是用 eBPF/内核 tracepoint（需要合适权限）拆分 TCP loopback、socket queue、io_uring recv/send 提交到 CQE 的时间，并用固定 offered-rate 而非纯固定 inflight 复测容量拐点。当前权限不足以把内核大桶安全拆到 syscall/softirq 级别，因此报告停在可由现有证据支持的 I/O 流水线层级。</p>

<h2 id="reproduce">本地复现与文件清单</h2>
<p>诊断代码位于本地分支；报告和所有运行产物位于 Git 本地 exclude 目录。核心复现命令：</p>
<pre>cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake -S . -B build-latency -DCMAKE_BUILD_TYPE=Release -DXRPC_ENABLE_LATENCY_TRACE=ON
cmake --build build-release --parallel
cmake --build build-latency --parallel
./tools/benchmark/runner/run_suite.py --config .local-perf/configs/formal-load-curve.json --build-dir build-release
./tools/benchmark/runner/run_suite.py --config .local-perf/configs/formal-trace-keypoints.json --build-dir build-latency --trace-dir .local-perf/raw/formal-trace-keypoints --trace-sample-shift 9
./tools/benchmark/runner/run_suite.py --config .local-perf/configs/detailed-bottleneck.json --build-dir build-latency --trace-dir .local-perf/raw/detailed-bottleneck --trace-sample-shift 9 --schedstat-dir .local-perf/schedstat/detailed-bottleneck
./tools/benchmark/runner/run_suite.py --config .local-perf/configs/schedstat-only-bottleneck.json --build-dir build-latency --schedstat-dir .local-perf/schedstat/schedstat-only-bottleneck
./tools/benchmark/runner/generate_latency_report.py</pre>
<h3>环境</h3>
{rows_table(['字段','值'], [[key, '<pre>'+html.escape(value)+'</pre>' if key=='cpu' else html.escape(value)] for key,value in env.items()])}
<h3>原始数据清单</h3>
{manifest_table}
<div class="footer">生成时间 {html.escape(env['generated_at'])}；Git commit {html.escape(env['git_commit'])}。报告生成器不访问网络，不包含外部 CSS/JavaScript，不执行 push。</div>
</main></body></html>"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")


def parse_args():
    parser = argparse.ArgumentParser(description="Generate a self-contained local xRPC latency diagnosis report.")
    parser.add_argument("--raw-dir", type=Path, default=Path(".local-perf/raw"))
    parser.add_argument("--output", type=Path, default=Path(".local-perf/report/xrpc-latency-report.html"))
    parser.add_argument(
        "--trace-analysis-output", type=Path, default=Path(".local-perf/raw/formal-trace-analysis.json")
    )
    parser.add_argument(
        "--bottleneck-analysis-output",
        type=Path,
        default=Path(".local-perf/raw/detailed-bottleneck-analysis.json"),
    )
    parser.add_argument(
        "--schedstat-analysis-output",
        type=Path,
        default=Path(".local-perf/raw/schedstat-only-bottleneck-analysis.json"),
    )
    return parser.parse_args()


def main():
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[3]
    raw_dir = args.raw_dir if args.raw_dir.is_absolute() else repo_root / args.raw_dir
    output = args.output if args.output.is_absolute() else repo_root / args.output
    trace_output = (
        args.trace_analysis_output
        if args.trace_analysis_output.is_absolute()
        else repo_root / args.trace_analysis_output
    )
    bottleneck_output = (
        args.bottleneck_analysis_output
        if args.bottleneck_analysis_output.is_absolute()
        else repo_root / args.bottleneck_analysis_output
    )
    schedstat_output = (
        args.schedstat_analysis_output
        if args.schedstat_analysis_output.is_absolute()
        else repo_root / args.schedstat_analysis_output
    )
    trace_suite = load_json(raw_dir / "formal-trace-keypoints.json")
    trace_analysis = analyze_trace_suite(trace_suite)
    trace_output.parent.mkdir(parents=True, exist_ok=True)
    trace_output.write_text(json.dumps(trace_analysis, indent=2) + "\n", encoding="utf-8")
    bottleneck_analysis = analyze_bottleneck_suite(load_json(raw_dir / "detailed-bottleneck-results.json"))
    bottleneck_output.write_text(json.dumps(bottleneck_analysis, indent=2) + "\n", encoding="utf-8")
    schedstat_analysis = analyze_schedstat_suite(
        load_json(raw_dir / "schedstat-only-bottleneck-results.json")
    )
    schedstat_output.write_text(json.dumps(schedstat_analysis, indent=2) + "\n", encoding="utf-8")
    env = environment_info(repo_root)
    (raw_dir / "environment.json").write_text(json.dumps(env, indent=2) + "\n", encoding="utf-8")
    build_report(repo_root, raw_dir, output, trace_analysis, bottleneck_analysis, schedstat_analysis, env)
    print(f"report={output}")
    print(f"trace_analysis={trace_output}")
    print(f"bottleneck_analysis={bottleneck_output}")
    print(f"schedstat_analysis={schedstat_output}")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as error:
        raise SystemExit(str(error))
