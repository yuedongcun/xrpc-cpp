#!/usr/bin/env python3
"""Generate the README performance charts from the published benchmark summary.

The data below is the formal three-run median/min/max summary in
tools/benchmark/README.md.  Keep this file and that table in sync when a new
formal benchmark report is published.
"""

from pathlib import Path


LOAD_POINTS = [
    (24, 44198, 44193, 45061, 0.98, 0.93, 0.99),
    (48, 91993, 90298, 94274, 0.88, 0.87, 0.92),
    (96, 181768, 175461, 188363, 0.88, 0.84, 0.92),
    (192, 175148, 167555, 185207, 1.96, 1.57, 2.18),
    (384, 256151, 247831, 274315, 3.17, 2.84, 3.28),
    (768, 335793, 314402, 380125, 5.62, 4.98, 6.08),
    (1536, 476714, 460927, 489932, 6.94, 6.48, 7.96),
    (3072, 572899, 560058, 607174, 11.23, 9.97, 11.28),
    (6144, 648161, 619058, 653570, 19.62, 18.58, 23.58),
    (8192, 652368, 627799, 652569, 26.02, 25.67, 29.28),
    (12288, 686641, 638013, 734983, 40.59, 38.81, 40.60),
]

CONNECTION_POINTS = [
    (12, 24032, 23121, 24674, 0.86, 0.82, 0.89),
    (48, 45783, 42084, 46031, 2.27, 2.20, 2.47),
    (192, 58836, 56675, 60653, 9.33, 8.98, 10.49),
    (768, 66881, 57738, 71952, 27.06, 24.86, 30.85),
    (1536, 65153, 60967, 73752, 50.00, 42.93, 55.04),
    (3072, 48774, 48497, 52603, 113.02, 110.03, 118.00),
]

WIDTH = 960
HEIGHT = 720
LEFT = 92
RIGHT = 32
PANEL_WIDTH = WIDTH - LEFT - RIGHT
PANEL_HEIGHT = 215
TOP_Y = 105
BOTTOM_Y = 435
BLUE = "#2166ac"
ORANGE = "#e89532"
RED = "#b42318"
GRID = "#d0d5dd"
TEXT = "#344054"


def esc(text):
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def text(x, y, value, size=12, anchor="start", weight=None, fill=TEXT):
    style = f"font-size:{size}px;font-family:'Noto Sans CJK SC','Microsoft YaHei','DejaVu Sans',sans-serif;fill:{fill}"
    if weight is not None:
        style += f";font-weight:{weight}"
    return f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="{anchor}" style="{style}">{esc(value)}</text>'


def scale(value, low, high, top):
    return top + PANEL_HEIGHT - (value - low) / (high - low) * PANEL_HEIGHT


def nice_upper(value, unit):
    return int((value + unit - 1) // unit) * unit


def x_positions(points):
    count = len(points)
    return [LEFT + i * PANEL_WIDTH / (count - 1) for i in range(count)]


def panel(points, y, metric, title, ylabel, tick_formatter, colour, mark_failed=False):
    if metric == "qps":
        median_index, min_index, max_index = 1, 2, 3
        high = nice_upper(max(point[max_index] for point in points), 100000)
    else:
        median_index, min_index, max_index = 4, 5, 6
        high = nice_upper(max(point[max_index] for point in points), 10)
    low = 0
    positions = x_positions(points)
    chunks = [text(LEFT, y - 18, title, size=16, weight=700, fill="#101828")]
    chunks.append(f'<rect x="{LEFT}" y="{y}" width="{PANEL_WIDTH}" height="{PANEL_HEIGHT}" fill="#ffffff" stroke="{GRID}"/>')
    for tick in range(5):
        value = high * tick / 4
        tick_y = scale(value, low, high, y)
        chunks.append(f'<line x1="{LEFT}" y1="{tick_y:.1f}" x2="{LEFT + PANEL_WIDTH}" y2="{tick_y:.1f}" stroke="{GRID}" stroke-width="1"/>')
        chunks.append(text(LEFT - 10, tick_y + 4, tick_formatter(value), size=11, anchor="end"))
    chunks.append(text(25, y + PANEL_HEIGHT / 2, ylabel, size=12, anchor="middle").replace(
        '<text ', f'<text transform="rotate(-90 25 {y + PANEL_HEIGHT / 2:.1f})" '))
    path = []
    for index, point in enumerate(points):
        x = positions[index]
        median, minimum, maximum = point[median_index], point[min_index], point[max_index]
        median_y = scale(median, low, high, y)
        minimum_y = scale(minimum, low, high, y)
        maximum_y = scale(maximum, low, high, y)
        chunks.append(f'<line x1="{x:.1f}" y1="{minimum_y:.1f}" x2="{x:.1f}" y2="{maximum_y:.1f}" stroke="{colour}" stroke-opacity="0.38" stroke-width="6" stroke-linecap="round"/>')
        path.append(f'{x:.1f},{median_y:.1f}')
    chunks.append(f'<polyline points="{" ".join(path)}" fill="none" stroke="{colour}" stroke-width="2.5"/>')
    for index, point in enumerate(points):
        x = positions[index]
        median_y = scale(point[median_index], low, high, y)
        chunks.append(f'<circle cx="{x:.1f}" cy="{median_y:.1f}" r="4.2" fill="#ffffff" stroke="{colour}" stroke-width="2"/>')
        if mark_failed and index == len(points) - 1:
            chunks.append(f'<circle cx="{x:.1f}" cy="{median_y:.1f}" r="6.7" fill="none" stroke="{RED}" stroke-width="2.2"/>')
    legend_x = LEFT + 12
    legend_y = y + 18
    chunks.append(f'<line x1="{legend_x}" y1="{legend_y}" x2="{legend_x + 28}" y2="{legend_y}" stroke="{colour}" stroke-width="2.5"/>')
    chunks.append(f'<circle cx="{legend_x + 14}" cy="{legend_y}" r="3.7" fill="#ffffff" stroke="{colour}" stroke-width="1.8"/>')
    chunks.append(text(legend_x + 36, legend_y + 4, "中位数；淡色竖线为三轮最小值–最大值", size=11))
    if mark_failed:
        chunks.append(f'<circle cx="{legend_x + 320}" cy="{legend_y}" r="5.8" fill="none" stroke="{RED}" stroke-width="2"/>')
        chunks.append(text(legend_x + 332, legend_y + 4, "该点有失败，不作为容量成绩", size=11, fill=RED))
    return "\n".join(chunks)


def chart(title, subtitle, points, x_label, path, failed_last=False):
    chunks = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        text(WIDTH / 2, 35, title, size=22, anchor="middle", weight=700, fill="#101828"),
        text(WIDTH / 2, 58, subtitle, size=12, anchor="middle"),
        panel(points, TOP_Y, "qps", "吞吐量（QPS）", "QPS", lambda value: f"{value / 1000:.0f}k", BLUE, failed_last),
        panel(points, BOTTOM_Y, "p99", "尾延迟（p99）", "p99 延迟（ms）", lambda value: f"{value:.0f}", ORANGE, failed_last),
    ]
    positions = x_positions(points)
    for x, point in zip(positions, points):
        chunks.append(f'<line x1="{x:.1f}" y1="{BOTTOM_Y + PANEL_HEIGHT}" x2="{x:.1f}" y2="{BOTTOM_Y + PANEL_HEIGHT + 5}" stroke="{TEXT}"/>')
        chunks.append(text(x, BOTTOM_Y + PANEL_HEIGHT + 21, f"{point[0]:,}", size=11, anchor="middle"))
    chunks.append(text(WIDTH / 2, HEIGHT - 16, x_label, size=13, anchor="middle"))
    chunks.append('</svg>')
    path.write_text("\n".join(chunks), encoding="utf-8")


def main():
    repo = Path(__file__).resolve().parents[3]
    assets = repo / "docs" / "assets"
    chart(
        "xRPC 服务端负载曲线",
        "12 条 TCP 连接 · 128 B Protobuf Echo · 3 次测量",
        LOAD_POINTS,
        "全局在途请求数",
        assets / "server-performance.svg",
        failed_last=True,
    )
    chart(
        "xRPC 活跃连接规模",
        "每条连接固定 1 个在途 RPC · 128 B Protobuf Echo · 3 次测量",
        CONNECTION_POINTS,
        "活跃 TCP 连接数",
        assets / "connection-scale.svg",
    )


if __name__ == "__main__":
    main()
