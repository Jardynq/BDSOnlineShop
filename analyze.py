import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

df = pd.read_csv("latencies.csv")

start_cols = [c for c in df.columns if "start" in c]
end_cols = [c for c in df.columns if "end" in c]

start_df = df.melt(
    id_vars=["epoch", "thread"],
    value_vars=start_cols,
    var_name="op_start",
    value_name="start_ns",
)
end_df = df.melt(
    id_vars=["epoch", "thread"],
    value_vars=end_cols,
    var_name="op_end",
    value_name="end_ns",
)

# Map start{i}_ns and end{i}_ns to {i}
start_df["op_idx"] = start_df["op_start"].str.extract(r'(\d+)').astype(int)
end_df["op_idx"] = end_df["op_end"].str.extract(r'(\d+)').astype(int)

df = pd.merge(
    start_df.drop(columns=["op_start"]),
    end_df.drop(columns=["op_end"]),
    on=["epoch", "thread", "op_idx"]
).dropna()
df = df.sort_values(by=["epoch", "thread", "op_idx"])

df["latency_ms"] = (df["end_ns"] - df["start_ns"]) / 1_000_000.0

global_min_ns = df["start_ns"].min()
df["start_ms"] = (df["start_ns"] - global_min_ns) / 1_000_000.0

max_thread = df["thread"].max()
df["Thread Type"] = np.where(df["thread"] == max_thread, "End-To-End", "Workload")

bin_size_ms = 1000.0
df["time_bin_ms"] = (df["start_ms"] // bin_size_ms) * bin_size_ms

epoch_level_stats = (
    df.groupby(["time_bin_ms", "Thread Type", "epoch"])
    .agg(
        avg_latency_ms=("latency_ms", "mean"),
        p95_latency_ms=("latency_ms", lambda x: np.percentile(x, 95)),
        total_ops=("latency_ms", "count"),
    )
    .reset_index()
)
epoch_level_stats["epoch_throughput"] = epoch_level_stats["total_ops"] / (bin_size_ms / 1000.0)

stats = (
    epoch_level_stats.groupby(["time_bin_ms", "Thread Type"])
    .agg(
        avg_latency_ms=("avg_latency_ms", "mean"),
        p95_latency_ms=("p95_latency_ms", "mean"),
        std_latency_ms=("avg_latency_ms", "std"),
        total_throughput=("epoch_throughput", "mean"),
        std_throughput=("epoch_throughput", "std"),
    )
    .reset_index()
)


categories = ["Workload", "End-To-End"]
colors = {"Workload": "#1060FF", "End-To-End": "#FF2010"}
filenames = {"Workload": "graph_workload.png", "End-To-End": "graph_end_to_end.png"}

for cat in categories:
    cat_data = stats[stats["Thread Type"] == cat].sort_values("time_bin_ms")

    if cat_data.empty:
        print(f"{cat} is empty")
        continue

    fig, axes = plt.subplots(2, 1, figsize=(9, 8), sharex=True)

    ax_lat = axes[0]
    ax_lat.plot(
        cat_data["time_bin_ms"],
        cat_data["avg_latency_ms"],
        label="Latency Avg",
        color=colors[cat],
        linewidth=2,
        marker="o"
    )
    ax_lat.plot(
        cat_data["time_bin_ms"],
        cat_data["p95_latency_ms"],
        label="Latency P95",
        color=colors[cat],
        linestyle="--",
        linewidth=1.5,
        marker="x",
        alpha=0.85
    )
    ax_lat.fill_between(
        cat_data["time_bin_ms"],
        np.maximum(0, cat_data["avg_latency_ms"] - cat_data["std_latency_ms"]),
        cat_data["avg_latency_ms"] + cat_data["std_latency_ms"],
        color=colors[cat],
        alpha=0.15,
        label="Latency Std Dev"
    )

    ax_lat.set_title(f"{cat} Latency (Averaged Across Epochs)", fontsize=13, fontweight='bold')
    ax_lat.set_ylabel("Latency [ms]", fontsize=11)
    ax_lat.legend(loc="upper right")

    ax_tput = axes[1]
    ax_tput.plot(
        cat_data["time_bin_ms"],
        cat_data["total_throughput"],
        label="Throughput Total",
        color=colors[cat],
        linewidth=2,
        marker="s"
    )
    ax_tput.fill_between(
        cat_data["time_bin_ms"],
        np.maximum(0, cat_data["total_throughput"] - cat_data["std_throughput"]),
        cat_data["total_throughput"] + cat_data["std_throughput"],
        color=colors[cat],
        alpha=0.15,
        label="Throughput Thread Variance"
    )

    ax_tput.set_title(f"{cat} Throughput (Averaged Across Epochs)", fontsize=13, fontweight='bold')
    ax_tput.set_ylabel("Throughput [ops/s]", fontsize=11)
    ax_tput.set_xlabel("Time [ms]", fontsize=11)
    ax_tput.legend(loc="upper right")

    plt.tight_layout()
    plt.savefig(filenames[cat])
    plt.close(fig)
