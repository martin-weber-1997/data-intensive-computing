from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


ROOT = Path(__file__).resolve().parents[1]
GRID_PATH = ROOT / "data" / "output_part3.json"
FULL_PATH = ROOT / "data" / "output_part3_full_best.json"
OUT_DIR = ROOT / "docs" / "part3_plots"


PALETTE = {
    "chisq_top2000": "#33658a",
    "variance_threshold=0.001": "#f26419",
    "Development grid test": "#2f855a",
    "Full dataset test": "#7c3aed",
}


def load_results() -> tuple[pd.DataFrame, dict]:
    with GRID_PATH.open() as f:
        grid = json.load(f)
    with FULL_PATH.open() as f:
        full = json.load(f)

    rows = grid["all_results"]
    df = pd.DataFrame(rows)
    df["fit_minutes"] = df["fit_seconds"] / 60.0
    df["standardization"] = df["standardization"].map({True: "standardized", False: "not standardized"})
    df["config"] = (
        "C="
        + df["regParam"].astype(str)
        + ", "
        + df["standardization"]
        + ", iter="
        + df["maxIter"].astype(str)
    )
    return df, {"grid": grid, "full": full}


def save_current(name: str) -> None:
    for suffix in ("png", "svg"):
        plt.savefig(OUT_DIR / f"{name}.{suffix}", bbox_inches="tight", dpi=180)
    plt.close()


def style_axes(ax, title: str, xlabel: str = "", ylabel: str = "") -> None:
    ax.set_title(title, loc="left", fontsize=15, fontweight="bold", pad=14)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(True, axis="x", alpha=0.18)
    sns.despine(ax=ax, left=False, bottom=False)


def plot_top_configs(df: pd.DataFrame) -> None:
    top = df.sort_values("val_f1", ascending=False).head(10).copy()
    top = top.sort_values("val_f1")
    labels = [
        f"{row.variant.replace('variance_threshold=0.001', 'variance 0.001')}\n"
        f"C={row.regParam}, {row.standardization}, iter={row.maxIter}"
        for row in top.itertuples()
    ]

    fig, ax = plt.subplots(figsize=(10, 6.2))
    colors = [PALETTE[v] for v in top["variant"]]
    ax.barh(labels, top["val_f1"], color=colors, height=0.72)
    ax.set_xlim(0.54, 0.615)
    style_axes(ax, "Top validation F1 configurations", "Validation F1")
    for value, y in zip(top["val_f1"], ax.get_yticks(), strict=False):
        ax.text(value + 0.001, y, f"{value:.4f}", va="center", fontsize=9)
    save_current("01_top_validation_configs")


def plot_selector_best(df: pd.DataFrame) -> None:
    best = df.loc[df.groupby("variant")["val_f1"].idxmax()].copy()
    best["selector"] = best["variant"].replace({"variance_threshold=0.001": "variance threshold 0.001"})
    best = best.sort_values("val_f1", ascending=False)

    fig, ax = plt.subplots(figsize=(7.4, 4.8))
    sns.barplot(
        data=best,
        y="selector",
        x="val_f1",
        hue="variant",
        palette=PALETTE,
        dodge=False,
        ax=ax,
        legend=False,
    )
    ax.set_xlim(0.56, 0.615)
    style_axes(ax, "Best validation F1 by feature selector", "Validation F1", "")
    for patch, row in zip(ax.patches, best.itertuples(), strict=False):
        ax.text(
            patch.get_width() + 0.001,
            patch.get_y() + patch.get_height() / 2,
            f"{row.val_f1:.4f}",
            va="center",
            fontsize=10,
        )
    save_current("02_best_selector_comparison")


def plot_runtime_tradeoff(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(8.2, 5.6))
    sns.scatterplot(
        data=df,
        x="fit_minutes",
        y="val_f1",
        hue="variant",
        style="maxIter",
        size="regParam",
        sizes=(55, 170),
        palette=PALETTE,
        edgecolor="white",
        linewidth=0.8,
        ax=ax,
    )
    best = df.loc[df["val_f1"].idxmax()]
    ax.annotate(
        "best",
        xy=(best["fit_minutes"], best["val_f1"]),
        xytext=(best["fit_minutes"] + 2.3, best["val_f1"] - 0.011),
        arrowprops={"arrowstyle": "->", "color": "#1f2937", "lw": 1.2},
        fontsize=10,
    )
    style_axes(ax, "Runtime versus validation F1", "Fit time per config (minutes)", "Validation F1")
    ax.legend(frameon=False, bbox_to_anchor=(1.02, 1), loc="upper left", title="")
    save_current("03_runtime_vs_validation_f1")


def plot_standardization_effect(df: pd.DataFrame) -> None:
    summary = (
        df.groupby(["standardization", "maxIter"], as_index=False)
        .agg(mean_val_f1=("val_f1", "mean"), best_val_f1=("val_f1", "max"))
        .sort_values(["standardization", "maxIter"])
    )

    fig, ax = plt.subplots(figsize=(7.6, 5.0))
    sns.barplot(
        data=summary,
        x="standardization",
        y="mean_val_f1",
        hue="maxIter",
        palette=["#6b7280", "#14b8a6"],
        ax=ax,
    )
    ax.set_ylim(0.25, 0.61)
    style_axes(ax, "Standardization effect", "", "Mean validation F1")
    ax.grid(True, axis="y", alpha=0.18)
    ax.grid(False, axis="x")
    for patch in ax.patches:
        height = patch.get_height()
        if pd.notna(height):
            ax.text(
                patch.get_x() + patch.get_width() / 2,
                height + 0.008,
                f"{height:.3f}",
                ha="center",
                va="bottom",
                fontsize=9,
            )
    ax.legend(title="maxIter", frameon=False)
    save_current("04_standardization_effect")


def plot_dev_vs_full(meta: dict) -> None:
    grid = meta["grid"]
    full = meta["full"]
    mode = next(iter(grid["per_mode"].values()))
    scores = pd.DataFrame(
        [
            {"run": "Development grid test", "test_f1": mode["test_f1"], "fit_hours": mode["elapsed_seconds"] / 3600},
            {"run": "Full dataset test", "test_f1": full["test_f1"], "fit_hours": full["fit_seconds"] / 3600},
        ]
    )

    fig, ax = plt.subplots(figsize=(7.2, 4.8))
    sns.barplot(data=scores, x="run", y="test_f1", hue="run", palette=PALETTE, ax=ax, legend=False)
    ax.set_ylim(0.54, 0.615)
    style_axes(ax, "Best config test F1: development versus full data", "", "Test F1")
    ax.grid(True, axis="y", alpha=0.18)
    ax.grid(False, axis="x")
    for patch, row in zip(ax.patches, scores.itertuples(), strict=False):
        ax.text(
            patch.get_x() + patch.get_width() / 2,
            patch.get_height() + 0.002,
            f"{row.test_f1:.4f}\nfit {row.fit_hours:.1f}h",
            ha="center",
            va="bottom",
            fontsize=9,
        )
    save_current("05_dev_vs_full_test_f1")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid", context="notebook")
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "axes.titleweight": "bold",
            "axes.labelsize": 11,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
        }
    )

    df, meta = load_results()
    plot_top_configs(df)
    plot_selector_best(df)
    plot_runtime_tradeoff(df)
    plot_standardization_effect(df)
    plot_dev_vs_full(meta)


if __name__ == "__main__":
    main()
