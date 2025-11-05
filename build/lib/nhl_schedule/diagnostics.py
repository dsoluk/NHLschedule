from __future__ import annotations
import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
from .config import SAVE_PLOTS, PLOTS_DIR


def normality_report(series: pd.Series, label: str = "values") -> dict:
    """Run normality diagnostics (D'Agostino K² and Shapiro-Wilk).

    Returns a dict with statistics and p-values. Also optionally saves a histogram + QQ plot.
    """
    x = pd.to_numeric(series.dropna(), errors="coerce").dropna().values
    result = {
        "n": int(x.size),
        "mean": float(np.mean(x)) if x.size else None,
        "std": float(np.std(x, ddof=0)) if x.size else None,
        "dagostino_k2_stat": None,
        "dagostino_k2_p": None,
        "shapiro_stat": None,
        "shapiro_p": None,
        "is_normal_alpha_0.05": None,
    }
    if x.size >= 8:  # scipy normaltest minimum recommendation
        k2_stat, k2_p = stats.normaltest(x, nan_policy='omit')
        result["dagostino_k2_stat"] = float(k2_stat)
        result["dagostino_k2_p"] = float(k2_p)
    if 3 <= x.size <= 5000:  # Shapiro recommended upper bound
        sh_stat, sh_p = stats.shapiro(x)
        result["shapiro_stat"] = float(sh_stat)
        result["shapiro_p"] = float(sh_p)
    # Determine normality by both tests when available (p>0.05)
    pvals = [p for p in [result["dagostino_k2_p"], result["shapiro_p"]] if p is not None]
    result["is_normal_alpha_0.05"] = all(p > 0.05 for p in pvals) if pvals else None

    if SAVE_PLOTS and x.size >= 3:
        fig, axes = plt.subplots(1, 2, figsize=(10, 4))
        axes[0].hist(x, bins=10, color="#4e79a7", edgecolor="white")
        axes[0].set_title(f"Histogram: {label}")
        stats.probplot(x, dist="norm", plot=axes[1])
        axes[1].set_title("QQ Plot vs Normal")
        fig.tight_layout()
        out_path = PLOTS_DIR / f"normality_{label.replace(' ', '_')}.png"
        fig.savefig(out_path, dpi=150)
        plt.close(fig)
        result["plot_path"] = str(out_path)
    return result
