from __future__ import annotations
import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
from .config import SAVE_PLOTS, PLOTS_DIR


def _safe_numeric_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    keep = [c for c in cols if c in df.columns]
    out = df[keep].apply(pd.to_numeric, errors="coerce")
    return out


def features_diagnostics(df: pd.DataFrame, features: list[str], label_prefix: str = "features") -> dict:
    """Assess normality for each feature and compute correlation matrix.

    Returns a dict with per-feature normality test results and a Pearson correlation
    matrix (as nested dict). Optionally saves plots for each feature and a heatmap.
    """
    out: dict[str, dict] = {"per_feature": {}, "correlation": {}}
    x = _safe_numeric_cols(df, features)

    # Per-feature normality
    for c in x.columns:
        out["per_feature"][c] = normality_report(x[c], label=f"{label_prefix}_{c}")

    # Correlation matrix
    if x.shape[1] >= 2:
        corr = x.corr(method="pearson")
        out["correlation"] = corr.round(3).to_dict()

        if SAVE_PLOTS:
            try:
                import seaborn as sns  # optional
                fig = sns.heatmap(corr, vmin=-1, vmax=1, cmap="coolwarm", annot=True, fmt=".2f").get_figure()
                fig.tight_layout()
                out_path = PLOTS_DIR / f"corr_{label_prefix}.png"
                fig.savefig(out_path, dpi=150)
                plt.close(fig)
                out["correlation_plot_path"] = str(out_path)
            except Exception:
                # Fallback simple matplotlib plot
                fig, ax = plt.subplots(figsize=(6, 5))
                im = ax.imshow(corr.values, vmin=-1, vmax=1, cmap="coolwarm")
                ax.set_xticks(range(len(corr.columns)))
                ax.set_yticks(range(len(corr.index)))
                ax.set_xticklabels(corr.columns, rotation=90)
                ax.set_yticklabels(corr.index)
                fig.colorbar(im, ax=ax)
                fig.tight_layout()
                out_path = PLOTS_DIR / f"corr_{label_prefix}.png"
                fig.savefig(out_path, dpi=150)
                plt.close(fig)
                out["correlation_plot_path"] = str(out_path)

        # Flag highly correlated pairs
        flags = []
        for i, a in enumerate(x.columns):
            for j, b in enumerate(x.columns):
                if j <= i:
                    continue
                r = float(corr.loc[a, b]) if pd.notna(corr.loc[a, b]) else np.nan
                if np.isfinite(r) and abs(r) >= 0.8:
                    flags.append({"feature_a": a, "feature_b": b, "pearson_r": r})
        out["high_correlation_pairs_abs_ge_0.8"] = flags

    return out


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
