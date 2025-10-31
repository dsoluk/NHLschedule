from __future__ import annotations
import numpy as np
import pandas as pd
from .config import FEATURE_WEIGHTS, SITUATION_WEIGHTS

# Tier mapping per user categories
# 0-30 = Excellent, 31-50 = Good, 51-70 = Average, 71-100 = Difficult
TIER_BINS = [-1, 30, 50, 70, 100]
TIER_LABELS = ["Excellent", "Good", "Average", "Difficult"]


def _ease_from_defense(df_def: pd.DataFrame) -> pd.DataFrame:
    """Compute 0–100 ease score from defense metrics (lower against => harder defense).

    Input columns: team + FEATURE_WEIGHTS keys
    Robust to small/empty inputs and missing values. Returns neutral 50s if
    distribution stats cannot be computed.
    """
    # Debug input dataframe
    print(f"_ease_from_defense input: {len(df_def) if df_def is not None else 0} rows")
    if df_def is not None and len(df_def) > 0:
        print(f"Input columns: {df_def.columns.tolist()}")
        print(f"First few rows:\n{df_def.head(3).to_string()}")

    if df_def is None or len(df_def) == 0:
        print("WARNING: Empty defense dataframe")
        return pd.DataFrame({"team": [], "ease_score": []})

    # Ensure required columns exist
    required = ["team"] + list(FEATURE_WEIGHTS.keys())
    missing = [c for c in required if c not in df_def.columns]
    if missing:
        print(f"WARNING: Missing columns in defense dataframe: {missing}")
        # If structure changed upstream, return neutral scores to avoid crash
        teams = pd.Index(df_def.get("team", pd.Series([], dtype=str))).astype(str)
        return pd.DataFrame({"team": teams, "ease_score": np.full(len(teams), 50.0)})

    # Drop rows with any NA in required feature columns
    df = df_def[required].copy()
    na_before = len(df)
    df = df.dropna(subset=FEATURE_WEIGHTS.keys())
    na_after = len(df)
    if na_before > na_after:
        print(f"Dropped {na_before - na_after} rows with NA values")

    if len(df) < 3:
        print("WARNING: Not enough teams to compute percentiles reliably")
        # Not enough teams to compute percentiles reliably; return neutral
        teams = pd.Index(df_def["team"].astype(str)).unique()
        return pd.DataFrame({"team": teams, "ease_score": np.full(len(teams), 50.0)})

    # Z-score per feature; guard zero std
    for col in FEATURE_WEIGHTS:
        mu = df[col].mean()
        sigma = df[col].std(ddof=0)
        print(f"Feature {col}: mean={mu:.2f}, std={sigma:.2f}")
        if not np.isfinite(sigma) or sigma == 0:
            print(f"WARNING: Zero or invalid std for {col}")
            z = pd.Series(0.0, index=df.index)
        else:
            z = (df[col] - mu) / sigma
        df[col + "_z"] = z

    # Calculate weighted composite score
    composite = sum(FEATURE_WEIGHTS[c] * df[c + "_z"] for c in FEATURE_WEIGHTS)
    print(
        f"Composite score stats: min={composite.min():.2f}, max={composite.max():.2f}, mean={composite.mean():.2f}, std={composite.std():.2f}")

    # Lower against numbers = tougher defense for skaters => ease is negative of composite
    ease_z = -composite
    print(
        f"Ease_z stats: min={ease_z.min():.2f}, max={ease_z.max():.2f}, mean={ease_z.mean():.2f}, std={ease_z.std():.2f}")

    vals = ease_z.to_numpy()
    vals = vals[np.isfinite(vals)]
    if vals.size == 0:
        print("WARNING: No finite values in ease_z")
        teams = pd.Index(df["team"].astype(str)).unique()
        return pd.DataFrame({"team": teams, "ease_score": np.full(len(teams), 50.0)})

    q5, q95 = np.nanpercentile(vals, [5, 95])
    print(f"Percentiles: q5={q5:.2f}, q95={q95:.2f}")

    if not np.isfinite(q5) or not np.isfinite(q95) or q95 == q5:
        print("WARNING: Invalid percentiles")
        score = np.full(len(ease_z), 50.0)
    else:
        score = 100 * (ease_z - q5) / (q95 - q5)
        score = np.clip(score, 0, 100)
        print(
            f"Final score stats: min={np.min(score):.1f}, max={np.max(score):.1f}, mean={np.mean(score):.1f}, std={np.std(score):.1f}")

    out = df[["team"]].copy()
    out["ease_score"] = score
    print(f"Output ease scores (first few):\n{out.head(5).to_string()}")
    return out


def build_combined_ease(situ_dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Combine situation-specific ease scores into a single opponent ease 0–100.

    situ_dfs keys expected: 'sva', 'pp', 'pk'
    Situation weights drawn from config SITUATION_WEIGHTS (sva=0.75, pp=0.10, pk=0.10)
    """
    print("Building combined ease scores from situation dataframes")
    for key in situ_dfs:
        print(f"Situation {key}: {len(situ_dfs[key])} rows")

    parts = {}
    for key, df in situ_dfs.items():
        print(f"Processing {key} situation...")
        parts[key] = _ease_from_defense(df).rename(columns={"ease_score": f"ease_{key}"})
        print(f"Generated ease scores for {key}: {len(parts[key])} teams")
        if len(parts[key]) > 0:
            print(f"Sample of {key} scores:\n{parts[key].head(3).to_string()}")

    # Establish team universe
    team_sets = [p["team"] for p in parts.values() if "team" in p and len(p) > 0]
    if not team_sets:
        print("ERROR: No teams found in any situation")
        return pd.DataFrame({"team": [], "OppDefenseScore0to100": [], "OppDefenseTier": []})

    base = pd.DataFrame({"team": pd.Index(pd.concat(team_sets)).unique()})
    print(f"Combined team universe: {len(base)} teams")

    # Merge each part
    for key in parts:
        if len(parts[key]) > 0:  # Only merge if there are teams
            before_merge = len(base)
            base = base.merge(parts[key], on="team", how="left")
            print(f"Merged {key} data: {before_merge} rows before, {len(base)} rows after")

    # Fill potential missing with mean per column; if mean is NaN, use neutral 50
    ease_cols = [c for c in base.columns if c.startswith("ease_")]
    for c in ease_cols:
        col_mean = pd.to_numeric(base[c], errors="coerce").mean()
        fill_value = float(col_mean) if np.isfinite(col_mean) else 50.0
        missing_before = base[c].isna().sum()
        base[c] = pd.to_numeric(base[c], errors="coerce").fillna(fill_value)
        print(f"Filled {missing_before} missing values in {c} with {fill_value:.1f}")

    # Combine scores using weights
    w_sva = SITUATION_WEIGHTS.get("sva", 0.75)
    w_pp = SITUATION_WEIGHTS.get("pp", 0.10)
    w_pk = SITUATION_WEIGHTS.get("pk", 0.10)
    total_w = w_sva + w_pp + w_pk
    if total_w <= 0:
        print("WARNING: Invalid weights, using defaults")
        w_sva, w_pp, w_pk = 0.75, 0.10, 0.10
        total_w = 0.95
    w_sva, w_pp, w_pk = [w / total_w for w in (w_sva, w_pp, w_pk)]
    print(f"Weights (normalized): sva={w_sva:.2f}, pp={w_pp:.2f}, pk={w_pk:.2f}")

    # Calculate combined score
    combined = (
            w_sva * base.get("ease_sva", 50.0)
            + w_pp * base.get("ease_pp", 50.0)
            + w_pk * base.get("ease_pk", 50.0)
    )
    combined = np.clip(combined, 0, 100)

    print(
        f"Combined score stats: min={combined.min():.1f}, max={combined.max():.1f}, mean={combined.mean():.1f}, std={combined.std():.1f}")
    print(f"Sample combined scores: {combined.head(10).tolist()}")

    out = base[["team"]].copy()
    out["OppDefenseScore0to100"] = np.rint(combined).astype(int)
    out["OppDefenseTier"] = pd.cut(out["OppDefenseScore0to100"], bins=TIER_BINS, labels=TIER_LABELS)

    print(f"Final output dataframe: {len(out)} rows")
    print(f"OppDefenseScore0to100 values: {out['OppDefenseScore0to100'].tolist()}")
    print(f"OppDefenseTier distribution: {out['OppDefenseTier'].value_counts().to_dict()}")

    return out