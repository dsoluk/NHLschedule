
from __future__ import annotations
import argparse
from pathlib import Path
import json
import pandas as pd
from .config import (
    SCHEDULE_XLSX,
    SCHEDULE_SHEET_OR_TABLE,
    OUTPUT_CSV,
    OUTPUT_XLSX,
)
from .schedule_io import read_schedule
from .nst_fetch import get_all_situations
from . import nst_fetch as nst_mod
from .ratings import build_combined_ease
from .export import to_lookup_table, write_outputs
from .diagnostics import normality_report, features_diagnostics
from .config import SAVE_PLOTS, PLOTS_DIR
from .config import SEASON_LABEL, OUTPUT_DIR


def build(schedule_path: str | None = None, sheet_or_table: str | None = None,
          out_csv: str | None = None, out_xlsx: str | None = None,
          refresh_cache: bool = False,
          include_last_season: bool = False,
          weeks: int = 25) -> str:
    schedule_path = schedule_path or SCHEDULE_XLSX
    sheet_or_table = sheet_or_table or SCHEDULE_SHEET_OR_TABLE
    out_csv = out_csv or str(OUTPUT_CSV)
    out_xlsx = out_xlsx or str(OUTPUT_XLSX)

    # 1) Read schedule
    print("Reading schedule...")
    matchups = read_schedule(schedule_path, sheet_or_table)
    print(f"Schedule loaded: {len(matchups)} matchups, {matchups['team'].nunique()} teams")

    # 2) Fetch NST team metrics for SVA, PP, PK (current season)
    print("Fetching NST team metrics...")
    # Apply refresh flag to NST fetch module
    if refresh_cache:
        print("Forcing NST cache refresh per --refresh-cache flag")
        nst_mod.FORCE_CACHE_REFRESH = True
    situ = get_all_situations()
    for key, df in situ.items():
        print(f"Fetched {key} data: {len(df)} teams, columns: {df.columns.tolist()}")

    # Optionally fetch prior season situational data
    situ_last = None
    if include_last_season:
        y1 = int(SEASON_LABEL[:4]) - 1
        y2 = int(SEASON_LABEL[4:]) - 1
        prev_label = f"{y1:04d}{y2:04d}"
        print(f"Including last season: {prev_label}")
        situ_last = get_all_situations(season_label=prev_label)

    # 3) Build combined team defense (formerly "opponent ease") 0-100 and tiers
    print("Building combined Team Defense scores (0-100)...")
    opp_ease = build_combined_ease(situ)
    print(f"Team Defense scores built: {len(opp_ease)} teams")
    print("Sample Team Defense values:")
    print(opp_ease.head(10).to_string())

    # 3b) If last season requested, build its team defense for blending
    opp_ease_last = None
    if include_last_season and situ_last is not None:
        print("Building last-season combined Team Defense...")
        opp_ease_last = build_combined_ease(situ_last)

    # 4) Aggregate into lookup table (with optional blending by week)
    print("Building lookup table...")
    lookup = to_lookup_table(matchups, opp_ease, opp_ease_last=opp_ease_last, weeks=weeks)

    # 5) Write outputs
    print("Writing outputs...")
    write_outputs(lookup, csv_path=out_csv, xlsx_path=None)  # user prefers CSV for Power Query

    # 5b) Additional Team->Score (Team Defense) lookup for library use
    try:
        # Team Defense tiers for goalie evaluation (opposite labels vs skater ease)
        TEAM_DEF_TIER_BINS = [-1, 25, 50, 75, 100]
        TEAM_DEF_TIER_LABELS = ["Weak", "Meh", "Good", "Excellent"]

        def _tier_from_score_team_def(val: float | int) -> str:
            try:
                v = float(val)
            except Exception:
                v = 50.0
            # Bin manually to avoid pandas dependency for this simple map
            if v <= TEAM_DEF_TIER_BINS[1]:
                return TEAM_DEF_TIER_LABELS[0]
            if v <= TEAM_DEF_TIER_BINS[2]:
                return TEAM_DEF_TIER_LABELS[1]
            if v <= TEAM_DEF_TIER_BINS[3]:
                return TEAM_DEF_TIER_LABELS[2]
            return TEAM_DEF_TIER_LABELS[3]

        opp_lookup = opp_ease.rename(columns={"team": "Team", "OppDefenseScore0to100": "Score"})[["Team", "Score"]].copy()
        opp_lookup["TIER"] = opp_lookup["Score"].map(_tier_from_score_team_def)

        # Convert Team codes to NST dotted convention to match lookup_table (e.g., L.A, T.B, S.J, N.J)
        def _to_nst_dotted(tm: str) -> str:
            m = {
                "LAK": "L.A",
                "TBL": "T.B",
                "SJS": "S.J",
                "NJD": "N.J",
            }
            tms = str(tm).upper().strip()
            return m.get(tms, tms)

        opp_lookup["Team"] = opp_lookup["Team"].map(_to_nst_dotted)
        # Write as Team Defense lookup
        opp_lookup_path = str(Path(out_csv).with_name("team_defense_lookup.csv"))
        opp_lookup.to_csv(opp_lookup_path, index=False)
        print(f"Team Defense lookup written to: {opp_lookup_path}")

        # Generate plots: xga60 and ga60 vs Team Defense score (using SVA situation)
        if SAVE_PLOTS and "sva" in situ:
            try:
                import matplotlib.pyplot as plt
                # Merge raw SVA metrics with scores (use original team codes for join)
                sva = situ["sva"][["team", "xga60", "ga60"]].copy()
                merged = opp_ease.merge(sva, on="team", how="left")

                def _scatter(xcol: str, title: str, filename: str):
                    x = pd.to_numeric(merged[xcol], errors="coerce")
                    y = pd.to_numeric(merged["OppDefenseScore0to100"], errors="coerce")
                    m = pd.notna(x) & pd.notna(y)
                    if m.sum() == 0:
                        return
                    fig, ax = plt.subplots(figsize=(6, 4))
                    ax.scatter(x[m], y[m], color="#4e79a7", alpha=0.8, edgecolor="white")
                    ax.set_xlabel(xcol)
                    ax.set_ylabel("Team Defense Score (0-100)")
                    ax.set_title(title)
                    fig.tight_layout()
                    out_path = PLOTS_DIR / filename
                    fig.savefig(out_path, dpi=150)
                    plt.close(fig)
                    print(f"Saved plot: {out_path}")

                _scatter("xga60", "xGA/60 vs Team Defense Score (SVA)", "team_defense_vs_xga60.png")
                _scatter("ga60", "GA/60 vs Team Defense Score (SVA)", "team_defense_vs_ga60.png")
            except Exception as pe:
                print(f"WARNING: Failed to generate Team Defense plots: {pe}")
    except Exception as e:
        print(f"WARNING: Failed to write Team Defense lookup CSV: {e}")

    # 6) Diagnostics: check normality of per-team ease scores and per-week SOS
    print("Generating diagnostics...")
    # Feature diagnostics for each situation (current season)
    feature_cols = [
        "xga60", "sca60", "hdca60", "ga60", "sa60"
    ]
    feature_diag = {
        k: features_diagnostics(df, feature_cols, label_prefix=f"{k}") for k, df in situ.items()
    }

    diag = {
        "opponent_ease": normality_report(opp_ease["OppDefenseScore0to100"], label="Opponent_Ease_0_100"),
        "teamweek_sos": normality_report(lookup["SOS"].str.rstrip('%').astype(float), label="TeamWeek_SOS_0_100"),
        "features": feature_diag,
    }
    diag_path = Path(out_csv).with_suffix(".diagnostics.json")
    diag_path.write_text(json.dumps(diag, indent=2))

    return out_csv


def main():
    p = argparse.ArgumentParser(description="Build NHL Excel lookup table from schedule + NST metrics")
    p.add_argument("--schedule", default=SCHEDULE_XLSX, help="Path to schedule Excel file")
    p.add_argument("--table", default=SCHEDULE_SHEET_OR_TABLE, help="Sheet or table name")
    p.add_argument("--out_csv", default=str(OUTPUT_CSV), help="Output CSV path")
    p.add_argument("--out_xlsx", default=str(OUTPUT_XLSX), help="Optional XLSX output path")
    p.add_argument("--refresh-cache", action="store_true", help="Bypass NST cache and refetch all team tables")
    p.add_argument("--include-last-season", action="store_true", help="Blend prior season into Opponent Ease using sliding week weights")
    p.add_argument("--weeks", type=int, default=25, help="Number of regular-season weeks for blending scale (default 25)")
    args = p.parse_args()
    out_path = build(
        args.schedule,
        args.table,
        args.out_csv,
        args.out_xlsx,
        refresh_cache=args.refresh_cache,
        include_last_season=args.include_last_season,
        weeks=args.weeks,
    )
    print(f"Lookup table written to: {out_path}")


if __name__ == "__main__":
    main()