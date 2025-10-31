
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
from .ratings import build_combined_ease
from .export import to_lookup_table, write_outputs
from .diagnostics import normality_report


def build(schedule_path: str | None = None, sheet_or_table: str | None = None,
          out_csv: str | None = None, out_xlsx: str | None = None) -> str:
    schedule_path = schedule_path or SCHEDULE_XLSX
    sheet_or_table = sheet_or_table or SCHEDULE_SHEET_OR_TABLE
    out_csv = out_csv or str(OUTPUT_CSV)
    out_xlsx = out_xlsx or str(OUTPUT_XLSX)

    # 1) Read schedule
    print("Reading schedule...")
    matchups = read_schedule(schedule_path, sheet_or_table)
    print(f"Schedule loaded: {len(matchups)} matchups, {matchups['team'].nunique()} teams")

    # 2) Fetch NST team metrics for SVA, PP, PK
    print("Fetching NST team metrics...")
    situ = get_all_situations()
    for key, df in situ.items():
        print(f"Fetched {key} data: {len(df)} teams, columns: {df.columns.tolist()}")

    # 3) Build combined opponent ease 0-100 and tiers
    print("Building combined opponent ease...")
    opp_ease = build_combined_ease(situ)
    print(f"Opponent ease built: {len(opp_ease)} teams")
    print("Sample opponent ease values:")
    print(opp_ease.head(10).to_string())

    # 4) Aggregate into lookup table
    print("Building lookup table...")
    lookup = to_lookup_table(matchups, opp_ease)

    # 5) Write outputs
    print("Writing outputs...")
    write_outputs(lookup, csv_path=out_csv, xlsx_path=None)  # user prefers CSV for Power Query

    # 6) Diagnostics: check normality of per-team ease scores and per-week SOS
    print("Generating diagnostics...")
    diag = {
        "opponent_ease": normality_report(opp_ease["OppDefenseScore0to100"], label="Opponent_Ease_0_100"),
        "teamweek_sos": normality_report(lookup["SOS"].str.rstrip('%').astype(float), label="TeamWeek_SOS_0_100"),
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
    args = p.parse_args()
    out_path = build(args.schedule, args.table, args.out_csv, args.out_xlsx)
    print(f"Lookup table written to: {out_path}")


if __name__ == "__main__":
    main()