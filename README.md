NHLschedule

Overview
- This repository builds an NHL schedule “lookup table” that blends team schedule information with Natural Stat Trick (NST) team metrics (5v5 SVA, Power Play, Penalty Kill) to produce:
  - output/lookup_table.csv — a team-week table with Opponent Defense strength (SOS) and related fields suitable for Excel/Power Query or downstream analytics.
  - output/opponent_offense_lookup.csv — a parallel team-week table focusing on the opponent’s Offense matchup per week.
  - output/lookup_table.diagnostics.json — normality checks and feature diagnostics for transparency.
  - output/team_defense_lookup.csv — simple Team → Opponent Defense score (0–100) mapping for quick consumption.
  - output/team_offense_lookup.csv — simple Team → Opponent Offense score (0–100) mapping.

You can run this project as a standalone CLI or import it from another project (for example, NSTstats) as a small library.


Requirements
- Python 3.10 or newer
- Dependencies listed in requirements.txt (install instructions below)
- An NHL schedule spreadsheet (default: Team2TM.xlsx at the repo root). The code can also read a named Excel sheet or an Excel table.


Installation
1) Clone this repository.
2) Install dependencies (choose one):
   - pip install -r requirements.txt
   - Or install in editable mode for development: pip install -e .


Configuration
Key settings live in nhl_schedule/config.py:
- SCHEDULE_XLSX: default path to the schedule spreadsheet (Team2TM.xlsx).
- SCHEDULE_SHEET_OR_TABLE: sheet or Excel table name to read from SCHEDULE_XLSX.
- OUTPUT_DIR: output directory (default: output/).
- OUTPUT_CSV / OUTPUT_XLSX: output file paths.
- SEASON_LABEL: season string like 20252026 used for fetching/caching NST data.

Caching of NST data
- When fetching team tables from Natural Stat Trick, responses are cached as Parquet files under _cache/ to speed up repeated runs.
- You can force-refresh all caches with the CLI flag --refresh-cache.


Running as a standalone tool (CLI)
After installing dependencies, run the builder module directly:

  python -m nhl_schedule.build_lookup \
    --schedule Team2TM.xlsx \
    --table Sheet1 \
    --out_csv output/lookup_table.csv \
    --refresh-cache \
    --include-last-season \
    --weeks 25

Flags (all optional unless your setup differs from defaults):
- --schedule: Path to the Excel file with the schedule (defaults to config.SCHEDULE_XLSX).
- --table: Sheet or Excel Table name (defaults to config.SCHEDULE_SHEET_OR_TABLE).
- --out_csv: Where to write the CSV (defaults to config.OUTPUT_CSV).
- --out_xlsx: Optional XLSX output path (CSV is primary and preferred for Power Query).
- --refresh-cache: Refetch NST team tables even if cached locally.
- --include-last-season: Blend prior season scores (defense and offense) into early weeks for stability.
- --weeks: Number of regular-season weeks to consider for blending scale (default 25).

Outputs
- output/lookup_table.csv — primary deliverable (Defense-facing SOS and weekly matchup tiers)
- output/opponent_offense_lookup.csv — weekly Opponent Offense matchup per team/week
- output/lookup_table.diagnostics.json — diagnostic JSON
- output/team_defense_lookup.csv — per-Team Opponent Defense score and tier
- output/team_offense_lookup.csv — per-Team Opponent Offense score and tier
- _plots/*.png — normality plots produced during diagnostics


Using from Python (library style)
You can call the build() function to produce outputs programmatically:

  from nhl_schedule.build_lookup import build

  out_csv_path = build(
      schedule_path="Team2TM.xlsx",       # or a custom path
      sheet_or_table="Sheet1",            # or a named Excel table
      out_csv="output/lookup_table.csv",
      out_xlsx=None,                       # optional
      refresh_cache=False,
      include_last_season=True,
      weeks=25,
  )
  print("Wrote:", out_csv_path)

The function returns the path to the written CSV. It also emits:
- opponent_offense_lookup.csv next to out_csv (weekly offense matchup table)
- team_defense_lookup.csv and team_offense_lookup.csv (simple per-Team score maps)


Integrating with the NSTstats project
There are two common ways to integrate:

1) Consume the generated CSVs (no Python dependency):
   - Add this repo (or its output directory) as a data source.
   - Run the CLI here to refresh output/lookup_table.csv.
   - In NSTstats, import output/lookup_table.csv (e.g., with pandas or Power Query) and join on Team/Week fields as needed.

2) Use as a dependency and call from code:
   - Add this project to NSTstats requirements (either a local path or VCS URL) and pip install it.
   - Import and run the builder when you need to refresh data:

       from nhl_schedule.build_lookup import build
       csv_path = build(refresh_cache=True)
       # Optionally read the small opponent-ease mapping:
       # Path is derived from csv_path
       import pandas as pd
       from pathlib import Path
       opp_lookup = pd.read_csv(Path(csv_path).with_name("opponent_ease_lookup.csv"))

   - Alternatively, if you already have your own schedule DataFrame and want just the opponent defense/offense scores, explore:
     - nhl_schedule.nst_fetch.get_all_situations() — fetches team metrics per situation
     - nhl_schedule.ratings.build_combined_ease() — produces per-team OppDefenseScore0to100 (0–100)
     - nhl_schedule.ratings.build_combined_offense() — produces per-team OppOffenseScore0to100 (0–100)
     - nhl_schedule.export.to_lookup_table() — builds the full team-week table from a schedule + defense scores
     - nhl_schedule.export.to_offense_lookup_table() — builds the weekly offense matchup table from a schedule + offense scores

Opponent Offense methodology
- We mirror the Team Defense methodology but use the “For” versions of the same metrics: xGF/60, SCF/60, HDCF/60, GF/60, SF/60.
- Each metric is standardized (z-score) across teams, then combined using weights matching defense by default:
  xGF/60 0.35, SCF/60 0.20, HDCF/60 0.20, GF/60 0.15, SF/60 0.10.
- The composite is scaled to 0–100 using the 5th–95th percentile window and clipped to [0, 100]. Higher means a stronger offense.
- Situation weighting mirrors defense (config.SITUATION_WEIGHTS): SVA heavy, PP and PK lighter. You may tune these in nhl_schedule/config.py.


Schedule input
- The schedule reader expects a tabular structure with at least team and opponent columns. See nhl_schedule/schedule_io.py for exact expectations.
- Default file Team2TM.xlsx is included for convenience; you can point to your own schedule file via --schedule and --table.


Troubleshooting
- Getting old NST values? Re-run with --refresh-cache to bypass _cache/.
- Wrong sheet/table name? Verify SCHEDULE_SHEET_OR_TABLE in config.py or pass --table.
- Windows path issues? Use quotes around paths that contain spaces.
- Networking limits when hitting NST? Try again later or reduce refreshes; caching minimizes repeated requests.


Development notes
- Source layout is under nhl_schedule/.
- Entry point is build_lookup.py (CLI via python -m nhl_schedule.build_lookup).
- Outputs go to output/ by default; plots go to _plots/; caches go to _cache/.


### Maintenance & Updates

If you make changes to the source code and use this project as a dependency in another tool (like `NSTstats`), you need to update the installed version in that environment.

**1. If you installed in "Editable" mode:**
If you originally ran `pip install -e .`, your changes should reflect immediately without a reinstall.

**2. If you installed normally:**
Run the following from this repository's root to push your latest changes to your Python environment:
```bash
pip install . --upgrade
```
