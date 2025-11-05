
from __future__ import annotations
from pathlib import Path
from datetime import date

# ---- User-configurable settings ----
# Excel schedule source
SCHEDULE_XLSX = r"C:\\Users\\soluk\\OneDrive\\Documents\\FantasyNHL\\Sources\\AG_2526_Schedule.xlsx"
SCHEDULE_SHEET_OR_TABLE = "Schedule"
TEAM_MAPPING_XLSX = r"C:\\Users\\soluk\\PycharmProjects\\NHLschedule\\Team2TM.xlsx"
TEAM_MAPPING_SHEET = "Team2TM"

# Season label used by Natural Stat Trick (YYYYYYYY for single NHL season)
SEASON_LABEL = "20252026"
SEASON_START = date(2025, 10, 1)

# Week handling
# If your schedule already has a Week column, it will be used. Otherwise, derive week
# numbers using the start day below.
WEEK_START_DAY = "MON"  # options: MON, TUE, WED, THU, FRI, SAT, SUN

# LiteNite configuration
# Choose one method:
# - method = "by_games_threshold": light night if total games that day <= LITENITE_MAX_GAMES
# - method = "by_fraction_of_teams": light night if games that day < LITENITE_FRACTION * total_teams
LITENITE_METHOD = "by_games_threshold"
LITENITE_MAX_GAMES = 5
LITENITE_FRACTION = 0.4  # used only for by_fraction_of_teams

# Situation weights for opponent difficulty (must sum to 1.0)
SITUATION_WEIGHTS = {
    "sva": 0.75,  # 5v5 score & venue adjusted (NST 'sva'). We will also set loc=B for both venues combined via backend param.
    "pp": 0.10,   # Power Play
    "pk": 0.10,   # Penalty Kill (opponent's PK strength lowers ease for skaters on PP)
    "remainder": 0.05,  # kept for future expansion; currently unused but ensures total 1.0 if tweaked
}

# Defense metric feature weights (these match your preference from earlier)
FEATURE_WEIGHTS = {  # higher against numbers -> tougher defense; we invert later for ease
    "xga60": 0.35,
    "sca60": 0.20,
    "hdca60": 0.20,
    "ga60": 0.15,
    "sa60": 0.10,
}

# Caching
CACHE_DIR = Path.cwd() / "_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_REFRESH_DAYS = 1  # refresh NST tables at most once per day

# Output paths
OUTPUT_DIR = Path.cwd() / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_CSV = OUTPUT_DIR / "lookup_table.csv"
OUTPUT_XLSX = OUTPUT_DIR / "lookup_table.xlsx"

# Diagnostics
SAVE_PLOTS = True
PLOTS_DIR = Path.cwd() / "_plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)