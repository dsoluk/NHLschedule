
from __future__ import annotations
import pandas as pd
from pandas import Timestamp
from .config import WEEK_START_DAY, LITENITE_METHOD, LITENITE_MAX_GAMES, LITENITE_FRACTION, TEAM_MAPPING_XLSX, TEAM_MAPPING_SHEET


EXPECTED_COLS = {
    "date": ["date", "game_date"],
    "home": ["home", "home_team", "h"],
    "away": ["away", "away_team", "a"],
    "week": ["week", "wk"],
}


def _find_col(df: pd.DataFrame, keys: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for k in keys:
        if k in cols:
            return cols[k]
    return None


def _load_team_mapping():
    """Load team name mapping from Team2TM.xlsx"""
    try:
        team_map_df = pd.read_excel(TEAM_MAPPING_XLSX, sheet_name=TEAM_MAPPING_SHEET)
        # Assuming the columns are 'City' (from schedule) and 'TM' (NST code)
        return dict(zip(team_map_df['City'].str.upper(), team_map_df['TM']))
    except Exception as e:
        print(f"Warning: Could not load team mapping from {TEAM_MAPPING_XLSX}: {e}")
        # Fallback mapping for common teams (basic 3-letter codes)
        return {
            "ANAHEIM": "ANA",
            "BOSTON": "BOS",
            "BUFFALO": "BUF",
            "CALGARY": "CGY",
            "CAROLINA": "CAR",
            "CHICAGO": "CHI",
            "COLORADO": "COL",
            "COLUMBUS": "CBJ",
            "DALLAS": "DAL",
            "DETROIT": "DET",
            "EDMONTON": "EDM",
            "FLORIDA": "FLA",
            "LOS ANGELES": "LAK",
            "MINNESOTA": "MIN",
            "MONTREAL": "MTL",
            "NASHVILLE": "NSH",
            "NEW JERSEY": "NJD",
            "NEW YORK ISLANDERS": "NYI",
            "NEW YORK RANGERS": "NYR",
            "OTTAWA": "OTT",
            "PHILADELPHIA": "PHI",
            "PITTSBURGH": "PIT",
            "SAN JOSE": "SJS",
            "SEATTLE": "SEA",
            "ST. LOUIS": "STL",
            "TAMPA BAY": "TBL",
            "TORONTO": "TOR",
            "UTAH": "UTA",
            "VANCOUVER": "VAN",
            "VEGAS": "VGK",
            "WASHINGTON": "WSH",
            "WINNIPEG": "WPG",
        }


# Load the team mapping once
TEAM_MAPPING = _load_team_mapping()


def read_schedule(xlsx_path: str, sheet_or_table: str = "schedule") -> pd.DataFrame:
    """Read the Excel schedule and return per-team matchups rows.

    Output columns: date (date), week (int), team (str), opponent (str), is_home (bool), is_light_night (bool)
    """
    df = pd.read_excel(xlsx_path, sheet_name=sheet_or_table)
    # Normalize column names and find required columns
    date_col = _find_col(df, EXPECTED_COLS["date"]) or "Date"
    home_col = _find_col(df, EXPECTED_COLS["home"]) or "Home"
    away_col = _find_col(df, EXPECTED_COLS["away"]) or "Away"
    week_col = _find_col(df, EXPECTED_COLS["week"])  # optional

    df[date_col] = pd.to_datetime(df[date_col]).dt.date

    if week_col is None:
        # derive week numbers aligned to WEEK_START_DAY
        s = pd.to_datetime(df[date_col])
        df["week"] = s.dt.to_period(f"W-{WEEK_START_DAY}").apply(lambda p: p.week)
    else:
        df = df.rename(columns={week_col: "week"})

    # Normalize to team/opponent rows (double-entry)
    home = df[[date_col, "week", home_col, away_col]].rename(columns={date_col: "date", home_col: "team", away_col: "opponent"})
    home["is_home"] = True
    away = df[[date_col, "week", home_col, away_col]].rename(columns={date_col: "date", away_col: "team", home_col: "opponent"})
    away["is_home"] = False
    matchups = pd.concat([home, away], ignore_index=True)

    # game count per calendar day (unique NHL games)
    games_per_day = matchups.groupby("date").size().div(2)  # two rows per game

    # LiteNite calculation per config
    if LITENITE_METHOD == "by_games_threshold":
        light_mask = games_per_day <= LITENITE_MAX_GAMES
    elif LITENITE_METHOD == "by_fraction_of_teams":
        # total teams approximated by unique teams in schedule
        total_teams = pd.Index(pd.unique(pd.concat([matchups["team"], matchups["opponent"]]))).nunique()
        light_mask = games_per_day < (LITENITE_FRACTION * (total_teams / 2))
    else:
        raise ValueError(f"Unknown LITENITE_METHOD: {LITENITE_METHOD}")

    matchups = matchups.merge(light_mask.rename("is_light_night"), left_on="date", right_index=True, how="left")
    # Avoid chained-assignment; assign the filled series back to the column
    matchups["is_light_night"] = matchups["is_light_night"].fillna(False)

    # Map teams to NST 3-letter abbreviations using the team mapping
    matchups["team"] = matchups["team"].astype(str).str.upper().map(
        lambda x: TEAM_MAPPING.get(x, x[:3])
    )
    matchups["opponent"] = matchups["opponent"].astype(str).str.upper().map(
        lambda x: TEAM_MAPPING.get(x, x[:3])
    )

    return matchups[["date", "week", "team", "opponent", "is_home", "is_light_night"]]