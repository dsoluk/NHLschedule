
from __future__ import annotations
import re
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


def _normalize_key(s: str) -> str:
    """Uppercase, remove punctuation/spaces/dots to normalize mapping keys."""
    if s is None:
        return ""
    s = str(s).upper().strip()
    # Replace common unicode apostrophes/dots, then remove non-letters
    s = s.replace("É", "E").replace("É", "E")
    s = re.sub(r"[^A-Z]", "", s)  # keep only A-Z
    return s


def _load_team_mapping():
    """Load team name mapping from Team2TM.xlsx with robust column detection and aliases."""
    base_map: dict[str, str] = {}
    try:
        team_map_df = pd.read_excel(TEAM_MAPPING_XLSX, sheet_name=TEAM_MAPPING_SHEET)
        cols = {c.lower(): c for c in team_map_df.columns}
        # Try multiple possible header names
        city_col = cols.get("city") or cols.get("club") or cols.get("team") or list(team_map_df.columns)[0]
        tm_col = cols.get("tm") or cols.get("abbrev") or list(team_map_df.columns)[-1]
        for _, row in team_map_df.iterrows():
            city = _normalize_key(row[city_col])
            tm = str(row[tm_col]).upper().strip()
            if city and tm:
                base_map[city] = tm
    except Exception as e:
        print(f"Warning: Could not load team mapping from {TEAM_MAPPING_XLSX}: {e}")
        # Fallback mapping for common teams (City/Club -> TM)
        fallback = {
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
            "LOSANGELES": "LAK",
            "MINNESOTA": "MIN",
            "MONTREAL": "MTL",
            "NASHVILLE": "NSH",
            "NEWJERSEY": "NJD",
            "NEWYORKISLANDERS": "NYI",
            "NEWYORKRANGERS": "NYR",
            "OTTAWA": "OTT",
            "PHILADELPHIA": "PHI",
            "PITTSBURGH": "PIT",
            "SANJOSE": "SJS",
            "SEATTLE": "SEA",
            "STLOUIS": "STL",
            "TAMPABAY": "TBL",
            "TORONTO": "TOR",
            "UTAH": "UTA",
            "VANCOUVER": "VAN",
            "VEGAS": "VGK",
            "WASHINGTON": "WSH",
            "WINNIPEG": "WPG",
        }
        base_map.update(fallback)

    # Add alias keys: dotted/short forms mapping directly to TM codes
    alias = {
        "NJ": "NJD", "NJDEVILS": "NJD", "NJERSEY": "NJD", "NJDS": "NJD", "NJ": "NJD",
        "NJDOT": "NJD", "NJNEWJERSEY": "NJD", "NJDEV": "NJD",
        "LA": "LAK", "LAKINGS": "LAK", "LOSANGELESKINGS": "LAK",
        "SJ": "SJS", "SJSANJOSE": "SJS",
        "TB": "TBL", "TBB": "TBL", "TAMPABAYLIGHTNING": "TBL",
    }

    # Also map dotted forms like 'N.J' -> 'NJD', 'L.A' -> 'LAK', etc.
    dotted_alias = {"NJ": "NJD", "LA": "LAK", "SJ": "SJS", "TB": "TBL"}

    # Build final mapping with normalized keys
    mapping: dict[str, str] = {}
    for k, v in base_map.items():
        mapping[_normalize_key(k)] = v
    for k, v in alias.items():
        mapping[_normalize_key(k)] = v
    for k, v in dotted_alias.items():
        mapping[_normalize_key(k)] = v

    # Also map already-correct 3-letter codes to themselves
    for tm in set(mapping.values()):
        mapping[_normalize_key(tm)] = tm

    return mapping


# Load the team mapping once
TEAM_MAPPING = _load_team_mapping()


def _map_to_tm(val: str) -> str:
    key = _normalize_key(val)
    # Handle common dotted inputs explicitly before lookup
    if key in ("NJ", "NJD"):
        return "NJD"
    if key in ("LA", "LAK"):
        return "LAK"
    if key in ("SJ", "SJS"):
        return "SJS"
    if key in ("TB", "TBL"):
        return "TBL"
    return TEAM_MAPPING.get(key, (str(val).upper().strip()[:3]))


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

    # Map teams to NST 3-letter abbreviations using robust normalization
    matchups["team"] = matchups["team"].map(_map_to_tm)
    matchups["opponent"] = matchups["opponent"].map(_map_to_tm)

    return matchups[["date", "week", "team", "opponent", "is_home", "is_light_night"]]