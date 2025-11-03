from __future__ import annotations
import pandas as pd
import numpy as np
from datetime import date, timedelta
from .config import WEEK_START_DAY


def to_lookup_table(matchups: pd.DataFrame, opp_ease: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per team/week and attach opponent difficulty.

    Returns columns: TM, Week, Games, LiteNite, Opponents, SOS, MatchUp, Key
    - SOS is the average OppDefenseScore0to100 as a percent string
    - MatchUp uses opponent tier mapping already encoded
    """
    # Debug info about input data
    print(f"Matchups dataframe: {len(matchups)} rows")
    print(f"Team codes in matchups: {matchups['team'].unique().tolist()[:5]}...")
    print(f"Opponent codes in matchups: {matchups['opponent'].unique().tolist()[:5]}...")
    print(f"opp_ease shape: {opp_ease.shape}, columns: {opp_ease.columns.tolist()}")
    print(f"Team codes in opp_ease: {opp_ease['team'].unique().tolist()}")

    # Verify OppDefenseScore0to100 values are not all the same
    if 'OppDefenseScore0to100' in opp_ease.columns:
        scores = opp_ease['OppDefenseScore0to100']
        print(
            f"OppDefenseScore0to100 stats: min={scores.min()}, max={scores.max()}, mean={scores.mean():.1f}, std={scores.std():.1f}")
        print(f"OppDefenseScore0to100 values: {scores.tolist()}")
    else:
        print("ERROR: OppDefenseScore0to100 column missing from opp_ease dataframe")

    # Important: Make sure team codes match between dataframes
    matchup_teams = set(matchups['opponent'].unique())
    ease_teams = set(opp_ease['team'].unique())
    missing_teams = matchup_teams - ease_teams
    if missing_teams:
        print(f"WARNING: {len(missing_teams)} teams in matchups not found in opp_ease: {missing_teams}")

    # Merge the dataframes
    t = matchups.merge(
        opp_ease.rename(columns={"team": "opponent"}),
        on="opponent",
        how="left"
    )

    # Helper flags at matchup level
    # Back-to-back: mark both games when a team plays on consecutive days
    t = t.sort_values(["team", "date"]).copy()
    t["prev_date"] = t.groupby("team")["date"].shift(1)
    t["next_date"] = t.groupby("team")["date"].shift(-1)
    t["is_b2b"] = (
        (pd.to_datetime(t["date"]) - pd.to_datetime(t["prev_date"]) == pd.Timedelta(days=1)) |
        (pd.to_datetime(t["next_date"]) - pd.to_datetime(t["date"]) == pd.Timedelta(days=1))
    )
    t["is_b2b"] = t["is_b2b"].fillna(False)
    t["is_away"] = ~t["is_home"].astype(bool)

    # Today-aware metrics
    today = date.today()
    # Determine current season week label using the next scheduled game on/after today
    # This avoids mismatches between calendar ISO week and season week numbering in the sheet
    future_mask = pd.to_datetime(t["date"]).dt.date >= today
    if future_mask.any():
        next_date = pd.to_datetime(t.loc[future_mask, "date"]).dt.date.min()
        cweek_series = t.loc[pd.to_datetime(t["date"]).dt.date == next_date, "week"]
        current_week = int(cweek_series.mode().iloc[0]) if not cweek_series.empty else int(t["week"].max())
    else:
        # Season completed relative to today; use the last week label
        current_week = int(t["week"].max())
    # Remaining games this week (incl today) per team
    rem_cur_week = (
        t[(t["week"] == current_week) & (pd.to_datetime(t["date"]).dt.date >= today)]
        .groupby("team")["opponent"].count()
    )
    print(f"Current week detected: {current_week}; teams with remaining games this week: {len(rem_cur_week)}")
    try:
        print(rem_cur_week.head().to_string())
    except Exception:
        pass

    # Check if merge worked properly
    if 'OppDefenseScore0to100' not in t.columns:
        print("ERROR: OppDefenseScore0to100 column missing after merge!")
        print(f"Columns in merged dataframe: {t.columns.tolist()}")
        # Add a temporary column with varying values (not 50) for testing
        t['OppDefenseScore0to100'] = np.random.randint(20, 80, size=len(t))
    else:
        # Verify merged OppDefenseScore0to100 values
        merged_scores = t['OppDefenseScore0to100']
        print(
            f"Merged OppDefenseScore0to100 stats: min={merged_scores.min()}, max={merged_scores.max()}, mean={merged_scores.mean():.1f}, std={merged_scores.std():.1f}")
        print(f"Sample of merged scores: {merged_scores.head(10).tolist()}")
        print(f"Missing values in merged scores: {merged_scores.isna().sum()} out of {len(merged_scores)}")

    # Group by team and week
    grp = t.groupby(["team", "week"], as_index=False).agg(
        Games=("opponent", "count"),
        LiteNite=("is_light_night", "sum"),
        SOS=("OppDefenseScore0to100", "mean"),
        Opponents=("opponent", lambda s: ", ".join(list(s))),
    )

    # Weekly B2B and Away counts
    weekly_b2b = t.groupby(["team", "week"])['is_b2b'].sum().rename('B2B')
    weekly_away = t.groupby(["team", "week"])['is_away'].sum().rename('Away')
    grp = grp.merge(weekly_b2b, on=["team", "week"], how="left").merge(weekly_away, on=["team", "week"], how="left")
    grp['B2B'] = grp['B2B'].fillna(0).astype(int)
    grp['Away'] = grp['Away'].fillna(0).astype(int)

    # Games remaining this week (only populated for the current week rows)
    grp['GamesRestOfWeek'] = np.where(
        grp['week'].eq(current_week),
        grp['team'].map(rem_cur_week).fillna(0).astype(int),
        0
    )

    # Games Rest of Season based on cumulative Games through the current week (per team)
    grp = grp.sort_values(['team', 'week']).copy()
    grp['GamesPlayedThrough'] = grp.groupby('team')['Games'].cumsum()
    grp['GamesROS'] = (82 - grp['GamesPlayedThrough']).clip(lower=0).astype(int)
    grp.drop(columns=['GamesPlayedThrough'], inplace=True)

    # Debug the grouped data
    print(f"After groupby, shape: {grp.shape}, teams: {grp['team'].nunique()}, weeks: {grp['week'].nunique()}")
    print(
        f"SOS stats before formatting: min={grp['SOS'].min()}, max={grp['SOS'].max()}, mean={grp['SOS'].mean():.1f}, std={grp['SOS'].std():.1f}")
    print(f"Sample of SOS values: {grp['SOS'].head(10).tolist()}")
    print(f"Missing SOS values: {grp['SOS'].isna().sum()} out of {len(grp)}")

    # If all SOS values are missing or the same, there's a problem
    if grp['SOS'].isna().all() or grp['SOS'].std() < 0.1:
        print("WARNING: SOS values are all missing or have no variation!")
        # Use random values for testing to see if the rest of the pipeline works
        grp['SOS'] = np.random.randint(20, 80, size=len(grp))
        print(f"Generated random SOS values for testing: {grp['SOS'].head(10).tolist()}")

    # Handle NA/inf values before converting to int
    grp["SOS"] = (
                     grp["SOS"]
                     .fillna(50)  # Use 50 (neutral) instead of 0 for missing values
                     .replace([float('inf'), -float('inf')], 50)
                     .round(0)
                     .astype(int)
                 ).astype(str) + "%"

    print(f"Final formatted SOS values (first 10): {grp['SOS'].head(10).tolist()}")

    def tier_from_score(s):
        v = int(str(s).rstrip("%"))
        if v <= 30:
            return "Excellent"
        if v <= 50:
            return "Good"
        if v <= 70:
            return "Average"
        return "Difficult"

    grp["MatchUp"] = grp["SOS"].map(tier_from_score)
    grp["TM"] = grp["team"].astype(str)  # Keep the NST 3-letter code
    grp["Week"] = grp["week"].astype(int)
    grp["Key"] = grp["TM"] + grp["Week"].astype(str)
    return grp[[
        "TM", "Week", "Games", "LiteNite", "Opponents", "SOS", "MatchUp", "B2B", "Away",
        "GamesRestOfWeek", "GamesROS", "Key"
    ]]


def write_outputs(df_lookup: pd.DataFrame, csv_path: str | None = None, xlsx_path: str | None = None) -> None:
    if csv_path:
        df_lookup.to_csv(csv_path, index=False)
    if xlsx_path:
        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as xw:
            df_lookup.to_excel(xw, index=False, sheet_name="lookup")