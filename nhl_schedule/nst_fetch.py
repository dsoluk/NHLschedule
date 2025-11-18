from __future__ import annotations
import time
from pathlib import Path
from io import StringIO
import pandas as pd
import requests
from .config import SEASON_LABEL, CACHE_DIR, CACHE_REFRESH_DAYS

# Global flag to force bypassing cache (can be set by CLI)
FORCE_CACHE_REFRESH = False

TEAMTABLE_URL = "https://www.naturalstattrick.com/teamtable.php"

# Common columns we need from NST team table
# Include both Against and For versions so downstream can compute
# defensive (against) and offensive (for) scores from the same fetch.
FEATURE_MAP = {
    # Against (defense faced by skaters, goalie-friendly)
    "xGA/60": "xga60",
    "SCA/60": "sca60",
    "HDCA/60": "hdca60",
    "SA/60": "sa60",
    "GA/60": "ga60",

    # For (team offensive generation rates)
    "xGF/60": "xgf60",
    "SCF/60": "scf60",
    "HDCF/60": "hdcf60",
    "SF/60": "sf60",
    "GF/60": "gf60",
}


def _cache_file(key: str, season_label: str | None = None) -> Path:
    season = season_label or SEASON_LABEL
    return CACHE_DIR / f"nst_{key}_{season}.parquet"


def _read_html_table(url: str, params: dict) -> pd.DataFrame:
    print(f"Making request to {url} with params: {params}")
    resp = requests.get(url, params=params, timeout=30)
    print(f"Response status code: {resp.status_code}")

    # Print the actual URL for debugging
    print(f"Final URL: {resp.url}")

    resp.raise_for_status()
    html = resp.text

    # Debug the HTML response size
    print(f"HTML response size: {len(html)} bytes")

    # Check if the response contains "No data" indicators
    if "No teams matched the filter criteria" in html or "No data available" in html:
        print("WARNING: NST response indicates no data available")

    tables = pd.read_html(StringIO(html))
    if not tables:
        print("WARNING: No tables found in HTML response")
        raise RuntimeError("NST: No tables found in response")

    print(f"Found {len(tables)} tables in the response, first table has shape: {tables[0].shape}")
    return tables[0]


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    # Debug incoming dataframe
    print(f"Normalizing columns for dataframe with shape: {df.shape}")
    print(f"Original columns: {df.columns.tolist()}")

    df = df.rename(columns={c: c.strip() for c in df.columns})
    if "Team" in df.columns:
        df = df.rename(columns={"Team": "team"})

    # Keep only columns we care about
    keep = ["team"] + list(FEATURE_WEIGHTS_KEYS := list(FEATURE_MAP.keys()))
    missing = [c for c in keep if c not in df.columns]
    if missing:
        print(f"WARNING: Missing expected columns: {missing}")

        # Try alternate column names that NST might use
        alternate_names = {
            # Against
            "xGA/60": ["xGA60", "xGA"],
            "SCA/60": ["SCA60", "SCA"],
            "HDCA/60": ["HDCA60", "HDCA"],
            "SA/60": ["SA60", "SA"],
            "GA/60": ["GA60", "GA"],
            # For
            "xGF/60": ["xGF60", "xGF"],
            "SCF/60": ["SCF60", "SCF"],
            "HDCF/60": ["HDCF60", "HDCF"],
            "SF/60": ["SF60", "SF"],
            "GF/60": ["GF60", "GF"],
        }

        for col in missing[:]:
            if col in alternate_names:
                for alt in alternate_names[col]:
                    if alt in df.columns:
                        print(f"Found alternate column name: {alt} for {col}")
                        df[col] = df[alt]
                        missing.remove(col)
                        break

    if missing:
        # If still missing columns, print columns that are available
        print(f"Available columns in NST data: {df.columns.tolist()}")
        print("Cannot find required columns, using subset of available columns")

    # Create a copy with only the columns we need
    available_keys = [k for k in FEATURE_WEIGHTS_KEYS if k in df.columns]
    if not available_keys:
        print("ERROR: No usable metrics found in the data")
        # Return empty dataframe with expected structure
        return pd.DataFrame(columns=["team"] + list(FEATURE_MAP.values()))

    keep = ["team"] + available_keys
    df = df[keep].copy()

    # Rename to our internal names
    rename_dict = {k: FEATURE_MAP[k] for k in available_keys}
    renamed = df.rename(columns=rename_dict)
    print(f"Final columns after normalization: {renamed.columns.tolist()}")
    return renamed


def fetch_team_table(sit: str, loc: str | None = None, *, season_label: str | None = None) -> pd.DataFrame:
    """Fetch a team table for given situation.

    sit examples: 'sva' (5v5 score & venue adjusted), 'pp', 'pk'
    loc: 'B' both, 'H' home, 'A' away. For sva we use 'B'.
    """
    key = f"team_{sit}_{loc or 'NA'}"
    fp = _cache_file(key, season_label)

    # Check if we want to force refresh by setting refresh days to 0 or via global flag
    force_refresh = (CACHE_REFRESH_DAYS <= 0) or FORCE_CACHE_REFRESH

    # Use cache if available and not forcing refresh
    if not force_refresh and fp.exists() and (time.time() - fp.stat().st_mtime) < CACHE_REFRESH_DAYS * 86400:
        print(f"Loading from cache: {fp}")
        cached = pd.read_parquet(fp)
        # Validate cached content
        required_cols = ["team"] + list(FEATURE_MAP.values())
        has_cols = all(c in cached.columns for c in required_cols)
        non_empty = len(cached) > 0
        if non_empty and has_cols and cached["team"].nunique() >= 20:
            return cached
        else:
            print("WARNING: Cached NST data is empty or invalid; ignoring cache and refetching")
            try:
                fp.unlink(missing_ok=True)
            except Exception:
                pass

    # Additional parameters based on the URL you provided
    params = dict(
        fromseason=(season_label or SEASON_LABEL),
        thruseason=(season_label or SEASON_LABEL),
        stype=2,  # regular season
        sit=sit,
        score="all",
        rate="y",
        team="all",  # Use lowercase "all" as per the URL example
        gpf=410,  # Additional parameter from your example
        fd="",  # Additional parameter from your example
        td="",  # Additional parameter from your example
    )
    if loc is not None:
        params["loc"] = loc

    try:
        print(f"Fetching NST data for {sit} situation, location: {loc or 'default'}, season={season_label or SEASON_LABEL}")
        raw = _read_html_table(TEAMTABLE_URL, params)
        df = _normalize_cols(raw)

        if len(df) == 0:
            print(f"WARNING: No teams returned for {sit}, loc={loc}")
            return pd.DataFrame(columns=["team"] + list(FEATURE_MAP.values()))

        # Map team names to standard 3-letter NHL abbreviations
        def to_code(name: str) -> str:
            if pd.isna(name):
                return "UNK"  # Unknown team code for NA values

            n = str(name).lower()
            # remove accents common in MTL
            n = (
                n.replace("é", "e").replace("É", "E")
            )
            mapping = {
                "anaheim": "ANA",
                "arizona": "ARI",
                "boston": "BOS",
                "buffalo": "BUF",
                "calgary": "CGY",
                "carolina": "CAR",
                "chicago": "CHI",
                "colorado": "COL",
                "columbus": "CBJ",
                "dallas": "DAL",
                "detroit": "DET",
                "edmonton": "EDM",
                "florida": "FLA",
                "los angeles": "LAK",
                "minnesota": "MIN",
                "montreal": "MTL",
                "nashville": "NSH",
                "new jersey": "NJD",
                "ny islanders": "NYI",
                "new york islanders": "NYI",
                "ny rangers": "NYR",
                "new york rangers": "NYR",
                "ottawa": "OTT",
                "philadelphia": "PHI",
                "pittsburgh": "PIT",
                "san jose": "SJS",
                "seattle": "SEA",
                "st. louis": "STL",
                "st louis": "STL",
                "tampa bay": "TBL",
                "toronto": "TOR",
                "utah": "UTA",
                "vancouver": "VAN",
                "vegas": "VGK",
                "washington": "WSH",
                "winnipeg": "WPG",
            }
            for k, v in mapping.items():
                if n.startswith(k):
                    return v
            # Fallback: take uppercase first 3 letters
            return str(name).upper()[:3]

        df["team"] = df["team"].map(to_code)
        print(f"Mapped {len(df)} teams to codes")

        # Post-fetch validation
        required_cols = ["team"] + list(FEATURE_MAP.values())
        has_cols = all(c in df.columns for c in required_cols)
        non_empty = len(df) > 0
        unique_teams = df["team"].nunique() if non_empty and "team" in df.columns else 0
        if not non_empty or not has_cols or unique_teams < 20:
            print(
                f"WARNING: Fetched NST data seems incomplete (rows={len(df)}, unique_teams={unique_teams}). Will NOT cache this result."
            )
        else:
            # Save to cache only when valid
            df.to_parquet(fp, index=False)
            print(f"Saved to cache: {fp}")

        return df

    except Exception as e:
        print(f"ERROR fetching NST data for {sit}: {e}")
        # If error occurs, return an empty dataframe with correct columns
        return pd.DataFrame(columns=["team"] + list(FEATURE_MAP.values()))


def get_all_situations(*, season_label: str | None = None) -> dict[str, pd.DataFrame]:
    """Return dict with keys 'sva', 'pp', 'pk' dataframes.

    Always attempts to fetch real NST data. Falls back to empty frames on errors (neutral handling downstream).
    """
    try:
        sva = fetch_team_table("sva", loc="B", season_label=season_label)
    except Exception as e:
        print(f"ERROR fetching SVA data: {e}")
        sva = pd.DataFrame(columns=["team"] + list(FEATURE_MAP.values()))
    try:
        pp = fetch_team_table("pp", season_label=season_label)
    except Exception as e:
        print(f"ERROR fetching PP data: {e}")
        pp = pd.DataFrame(columns=["team"] + list(FEATURE_MAP.values()))
    try:
        pk = fetch_team_table("pk", season_label=season_label)
    except Exception as e:
        print(f"ERROR fetching PK data: {e}")
        pk = pd.DataFrame(columns=["team"] + list(FEATURE_MAP.values()))

    return {"sva": sva, "pp": pp, "pk": pk}


def _get_simulated_data() -> dict[str, pd.DataFrame]:
    """Generate simulated data for testing when real data isn't available."""
    # Get all NHL team codes
    teams = [
        "ANA", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ",
        "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH",
        "NJD", "NYI", "NYR", "OTT", "PHI", "PIT", "SJS", "SEA",
        "STL", "TBL", "TOR", "UTA", "VAN", "VGK", "WSH", "WPG"
    ]

    import numpy as np

    # Create a function to generate random metrics with appropriate ranges
    def create_random_df(teams, seed=None):
        if seed is not None:
            np.random.seed(seed)

        n = len(teams)
        data = {
            "team": teams,
            "xga60": np.random.normal(2.3, 0.3, n),  # Range ~1.7-2.9
            "sca60": np.random.normal(25, 3, n),  # Range ~19-31
            "hdca60": np.random.normal(10, 1.5, n),  # Range ~7-13
            "sa60": np.random.normal(30, 3, n),  # Range ~24-36
            "ga60": np.random.normal(2.8, 0.4, n),  # Range ~2.0-3.6
        }

        # Ensure values are positive
        for col in ["xga60", "sca60", "hdca60", "sa60", "ga60"]:
            data[col] = np.maximum(data[col], 0.1)

        return pd.DataFrame(data)

    # Generate slightly different data for each situation
    return {
        "sva": create_random_df(teams, seed=42),
        "pp": create_random_df(teams, seed=43),
        "pk": create_random_df(teams, seed=44),
    }