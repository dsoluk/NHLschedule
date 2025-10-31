from __future__ import annotations
import time
from pathlib import Path
from io import StringIO
import pandas as pd
import requests
from .config import SEASON_LABEL, CACHE_DIR, CACHE_REFRESH_DAYS

TEAMTABLE_URL = "https://www.naturalstattrick.com/teamtable.php"

# Common columns we need from NST team table
FEATURE_MAP = {
    "xGA/60": "xga60",
    "SCA/60": "sca60",
    "HDCA/60": "hdca60",
    "SA/60": "sa60",
    "GA/60": "ga60",
}


def _cache_file(key: str) -> Path:
    return CACHE_DIR / f"nst_{key}_{SEASON_LABEL}.parquet"


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
            "xGA/60": ["xGA60", "xGA"],
            "SCA/60": ["SCA60", "SCA"],
            "HDCA/60": ["HDCA60", "HDCA"],
            "SA/60": ["SA60", "SA"],
            "GA/60": ["GA60", "GA"]
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


def fetch_team_table(sit: str, loc: str | None = None) -> pd.DataFrame:
    """Fetch a team table for given situation.

    sit examples: 'sva' (5v5 score & venue adjusted), 'pp', 'pk'
    loc: 'B' both, 'H' home, 'A' away. For sva we use 'B'.
    """
    key = f"team_{sit}_{loc or 'NA'}"
    fp = _cache_file(key)

    # Check if we want to force refresh by setting refresh days to 0
    force_refresh = CACHE_REFRESH_DAYS <= 0

    # Use cache if available and not forcing refresh
    if not force_refresh and fp.exists() and (time.time() - fp.stat().st_mtime) < CACHE_REFRESH_DAYS * 86400:
        print(f"Loading from cache: {fp}")
        return pd.read_parquet(fp)

    # Additional parameters based on the URL you provided
    params = dict(
        fromseason=SEASON_LABEL,
        thruseason=SEASON_LABEL,
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
        print(f"Fetching NST data for {sit} situation, location: {loc or 'default'}")
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

        # Save to cache
        df.to_parquet(fp, index=False)
        print(f"Saved to cache: {fp}")

        return df

    except Exception as e:
        print(f"ERROR fetching NST data for {sit}: {e}")
        # If error occurs, return an empty dataframe with correct columns
        return pd.DataFrame(columns=["team"] + list(FEATURE_MAP.values()))


def get_all_situations() -> dict[str, pd.DataFrame]:
    """Return dict with keys 'sva', 'pp', 'pk' dataframes."""
    # Since the 2025-2026 season hasn't started yet, we need to simulate data
    # for testing purposes
    if SEASON_LABEL == "20252026":
        print("WARNING: Using simulated data for 2025-2026 season")
        return _get_simulated_data()

    return {
        "sva": fetch_team_table("sva", loc="B"),
        "pp": fetch_team_table("pp"),
        "pk": fetch_team_table("pk"),
    }


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