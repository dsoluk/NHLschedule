"""NHL Schedule to Excel lookup table builder.

Modules:
- config: user-adjustable settings
- schedule_io: read/normalize schedule and compute LiteNite flags
- nst_fetch: download + cache Natural Stat Trick team tables
- ratings: compute opponent defense ease 0–100 and tiers
- export: aggregate per team-week and export files
- diagnostics: normality checks and optional plots
- build_lookup: CLI entry point to run the full pipeline
"""

__all__ = [
    "config",
    "schedule_io",
    "nst_fetch",
    "ratings",
    "export",
    "diagnostics",
]
