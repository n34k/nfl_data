"""
filter_nfl_stats.py

Filters a large NFL player stats JSON file to only include records
where "year" is between "2003" and "2023" (inclusive).

Uses ijson for memory-efficient streaming — safe for files 1 GB+.

Usage:
    python filter_nfl_stats.py                          # uses defaults below
    python filter_nfl_stats.py input.json output.json   # custom paths

Install dependency first:
    pip install ijson
"""

import sys
import json
import ijson
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return int(o) if o == o.to_integral_value() else float(o)
        return super().default(o)

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_FILE  = "nfl_stats.json"   # change to your actual filename
OUTPUT_FILE = "nfl_stats_2003_2023.json"
YEAR_START  = "2003"
YEAR_END    = "2023"
# ─────────────────────────────────────────────────────────────────────────────

def in_range(year_str: str) -> bool:
    """Return True if the year string falls within [YEAR_START, YEAR_END]."""
    return YEAR_START <= year_str <= YEAR_END   # works because strings compare lexicographically for 4-digit years


def filter_stats(input_path: str, output_path: str) -> None:
    kept = 0
    skipped = 0

    print(f"Reading : {input_path}")
    print(f"Writing : {output_path}")
    print(f"Keeping : years {YEAR_START}–{YEAR_END}\n")

    with open(input_path, "rb") as infile, \
         open(output_path, "w", encoding="utf-8") as outfile:

        outfile.write("[\n")
        first = True

        # top-level array — stream each element one at a time
        for record in ijson.items(infile, "item"):
            year = record.get("year", "")
            if in_range(year):
                if not first:
                    outfile.write(",\n")
                json.dump(record, outfile, ensure_ascii=False, cls=DecimalEncoder)
                first = False
                kept += 1
            else:
                skipped += 1

            # Progress indicator every 100k records
            total = kept + skipped
            if total % 100_000 == 0:
                print(f"  processed {total:,} records  (kept {kept:,})", flush=True)

        outfile.write("\n]\n")

    print(f"\nDone!")
    print(f"  Kept    : {kept:,} records")
    print(f"  Skipped : {skipped:,} records")


if __name__ == "__main__":
    input_path  = sys.argv[1] if len(sys.argv) > 1 else INPUT_FILE
    output_path = sys.argv[2] if len(sys.argv) > 2 else OUTPUT_FILE
    filter_stats(input_path, output_path)