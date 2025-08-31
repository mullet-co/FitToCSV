#!/usr/bin/env python3
"""
FIT -> CSV converter

Outputs:
  - <prefix>_records.csv  (per-second records with lap_index)
  - <prefix>_laps.csv     (lap summary table)

Minimal record fields:
  timestamp, power, heart_rate, cadence, distance_m, lap_index

Lap fields:
  lap_index, lap_start_time, lap_end_time, lap_duration_s,
  lap_avg_power, lap_avg_heart_rate, lap_avg_cadence, lap_total_distance_m

Usage:
  pip install fitparse
  python fit_to_csv.py input.fit output_prefix
"""
import csv
import os
import sys
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

try:
    from fitparse import FitFile
except Exception as e:
    print("Error: fitparse is required. Install with `pip install fitparse`.")
    raise

def safe_val(msg, name, default=None):
    field = msg.get_value(name)
    return field if field is not None else default

def iso_dt(dt) -> Optional[str]:
    if dt is None:
        return None
    try:
        return dt.isoformat()
    except Exception:
        return str(dt)

def collect_laps(fitfile: FitFile) -> List[Dict[str, Any]]:
    """Collect lap summaries with start/end/duration and averages."""
    laps = []
    for i, lap in enumerate(fitfile.get_messages("lap")):
        start = safe_val(lap, "start_time")
        # Prefer explicit timer time, fallback to elapsed time, then 'timestamp' as end
        total_timer_time = safe_val(lap, "total_timer_time")
        total_elapsed_time = safe_val(lap, "total_elapsed_time")
        end = None
        if start is not None and isinstance(total_timer_time, (int, float)):
            end = start + timedelta(seconds=float(total_timer_time))
        elif start is not None and isinstance(total_elapsed_time, (int, float)):
            end = start + timedelta(seconds=float(total_elapsed_time))
        else:
            # Some FIT writers use 'timestamp' on lap as the end time
            end = safe_val(lap, "timestamp")

        duration_s = None
        if isinstance(total_timer_time, (int, float)):
            duration_s = float(total_timer_time)
        elif isinstance(total_elapsed_time, (int, float)):
            duration_s = float(total_elapsed_time)
        elif start is not None and end is not None:
            duration_s = (end - start).total_seconds()

        laps.append({
            "lap_index": i + 1,
            "lap_start_time": start,
            "lap_end_time": end,
            "lap_duration_s": duration_s,
            "lap_avg_power": safe_val(lap, "avg_power"),
            "lap_avg_heart_rate": safe_val(lap, "avg_heart_rate"),
            "lap_avg_cadence": safe_val(lap, "avg_cadence"),
            "lap_total_distance_m": safe_val(lap, "total_distance"),
        })
    # Sort laps by start time just in case
    laps.sort(key=lambda d: (d["lap_start_time"] or 0))
    return laps

def map_record_to_lap_index(ts, lap_intervals: List[Tuple[int, Any, Any]], cur_idx: int) -> Tuple[Optional[int], int]:
    """
    Given a timestamp and ordered lap intervals (lap_index, start, end),
    return the lap_index containing ts and an updated search pointer.
    """
    if ts is None or not lap_intervals:
        return None, cur_idx

    # Advance pointer while ts is past current lap end
    n = len(lap_intervals)
    i = max(0, min(cur_idx, n - 1))
    while i < n:
        lap_id, start, end = lap_intervals[i]
        if start is not None and end is not None:
            if start <= ts <= end:
                return lap_id, i
            if ts > end:
                i += 1
                continue
        elif start is not None and ts >= start:
            # If no end given, assume this and all following are after
            return lap_id, i
        else:
            # If start unknown, we can't match reliably—move on
            i += 1
            continue
        break
    return None, i

def write_records_csv(fitfile: FitFile, out_path: str, lap_intervals: List[Tuple[int, Any, Any]]):
    """Write per-second records with minimal fields plus lap_index."""
    fields = ["timestamp", "power", "heart_rate", "cadence", "distance_m", "lap_index"]
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        ptr = 0
        for rec in fitfile.get_messages("record"):
            ts = safe_val(rec, "timestamp")
            power = safe_val(rec, "power")
            hr = safe_val(rec, "heart_rate")
            cad = safe_val(rec, "cadence")
            dist = safe_val(rec, "distance")  # meters

            lap_idx, ptr = map_record_to_lap_index(ts, lap_intervals, ptr)
            w.writerow({
                "timestamp": iso_dt(ts),
                "power": power,
                "heart_rate": hr,
                "cadence": cad,
                "distance_m": dist,
                "lap_index": lap_idx,
            })

def write_laps_csv(laps: List[Dict[str, Any]], out_path: str):
    """Write lap summary table."""
    fields = [
        "lap_index",
        "lap_start_time",
        "lap_end_time",
        "lap_duration_s",
        "lap_avg_power",
        "lap_avg_heart_rate",
        "lap_avg_cadence",
        "lap_total_distance_m",
    ]
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for lap in laps:
            row = {k: lap.get(k) for k in fields}
            row["lap_start_time"] = iso_dt(row["lap_start_time"])
            row["lap_end_time"] = iso_dt(row["lap_end_time"])
            w.writerow(row)

def convert(fit_path: str, out_prefix: str):
    fitfile = FitFile(fit_path)

    # Collect lap summaries first
    laps = collect_laps(fitfile)

    # Build lap intervals for quick membership checks
    lap_intervals: List[Tuple[int, Any, Any]] = [
        (lap["lap_index"], lap["lap_start_time"], lap["lap_end_time"]) for lap in laps
    ]

    # Re-open fitfile for records iteration (safer for some readers)
    fitfile_records = FitFile(fit_path)

    records_csv = f"{out_prefix}_records.csv"
    laps_csv = f"{out_prefix}_laps.csv"

    write_records_csv(fitfile_records, records_csv, lap_intervals)
    write_laps_csv(laps, laps_csv)

    print(f"✅ Wrote:\n  • {records_csv}\n  • {laps_csv}")

def main():
    if len(sys.argv) != 3:
        print("Usage: python fit_to_csv.py input.fit output_prefix")
        sys.exit(1)
    fit_path = sys.argv[1]
    out_prefix = sys.argv[2]
    convert(fit_path, out_prefix)

if __name__ == "__main__":
    main()
