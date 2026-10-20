
import csv
import json
import math
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict

ROOT = Path(__file__).resolve().parent
HTML = ROOT / "dashboard" / "index.html"
HOURLY_CSV = ROOT / "dashboard" / "data" / "eia_hourly_silver.csv"
DAILY_CSV = ROOT / "dashboard" / "data" / "eia_daily_gold.csv"

failures = []
checks = 0

def check(name, condition, details=""):
    global checks
    checks += 1
    result = "PASS" if condition else "FAIL"
    print(f"[{result}] {name} {details}")
    if not condition:
        failures.append(name)

def close(a, b, tolerance=0.01):
    return math.isclose(float(a), float(b), abs_tol=tolerance, rel_tol=1e-9)

def load_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def extract_array(html, name):
    pattern = rf"\bconst\s+{name}\s*=\s*(\[.*?\])\s*;"
    match = re.search(pattern, html, re.DOTALL)
    if not match:
        raise ValueError(f"Cannot find {name} in dashboard HTML")
    return json.loads(match.group(1))

def first_numeric(row, candidates):
    for name in candidates:
        if name in row and row[name] not in ("", None):
            return float(row[name])
    raise KeyError(f"None of {candidates} found. CSV columns: {list(row)}")

def first_text(row, candidates):
    for name in candidates:
        if name in row and row[name]:
            return row[name]
    raise KeyError(f"None of {candidates} found. CSV columns: {list(row)}")

def normalize_date(value):
    return str(value)[:10]

def normalize_hour(value):
    value = str(value).replace("T", " ").replace("Z", "")
    return value[:13]

def main():
    html = HTML.read_text(encoding="utf-8")
    hourly = load_csv(HOURLY_CSV)
    daily = load_csv(DAILY_CSV)
    embedded_hourly = extract_array(html, "HOURLY")
    embedded_daily = extract_array(html, "DAILY")

    check("CSV hourly count", len(hourly) == 240)
    check("CSV daily count", len(daily) == 10)
    check("Dashboard hourly count", len(embedded_hourly) == len(hourly))
    check("Dashboard daily count", len(embedded_daily) == len(daily))

    csv_hours = {}
    for row in hourly:
        timestamp = first_text(row, ["period", "period_ts", "time", "timestamp"])
        key = normalize_hour(timestamp)
        if key in csv_hours:
            check(f"Duplicate hourly CSV key {key}", False)
        csv_hours[key] = (
            first_numeric(row, ["actual_demand_mwh", "actual"]),
            first_numeric(row, ["forecast_demand_mwh", "forecast"]),
        )

    html_hours = {}
    for row in embedded_hourly:
        key = normalize_hour(row["time"])
        if key in html_hours:
            check(f"Duplicate HTML hourly key {key}", False)
        html_hours[key] = (float(row["actual"]), float(row["forecast"]))

    check("Hourly timestamps match", set(csv_hours) == set(html_hours))

    for key in sorted(set(csv_hours) & set(html_hours)):
        actual_csv, forecast_csv = csv_hours[key]
        actual_html, forecast_html = html_hours[key]
        check(f"Actual {key}", close(actual_csv, actual_html))
        check(f"Forecast {key}", close(forecast_csv, forecast_html))

    csv_days = {}
    for row in daily:
        date = normalize_date(first_text(row, ["date"]))
        if date in csv_days:
            check(f"Duplicate daily CSV key {date}", False)
        csv_days[date] = row

    html_days = {str(row["date"]): row for row in embedded_daily}
    check("Daily dates match", set(csv_days) == set(html_days))

    for date in sorted(set(csv_days) & set(html_days)):
        csv_energy = first_numeric(
            csv_days[date], ["total_daily_energy_mwh", "energy"]
        )
        csv_peak = first_numeric(
            csv_days[date], ["peak_demand_mwh", "peak"]
        )
        csv_mae = first_numeric(
            csv_days[date], ["mae_mwh", "mae"]
        )
        d = html_days[date]
        check(f"Daily energy {date}", close(csv_energy, d["energy"]))
        check(f"Daily peak {date}", close(csv_peak, d["peak"]))
        check(f"Daily MAE {date}", close(csv_mae, d["mae"]))

    dates = sorted(html_days)

    for n in (3, 7, 10):
        selected = set(dates[-n:])
        hours = [
            (date, actual, forecast)
            for key, (actual, forecast) in csv_hours.items()
            for date in [key[:10]]
            if date in selected
        ]

        actual_sum = sum(a for _, a, _ in hours)
        abs_error = sum(abs(a - f) for _, a, f in hours)
        energy = sum(
            float(html_days[d]["energy"]) for d in selected
        )
        peak = max(a for _, a, _ in hours)
        mae = abs_error / len(hours)
        accuracy = 100 * (1 - abs_error / actual_sum)

        check(f"{n}-day hourly coverage", len(hours) == n * 24)
        check(f"{n}-day total energy", close(energy, actual_sum))

        print(
            f"\n{n}-DAY KPIs:"
            f"\n  Total Energy: {energy:,.0f} MWh"
            f"\n  Peak Demand: {peak:,.0f} MWh"
            f"\n  MAE: {mae:,.2f} MWh"
            f"\n  Accuracy: {accuracy:.4f}%\n"
        )

    print(f"\nRESULT: {checks - len(failures)}/{checks} checks passed")
    if failures:
        print("FAILED CHECKS:", failures[:30])
        raise SystemExit(1)

    print("All data reconciliation checks passed.")

if __name__ == "__main__":
    main()
