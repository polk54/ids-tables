#!/usr/bin/env python3
"""
Build static IDS (source=6) JSON for GitHub Pages, using 4D + counterpart-area/WLD.

Outputs:
  data/countries.json                  -> [{ "id": "AFG", "name": "Afghanistan" }, ...]
  data/{ISO3}.json per country        -> {
      "country": "AFG",
      "name": "Afghanistan",
      "latest_year": 2023,
      "years": ["2013", ... , "2023"],           # last 11 available years
      "series": {
          "DT.DOD.DECT.CD": {"2013": 12345.0, ...},
          ...
      }
  }

Notes:
- Stays on source 6 and uses counterpart-area/WLD for every series.
- If a series truly has no WLD values, it will be missing from "series" or have holes; the frontend renders em-dashes.
"""

import os, sys, json, time, random
from typing import Dict, List, Any, Tuple
import requests

WB = "https://api.worldbank.org/v2"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Keep this list in sync with index.html
INDICATORS = [
    "DT.DOD.DECT.CD",
    "DT.DOD.DLXF.CD",
    "DT.DOD.DPPG.CD",
    "DT.DOD.DPNG.CD",
    "DT.DOD.DIMF.CD",
    "DT.DOD.DSTC.CD",
    "DT.TDS.DECT.CD",
    "DT.AMT.DECT.CD",
    "DT.INT.DECT.CD",
    "DT.DIS.DLXF.CD",
    "DT.DOD.DECT.GN.ZS",
    "DT.TDS.DECT.EX.ZS",
    "DT.INT.DECT.EX.ZS",
]

YEARS_TO_SHOW = 11
SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})


def _sleep_backoff(attempt: int) -> None:
    time.sleep(min(1 + attempt * 0.5, 4) + random.random() * 0.4)


def get_json(url: str, max_attempts: int = 4) -> Any:
    last = None
    for attempt in range(max_attempts):
        try:
            r = SESSION.get(url, timeout=25)
            if r.status_code >= 400:
                last = f"HTTP {r.status_code}"
                _sleep_backoff(attempt)
                continue
            return r.json()
        except requests.RequestException as e:
            last = str(e)
            _sleep_backoff(attempt)
    raise RuntimeError(f"GET failed: {url} :: {last}")


def extract_year(vars_arr: List[Dict[str, Any]]) -> str:
    if not isinstance(vars_arr, list): return ""
    # Prefer an entry marked time; otherwise try index 1; else first that looks like a year
    v = next((v for v in vars_arr if str(v.get("name") or v.get("concept") or "").lower().find("time") >= 0), None)
    if v is None and len(vars_arr) > 1:
        v = vars_arr[1]
    cand = str(v.get("id") if v else "")
    for token in [cand] + [str(v.get("value","")) for v in vars_arr]:
        if not token: continue
        token = str(token)
        # Accept "yr2023", "YR2019", "2021"
        if token.lower().startswith("yr") and token[2:6].isdigit(): return token[2:6]
        if len(token) >= 4:
            for i in range(len(token)-3):
                s = token[i:i+4]
                if s.isdigit():
                    return s
    return ""


def extract_iso3(vars_arr: List[Dict[str, Any]]) -> str:
    if not isinstance(vars_arr, list): return ""
    # Prefer explicit country/economy concept, else any ABC ISO3 token
    for v in vars_arr:
        name = str(v.get("name") or v.get("concept") or "")
        if name and ("country" in name.lower() or "economy" in name.lower()):
            code = str(v.get("id") or v.get("value") or "")
            for i in range(len(code)-2):
                s = code[i:i+3]
                if s.isalpha() and s.isupper():
                    return s
    # fallback scan
    for v in vars_arr:
        for token in (str(v.get("id") or ""), str(v.get("value") or "")):
            for i in range(len(token)-2):
                s = token[i:i+3]
                if s.isalpha() and s.isupper():
                    return s
    return ""


def fetch_source6_countries() -> List[Dict[str, str]]:
    url = f"{WB}/sources/6/country?format=json&per_page=500"
    try:
        j = get_json(url)
        arr = j[1] if isinstance(j, list) and len(j) > 1 else j.get("countries", [])
        out = [{"id": c["id"], "name": c["name"]} for c in arr if c.get("id") and c.get("name")]
        if out:
            return out
    except Exception:
        pass

    # Fallback: derive from 4D across all
    probe = f"{WB}/sources/6/country/all/series/{'DT.DOD.DECT.CD'}/counterpart-area/WLD/time/all?format=json&per_page=25000"
    j = get_json(probe)
    rows = (j.get("source") or {}).get("data", [])
    seen = {}
    for r in rows:
        iso = extract_iso3(r.get("variable", []))
        if iso:
            seen.setdefault(iso, {"id": iso, "name": iso})
    # Map names via /country (not used for data — just better labels)
    try:
        cj = get_json(f"{WB}/country?format=json&per_page=1200")
        carr = cj[1] if isinstance(cj, list) and len(cj) > 1 else []
        names = {c["id"]: c["name"] for c in carr if c.get("id") and c.get("name")}
        for k, v in seen.items():
            if k in names: v["name"] = names[k]
    except Exception:
        pass
    return sorted(seen.values(), key=lambda x: x["id"])


def find_latest_year(iso3: str) -> str:
    # One probe on a headline series to get max year
    url = f"{WB}/sources/6/country/{iso3}/series/DT.DOD.DECT.CD/counterpart-area/WLD/time/all?format=json&per_page=25000"
    j = get_json(url)
    rows = (j.get("source") or {}).get("data", [])
    best = ""
    for r in rows:
        y = extract_year(r.get("variable", []))
        if y and (not best or int(y) > int(best)):
            best = y
    if not best:
        # fallback: conservative guess
        best = str(max(2000, (time.gmtime().tm_year - 2)))
    return best


def fetch_series_range(iso3: str, code: str, y_start: str, y_end: str) -> Dict[str, float]:
    url = f"{WB}/sources/6/country/{iso3}/series/{code}/counterpart-area/WLD/time/yr{y_start}:yr{y_end}?format=json&per_page=25000"
    j = get_json(url)
    rows = (j.get("source") or {}).get("data", [])
    out: Dict[str, float] = {}
    for r in rows:
        y = extract_year(r.get("variable", []))
        v = r.get("value", None)
        if y and (v is not None):
            try:
                out[str(y)] = float(v)
            except Exception:
                pass
    return out


def main() -> int:
    print("Fetching IDS Source 6 country list…", flush=True)
    countries = fetch_source6_countries()
    if not countries:
        print("ERROR: Could not derive country list", file=sys.stderr)
        return 2

    countries_path = os.path.join(DATA_DIR, "countries.json")
    with open(countries_path, "w", encoding="utf-8") as f:
        json.dump(countries, f, ensure_ascii=False, indent=2)
    print(f"Wrote {countries_path}  ({len(countries)} countries)")

    for i, c in enumerate(countries, 1):
        iso3 = c["id"]
        name = c["name"]
        try:
            latest = find_latest_year(iso3)
            end = int(latest)
            years = [str(y) for y in range(end - YEARS_TO_SHOW + 1, end + 1)]
            y_start, y_end = years[0], years[-1]

            series_map: Dict[str, Dict[str, float]] = {}
            for code in INDICATORS:
                series_map[code] = fetch_series_range(iso3, code, y_start, y_end)
                _sleep_backoff(0)  # light pacing

            out = {
                "country": iso3,
                "name": name,
                "latest_year": end,
                "years": years,
                "series": series_map,
                "source": {
                    "api": "https://api.worldbank.org/v2/sources/6",
                    "counterpart_area": "WLD",
                },
            }
            p = os.path.join(DATA_DIR, f"{iso3}.json")
            with open(p, "w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False, indent=2)
            print(f"[{i:>3}/{len(countries)}] {iso3}  -> data/{iso3}.json  (latest={end})")
        except Exception as e:
            print(f"[{i:>3}] {iso3} FAILED: {e}", file=sys.stderr)
            # keep going

    return 0


if __name__ == "__main__":
    sys.exit(main())
