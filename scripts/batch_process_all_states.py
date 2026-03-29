#!/usr/bin/env python3
"""
Batch processor: downloads all supporting data and runs the pipeline for all 50 US states + DC.

Phases (run in order, each is skip-if-already-done):
  1. census    — Download Census ACS 2023 + 2018 for each state via Census API
  2. crosswalk — Generate place-county crosswalk files via Census Gazetteer + Geocoder
  3. fcc       — Pull FCC CSVs from Google Drive (FTTP + place summary per state)
  4. pipeline  — Run process_state_data.py for each state → outputs data/<state>-unified-data.json

Usage:
    python3 scripts/batch_process_all_states.py                      # run all phases, all states
    python3 scripts/batch_process_all_states.py --states NC GA FL    # subset of states
    python3 scripts/batch_process_all_states.py --phase census       # single phase only
    python3 scripts/batch_process_all_states.py --phase fcc          # re-pull FCC from Drive
    python3 scripts/batch_process_all_states.py --force              # re-run even if output exists
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

SCRIPT_DIR  = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
RAW_DIR     = PROJECT_DIR / "data" / "raw"
CENSUS_DIR  = RAW_DIR / "census"
FCC_DIR     = RAW_DIR / "fcc" / "jun2025"
OUT_DIR     = PROJECT_DIR / "data"

# All 50 states + DC (excludes territories — no reliable county-level Census data)
ALL_STATES = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "FL": "12", "GA": "13",
    "HI": "15", "ID": "16", "IL": "17", "IN": "18", "IA": "19",
    "KS": "20", "KY": "21", "LA": "22", "ME": "23", "MD": "24",
    "MA": "25", "MI": "26", "MN": "27", "MS": "28", "MO": "29",
    "MT": "30", "NE": "31", "NV": "32", "NH": "33", "NJ": "34",
    "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45",
    "SD": "46", "TN": "47", "TX": "48", "UT": "49", "VT": "50",
    "VA": "51", "WA": "53", "WV": "54", "WI": "55", "WY": "56",
    "DC": "11",
}

# Google Drive folder/file naming
DRIVE_ROOT          = "Fiber Website"
DRIVE_DATE          = "2025-06-30"
DRIVE_LOC_ZIP_PATH  = f"{DRIVE_ROOT}/{DRIVE_DATE}/location_coverage/zips"
DRIVE_SUM_ZIP_PATH  = f"{DRIVE_ROOT}/{DRIVE_DATE}/summaries/zips"

# FCC file name patterns (ZIPs on Drive)
FTTP_ZIP_PATTERN    = "bdc_{fips}_FibertothePremises_fixed_broadband_J25_19mar2026.zip"
PLACE_ZIP_PATTERN   = "bdc_{fips}_fixed_broadband_summary_by_geography_place_J25_19mar2026.zip"
PROVIDER_ZIP_NAME   = "bdc_us_provider_summary_by_geography_J25_19mar2026.zip"

# Census API
CENSUS_BASE         = "https://api.census.gov/data"
CENSUS_VARS_2023    = "NAME,B01003_001E,B25001_001E,B25003_001E,B25003_002E,B08006_001E,B08006_017E,B19013_001E,B25064_001E,B25077_001E"
CENSUS_VARS_2018    = "NAME,B01003_001E,B25001_001E"

# Census Gazetteer + Geocoder (for crosswalks)
GAZ_URL      = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2023_Gazetteer/2023_gaz_place_{fips}.txt"
GEOCODER_URL = ("https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
                "?x={lon}&y={lat}&benchmark=Public_AR_Current&vintage=Current_Current"
                "&layers=Counties&format=json")


# ─── Helpers ──────────────────────────────────────────────────────────────────

def log(msg):
    print(msg, flush=True)


def census_get(url):
    with urllib.request.urlopen(url, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


# ─── Phase 1: Census ACS ──────────────────────────────────────────────────────

def phase_census(states, force=False):
    log("\n" + "="*60)
    log("PHASE 1: Census ACS Data")
    log("="*60)
    CENSUS_DIR.mkdir(parents=True, exist_ok=True)
    results = {}

    for abbrev, fips in states.items():
        sl = abbrev.lower()
        p2023 = CENSUS_DIR / f"census_acs_{sl}.json"
        p2018 = CENSUS_DIR / f"census_acs_{sl}_2018.json"

        if not force and p2023.exists() and p2018.exists():
            log(f"  [{abbrev}] ACS already exists — skipping")
            results[abbrev] = "skipped"
            continue

        log(f"\n  [{abbrev}] Downloading ACS...")
        ok = True
        for year, vars_, path in [
            ("2023", CENSUS_VARS_2023, p2023),
            ("2018", CENSUS_VARS_2018, p2018),
        ]:
            url = f"{CENSUS_BASE}/{year}/acs/acs5?get={vars_}&for=county:*&in=state:{fips}"
            try:
                data = census_get(url)
                with open(path, "w") as f:
                    json.dump(data, f)
                log(f"    {year}: {len(data)-1} counties → {path.name}")
                time.sleep(1)
            except Exception as e:
                log(f"    {year}: FAILED — {e}")
                ok = False

        results[abbrev] = "ok" if ok else "failed"
        time.sleep(1)

    _print_summary("Census ACS", results)
    return results


# ─── Phase 2: Place-County Crosswalks ─────────────────────────────────────────

def _fetch_gazetteer(state_fips):
    url = GAZ_URL.format(fips=state_fips)
    with urllib.request.urlopen(url, timeout=30) as r:
        text = r.read().decode("utf-8")
    places = []
    for line in text.strip().split("\n")[1:]:
        parts = line.strip().split("\t")
        if len(parts) < 12:
            continue
        geoid = parts[1].strip().zfill(7)
        name  = parts[3].strip()
        try:
            lat = float(parts[10].strip())
            lon = float(parts[11].strip())
        except (ValueError, IndexError):
            continue
        places.append({"geoid": geoid, "name": name, "lat": lat, "lon": lon})
    return places


def _geocode_place(place):
    url = GEOCODER_URL.format(lat=place["lat"], lon=place["lon"])
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            data = json.loads(r.read().decode("utf-8"))
        counties = data.get("result", {}).get("geographies", {}).get("Counties", [])
        if counties:
            c = counties[0]
            return (place["geoid"], c.get("STATE","") + c.get("COUNTY",""), c.get("NAME",""))
    except Exception:
        pass
    return None


def _fetch_county_names(state_fips):
    url = f"{CENSUS_BASE}/2023/acs/acs5?get=NAME&for=county:*&in=state:{state_fips}"
    data = census_get(url)
    result = {}
    for row in data[1:]:
        d = dict(zip(data[0], row))
        fips = d["state"] + d["county"]
        result[fips] = d["NAME"].replace(" County", "").split(",")[0].strip()
    return result


def _build_crosswalk(abbrev, state_fips, force=False):
    out = CENSUS_DIR / f"place_county_crosswalk_{abbrev.lower()}.txt"
    if not force and out.exists():
        log(f"  [{abbrev}] Crosswalk already exists — skipping")
        return "skipped"

    log(f"\n  [{abbrev}] Building crosswalk (FIPS {state_fips})...")
    try:
        places = _fetch_gazetteer(state_fips)
        log(f"    {len(places)} places in gazetteer")
    except Exception as e:
        log(f"    FAILED fetching gazetteer: {e}")
        return "failed"

    try:
        county_names = _fetch_county_names(state_fips)
        log(f"    {len(county_names)} counties from ACS")
    except Exception as e:
        log(f"    WARNING: Could not fetch county names: {e}")
        county_names = {}

    log(f"    Geocoding {len(places)} place centroids...")
    place_to_county = {}
    failed = 0
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(_geocode_place, p): p for p in places}
        done = 0
        for future in as_completed(futures):
            done += 1
            if done % 200 == 0:
                log(f"      {done}/{len(places)} geocoded...")
            result = future.result()
            if result:
                geoid, county_fips, _ = result
                place_to_county[geoid] = county_fips
            else:
                failed += 1

    log(f"    Mapped {len(place_to_county)} places ({failed} failed)")

    with open(out, "w") as f:
        f.write("STATE|STATEFP|PLACEFP|PLACENS|PLACENAME|TYPE|CLASSFP|FUNCSTAT|COUNTIES\n")
        for place in sorted(places, key=lambda p: p["geoid"]):
            county_fips = place_to_county.get(place["geoid"])
            if not county_fips:
                continue
            county_name = county_names.get(county_fips, "")
            if not county_name:
                continue
            place_fips = place["geoid"][2:]
            f.write(f"{abbrev.upper()}|{state_fips}|{place_fips}|00000000|"
                    f"{place['name']}|PLACE|C1|A|{county_name} County\n")

    lines = sum(1 for _ in open(out)) - 1
    log(f"    {lines} mappings → {out.name}")
    return "ok"


def phase_crosswalks(states, force=False):
    log("\n" + "="*60)
    log("PHASE 2: Place-County Crosswalks")
    log("="*60)
    CENSUS_DIR.mkdir(parents=True, exist_ok=True)
    results = {}
    for abbrev, fips in states.items():
        results[abbrev] = _build_crosswalk(abbrev, fips, force=force)
        time.sleep(0.5)
    _print_summary("Crosswalks", results)
    return results


# ─── Phase 3: FCC Data from Google Drive ──────────────────────────────────────

def _get_drive_service():
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build

    SCOPES = ["https://www.googleapis.com/auth/drive"]
    token_path = PROJECT_DIR / "token.json"
    creds_path = PROJECT_DIR / "credentials.json"
    creds = None

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not creds_path.exists():
                log("  ERROR: credentials.json not found in project root")
                return None
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w") as f:
            f.write(creds.to_json())

    return build("drive", "v3", credentials=creds)


def _find_drive_folder(service, path):
    """Walk a path like 'Root/Sub/Leaf' and return the leaf folder ID, or None."""
    parts = path.split("/")
    parent_id = None
    for name in parts:
        q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_id:
            q += f" and '{parent_id}' in parents"
        res = service.files().list(q=q, fields="files(id)").execute()
        files = res.get("files", [])
        if not files:
            return None
        parent_id = files[0]["id"]
    return parent_id


def _find_file_in_folder(service, filename, folder_id):
    """Return file ID of filename inside folder_id, or None."""
    q = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    res = service.files().list(q=q, fields="files(id,name)").execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None


def _download_drive_file(service, file_id, dest_path):
    """Download a Drive file to dest_path."""
    from googleapiclient.http import MediaIoBaseDownload
    import io
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(str(dest_path), "wb")
    downloader = MediaIoBaseDownload(fh, request, chunksize=10*1024*1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.close()


def _download_and_extract(service, zip_name, folder_id, dest_csv_path, rename_to):
    """Download a ZIP from Drive, extract the first CSV, rename to rename_to. Returns True on success."""
    import zipfile, io, tempfile

    fid = _find_file_in_folder(service, zip_name, folder_id)
    if not fid:
        log(f"    WARNING: {zip_name} not found in Drive")
        return False

    log(f"    Downloading {zip_name}...", )
    tmp_zip = dest_csv_path.parent / (zip_name)
    _download_drive_file(service, fid, tmp_zip)
    size_mb = tmp_zip.stat().st_size / (1024*1024)
    log(f"    done ({size_mb:.1f} MB) — extracting...")

    try:
        with zipfile.ZipFile(tmp_zip, "r") as z:
            csvs = [n for n in z.namelist() if n.endswith(".csv")]
            if not csvs:
                log(f"    WARNING: no CSV found inside {zip_name}")
                tmp_zip.unlink()
                return False
            # Extract first CSV to a temp name then rename
            extracted = dest_csv_path.parent / csvs[0]
            z.extract(csvs[0], dest_csv_path.parent)
            extracted.rename(rename_to)
        tmp_zip.unlink()
        log(f"    → {rename_to.name}")
        return True
    except Exception as e:
        log(f"    ERROR extracting {zip_name}: {e}")
        if tmp_zip.exists():
            tmp_zip.unlink()
        return False


def phase_fcc(states, force=False):
    log("\n" + "="*60)
    log("PHASE 3: FCC Data from Google Drive")
    log("="*60)
    FCC_DIR.mkdir(parents=True, exist_ok=True)

    try:
        service = _get_drive_service()
    except Exception as e:
        log(f"  ERROR: Could not connect to Google Drive: {e}")
        return {}

    # Locate Drive zip folders
    loc_folder_id = _find_drive_folder(service, DRIVE_LOC_ZIP_PATH)
    sum_folder_id = _find_drive_folder(service, DRIVE_SUM_ZIP_PATH)
    if not loc_folder_id or not sum_folder_id:
        log(f"  ERROR: Could not find Drive zip folders.")
        log(f"  Expected: {DRIVE_LOC_ZIP_PATH}")
        log(f"  Make sure the FCC download script ran successfully first.")
        return {}

    # Pull national provider summary once
    provider_dest = FCC_DIR / "provider_summary_by_geography.csv"
    if force or not provider_dest.exists():
        ok = _download_and_extract(service, PROVIDER_ZIP_NAME, sum_folder_id,
                                   provider_dest, provider_dest)
        if not ok:
            log(f"  WARNING: Could not get national provider summary from Drive")
    else:
        log(f"  Provider summary already exists — skipping")

    results = {}
    for abbrev, fips in states.items():
        sl = abbrev.lower()
        fttp_dest  = FCC_DIR / f"fttp_locations_{sl}.csv"
        place_dest = FCC_DIR / f"broadband_summary_place_{sl}.csv"
        needs_fttp  = force or not fttp_dest.exists()
        needs_place = force or not place_dest.exists()

        if not needs_fttp and not needs_place:
            log(f"  [{abbrev}] FCC CSVs already exist — skipping")
            results[abbrev] = "skipped"
            continue

        log(f"\n  [{abbrev}]")
        ok = True

        if needs_fttp:
            ok &= _download_and_extract(
                service, FTTP_ZIP_PATTERN.format(fips=fips),
                loc_folder_id, fttp_dest, fttp_dest
            )

        if needs_place:
            ok &= _download_and_extract(
                service, PLACE_ZIP_PATTERN.format(fips=fips),
                sum_folder_id, place_dest, place_dest
            )

        results[abbrev] = "ok" if ok else "partial"

    _print_summary("FCC Data", results)
    return results


# ─── Phase 4: Run Pipeline ────────────────────────────────────────────────────

def phase_pipeline(states, force=False):
    log("\n" + "="*60)
    log("PHASE 4: State Data Pipeline")
    log("="*60)
    pipeline_script = SCRIPT_DIR / "process_state_data.py"
    results = {}

    for abbrev, fips in states.items():
        sl = abbrev.lower()
        out_json = OUT_DIR / f"{sl}-unified-data.json"

        if not force and out_json.exists():
            log(f"  [{abbrev}] Output already exists — skipping ({out_json.name})")
            results[abbrev] = "skipped"
            continue

        # Check required inputs exist
        missing = []
        for path in [
            FCC_DIR / f"fttp_locations_{sl}.csv",
            FCC_DIR / "provider_summary_by_geography.csv",
            CENSUS_DIR / f"census_acs_{sl}.json",
            CENSUS_DIR / f"census_acs_{sl}_2018.json",
        ]:
            if not path.exists():
                missing.append(path.name)
        if missing:
            log(f"  [{abbrev}] SKIPPED — missing inputs: {', '.join(missing)}")
            results[abbrev] = "skipped_missing_inputs"
            continue

        log(f"\n  [{abbrev}] Running pipeline...")
        try:
            result = subprocess.run(
                [sys.executable, str(pipeline_script),
                 "--state", abbrev, "--fips-prefix", fips],
                cwd=str(PROJECT_DIR),
                capture_output=False,
                timeout=600,
            )
            if result.returncode == 0:
                log(f"  [{abbrev}] Pipeline complete → {out_json.name}")
                results[abbrev] = "ok"
            else:
                log(f"  [{abbrev}] Pipeline FAILED (exit code {result.returncode})")
                results[abbrev] = "failed"
        except subprocess.TimeoutExpired:
            log(f"  [{abbrev}] Pipeline TIMED OUT (>10 min)")
            results[abbrev] = "timeout"
        except Exception as e:
            log(f"  [{abbrev}] Pipeline ERROR: {e}")
            results[abbrev] = "error"

    _print_summary("Pipeline", results)
    return results


# ─── Summary Printer ──────────────────────────────────────────────────────────

def _print_summary(phase_name, results):
    ok       = [k for k,v in results.items() if v in ("ok", "skipped")]
    failed   = [k for k,v in results.items() if v not in ("ok", "skipped", "skipped_missing_inputs")]
    skipped  = [k for k,v in results.items() if v == "skipped"]
    missing  = [k for k,v in results.items() if v == "skipped_missing_inputs"]

    log(f"\n  {phase_name} Summary:")
    log(f"    Done:    {len(ok)} ({', '.join(sorted(skipped)[:5])}{'...' if len(skipped)>5 else ''} skipped)")
    if failed:
        log(f"    Failed:  {len(failed)} — {', '.join(failed)}")
    if missing:
        log(f"    Missing inputs: {len(missing)} — {', '.join(missing)}")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Batch process all states")
    parser.add_argument("--states", nargs="+", metavar="STATE",
                        help="Subset of states to process (default: all 50 + DC)")
    parser.add_argument("--phase", choices=["census", "crosswalk", "fcc", "pipeline"],
                        help="Run only this phase (default: all phases in order)")
    parser.add_argument("--force", action="store_true",
                        help="Re-run even if output files already exist")
    args = parser.parse_args()

    # Resolve state list
    if args.states:
        states = {s.upper(): ALL_STATES[s.upper()] for s in args.states
                  if s.upper() in ALL_STATES}
        unknown = [s for s in args.states if s.upper() not in ALL_STATES]
        if unknown:
            log(f"Warning: unknown states ignored: {unknown}")
    else:
        states = ALL_STATES

    log("=" * 60)
    log("  FiberMapUSA — Batch State Processor")
    log("=" * 60)
    log(f"  States:  {len(states)} ({', '.join(list(states)[:10])}{'...' if len(states)>10 else ''})")
    log(f"  Phase:   {args.phase or 'all'}")
    log(f"  Force:   {args.force}")

    phases = args.phase or "all"

    if phases in ("all", "census"):
        phase_census(states, force=args.force)
    if phases in ("all", "crosswalk"):
        phase_crosswalks(states, force=args.force)
    if phases in ("all", "fcc"):
        phase_fcc(states, force=args.force)
    if phases in ("all", "pipeline"):
        phase_pipeline(states, force=args.force)

    log("\n" + "="*60)
    log("  All phases complete.")
    log("="*60)
    log("\nNext steps:")
    log("  1. Review any failed states above and re-run with --states <failed>")
    log("  2. Add all states to js/data.js and js/map.js")
    log("  3. Test the site locally")
    log("  4. Deploy")


if __name__ == "__main__":
    main()
