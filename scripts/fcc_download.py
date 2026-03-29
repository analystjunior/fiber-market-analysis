"""
FCC Broadband Availability Data Downloader + Google Drive Uploader
==================================================================
Downloads fixed broadband availability CSVs from the FCC National Broadband Map
and uploads them to an organized folder structure in Google Drive.

SETUP (run these in your terminal first):
    pip install requests google-api-python-client google-auth-httplib2 google-auth-oauthlib

FCC CREDENTIALS:
    Set these as environment variables (or edit the CONFIG section below):
        export FCC_USERNAME=your_fcc_email@example.com
        export FCC_API_TOKEN=your_44_char_token

    To get a token: broadbandmap.fcc.gov → login → Account → Manage API Access → Generate

GOOGLE DRIVE CREDENTIALS:
    1. Go to https://console.cloud.google.com/
    2. Create a project > Enable "Google Drive API"
    3. Create OAuth 2.0 credentials (Desktop app type)
    4. Download the JSON and save as 'credentials.json' in the same folder as this script
    5. On first run, a browser window will open to authorize — after that it saves a token.json

USAGE:
    python fcc_download.py                              # Download all states, all fixed broadband
    python fcc_download.py --states MO NY TX            # Download specific states
    python fcc_download.py --states MO --tech 50        # Only Fiber to the Premises (tech code 50)
    python fcc_download.py --states MO --no-drive       # Download only, skip Google Drive
    python fcc_download.py --states MO --drive-only     # Upload to Drive, delete local files after
    python fcc_download.py --list-dates                 # Show available as-of dates and exit
    python fcc_download.py --list-files MO              # List available files for a state and exit

TECHNOLOGY CODES:
    10 = Copper DSL
    40 = Cable (DOCSIS)
    50 = Fiber to the Premises (FTTP)
    60 = GSO Satellite
    61 = NGSO Satellite (Starlink etc)
    70 = Unlicensed Fixed Wireless
    71 = Licensed Fixed Wireless
    72 = LBR Fixed Wireless
"""

import os
import sys
import json
import time
import zipfile
import argparse
import requests
from pathlib import Path

# ─── CONFIGURATION ────────────────────────────────────────────────────────────

FCC_USERNAME  = os.getenv("FCC_USERNAME",  "YOUR_FCC_USERNAME_HERE")
FCC_API_TOKEN = os.getenv("FCC_API_TOKEN", "YOUR_FCC_API_TOKEN_HERE")

# Google Drive folder name (will be created if it doesn't exist)
GDRIVE_ROOT_FOLDER = "Fiber Website"

# Local directory to store downloaded files
LOCAL_DOWNLOAD_DIR = Path("./fcc_data_downloads")

# FCC BDC API base URL
FCC_BASE_URL = "https://broadbandmap.fcc.gov/api/public/map"

# All US states + territories with their FIPS codes
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
    "DC": "11", "PR": "72", "VI": "78", "GU": "66", "AS": "60",
    "MP": "69",
}

# ─── FCC API FUNCTIONS ────────────────────────────────────────────────────────

def get_fcc_headers():
    return {
        "username":   FCC_USERNAME,
        "hash_value": FCC_API_TOKEN,
        "Accept":     "application/json",
        "User-Agent": "FiberMapUSA/1.0 (fibermapusa.com)",
    }


def get_latest_filing_date():
    """Fetch the most recent availability as-of date from the FCC API."""
    try:
        url = f"{FCC_BASE_URL}/listAsOfDates"
        resp = requests.get(url, headers=get_fcc_headers(), timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status_code") != 200:
            raise ValueError(data.get("message", "unknown error"))
        dates = sorted(
            [item["as_of_date"] for item in data.get("data", [])
             if item.get("data_type") == "availability"],
            reverse=True,
        )
        if dates:
            print(f"  Latest FCC availability date: {dates[0]}")
            return dates[0]
    except Exception as e:
        print(f"  Warning: Could not auto-detect filing date ({e}). Using fallback.")
    return "2025-06-30"


def _fetch_listing(as_of_date, category):
    """Fetch the full file listing for a given category from the FCC API."""
    url = f"{FCC_BASE_URL}/downloads/listAvailabilityData/{as_of_date}"
    resp = requests.get(url, headers=get_fcc_headers(), params={"category": category}, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if data.get("status_code") != 200:
        raise ValueError(data.get("message", "unknown error"))
    return data.get("data", [])


def list_location_files(as_of_date, state_fips=None, tech_codes=None):
    """
    List State-level Fixed Broadband Location Coverage files.
    Optionally filter by state_fips and/or tech_codes (e.g. ['40','50','70','71','72']).
    """
    try:
        files = _fetch_listing(as_of_date, "State")
        files = [f for f in files
                 if f.get("technology_type") == "Fixed Broadband"
                 and f.get("subcategory") == "Location Coverage"]
        if state_fips:
            files = [f for f in files if f.get("state_fips") == state_fips]
        if tech_codes:
            files = [f for f in files if str(f.get("technology_code")) in tech_codes]
        return files
    except Exception as e:
        print(f"  Error listing location files: {e}")
        return []


def list_summary_files(as_of_date, state_fips=None):
    """
    List Summary files: per-state Census Place summaries + national geography summaries.
    If state_fips given, returns only that state's place summary + national files.
    """
    try:
        files = _fetch_listing(as_of_date, "Summary")
        result = []
        for f in files:
            sub = f.get("subcategory", "")
            tech = f.get("technology_type", "")
            fips = f.get("state_fips")

            # National files (no state_fips) — always include once
            if fips is None and sub in (
                "Summary by Geography Type - Other Geographies",
                "Provider Summary by Geography Type",
            ) and tech in ("Fixed Broadband", ""):
                result.append(f)

            # Per-state Census Place summary (Fixed Broadband only)
            elif sub == "Summary by Geography Type - Census Place" and tech == "Fixed Broadband":
                if state_fips is None or fips == state_fips:
                    result.append(f)

        return result
    except Exception as e:
        print(f"  Error listing summary files: {e}")
        return []


# Keep backward compat alias used by --list-files
def list_available_files(as_of_date, state_fips=None, tech_codes=None):
    return list_location_files(as_of_date, state_fips=state_fips, tech_codes=tech_codes)


def download_file(file_id, file_name, output_dir):
    """Download a single file by its FCC file_id. Returns local path or None."""
    # Files are CSVs delivered as zip archives
    out_path = output_dir / f"{file_name}.zip"
    if out_path.exists():
        print(f"  ✓ Already downloaded: {out_path.name}")
        return out_path

    url = f"{FCC_BASE_URL}/downloads/downloadFile/availability/{file_id}"
    print(f"  Downloading {file_name}...", end=" ", flush=True)
    try:
        resp = requests.get(url, headers=get_fcc_headers(), timeout=300, stream=True)
        resp.raise_for_status()

        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)

        size_mb = out_path.stat().st_size / (1024 * 1024)
        print(f"done ({size_mb:.1f} MB)")
        return out_path

    except requests.HTTPError as e:
        print(f"FAILED ({e.response.status_code})")
        if e.response.status_code == 401:
            print("    → Check FCC_USERNAME and FCC_API_TOKEN")
        elif e.response.status_code == 404:
            print(f"    → File {file_id} not found")
        if out_path.exists():
            out_path.unlink()
        return None
    except Exception as e:
        print(f"FAILED ({e})")
        if out_path.exists():
            out_path.unlink()
        return None


def unzip_file(zip_path, extract_dir):
    """Unzip a downloaded file and return list of extracted CSV paths."""
    csv_files = []
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(extract_dir)
            csv_files = [extract_dir / name for name in z.namelist() if name.endswith(".csv")]
        if csv_files:
            print(f"    Extracted: {', '.join(p.name for p in csv_files)}")
    except Exception as e:
        print(f"    Warning: Could not unzip {zip_path.name} ({e})")
    return csv_files


# ─── GOOGLE DRIVE FUNCTIONS ───────────────────────────────────────────────────

def get_drive_service():
    """Authenticate and return a Google Drive API service object."""
    try:
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build

        SCOPES = ["https://www.googleapis.com/auth/drive.file"]
        creds = None

        if Path("token.json").exists():
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not Path("credentials.json").exists():
                    print("\n  credentials.json not found — skipping Drive upload.")
                    print("  See setup instructions at the top of this script.")
                    return None
                flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
                creds = flow.run_local_server(port=0)
            with open("token.json", "w") as token:
                token.write(creds.to_json())

        return build("drive", "v3", credentials=creds)

    except ImportError:
        print("\n  Google API packages not installed.")
        print("  Run: pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib")
        return None


def get_or_create_folder(service, name, parent_id=None):
    """Get a Drive folder by name (under parent), or create it if missing."""
    query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]
    metadata = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    if parent_id:
        metadata["parents"] = [parent_id]
    folder = service.files().create(body=metadata, fields="id").execute()
    print(f"    Created Drive folder: {name}")
    return folder["id"]


def file_exists_in_drive(service, filename, folder_id):
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    return len(results.get("files", [])) > 0


def upload_to_drive(service, local_path, folder_id):
    """Upload a local file to a specific Google Drive folder."""
    from googleapiclient.http import MediaFileUpload
    import mimetypes

    filename = local_path.name
    if file_exists_in_drive(service, filename, folder_id):
        print(f"    ✓ Already in Drive: {filename}")
        return

    mime_type, _ = mimetypes.guess_type(str(local_path))
    mime_type = mime_type or "application/octet-stream"
    media = MediaFileUpload(str(local_path), mimetype=mime_type, resumable=True)
    metadata = {"name": filename, "parents": [folder_id]}

    print(f"    Uploading to Drive: {filename}...", end=" ", flush=True)
    try:
        service.files().create(body=metadata, media_body=media, fields="id").execute()
        print("done")
    except Exception as e:
        print(f"FAILED ({e})")


def build_drive_structure(service, as_of_date):
    """Create the Drive folder structure and return folder IDs."""
    print("\n  Setting up Google Drive folder structure...")
    root_id = get_or_create_folder(service, GDRIVE_ROOT_FOLDER)
    date_id = get_or_create_folder(service, as_of_date, root_id)
    loc_id  = get_or_create_folder(service, "location_coverage", date_id)
    sum_id  = get_or_create_folder(service, "summaries", date_id)
    return {
        "location": {
            "zips": get_or_create_folder(service, "zips", loc_id),
            "csv":  get_or_create_folder(service, "csv",  loc_id),
        },
        "summary": {
            "zips": get_or_create_folder(service, "zips", sum_id),
            "csv":  get_or_create_folder(service, "csv",  sum_id),
        },
    }


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Download FCC broadband availability data and optionally upload to Google Drive"
    )
    parser.add_argument("--states", nargs="+", metavar="STATE",
                        help="State abbreviations (e.g. MO NY TX). Default: all states.")
    parser.add_argument("--tech", nargs="+", metavar="CODE",
                        help="Technology codes to download (e.g. 50 for FTTP). Default: all fixed broadband.")
    parser.add_argument("--as-of-date", metavar="YYYY-MM-DD",
                        help="FCC filing date. Auto-detects latest if not specified.")
    parser.add_argument("--no-drive", action="store_true",
                        help="Skip Google Drive upload (download only)")
    parser.add_argument("--drive-only", action="store_true",
                        help="Upload to Drive and delete local files after (no local copies kept)")
    parser.add_argument("--no-unzip", action="store_true",
                        help="Keep ZIPs, skip extracting CSVs")
    parser.add_argument("--no-summary", action="store_true",
                        help="Skip summary files (Census Place + Provider Summary by Geography)")
    parser.add_argument("--list-dates", action="store_true",
                        help="Print available as-of dates and exit")
    parser.add_argument("--list-files", metavar="STATE",
                        help="List available files for a state (e.g. MO) and exit")
    args = parser.parse_args()

    print("=" * 60)
    print("  FiberMapUSA — FCC Data Downloader")
    print("=" * 60)

    # Validate credentials
    if FCC_USERNAME == "YOUR_FCC_USERNAME_HERE" or FCC_API_TOKEN == "YOUR_FCC_API_TOKEN_HERE":
        print("\n  FCC credentials not set.")
        print("  Set environment variables before running:")
        print("    export FCC_USERNAME=your_email@example.com")
        print("    export FCC_API_TOKEN=your_token_here")
        print("  Get a token at: broadbandmap.fcc.gov → Account → Manage API Access")
        sys.exit(1)

    # --list-dates mode
    if args.list_dates:
        url = f"{FCC_BASE_URL}/listAsOfDates"
        resp = requests.get(url, headers=get_fcc_headers(), timeout=30)
        data = resp.json()
        avail = [d["as_of_date"] for d in data["data"] if d["data_type"] == "availability"]
        print("\n  Available availability as-of dates:")
        for d in sorted(avail, reverse=True):
            print(f"    {d}")
        return

    # Get filing date
    as_of_date = args.as_of_date or get_latest_filing_date()
    print(f"  As-of date: {as_of_date}")

    # --list-files mode
    if args.list_files:
        state = args.list_files.upper()
        fips = ALL_STATES.get(state)
        if not fips:
            print(f"  Unknown state: {state}")
            sys.exit(1)
        files = list_available_files(as_of_date, state_fips=fips)
        print(f"\n  Available Fixed Broadband files for {state}:")
        for f in files:
            print(f"    [{f['technology_code']:>3}] {f['technology_code_desc']:<30} file_id={f['file_id']}  records={f['record_count']}")
        return

    # Determine states
    if args.states:
        states = {s.upper(): ALL_STATES[s.upper()] for s in args.states if s.upper() in ALL_STATES}
        invalid = [s for s in args.states if s.upper() not in ALL_STATES]
        if invalid:
            print(f"  Warning: Unknown state codes skipped: {invalid}")
    else:
        states = ALL_STATES

    tech_codes = args.tech  # None = all

    print(f"  States: {', '.join(states)}")
    if tech_codes:
        print(f"  Technology codes: {', '.join(tech_codes)}")
    else:
        print("  Technology codes: all fixed broadband")
    if not args.no_summary:
        print("  Summary files:    yes (Census Place + Provider/Geography summaries)")

    # Set up Google Drive
    drive_service = None
    drive_folders = None
    if not args.no_drive:
        print("\n  Connecting to Google Drive...")
        drive_service = get_drive_service()
        if drive_service:
            drive_folders = build_drive_structure(drive_service, as_of_date)
        else:
            print("  Continuing without Drive upload.")

    total_downloaded = 0
    total_files = 0

    def process_file(f, zip_dest, csv_dest, drive_bucket):
        """Download, optionally unzip, optionally upload, optionally delete."""
        nonlocal total_downloaded, total_files
        total_files += 1
        zip_name = f"{f['file_name']}.zip"

        # In drive-only mode, skip everything if the zip is already in Drive
        if args.drive_only and drive_service and drive_folders:
            if file_exists_in_drive(drive_service, zip_name, drive_folders[drive_bucket]["zips"]):
                print(f"  ✓ Already in Drive: {zip_name}")
                total_downloaded += 1
                return

        zip_path = download_file(f["file_id"], f["file_name"], zip_dest)
        if not zip_path:
            return

        total_downloaded += 1

        if drive_service and drive_folders:
            upload_to_drive(drive_service, zip_path, drive_folders[drive_bucket]["zips"])

        if not args.no_unzip:
            csvs = unzip_file(zip_path, csv_dest)
            if drive_service and drive_folders:
                for csv_path in csvs:
                    upload_to_drive(drive_service, csv_path, drive_folders[drive_bucket]["csv"])
                    if args.drive_only:
                        csv_path.unlink()

        if args.drive_only and zip_path.exists():
            zip_path.unlink()

        time.sleep(0.3)

    # ── Location Coverage files (per state) ──────────────────────────────────
    print(f"\n  Fetching location coverage listings...")
    loc_zip_dir = LOCAL_DOWNLOAD_DIR / as_of_date / "location_coverage" / "zips"
    loc_csv_dir = LOCAL_DOWNLOAD_DIR / as_of_date / "location_coverage" / "csv"
    loc_zip_dir.mkdir(parents=True, exist_ok=True)
    loc_csv_dir.mkdir(parents=True, exist_ok=True)

    for abbr, fips in states.items():
        print(f"\n[{abbr}]")
        files = list_location_files(as_of_date, state_fips=fips, tech_codes=tech_codes)
        if not files:
            print(f"  No matching location files found for {abbr}")
            continue
        print(f"  {len(files)} file(s): " + ", ".join(f['technology_code_desc'] for f in files))
        for f in files:
            process_file(f, loc_zip_dir, loc_csv_dir, "location")

    # ── Summary files ─────────────────────────────────────────────────────────
    if not args.no_summary:
        print(f"\n\n  Fetching summary file listings...")
        sum_zip_dir = LOCAL_DOWNLOAD_DIR / as_of_date / "summaries" / "zips"
        sum_csv_dir = LOCAL_DOWNLOAD_DIR / as_of_date / "summaries" / "csv"
        sum_zip_dir.mkdir(parents=True, exist_ok=True)
        sum_csv_dir.mkdir(parents=True, exist_ok=True)

        # National summary files — download once
        print("\n[National Summary Files]")
        national = list_summary_files(as_of_date, state_fips=None)
        national = [f for f in national if f.get("state_fips") is None]
        seen_national = set()
        for f in national:
            if f["file_id"] not in seen_national:
                seen_national.add(f["file_id"])
                process_file(f, sum_zip_dir, sum_csv_dir, "summary")

        # Per-state Census Place summaries
        for abbr, fips in states.items():
            state_sum = list_summary_files(as_of_date, state_fips=fips)
            state_sum = [f for f in state_sum if f.get("state_fips") == fips]
            if state_sum:
                print(f"\n[{abbr} Summary]")
                for f in state_sum:
                    process_file(f, sum_zip_dir, sum_csv_dir, "summary")

    # ── Final summary ─────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  Download Complete")
    print("=" * 60)
    print(f"  Files downloaded:  {total_downloaded} / {total_files}")
    if args.drive_only:
        print(f"  Local files:       deleted (drive-only mode)")
    else:
        print(f"  Local folder:      {LOCAL_DOWNLOAD_DIR.resolve()}")
    if drive_service:
        print(f"  Google Drive:      {GDRIVE_ROOT_FOLDER}/{as_of_date}/")
    print()


if __name__ == "__main__":
    main()
