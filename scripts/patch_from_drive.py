#!/usr/bin/env python3
"""
Download cable + DSL CSVs from Google Drive one state at a time,
patch the existing unified county JSONs with per-operator passings,
then delete the local temp file.

Requires:
  - credentials.json / token.json already set up (from fcc_download.py run)
  - Existing data/{state}-unified-data.json files

Usage:
    cd "/Users/andrewpetersen/Documents/Website 2/scripts"
    python3 patch_from_drive.py

    # Single state (for testing):
    python3 patch_from_drive.py --state MO
"""

import argparse
import io
import json
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd

SCRIPT_DIR  = Path(__file__).parent.resolve()
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR    = PROJECT_DIR / 'data'

GDRIVE_ROOT   = 'Fiber Website'
GDRIVE_SUBDIR = 'location_coverage/csv'  # navigated as nested folders

STATE_FIPS = {
    'al':'01','ak':'02','az':'04','ar':'05','ca':'06','co':'08','ct':'09',
    'dc':'11','de':'10','fl':'12','ga':'13','hi':'15','id':'16','il':'17',
    'in':'18','ia':'19','ks':'20','ky':'21','la':'22','me':'23','md':'24',
    'ma':'25','mi':'26','mn':'27','ms':'28','mo':'29','mt':'30','ne':'31',
    'nv':'32','nh':'33','nj':'34','nm':'35','ny':'36','nc':'37','nd':'38',
    'oh':'39','ok':'40','or':'41','pa':'42','ri':'44','sc':'45','sd':'46',
    'tn':'47','tx':'48','ut':'49','vt':'50','va':'51','wa':'53','wv':'54',
    'wi':'55','wy':'56',
}

ALL_STATES = sorted(STATE_FIPS.keys())


# ─── Google Drive helpers ─────────────────────────────────────────────────────

def get_drive_service():
    """Authenticate using existing credentials.json / token.json."""
    try:
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build

        SCOPES = ['https://www.googleapis.com/auth/drive.file']
        token_path = SCRIPT_DIR / 'token.json'
        creds_path = SCRIPT_DIR / 'credentials.json'
        creds = None

        if token_path.exists():
            creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not creds_path.exists():
                    print('ERROR: credentials.json not found in scripts/')
                    sys.exit(1)
                flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), SCOPES)
                creds = flow.run_local_server(port=0)
            with open(token_path, 'w') as f:
                f.write(creds.to_json())

        return build('drive', 'v3', credentials=creds)

    except ImportError:
        print('ERROR: Google API packages not installed.')
        print('Run: pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib')
        sys.exit(1)


def get_folder_id(service, name, parent_id=None):
    """Return the Drive folder ID for a given name, or None if not found."""
    q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        q += f" and '{parent_id}' in parents"
    results = service.files().list(q=q, fields='files(id,name)').execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None


def find_csv_folder(service):
    """Navigate Fiber Website → <date folder> → location_coverage → csv."""
    root_id = get_folder_id(service, GDRIVE_ROOT)
    if not root_id:
        print(f'ERROR: Google Drive folder "{GDRIVE_ROOT}" not found.')
        sys.exit(1)

    # Find the date subfolder (e.g. 2025-06-30) — take the first/only one
    q = f"'{root_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    results = service.files().list(q=q, fields='files(id,name)').execute()
    date_folders = results.get('files', [])
    if not date_folders:
        print('ERROR: No date subfolder found inside "Fiber Website" on Drive.')
        sys.exit(1)

    date_id = date_folders[0]['id']
    date_name = date_folders[0]['name']
    print(f'  Using Drive folder: {GDRIVE_ROOT} / {date_name} / location_coverage / csv')

    loc_id = get_folder_id(service, 'location_coverage', date_id)
    if not loc_id:
        print('ERROR: location_coverage folder not found.')
        sys.exit(1)

    csv_id = get_folder_id(service, 'csv', loc_id)
    if not csv_id:
        print('ERROR: csv folder not found inside location_coverage.')
        sys.exit(1)

    return csv_id


def list_csv_files(service, folder_id):
    """Return dict of {filename: file_id} for all CSVs in the folder."""
    files = {}
    page_token = None
    while True:
        q = f"'{folder_id}' in parents and trashed=false and name contains '.csv'"
        kwargs = dict(q=q, fields='nextPageToken,files(id,name)', pageSize=500)
        if page_token:
            kwargs['pageToken'] = page_token
        results = service.files().list(**kwargs).execute()
        for f in results.get('files', []):
            files[f['name']] = f['id']
        page_token = results.get('nextPageToken')
        if not page_token:
            break
    return files


def download_file_to_df(service, file_id, filename):
    """Download a Drive file and return it as a pandas DataFrame."""
    from googleapiclient.http import MediaIoBaseDownload
    print(f'    Downloading {filename}...', end=' ', flush=True)
    request = service.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request, chunksize=10 * 1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)
    size_mb = buf.getbuffer().nbytes / 1024 / 1024
    print(f'done ({size_mb:.1f} MB)')
    return pd.read_csv(buf, dtype={
        'frn': str, 'provider_id': str, 'brand_name': str,
        'location_id': str, 'technology': str,
        'business_residential_code': str, 'state_usps': str,
        'block_geoid': str,
    }, low_memory=False)


# ─── Processing ───────────────────────────────────────────────────────────────

def aggregate_locations(df, fips_prefix):
    """Aggregate a location DataFrame to county-level operator passings."""
    df['county_fips'] = df['block_geoid'].str[:5]
    df = df[
        df['county_fips'].str.startswith(fips_prefix) &
        df['business_residential_code'].isin(['R', 'X'])
    ].copy()

    result = {}
    for fips, group in df.groupby('county_fips'):
        total = group['location_id'].nunique()
        by_provider = (
            group.groupby('brand_name')['location_id']
            .nunique()
            .sort_values(ascending=False)
        )
        result[fips] = {
            'total': int(total),
            'operators': {
                name.strip().strip('"'): int(count)
                for name, count in by_provider.items()
                if name and name.strip()
            }
        }
    return result


def patch_unified(state_lower, cable_data, dsl_data):
    """Merge cable + DSL data into existing unified JSON. Returns True on success."""
    unified_path = DATA_DIR / f'{state_lower}-unified-data.json'
    if not unified_path.exists():
        print(f'  SKIP: No unified JSON at {unified_path}')
        return False

    with open(unified_path) as f:
        counties = json.load(f)

    cable_data = cable_data or {}
    dsl_data   = dsl_data   or {}

    for fips, county in counties.items():
        cable = cable_data.get(fips, {})
        dsl   = dsl_data.get(fips, {})

        county['cable_served'] = cable.get('total', 0)
        county['dsl_served']   = dsl.get('total', 0)

        cable_by_name = cable.get('operators', {})
        dsl_by_name   = dsl.get('operators', {})

        cable_ops = sorted(
            [{'name': n, 'passings': p} for n, p in cable_by_name.items()],
            key=lambda x: x['passings'], reverse=True
        )
        dsl_ops = sorted(
            [{'name': n, 'passings': p} for n, p in dsl_by_name.items()],
            key=lambda x: x['passings'], reverse=True
        )
        county['cable_operators']    = cable_ops
        county['cable_operator_count'] = len(cable_ops)
        county['dsl_operators']      = dsl_ops
        county['dsl_operator_count'] = len(dsl_ops)

        existing_ops = county.get('operators', [])
        seen = set()
        for op in existing_ops:
            name = op.get('name', '')
            seen.add(name)
            fiber_p = op.get('passings', op.get('fiber_passings', 0))
            op['fiber_passings'] = fiber_p
            op['passings']       = fiber_p
            op['cable_passings'] = cable_by_name.get(name, 0)
            op['dsl_passings']   = dsl_by_name.get(name, 0)

        for name, p in cable_by_name.items():
            if name not in seen:
                seen.add(name)
                existing_ops.append({'name': name, 'passings': 0,
                                     'fiber_passings': 0, 'cable_passings': p, 'dsl_passings': 0})
        for name, p in dsl_by_name.items():
            if name not in seen:
                seen.add(name)
                existing_ops.append({'name': name, 'passings': 0,
                                     'fiber_passings': 0, 'cable_passings': 0, 'dsl_passings': p})
        county['operators'] = existing_ops

    with open(unified_path, 'w') as f:
        json.dump(counties, f, indent=2)

    print(f'  Saved → {unified_path.name}')
    return True


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Download cable/DSL CSVs from Google Drive and patch unified JSONs'
    )
    parser.add_argument('--state', help='Process a single state (e.g. MO) for testing')
    args = parser.parse_args()

    states = [args.state.lower()] if args.state else ALL_STATES

    print('Connecting to Google Drive...')
    service = get_drive_service()

    print('Locating CSV folder...')
    csv_folder_id = find_csv_folder(service)

    print('Listing available files...')
    all_files = list_csv_files(service, csv_folder_id)
    print(f'  Found {len(all_files)} CSV files in Drive')

    # Build lookup: fips → {cable: file_id, dsl: file_id}
    # FCC filename pattern: bdc_{fips}_{tech}_fixed_broadband_J25_{date}.csv
    drive_index = {}  # fips_prefix → {'cable': id, 'dsl': id}
    for fname, fid in all_files.items():
        parts = fname.split('_')
        if len(parts) < 3 or parts[0] != 'bdc':
            continue
        fips = parts[1].zfill(2)
        tech = parts[2].lower()  # 'cable' or 'copper'
        if fips not in drive_index:
            drive_index[fips] = {}
        if tech == 'cable':
            drive_index[fips]['cable'] = fid
        elif tech == 'copper':
            drive_index[fips]['dsl'] = fid

    print(f'  Indexed {len(drive_index)} states from Drive filenames\n')

    ok, skipped, failed = 0, 0, 0

    for state_lower in states:
        fips_prefix = STATE_FIPS.get(state_lower)
        if not fips_prefix:
            print(f'[{state_lower.upper()}] Unknown state — skipping')
            skipped += 1
            continue

        state_files = drive_index.get(fips_prefix, {})
        if not state_files:
            print(f'[{state_lower.upper()}] No files found in Drive for FIPS {fips_prefix} — skipping')
            skipped += 1
            continue

        print(f'[{state_lower.upper()}]')

        # Download and aggregate cable
        cable_data = None
        if 'cable' in state_files:
            fname = next(n for n, i in all_files.items()
                         if i == state_files['cable'])
            df = download_file_to_df(service, state_files['cable'], fname)
            cable_data = aggregate_locations(df, fips_prefix)
            print(f'    {len(cable_data)} counties with cable data')
            del df

        # Download and aggregate DSL
        dsl_data = None
        if 'dsl' in state_files:
            fname = next(n for n, i in all_files.items()
                         if i == state_files['dsl'])
            df = download_file_to_df(service, state_files['dsl'], fname)
            dsl_data = aggregate_locations(df, fips_prefix)
            print(f'    {len(dsl_data)} counties with DSL data')
            del df

        # Patch unified JSON
        if patch_unified(state_lower, cable_data, dsl_data):
            ok += 1
        else:
            failed += 1

    print(f'\n{"="*50}')
    print(f'Done. Patched: {ok} | Skipped: {skipped} | Failed: {failed}')
    print(f'{"="*50}')


if __name__ == '__main__':
    main()
