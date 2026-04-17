#!/usr/bin/env python3
"""
Fetch news from Fiber Broadband Association RSS feed, tag articles with
matching county GEOIDs and state codes, and upsert to Supabase.

Environment variables:
    SUPABASE_URL          https://xxx.supabase.co
    SUPABASE_SERVICE_KEY  sb_secret_... (service role — never use anon here)

Usage:
    python3 pipeline/fetch_news.py
    SUPABASE_URL=... SUPABASE_SERVICE_KEY=... python3 pipeline/fetch_news.py
"""

import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import requests
from supabase import create_client

# ── Config ────────────────────────────────────────────────────────────────────

RSS_URL = 'https://fiberbroadband.org/feed/'

SUPABASE_URL         = os.environ.get('SUPABASE_URL', '')
SUPABASE_SERVICE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

# ── State name lookup ─────────────────────────────────────────────────────────

STATE_NAMES = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
    'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
    'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
    'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
    'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
    'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
    'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
    'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
    'Wisconsin': 'WI', 'Wyoming': 'WY', 'District of Columbia': 'DC',
}
STATE_ABBREVS = set(STATE_NAMES.values())

# Pre-compiled state name pattern (longest first to avoid partial matches)
_state_name_pattern = re.compile(
    r'\b(' + '|'.join(re.escape(n) for n in sorted(STATE_NAMES, key=len, reverse=True)) + r')\b'
)
# Abbreviation pattern: look for ", MO" / "in MO" / "(MO)" context
_state_abbrev_pattern = re.compile(
    r'(?:,\s*|(?<=\bin\s)|(?<=\()|(?<=\s))(' + '|'.join(STATE_ABBREVS) + r')(?=\b|\))'
)

# ── Matching ──────────────────────────────────────────────────────────────────

def build_county_lookup(counties: list) -> dict:
    """
    Returns dict: county_name_lower → [(geoid, state_code), ...]
    Handles multi-state ambiguity (e.g. "Washington" exists in 30 states).
    """
    lookup: dict = {}
    for c in counties:
        key = c['name'].lower().strip()
        lookup.setdefault(key, []).append((c['geoid'], c['state_code']))
    return lookup


def extract_state_codes(text: str) -> set:
    codes = set()
    for m in _state_name_pattern.finditer(text):
        codes.add(STATE_NAMES[m.group(1)])
    for m in _state_abbrev_pattern.finditer(text):
        codes.add(m.group(1))
    return codes


def extract_tags(text: str, county_lookup: dict) -> tuple[list, list]:
    """
    Returns (county_geoids, state_codes) matched in the text.

    Strategy:
    1. Identify state mentions to disambiguate common county names.
    2. Match "X County" patterns against the county lookup.
    3. Only tag GEOIDs whose state_code appears in the identified states
       (or tag all if no states were identified — keeps broad articles broad).
    """
    state_codes = extract_state_codes(text)

    geoids: set = set()

    # Match "X County" — captures multi-word names like "St. Louis County"
    for m in re.finditer(r'\b([A-Z][A-Za-z.\s]{1,30}?)\s+County\b', text):
        raw = m.group(1).strip()
        name = raw.lower()
        # Skip generic phrases
        if name in ('the', 'a', 'an', 'this', 'that', 'each', 'every', 'any'):
            continue
        if name not in county_lookup:
            continue
        for geoid, state in county_lookup[name]:
            if not state_codes or state in state_codes:
                geoids.add(geoid)

    return sorted(geoids), sorted(state_codes)


# ── RSS parsing ───────────────────────────────────────────────────────────────

_PUB_DATE_FMTS = (
    '%a, %d %b %Y %H:%M:%S %z',
    '%a, %d %b %Y %H:%M:%S GMT',
    '%a, %d %b %Y %H:%M:%S +0000',
)

def _parse_date(raw: str):
    for fmt in _PUB_DATE_FMTS:
        try:
            dt = datetime.strptime(raw.strip(), fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass
    return None


def _strip_html(html: str) -> str:
    text = re.sub(r'<[^>]+>', ' ', html)
    return re.sub(r'\s+', ' ', text).strip()


def parse_rss(xml_text: str) -> list:
    root = ET.fromstring(xml_text)
    channel = root.find('channel')
    if channel is None:
        raise ValueError('RSS feed missing <channel> element')

    articles = []
    for item in channel.findall('item'):
        title    = (item.findtext('title') or '').strip()
        link     = (item.findtext('link')  or '').strip()
        pub_raw  = item.findtext('pubDate') or ''
        desc_raw = item.findtext('description') or ''

        # content:encoded often has more text
        content_encoded = item.findtext('{http://purl.org/rss/1.0/modules/content/}encoded') or ''
        full_text = _strip_html(content_encoded or desc_raw)
        excerpt   = full_text[:500] if len(full_text) > 500 else full_text
        match_text = title + ' ' + full_text

        dt = _parse_date(pub_raw)
        articles.append({
            'title':        title,
            'link':         link,
            'published_at': dt.isoformat() if dt else None,
            'excerpt':      excerpt,
            '_match_text':  match_text,
        })
    return articles


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print('ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables.')
        sys.exit(1)

    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # Load all counties for name matching
    print('Loading counties from Supabase...')
    result = sb.table('counties').select('geoid,name,state_code').execute()
    counties = result.data or []
    county_lookup = build_county_lookup(counties)
    print(f'  Loaded {len(counties)} counties across '
          f'{len(set(c["state_code"] for c in counties))} states.')

    # Fetch RSS feed
    print(f'\nFetching RSS: {RSS_URL}')
    resp = requests.get(RSS_URL, timeout=30, headers={'User-Agent': 'FiberMapUSA/1.0'})
    resp.raise_for_status()
    articles = parse_rss(resp.text)
    print(f'  Parsed {len(articles)} articles.')

    # Tag and upsert
    print('\nTagging and upserting...')
    upserted = skipped = 0
    for art in articles:
        if not art['link']:
            skipped += 1
            continue

        geoids, state_codes = extract_tags(art['_match_text'], county_lookup)

        row = {
            'title':        art['title'],
            'link':         art['link'],
            'published_at': art['published_at'],
            'excerpt':      art['excerpt'],
            'county_tags':  geoids,
            'state_tags':   state_codes,
        }

        try:
            sb.table('news_articles').upsert(row, on_conflict='link').execute()
            upserted += 1
            tag_str = ''
            if state_codes:
                tag_str = f'[{", ".join(state_codes)}]'
                if geoids:
                    tag_str += f' {len(geoids)} counties'
            else:
                tag_str = '[untagged]'
            print(f'  {tag_str:30s}  {art["title"][:60]}')
        except Exception as e:
            skipped += 1
            print(f'  ERROR: {e}  →  {art["link"][:60]}')

    print(f'\nDone. Upserted {upserted}, skipped {skipped}.')


if __name__ == '__main__':
    main()
