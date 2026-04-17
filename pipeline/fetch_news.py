#!/usr/bin/env python3
"""
Fetch news from the Fiber Broadband Association, tag articles with matching
county GEOIDs and state codes, and upsert to Supabase.

Two modes:
  RSS only (default / daily)  — fetches the last ~10-15 articles from the RSS
                                feed, which includes full excerpts.
  Scrape mode (--pages N)     — scrapes N listing pages from fiberbroadband.org
                                for historical backfill (title + date only).
                                Use --pages 0 to scrape until 404 (all pages).

Usage:
    python3 pipeline/fetch_news.py               # RSS only (daily cron)
    python3 pipeline/fetch_news.py --pages 50    # backfill last 50 pages
    python3 pipeline/fetch_news.py --pages 0     # backfill ALL pages

Environment:
    SUPABASE_URL          https://xxx.supabase.co
    SUPABASE_SERVICE_KEY  sb_secret_...
"""

import os
import re
import sys
import time
import argparse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import requests
from supabase import create_client

# ── Config ────────────────────────────────────────────────────────────────────

RSS_URL      = 'https://fiberbroadband.org/feed/'
POSTS_BASE   = 'https://fiberbroadband.org/posts/'
POSTS_PAGE   = 'https://fiberbroadband.org/posts/page/{}/'

SUPABASE_URL         = os.environ.get('SUPABASE_URL', '')
SUPABASE_SERVICE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

HEADERS = {'User-Agent': 'FiberMapUSA/1.0 (news aggregator; contact fibermapusa.com)'}

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

_state_name_re  = re.compile(
    r'\b(' + '|'.join(re.escape(n) for n in sorted(STATE_NAMES, key=len, reverse=True)) + r')\b'
)
_state_abbrev_re = re.compile(
    r'(?:,\s*|(?<=\bin\s)|(?<=\s))(' + '|'.join(STATE_ABBREVS) + r')(?=\b)'
)

# ── County matching ───────────────────────────────────────────────────────────

def build_county_lookup(counties):
    lookup = {}
    for c in counties:
        key = c['name'].lower().strip()
        lookup.setdefault(key, []).append((c['geoid'], c['state_code']))
    return lookup


def extract_state_codes(text):
    codes = set()
    for m in _state_name_re.finditer(text):
        codes.add(STATE_NAMES[m.group(1)])
    for m in _state_abbrev_re.finditer(text):
        codes.add(m.group(1))
    return codes


def extract_tags(text, county_lookup):
    state_codes = extract_state_codes(text)
    geoids = set()
    for m in re.finditer(r'\b([A-Z][A-Za-z.\s]{1,30}?)\s+County\b', text):
        name = m.group(1).strip().lower()
        if name in ('the', 'a', 'an', 'this', 'that', 'each', 'every', 'any'):
            continue
        if name not in county_lookup:
            continue
        for geoid, state in county_lookup[name]:
            if not state_codes or state in state_codes:
                geoids.add(geoid)
    return sorted(geoids), sorted(state_codes)

# ── RSS parsing ───────────────────────────────────────────────────────────────

_DATE_FMTS = (
    '%a, %d %b %Y %H:%M:%S %z',
    '%a, %d %b %Y %H:%M:%S GMT',
    '%a, %d %b %Y %H:%M:%S +0000',
)

def _parse_rss_date(raw):
    for fmt in _DATE_FMTS:
        try:
            dt = datetime.strptime(raw.strip(), fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None

def _strip_html(html):
    return re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', html)).strip()

def fetch_rss(session):
    print(f'Fetching RSS: {RSS_URL}')
    resp = session.get(RSS_URL, timeout=30)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    channel = root.find('channel')
    articles = []
    for item in channel.findall('item'):
        title   = (item.findtext('title') or '').strip()
        link    = (item.findtext('link')  or '').strip()
        pub_raw = item.findtext('pubDate') or ''
        desc    = item.findtext('description') or ''
        content = item.findtext('{http://purl.org/rss/1.0/modules/content/}encoded') or ''
        full    = _strip_html(content or desc)
        excerpt = full[:500]
        dt      = _parse_rss_date(pub_raw)
        articles.append({
            'title':        title,
            'link':         link,
            'published_at': dt.isoformat() if dt else None,
            'excerpt':      excerpt,
            '_match_text':  title + ' ' + full,
        })
    print(f'  Parsed {len(articles)} articles from RSS.')
    return articles

# ── Listing page scraper ──────────────────────────────────────────────────────

# Matches: <a href="https://fiberbroadband.org/YYYY/MM/DD/slug/">...<h3>TITLE</h3>
_ARTICLE_RE = re.compile(
    r'href="(https://fiberbroadband\.org/(\d{4})/(\d{2})/(\d{2})/[^"]+)"'
    r'.*?<h3[^>]*>(.*?)</h3>',
    re.DOTALL
)

def scrape_page(page_num, session):
    """Fetch one listing page. Returns list of articles, or None if 404."""
    url = POSTS_BASE if page_num == 1 else POSTS_PAGE.format(page_num)
    try:
        resp = session.get(url, timeout=30)
    except requests.RequestException as e:
        print(f'  Request error on page {page_num}: {e}')
        return []
    if resp.status_code == 404:
        return None   # signal: no more pages
    resp.raise_for_status()

    articles = []
    seen_links = set()
    for m in _ARTICLE_RE.finditer(resp.text):
        link  = m.group(1).rstrip('/')  + '/'
        year, month, day = int(m.group(2)), int(m.group(3)), int(m.group(4))
        title = _strip_html(m.group(5)).strip()
        if not title or link in seen_links:
            continue
        seen_links.add(link)
        pub_at = datetime(year, month, day, tzinfo=timezone.utc).isoformat()
        articles.append({
            'title':        title,
            'link':         link,
            'published_at': pub_at,
            'excerpt':      '',
            '_match_text':  title,
        })
    return articles


def fetch_scraped(session, max_pages, existing_links):
    """
    Scrape listing pages until max_pages reached, 404, or all articles
    on a page already exist in the DB (incremental mode).
    """
    all_articles = []
    page = 1
    while True:
        if max_pages and page > max_pages:
            break
        print(f'  Scraping page {page}…', end=' ', flush=True)
        arts = scrape_page(page, session)
        if arts is None:
            print('404 — done.')
            break
        if not arts:
            print('0 articles found, stopping.')
            break

        new = [a for a in arts if a['link'] not in existing_links]
        print(f'{len(arts)} found, {len(new)} new.')

        all_articles.extend(new)

        # If every article on this page is already in DB, stop crawling further back
        if len(new) == 0 and page > 1:
            print('  All articles on this page already in DB — stopping early.')
            break

        page += 1
        time.sleep(0.5)   # be polite

    return all_articles

# ── Upsert ────────────────────────────────────────────────────────────────────

def upsert_articles(sb, articles, county_lookup):
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
            tag = (f'[{", ".join(state_codes)}]' if state_codes else '[untagged]')
            if geoids:
                tag += f' {len(geoids)} co.'
            print(f'  {tag:28s}  {art["title"][:65]}')
        except Exception as e:
            skipped += 1
            print(f'  ERROR: {e}')
    return upserted, skipped

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Fetch FBA news into Supabase.')
    parser.add_argument(
        '--pages', type=int, default=None,
        help='Number of listing pages to scrape (0 = all). Omit for RSS-only mode.'
    )
    args = parser.parse_args()

    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print('ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_KEY.')
        sys.exit(1)

    sb      = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    session = requests.Session()
    session.headers.update(HEADERS)

    # Load counties
    print('Loading counties from Supabase…')
    result = sb.table('counties').select('geoid,name,state_code').execute()
    counties = result.data or []
    county_lookup = build_county_lookup(counties)
    print(f'  {len(counties)} counties across {len(set(c["state_code"] for c in counties))} states.\n')

    articles = []

    if args.pages is None:
        # Daily mode: RSS only
        articles = fetch_rss(session)
    else:
        # Backfill mode: scrape listing pages
        max_pages = args.pages if args.pages > 0 else None
        label = f'all' if max_pages is None else str(max_pages)
        print(f'Scraping listing pages (max={label})…')

        # Load existing links to enable incremental stop
        existing = sb.table('news_articles').select('link').execute()
        existing_links = {r['link'] for r in (existing.data or [])}
        print(f'  {len(existing_links)} articles already in DB.\n')

        scraped = fetch_scraped(session, max_pages, existing_links)
        articles.extend(scraped)

        # Also pull RSS for fresh excerpts on the newest articles
        print()
        rss_arts = fetch_rss(session)
        # Merge: RSS entries override scraped ones (better excerpts)
        scraped_links = {a['link'] for a in scraped}
        for a in rss_arts:
            if a['link'] not in scraped_links and a['link'] not in existing_links:
                articles.append(a)

    if not articles:
        print('\nNo new articles to upsert.')
        return

    print(f'\nTagging and upserting {len(articles)} articles…')
    upserted, skipped = upsert_articles(sb, articles, county_lookup)
    print(f'\nDone. Upserted {upserted}, skipped/errored {skipped}.')


if __name__ == '__main__':
    main()
