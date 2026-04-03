#!/usr/bin/env python3
"""
Reconcile FCC-derived operator passings against public earnings reports.

For each major operator, computes a per-technology scale factor:
    scale = public_reported_total / fcc_derived_total

Then applies it proportionally to every county's operator passings in Supabase.
Coverage metrics (fiber_served, cable_served, fiber_penetration) are NOT changed —
only the per-operator attribution figures are adjusted.

Usage:
    source .env

    # Preview scale factors without touching data
    python3 scripts/reconcile_operators.py --dry-run

    # Apply to Supabase
    python3 scripts/reconcile_operators.py

    # Single operator only
    python3 scripts/reconcile_operators.py --operators "AT&T" "Spectrum"

Update cycle: re-run each quarter after earnings releases.
Just update the PUBLIC_TOTALS dict below and re-run.
"""

import argparse
import os
import sys
from collections import defaultdict

SUPABASE_URL         = os.environ.get('SUPABASE_URL',         'https://sveqgyhncdrjemohpwho.supabase.co')
SUPABASE_SERVICE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

BATCH_SIZE = 200

# ── Public reported totals ────────────────────────────────────────────────────
# Update this dict each quarter as earnings are released.
# Keys must match canonical names in providers.js ALIASES.
# None = no public figure available for that tech; don't scale it.
# Sources cited inline.

PUBLIC_TOTALS = {
    # AT&T Q4 2025 Earnings, Jan 28 2026 (prnewswire.com/302672564)
    # Note: does NOT yet include Quantum Fiber acquisition (closed Feb 2 2026)
    'AT&T': {
        'fiber': 32_000_000,
        'cable': None,
        'dsl':   None,  # actively retiring copper — no authoritative figure
    },

    # Verizon Q4 2025 Earnings, Jan 30 2026 (verizon.com)
    # Estimated from ~25M legacy wireline territory minus un-fibered gaps;
    # does NOT include Frontier (acquisition closed Jan 20 2026, kept separate)
    'Verizon Fios': {
        'fiber': 21_500_000,
        'cable': None,
        'dsl':   None,
    },

    # Frontier: 9.0M reported via Verizon Q4 2025 call (Frontier acquired Jan 20 2026)
    # FCC Jun 2025 data shows Frontier as an independent filer — kept separate
    'Frontier': {
        'fiber': 9_000_000,
        'cable': None,
        'dsl':   None,  # legacy copper being retired; no public target
    },

    # Comcast Q4 2025 Earnings, Jan 29 2026 (cmcsa.com)
    'Xfinity': {
        'fiber': None,   # Comcast strategy is DOCSIS 4.0, not separate FTTP
        'cable': 65_000_000,
        'dsl':   None,
    },

    # Charter Q4 2025 Earnings, Jan 31 2026 (prnewswire.com/302674771)
    'Spectrum': {
        'fiber': None,   # Charter strategy is DOCSIS 4.0 upgrade, not separate FTTP
        'cable': 58_400_000,
        'dsl':   None,
    },

    # Cox: Charter-Cox merger filing, May 2025 (fierce-network.com)
    'Cox': {
        'fiber': None,
        'cable': 12_300_000,
        'dsl':   None,
    },

    # Lumen Q4 2025 Earnings, Feb 4 2026 (ir.lumen.com)
    # Consumer fiber sold to AT&T Feb 2 2026; DSL copper retained by Lumen
    'Quantum Fiber': {
        'fiber': 4_000_000,
        'cable': None,
        'dsl':   6_000_000,
    },

    # Brightspeed press release, Apr 2 2026 (prnewswire.com/302732416)
    # 3.0M fiber + ~4.3M legacy copper (7.3M total footprint minus fiber)
    'Brightspeed': {
        'fiber': 3_000_000,
        'cable': None,
        'dsl':   4_300_000,
    },

    # Windstream/Kinetic via Uniti Q4 2025 Earnings, Mar 2026 (lightwaveonline.com)
    'Windstream': {
        'fiber': 1_900_000,
        'cable': None,
        'dsl':   None,
    },

    # Altice USA Q3 2025, Nov 6 2025 (investors.optimum.com)
    # Q4 2025 results released Feb 12 2026 but granular passings not captured
    'Optimum': {
        'fiber': 3_050_000,
        'cable': 9_940_000,
        'dsl':   None,
    },

    # WOW Q3 2025, Nov 2025 — final public report (taken private Jan 2 2026)
    'WOW!': {
        'fiber': 107_000,
        'cable': 2_020_000,
        'dsl':   None,
    },

    # Mediacom company materials, 2025 (private company)
    'Mediacom': {
        'fiber': None,
        'cable': 3_000_000,
        'dsl':   None,
    },

    # Metronet press release, Nov 24 2025 (businesswire.com)
    # Acquired by T-Mobile/KKR joint venture July 2025; private
    'Metronet': {
        'fiber': 3_000_000,
        'cable': None,
        'dsl':   None,
    },

    # Midco company materials, 2025 (private)
    'Midco': {
        'fiber': 130_000,
        'cable': 870_000,
        'dsl':   None,
    },

    # TDS Q4 2025, Feb 20 2026 (prnewswire.com/302693316)
    'TDS Telecom': {
        'fiber': 1_060_000,
        'cable': None,
        'dsl':   None,
    },
}

# ── Alias map (mirrors providers.js ALIASES) ──────────────────────────────────
ALIASES = {
    'AT&T': 'AT&T', 'AT&T Inc.': 'AT&T', 'AT&T Services, Inc.': 'AT&T', 'SNET': 'AT&T',
    'Spectrum': 'Spectrum', 'Charter Spectrum': 'Spectrum',
    'Charter Communications': 'Spectrum', 'Bright House Networks': 'Spectrum',
    'TWC Telecom': 'Spectrum',
    'Xfinity': 'Xfinity', 'Comcast': 'Xfinity', 'Comcast Cable': 'Xfinity',
    'Frontier': 'Frontier', 'Frontier Communications': 'Frontier',
    'Citizens Telephone Company': 'Frontier',
    'Quantum Fiber': 'Quantum Fiber', 'CenturyLink': 'Quantum Fiber',
    'CenturyTel': 'Quantum Fiber', 'Lumen': 'Quantum Fiber',
    'Lumen Technologies': 'Quantum Fiber',
    'Verizon': 'Verizon Fios', 'Verizon Fios': 'Verizon Fios',
    'Verizon Online': 'Verizon Fios',
    'Windstream': 'Windstream', 'Windstream Communications, LLC.': 'Windstream',
    'Windstream Georgia Communications, LLC': 'Windstream',
    'Windstream Georgia, LLC': 'Windstream',
    'Georgia Windstream, LLC.': 'Windstream',
    'Windstream Iowa Communications, Inc.': 'Windstream',
    'Windstream Kentucky East, LLC': 'Windstream',
    'Windstream Pennsylvania, LLC.': 'Windstream',
    'Windstream Missouri, Inc.': 'Windstream',
    'Windstream North Carolina, LLC': 'Windstream',
    'Windstream Florida, Inc.': 'Windstream',
    'Windstream Arkansas, LLC': 'Windstream',
    'Windstream Nebraska, Inc.': 'Windstream',
    'Texas Windstream, Inc.': 'Windstream',
    'Windstream Standard, LLC': 'Windstream',
    'Windstream Western (WC)': 'Windstream',
    'Windstream Alabama, LLC': 'Windstream',
    'Windstream Mississippi, LLC': 'Windstream',
    'Windstream Oklahoma, Inc.': 'Windstream',
    'Windstream Ohio, Inc.': 'Windstream',
    'Windstream Indiana, LLC': 'Windstream',
    'Windstream Kentucky West, LLC': 'Windstream',
    'Brightspeed': 'Brightspeed',
    'Cox': 'Cox', 'Cox Communications': 'Cox',
    'Metronet': 'Metronet', 'Metronet Holdings': 'Metronet',
    'TDS Telecom': 'TDS Telecom', 'Telephone and Data Systems': 'TDS Telecom',
    'Optimum': 'Optimum', 'Optimum by Altice': 'Optimum', 'Altice USA': 'Optimum',
    'Cablevision': 'Optimum', 'Sudden Link': 'Optimum', 'Suddenlink': 'Optimum',
    'Midco': 'Midco', 'Midcontinent Communications': 'Midco',
    'WOW Internet, Cable & Phone': 'WOW!', 'WOW!': 'WOW!', 'WideOpenWest': 'WOW!',
    'Mediacom Xtream': 'Mediacom', 'Mediacom': 'Mediacom',
}


def get_supabase():
    try:
        from supabase import create_client
    except ImportError:
        print('ERROR: pip install supabase')
        sys.exit(1)
    if not SUPABASE_SERVICE_KEY:
        print('ERROR: SUPABASE_SERVICE_KEY not set. Run: source .env')
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def aggregate_fcc_totals(client, target_operators):
    """Sum fiber/cable/DSL passings per canonical operator across all counties."""
    totals = defaultdict(lambda: {'fiber': 0, 'cable': 0, 'dsl': 0})
    offset, page = 0, 1000
    print('  Reading operator data from Supabase...')
    while True:
        r = client.table('counties').select('operators').range(offset, offset + page - 1).execute()
        for row in r.data:
            for op in (row.get('operators') or []):
                canonical = ALIASES.get((op.get('name') or '').strip())
                if not canonical or canonical not in target_operators:
                    continue
                totals[canonical]['fiber'] += op.get('fiber_passings') or op.get('passings') or 0
                totals[canonical]['cable'] += op.get('cable_passings') or 0
                totals[canonical]['dsl']   += op.get('dsl_passings')   or 0
        if len(r.data) < page:
            break
        offset += page
    return totals


def compute_scale_factors(fcc_totals):
    """
    Returns: {canonical_name: {fiber: float|None, cable: float|None, dsl: float|None}}
    None means 'no scaling for this tech'.
    """
    scales = {}
    for op, pub in PUBLIC_TOTALS.items():
        fcc = fcc_totals.get(op, {'fiber': 0, 'cable': 0, 'dsl': 0})
        scales[op] = {}
        for tech in ('fiber', 'cable', 'dsl'):
            pub_val = pub.get(tech)
            fcc_val = fcc.get(tech, 0)
            if pub_val is None:
                scales[op][tech] = None          # skip
            elif fcc_val == 0:
                scales[op][tech] = None          # can't scale from zero
            else:
                scales[op][tech] = pub_val / fcc_val
    return scales


def print_scale_table(fcc_totals, scale_factors):
    print(f'\n{"Operator":<18} {"Tech":<6} {"FCC":>12} {"Public":>12} {"Scale":>8}  Note')
    print('─' * 72)
    for op in sorted(PUBLIC_TOTALS):
        pub   = PUBLIC_TOTALS[op]
        fcc   = fcc_totals.get(op, {'fiber': 0, 'cable': 0, 'dsl': 0})
        scale = scale_factors[op]
        for tech in ('fiber', 'cable', 'dsl'):
            pub_val   = pub.get(tech)
            fcc_val   = fcc.get(tech, 0)
            scale_val = scale.get(tech)
            if pub_val is None and fcc_val == 0:
                continue
            note = ''
            if scale_val is None and pub_val is not None and fcc_val == 0:
                note = '⚠ FCC=0, cannot scale'
            elif scale_val is None:
                note = '— not scaling'
            elif scale_val > 2.5:
                note = '⚠ very high'
            elif scale_val > 1.5:
                note = '↑ significant gap'
            elif scale_val < 0.8:
                note = '↓ FCC > public'
            s = f'{scale_val:.2f}x' if scale_val else '  —  '
            print(f'{op:<18} {tech:<6} {fcc_val:>12,} {pub_val or 0:>12,} {s:>8}  {note}')
    print()


def apply_scales(client, scale_factors, target_operators):
    """
    Fetch all counties, apply scale factors to operator passings, upsert back.
    Only modifies operators[].fiber_passings / cable_passings / dsl_passings.
    Does NOT touch fiber_served, cable_served, fiber_penetration, or any
    coverage/demographic fields.
    """
    print('Applying scale factors to Supabase...')
    offset, page = 0, 1000
    total_counties = 0
    total_ops_scaled = 0

    while True:
        r = client.table('counties').select('geoid,state_code,name,operators').range(offset, offset + page - 1).execute()
        if not r.data:
            break

        batch_rows = []
        for row in r.data:
            ops = row.get('operators') or []
            modified = False
            for op in ops:
                canonical = ALIASES.get((op.get('name') or '').strip())
                if not canonical or canonical not in target_operators:
                    continue
                sf = scale_factors.get(canonical, {})

                fiber_scale = sf.get('fiber')
                cable_scale = sf.get('cable')
                dsl_scale   = sf.get('dsl')

                if fiber_scale is not None:
                    old_fiber = op.get('fiber_passings') or op.get('passings') or 0
                    new_fiber = round(old_fiber * fiber_scale)
                    op['fiber_passings'] = new_fiber
                    op['passings']       = new_fiber   # keep backward-compat field in sync
                    modified = True

                if cable_scale is not None:
                    old_cable = op.get('cable_passings') or 0
                    op['cable_passings'] = round(old_cable * cable_scale)
                    modified = True

                if dsl_scale is not None:
                    old_dsl = op.get('dsl_passings') or 0
                    op['dsl_passings'] = round(old_dsl * dsl_scale)
                    modified = True

                if modified:
                    total_ops_scaled += 1

            if modified:
                batch_rows.append({
                    'geoid':      row['geoid'],
                    'state_code': row['state_code'],
                    'name':       row['name'],
                    'operators':  ops,
                })

        if batch_rows:
            for i in range(0, len(batch_rows), BATCH_SIZE):
                client.table('counties').upsert(
                    batch_rows[i:i + BATCH_SIZE], on_conflict='geoid'
                ).execute()

        total_counties += len(r.data)
        if len(r.data) < page:
            break
        offset += page
        print(f'  {total_counties:,} counties processed...')

    print(f'  Done — {total_counties:,} counties scanned, {total_ops_scaled:,} operator entries scaled')


def main():
    parser = argparse.ArgumentParser(
        description='Reconcile FCC operator passings to public earnings figures'
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='Show scale factors only, do not write to Supabase')
    parser.add_argument('--operators', nargs='+', metavar='NAME',
                        help='Limit to specific canonical operator names')
    args = parser.parse_args()

    target = set(args.operators) if args.operators else set(PUBLIC_TOTALS.keys())
    invalid = target - set(PUBLIC_TOTALS.keys())
    if invalid:
        print(f'ERROR: Unknown operator(s): {invalid}')
        print(f'Valid names: {sorted(PUBLIC_TOTALS.keys())}')
        sys.exit(1)

    print(f'Connecting to Supabase...')
    client = get_supabase()

    fcc_totals    = aggregate_fcc_totals(client, target)
    scale_factors = compute_scale_factors(fcc_totals)

    print(f'\nScale factors (Q4 2025 public reporting vs FCC Jun 2025):')
    print_scale_table(fcc_totals, scale_factors)

    if args.dry_run:
        print('Dry run — no changes written.')
        return

    apply_scales(client, scale_factors, target)
    print('\nDone. Re-run with --dry-run anytime to verify current state.')
    print('Update PUBLIC_TOTALS each quarter as new earnings are released.')


if __name__ == '__main__':
    main()
