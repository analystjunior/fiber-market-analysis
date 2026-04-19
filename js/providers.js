// Provider normalization and lookup
// Wrapped in IIFE, exports ProviderIndex to window

(function(global) {
    'use strict';

    // Maps every raw FCC brand_name to a canonical display name.
    // Add entries here as new brand names appear in the data.
    var ALIASES = {
        // AT&T
        'AT&T': 'AT&T',
        'AT&T Inc.': 'AT&T',
        'AT&T Services, Inc.': 'AT&T',
        'SNET': 'AT&T',

        // Spectrum / Charter
        'Spectrum': 'Spectrum',
        'Charter Spectrum': 'Spectrum',
        'Charter Communications': 'Spectrum',
        'Bright House Networks': 'Spectrum',
        'TWC Telecom': 'Spectrum',

        // Comcast / Xfinity
        'Xfinity': 'Xfinity',
        'Comcast': 'Xfinity',
        'Comcast Cable': 'Xfinity',

        // Frontier
        'Frontier': 'Frontier',
        'Frontier Communications': 'Frontier',
        'Citizens Telephone Company': 'Frontier',

        // Lumen / CenturyLink / Quantum Fiber
        'Quantum Fiber': 'Quantum Fiber',
        'CenturyLink': 'Quantum Fiber',
        'CenturyTel': 'Quantum Fiber',
        'Lumen': 'Quantum Fiber',
        'Lumen Technologies': 'Quantum Fiber',

        // Verizon
        'Verizon': 'Verizon Fios',
        'Verizon Fios': 'Verizon Fios',
        'Verizon Online': 'Verizon Fios',

        // Windstream (filed under many state-level subsidiaries)
        'Windstream': 'Windstream',
        'Windstream Communications, LLC.': 'Windstream',
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

        // Brightspeed
        'Brightspeed': 'Brightspeed',

        // Google Fiber
        'Google Fiber': 'Google Fiber',

        // Cox
        'Cox': 'Cox',
        'Cox Communications': 'Cox',

        // Metronet
        'Metronet': 'Metronet',
        'Metronet Holdings': 'Metronet',

        // TDS Telecom
        'TDS Telecom': 'TDS Telecom',
        'Telephone and Data Systems': 'TDS Telecom',

        // Ziply Fiber
        'Ziply Fiber': 'Ziply Fiber',

        // Optimum / Altice
        'Optimum': 'Optimum',
        'Optimum by Altice': 'Optimum',
        'Altice USA': 'Optimum',
        'Cablevision': 'Optimum',
        'Sudden Link': 'Optimum',
        'Suddenlink': 'Optimum',

        // Midco
        'Midco': 'Midco',
        'Midcontinent Communications': 'Midco',

        // Breezeline
        'Breezeline': 'Breezeline',
        'Atlantic Broadband': 'Breezeline',

        // C Spire
        'Cspire': 'C Spire',
        'C Spire': 'C Spire',
        'C Spire Fiber': 'C Spire',

        // Astound Broadband / Wave / RCN
        'Astound Broadband': 'Astound Broadband',
        'Wave Broadband': 'Astound Broadband',
        'RCN': 'Astound Broadband',

        // Hotwire Communications
        'Hotwire Communications': 'Hotwire',
        'Hotwire': 'Hotwire',

        // Dobson Fiber
        'Dobson Fiber': 'Dobson Fiber',

        // Mediacom
        'Mediacom Xtream': 'Mediacom',
        'Mediacom': 'Mediacom',

        // Shentel / Glo Fiber
        'Shentel': 'Shentel / Glo Fiber',
        'Glo Fiber': 'Shentel / Glo Fiber',

        // Sparklight / Cable ONE
        'Sparklight': 'Sparklight',
        'Cable ONE': 'Sparklight',

        // Fidium Fiber (formerly Consolidated Communications — rebranded Sep 2025)
        'Fidium Fiber': 'Fidium Fiber',
        'Fidium': 'Fidium Fiber',
        'Consolidated': 'Fidium Fiber',
        'Consolidated Communications': 'Fidium Fiber',
        'Consolidated Communications, Inc.': 'Fidium Fiber',

        // Lumos Networks
        'lumos': 'Lumos',
        'Lumos': 'Lumos',
        'Lumos Networks': 'Lumos',

        // Bluepeak
        'Bluepeak': 'Bluepeak',

        // Ting / DISH
        'Ting': 'Ting',

        // WOW!
        'WOW Internet, Cable & Phone': 'WOW!',
        'WOW!': 'WOW!',
        'WideOpenWest': 'WOW!',

        // Conexon (rural cooperative fiber)
        'Conexon Connect LLC': 'Conexon',
        'Conexon': 'Conexon',

        // Vyve Broadband
        'Vyve Broadband': 'Vyve Broadband',

        // Allo Communications
        'Allo Communications LLC': 'Allo Communications',
        'Allo Communications': 'Allo Communications',

        // Point Broadband
        'Point Broadband Fiber Holding LLC': 'Point Broadband',
        'Point Broadband': 'Point Broadband',

        // Valor Telecommunications (TX rural)
        'Valor Telecommunications of Texas, LP': 'Valor Telecom',

        // Nextlink (Texas rural fiber)
        'Nextlink': 'Nextlink',
        'Nextlink Internet': 'Nextlink',

        // Great Plains Broadband / Communications
        'Great Plains Broadband LLC': 'Great Plains Communications',
        'Great Plains Communications LLC': 'Great Plains Communications',

        // ARVIG (MN rural)
        'ARVIG': 'ARVIG',

        // Nex-Tech (KS)
        'Nex-Tech': 'Nex-Tech',

        // RiverStreet Networks
        'RiverStreet': 'RiverStreet',
        'RiverStreet Networks': 'RiverStreet',

        // Armstrong / Zito
        'ArmstrongUtilitiesInc': 'Armstrong / Zito',
        'Zito Media': 'Armstrong / Zito',

        // Surf Internet (IN/MI)
        'Surf Internet': 'Surf Internet',

        // IdeaTek (KS)
        'IdeaTek': 'IdeaTek',

        // ClearWave Fiber
        'ClearWave': 'ClearWave Fiber',
        'ClearWave Fiber': 'ClearWave Fiber',

        // Centric Fiber (TX)
        'Centric Fiber': 'Centric Fiber',

        // Vexus Fiber
        'Vexus Fiber': 'Vexus Fiber',
        'NTS Communications': 'Vexus Fiber',

        // Omni Fiber (OH)
        'Omni Fiber': 'Omni Fiber',

        // Empire Fiber (NY — Hudson Valley)
        'Empire Fiber': 'Empire Fiber',
        'Empire Fiber Networks': 'Empire Fiber',
        'Empire State Telephone': 'Empire Fiber',
    };

    // Public-reported national totals for the picker summary display.
    // Source: earnings releases or operator press releases. null = no public figure for that tech.
    // County-level map data still comes from FCC filings.
    //
    // SOURCE_NOTES tracks provenance: 'earnings' = quarterly filing, 'press_release' = operator PR.
    // Displayed as a badge in the provider picker so users know which figures are FCC vs self-reported.
    var SOURCE_NOTES = {
        // Earnings-backed figures (quarterly filings)
        'AT&T':              { type: 'earnings',      as_of: 'Q4 2025 + Quantum acq. Feb 2026' },
        'Verizon Fios':      { type: 'earnings',      as_of: 'Q4 2025 + Frontier acq. Jan 2026' },
        'Frontier':          { type: 'earnings',      as_of: 'Q4 2025 (now part of Verizon)' },
        'Xfinity':           { type: 'earnings',      as_of: 'Q4 2025' },
        'Spectrum':          { type: 'earnings',      as_of: 'Q4 2025' },
        'Cox':               { type: 'earnings',      as_of: 'Q4 2025' },
        'Quantum Fiber':     { type: 'earnings',      as_of: 'Q4 2025 (now part of AT&T)' },
        'Optimum':           { type: 'earnings',      as_of: 'Q3 2025' },
        'Windstream':        { type: 'earnings',      as_of: 'Q4 2025' },
        'TDS Telecom':       { type: 'earnings',      as_of: 'Q4 2025' },
        'WOW!':              { type: 'earnings',      as_of: 'Q3 2025' },
        'Shentel / Glo Fiber':{ type: 'earnings',     as_of: 'Q4 2025' },
        // Press-release / company-reported figures
        'Brightspeed':       { type: 'press_release', as_of: 'Apr 2026' },
        'Metronet':          { type: 'press_release', as_of: 'Nov 2025' },
        'Midco':             { type: 'press_release', as_of: '2025'    },
        'Mediacom':          { type: 'press_release', as_of: '2025'    },
        'Ziply Fiber':       { type: 'press_release', as_of: 'Aug 2025' },
        'Fidium Fiber':      { type: 'press_release', as_of: 'Sep 2025' },
        'C Spire':           { type: 'press_release', as_of: 'Apr 2026' },
        'Surf Internet':     { type: 'press_release', as_of: 'Dec 2025' },
        'Empire Fiber':      { type: 'press_release', as_of: 'Apr 2026' },
    };

    var PUBLIC_REPORTED = {
        // AT&T Q4 2025 (32M) + Quantum Fiber acq. closed Feb 2 2026 (~4M) = ~36M combined
        // Q1 2026 earnings (Apr 22 2026) will be first official combined report
        'AT&T':              { fiber: 36000000,  cable: null,     dsl: null     },
        // Verizon Q4 2025 + Frontier acq. closed Jan 20 2026 — combined ~30M
        // FCC map data still tracks Fios and Frontier separately until next BDC filing
        'Verizon Fios':      { fiber: 30000000,  cable: null,     dsl: null     },
        // Frontier final standalone figure before Verizon acquisition (Jan 20 2026)
        'Frontier':          { fiber: 9000000,   cable: null,     dsl: null     },
        // Comcast Q4 2025 (cmcsa.com)
        'Xfinity':           { fiber: null,      cable: 65000000, dsl: null     },
        // Charter Q4 2025 (prnewswire.com/302674771)
        'Spectrum':          { fiber: null,      cable: 58400000, dsl: null     },
        // Cox: Charter-Cox merger filing May 2025
        'Cox':               { fiber: null,      cable: 12300000, dsl: null     },
        // Quantum Fiber sold to AT&T, closed Feb 2 2026 — final standalone figure
        // FCC map data still shows Quantum Fiber separately until next BDC filing
        'Quantum Fiber':     { fiber: 4000000,   cable: null,     dsl: 6000000  },
        // Brightspeed press release Apr 2 2026 (prnewswire.com/302732416)
        'Brightspeed':       { fiber: 3000000,   cable: null,     dsl: 4300000  },
        // Altice USA Q3 2025 (investors.optimum.com)
        'Optimum':           { fiber: 3050000,   cable: 9940000,  dsl: null     },
        // Metronet press release Nov 2025 (businesswire.com) — acquired by T-Mobile/KKR JV
        'Metronet':          { fiber: 3000000,   cable: null,     dsl: null     },
        // Windstream/Kinetic via Uniti Q4 2025 earnings (Mar 2026)
        'Windstream':        { fiber: 1900000,   cable: null,     dsl: null     },
        // TDS Q4 2025 (prnewswire.com/302693316)
        'TDS Telecom':       { fiber: 1060000,   cable: null,     dsl: null     },
        // Ziply Fiber — BCE acquisition closed Aug 2025; BCE guided 1.5M passings end-2025
        'Ziply Fiber':       { fiber: 1500000,   cable: null,     dsl: null     },
        // Fidium Fiber (Consolidated rebranded Sep 2025) — ~54% of 2.6M total footprint is fiber
        'Fidium Fiber':      { fiber: 1400000,   cable: null,     dsl: null     },
        // WOW Q3 2025 (taken private Jan 2026)
        'WOW!':              { fiber: 107000,    cable: 2020000,  dsl: null     },
        // Midco 2025 company materials
        'Midco':             { fiber: 130000,    cable: 870000,   dsl: null     },
        // Mediacom 2025 company materials
        'Mediacom':          { fiber: null,      cable: 3000000,  dsl: null     },
        // Shentel Q4 2025 earnings — 427K Glo Fiber FTTH expansion passings; 679K total broadband
        'Shentel / Glo Fiber':{ fiber: 427000,   cable: null,     dsl: null     },
        // Surf Internet PR Dec 2025 — 250K passings after "record-breaking year" (from FBA news)
        'Surf Internet':     { fiber: 250000,    cable: null,     dsl: null     },
        // C Spire PR Apr 2026 — 217,900 MS passings confirmed; total footprint (MS/AL/FL/TN) higher
        'C Spire':           { fiber: 218000,    cable: null,     dsl: null     },
        // Empire Fiber press release Apr 15 2026 — self-reported, FCC figure pending next filing
        'Empire Fiber':      { fiber: 200000,    cable: null,     dsl: null     },
    };

    // Returns public-reported totals for a canonical provider, or null if not available.
    function getPublicTotals(canonicalName) {
        return PUBLIC_REPORTED[canonicalName] || null;
    }

    // Returns all canonical provider names that have public-reported figures.
    function publicProviderNames() {
        return Object.keys(PUBLIC_REPORTED);
    }

    // Returns source note for a provider: { type: 'earnings'|'press_release', as_of: 'Q4 2025' }
    // Returns null if no note (figure comes purely from FCC computation).
    function getSourceNote(canonicalName) {
        return SOURCE_NOTES[canonicalName] || null;
    }

    // Canonical provider list, grouped for the picker UI.
    // Only providers that appear in enough counties to be meaningful.
    var PROVIDER_GROUPS = [
        {
            group: 'National Carriers',
            providers: [
                'AT&T', 'Spectrum', 'Xfinity', 'Frontier', 'Quantum Fiber',
                'Verizon Fios', 'Cox', 'Windstream', 'Brightspeed', 'Optimum',
            ]
        },
        {
            group: 'Regional Fiber Builders',
            providers: [
                'Google Fiber', 'Metronet', 'Ziply Fiber', 'TDS Telecom',
                'Midco', 'Breezeline', 'C Spire', 'Astound Broadband',
                'Shentel / Glo Fiber', 'Sparklight', 'WOW!', 'Dobson Fiber',
                'Mediacom', 'Fidium Fiber', 'Lumos', 'Hotwire', 'Allo Communications',
                'Vyve Broadband', 'Bluepeak', 'Ting', 'ClearWave Fiber', 'Vexus Fiber',
                'Empire Fiber',
            ]
        },
        {
            group: 'Rural & Cooperative',
            providers: [
                'Conexon', 'Great Plains Communications', 'ARVIG', 'Nextlink',
                'Point Broadband', 'Valor Telecom', 'Nex-Tech', 'RiverStreet',
                'Surf Internet', 'Centric Fiber', 'Omni Fiber', 'Armstrong / Zito',
                'IdeaTek',
            ]
        }
    ];

    // Resolve a raw FCC brand name to its canonical name (or itself if unknown)
    function resolve(rawName) {
        if (!rawName) return null;
        var trimmed = rawName.trim();
        return ALIASES[trimmed] || trimmed;
    }

    // Return passings for a canonical provider in a county, optionally filtered by tech type.
    // techType: 'fiber' | 'cable' | 'dsl' | 'all' (default: 'fiber' for backward compat)
    function getPassings(countyData, canonicalName, techType) {
        if (!countyData || !countyData.operators) return 0;
        var tech = techType || 'fiber';
        var total = 0;
        for (var i = 0; i < countyData.operators.length; i++) {
            var op = countyData.operators[i];
            if (resolve(op.name) !== canonicalName) continue;
            if (tech === 'fiber') {
                total += (op.fiber_passings != null ? op.fiber_passings : (op.passings || 0));
            } else if (tech === 'cable') {
                total += (op.cable_passings || 0);
            } else if (tech === 'dsl') {
                total += (op.dsl_passings || 0);
            } else {
                // 'all' — sum every tech
                var fiber = op.fiber_passings != null ? op.fiber_passings : (op.passings || 0);
                total += fiber + (op.cable_passings || 0) + (op.dsl_passings || 0);
            }
        }
        return total;
    }

    // Return true if provider has any presence in the county for the given tech type
    function hasPresence(countyData, canonicalName, techType) {
        return getPassings(countyData, canonicalName, techType) > 0;
    }

    // Flat list of all canonical provider names in display order
    function allProviders() {
        var list = [];
        PROVIDER_GROUPS.forEach(function(g) {
            g.providers.forEach(function(p) { list.push(p); });
        });
        return list;
    }

    // Sum passings for every canonical provider across all loaded counties, split by tech.
    // Returns: { 'AT&T': { fiber: 12M, cable: 0, dsl: 41M, all: 53M }, ... }
    function computeNationalTotals() {
        var totals = {};
        DataHandler.iterateAllCounties(function(county) {
            if (!county.operators) return;
            for (var i = 0; i < county.operators.length; i++) {
                var op = county.operators[i];
                var canonical = resolve(op.name);
                if (!canonical) continue;
                if (!totals[canonical]) totals[canonical] = { fiber: 0, cable: 0, dsl: 0, all: 0 };
                var fiber = op.fiber_passings != null ? op.fiber_passings : (op.passings || 0);
                var cable = op.cable_passings || 0;
                var dsl   = op.dsl_passings   || 0;
                totals[canonical].fiber += fiber;
                totals[canonical].cable += cable;
                totals[canonical].dsl   += dsl;
                totals[canonical].all   += (fiber + cable + dsl);
            }
        });
        return totals;
    }

    // Get the fiber-only total for a provider (used for picker badge display)
    function getFiberTotal(totals, canonicalName) {
        var t = totals[canonicalName];
        return t ? t.fiber : 0;
    }

    // Format a raw passings number to a compact string: 1,234,567 → "1.2M"
    function formatPassings(n) {
        if (!n || n === 0) return null;
        if (n >= 1000000) return (n / 1000000).toFixed(1).replace(/\.0$/, '') + 'M';
        if (n >= 1000)    return (n / 1000).toFixed(0) + 'K';
        return String(n);
    }

    global.ProviderIndex = {
        GROUPS: PROVIDER_GROUPS,
        resolve: resolve,
        getPassings: getPassings,
        hasPresence: hasPresence,
        allProviders: allProviders,
        computeNationalTotals: computeNationalTotals,
        getFiberTotal: getFiberTotal,
        formatPassings: formatPassings,
        getPublicTotals: getPublicTotals,
        publicProviderNames: publicProviderNames,
        getSourceNote: getSourceNote,
    };

})(window);
