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

        // Fidium Fiber (Consolidated Communications)
        'Fidium Fiber': 'Fidium Fiber',
        'Consolidated': 'Fidium Fiber',
        'Consolidated Communications': 'Fidium Fiber',

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
    };

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
    };

})(window);
