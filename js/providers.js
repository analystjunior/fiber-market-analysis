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

        // Frontier is now shown as part of Verizon after the Jan 2026 acquisition.
        'Frontier': 'Verizon Fios',
        'Frontier Communications': 'Verizon Fios',
        'Citizens Telephone Company': 'Verizon Fios',

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
        'Kinetic': 'Windstream',
        'Kinetic by Windstream': 'Windstream',
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

        // altafiber / Cincinnati Bell
        'altafiber': 'altafiber',
        'Altafiber': 'altafiber',
        'Cincinnati Bell': 'altafiber',
        'Cincinnati Bell Telephone Company': 'altafiber',

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
        'Astound': 'Astound Broadband',
        'Wave Broadband': 'Astound Broadband',
        'RCN': 'Astound Broadband',

        // Hotwire Communications / Fision
        'Hotwire Communications': 'Hotwire',
        'Fision': 'Hotwire',
        'Fision Fiber': 'Hotwire',
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
        'Ting Internet': 'Ting',

        // WOW!
        'WOW Internet, Cable & Phone': 'WOW!',
        'WOW!': 'WOW!',
        'WideOpenWest': 'WOW!',

        // Conexon (rural cooperative fiber)
        'Conexon Connect LLC': 'Conexon',
        'Conexon Connect': 'Conexon',
        'Conexon': 'Conexon',

        // Vyve Broadband
        'Vyve Broadband': 'Vyve Broadband',

        // Allo Communications
        'Allo Communications LLC': 'Allo Communications',
        'Allo Communications': 'Allo Communications',
        'Allo': 'Allo Communications',

        // Point Broadband
        'Point Broadband Fiber Holding LLC': 'Point Broadband',
        'Point Broadband': 'Point Broadband',
        'ClearWave': 'Point Broadband',
        'ClearWave Fiber': 'Point Broadband',

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
        'Armstrong': 'Armstrong / Zito',
        'Armstrong Utilities': 'Armstrong / Zito',
        'Zito Media': 'Armstrong / Zito',

        // Surf Internet (IN/MI)
        'Surf Internet': 'Surf Internet',

        // IdeaTek (KS)
        'IdeaTek': 'IdeaTek',

        // Centric Fiber (TX)
        'Centric Fiber': 'Centric Fiber',

        // Vexus Fiber
        'Vexus Fiber': 'Vexus Fiber',
        'NTS Communications': 'Vexus Fiber',

        // Omni Fiber (OH)
        'Omni Fiber': 'Omni Fiber',

        // Hawaiian Telcom
        'Hawaiian Telcom': 'Hawaiian Telcom',
        'Hawaiian Telecom': 'Hawaiian Telcom',
        'Hawaiian Telcom, Inc.': 'Hawaiian Telcom',

        // Empire Fiber (NY — Hudson Valley)
        'Empire Fiber': 'Empire Fiber',
        'Empire Access': 'Empire Fiber',
        'Empire Fiber Networks': 'Empire Fiber',
        'Empire State Telephone': 'Empire Fiber',

        // GoNetspeed (Northeast + South — Searchlight Capital)
        'GoNetspeed': 'GoNetspeed',
        'GoNetspeed LLC': 'GoNetspeed',

        // Greenlight Networks (NY — Searchlight Capital)
        'Greenlight Networks': 'Greenlight Networks',
        'Greenlight Networks Inc': 'Greenlight Networks',
        'Greenlight Networks, Inc.': 'Greenlight Networks',

        // EverFast Fiber (Kansas City metro — Astatine Investment Partners)
        'Everfast': 'EverFast Fiber',
        'EverFast Fiber': 'EverFast Fiber',
        'EverFast Fiber Networks': 'EverFast Fiber',
        'EverFast Fiber Networks LLC': 'EverFast Fiber',

        // Omni Fiber (OH/MI/PA/TX — Oak Hill Capital; absorbed Lit Fiber Nov 2024)
        'Lit Fiber': 'Omni Fiber',
        'Lit Communities': 'Omni Fiber',

        // Ripple Fiber (multi-state — Post Road Group; merged HyperFiber Sep 2025)
        'Ripple Fiber': 'Ripple Fiber',
        'Ripple Fiber LLC': 'Ripple Fiber',
        'HyperFiber': 'Ripple Fiber',

        // LiveOak Fiber (Coastal GA — MEAG / InfraRed Capital)
        'LiveOak Fiber': 'LiveOak Fiber',
        'Live Oak Fiber': 'LiveOak Fiber',
        'LiveOak Fiber LLC': 'LiveOak Fiber',

        // Socket Telecom (MO — Oak Hill / Pamlico Capital)
        'Socket Telecom': 'Socket Telecom',
        'Socket Internet': 'Socket Telecom',

        // Bluepeak (SD/ND/MN/OK/WY/TX — GI Partners; formerly Vast Broadband)
        'Vast Broadband': 'Bluepeak',

        // Wyyerd Fiber (AZ — acquired Ting AZ assets; formerly Zona Communications)
        'Wyyerd Fiber': 'Wyyerd Fiber',
        'Wyyerd': 'Wyyerd Fiber',
        'Zona Communications': 'Wyyerd Fiber',

        // i3 Broadband (IL/MO — private, community fiber)
        'i3 Broadband': 'i3 Broadband',
        'i3 Broadband LLC': 'i3 Broadband',

        // LFT Fiber (Lafayette LA municipal — formerly LUS Fiber)
        'LFT Fiber': 'LFT Fiber',
        'LUS Fiber': 'LFT Fiber',
        'Lafayette Utility System Fiber': 'LFT Fiber',

        // Additional regional fiber builders to keep visible in provider mode
        'Sonic': 'Sonic',
        'Sonic Internet': 'Sonic',
        'Ezee Fiber': 'Ezee Fiber',
        'Tachus Fiber Internet': 'Ezee Fiber',
        'Tachus': 'Ezee Fiber',
        'UTOPIA Fiber': 'UTOPIA Fiber',
        'Utah Telecommunication Open Infrastructure Agency': 'UTOPIA Fiber',
        'FiberFirst': 'FiberFirst',
        'Fiber First': 'FiberFirst',
        'EPB': 'EPB',
        'EPB Fiber Optics': 'EPB',
        'Electric Power Board of Chattanooga': 'EPB',
        'Race Communications': 'Race Communications',
        'Race': 'Race Communications',
        'Carolina Connect': 'Carolina Connect',
        'CarolinaConnect': 'Carolina Connect',
        'CONXXUS': 'CONXXUS',
        'Conxxus': 'CONXXUS',
        'IQ Fiber': 'IQ Fiber',
        'KUB Fiber': 'KUB Fiber',
        'Knoxville Utilities Board': 'KUB Fiber',
        'U.S. Internet': 'U.S. Internet',
        'US Internet': 'U.S. Internet',
        'USI Fiber': 'U.S. Internet',

        // Lumos additional aliases (NorthState merged Aug 2022; T-Mobile/EQT JV Apr 2025)
        'NorthState': 'Lumos',
        'North State Telecommunications': 'Lumos',
        'Lumos Networks': 'Lumos',
    };

    // Public-reported national totals for the picker summary display.
    // Source: earnings releases or operator press releases. null = no public figure for that tech.
    // County-level map data still comes from FCC filings.
    //
    // SOURCE_NOTES tracks provenance: 'earnings' = quarterly filing, 'press_release' = operator PR.
    // Displayed as a clickable badge in the provider picker so users can audit public-reported figures.
    var SOURCE_NOTES = {
        // Earnings-backed figures (quarterly filings / investor releases)
        'AT&T':              { type: 'earnings',      as_of: 'Q4 2025',
            url: 'https://about.att.com/story/2026/4q-earnings-2025.html' },
        'Verizon Fios':      { type: 'earnings',      as_of: 'Jan 2026 Frontier transaction close',
            url: 'https://www.verizon.com/about/news/feed/verizon-and-frontier-regulatory-approval' },
        'Xfinity':           { type: 'earnings',      as_of: 'Q4 2025',
            url: 'https://www.cmcsa.com/news-releases/news-release-details/comcast-reports-4th-quarter-2025-results' },
        'Spectrum':          { type: 'earnings',      as_of: 'Q4 2025',
            url: 'https://ir.charter.com/news-releases/news-release-details/charter-announces-fourth-quarter-and-full-year-2025-results/' },
        'Cox':               { type: 'earnings',      as_of: 'May 2025 Charter/Cox transaction',
            url: 'https://corporate.charter.com/newsroom/charter-communications-and-cox-communications-announce-definitive-agreement-to-combine-companies' },
        'Quantum Fiber':     { type: 'earnings',      as_of: 'Lumen sale close Feb 2026',
            url: 'https://ir.lumen.com/news/news-details/2026/Lumen-Completes-Sale-of-Consumer-Fiber-to-the-Home-Business-to-ATT/' },
        'Optimum':           { type: 'earnings',      as_of: 'Q3 2025',
            url: 'https://investors.optimum.com/news-events/press-releases/detail/225/altice-usa-reports-third-quarter-2025-results/' },
        'Windstream':        { type: 'earnings',      as_of: 'Q4 2025',
            url: 'https://www.globenewswire.com/news-release/2026/03/02/3247334/0/en/Uniti-Group-Inc-Reports-Fourth-Quarter-and-Full-Year-2025-Results.html' },
        'TDS Telecom':       { type: 'earnings',      as_of: 'Q4 2025',
            url: 'https://www.tdsinc.com/news/news-details/2026/TDS-reports-fourth-quarter-and-full-year-2025-results/default.aspx' },
        'WOW!':              { type: 'earnings',      as_of: 'Q3 2025',
            url: 'https://www.prnewswire.com/news-releases/wow-reports-third-quarter-2025-results-302604732.html' },
        'Shentel / Glo Fiber':{ type: 'earnings',     as_of: 'Q4 2025',
            url: 'https://investor.shentel.com/news-releases/news-release-details/shenandoah-telecommunications-company-reports-fourth-quarter-13' },
        // Press-release / company-reported figures — amber PR badge shown, links to source
        'Brightspeed':   { type: 'press_release', as_of: 'Apr 2026',
            url: 'https://www.brightspeed.com/brightspeed-news/Brightspeed_Surpasses_3M_Fiber-Enabled_Locations.html' },
        'Metronet':      { type: 'press_release', as_of: 'Nov 2025',
            url: 'https://www.businesswire.com/news/home/20251124223372/en/Metronet-Fiber-Now-Reaches-Three-Million-Locations' },
        'Ziply Fiber':   { type: 'press_release', as_of: 'Aug 2025',
            url: 'https://ziplyfiber.com/news/press-release/ziply-bce' },
        'Fidium Fiber':  { type: 'press_release', as_of: 'Sep 2025',
            url: 'https://www.businesswire.com/news/home/20250922300560/en/Consolidated-Communications-Becomes-Fidium-Uniting-All-Services-Under-One-Bold-Identity' },
        'C Spire':       { type: 'press_release', as_of: 'Apr 2026',
            url: 'https://magnoliatribune.com/2026/04/10/c-spire-completes-work-under-mississippi-capital-projects-fund-to-expand-high-speed-broadband-infrastructure/' },
        'Surf Internet': { type: 'press_release', as_of: 'Dec 2025',
            url: 'https://fiberbroadband.org/2025/12/16/surf-internet-celebrates-250000-fiber-optic-passings-after-record-breaking-year-of-growth/' },
        'Empire Fiber':  { type: 'press_release', as_of: 'Apr 2026',
            url: 'https://fiberbroadband.org/2026/04/15/building-empire-fiber/' },
        'Midco':         { type: 'press_release', as_of: '2025',
            url: 'https://midco.com/about/broadband-development/' },
        'Mediacom':      { type: 'press_release', as_of: 'Dec 2025',
            url: 'https://mediacomcable.com/news/mediacom-communications-marks-20th-anniversary-by-announcing--1-billion-capital-investment' },
        'Lumos':         { type: 'press_release', as_of: 'Apr 2025',
            url: 'https://www.t-mobile.com/news/business/t-mobile-eqt-close-lumos-fiber-jv' },
        'altafiber':     { type: 'press_release', as_of: 'Aug 2023',
            url: 'https://www.altafiber.com/about-us/news/altafiber-raises-600-million-for-continued-fiber-network-expansion' },
        'Ezee Fiber':    { type: 'press_release', as_of: 'Sep 2025',
            url: 'https://ezeefiber.com/blog/ezee-fiber-announces-close-of-acquisition-of-tachus-fiber-internet' },
        'Conexon':       { type: 'press_release', as_of: 'Sep 2025',
            url: 'https://www.prnewswire.com/news-releases/conexon-connect-reach-in-rural-georgia-spans-158-000-homes-and-businesses-with-completion-of-twelfth-fiber-to-the-home-network-302558144.html' },
        'Bluepeak':      { type: 'press_release', as_of: 'Dec 2025',
            scope: 'South Dakota total reach',
            figure: 'more than 175,000 residents and businesses statewide',
            url: 'https://mybluepeak.com/bluepeak-expands-investment-in-south-dakota-broadband-infrastructure/' },
        'Ripple Fiber':  { type: 'press_release', as_of: 'May 2025',
            url: 'https://ripplefiber.com/newsroom/ripple-fiber-expands-debt-capacity-to-350-million-to-fuel-nationwide-fiber-network-growth' },
        'Point Broadband':{ type: 'press_release', as_of: 'Jan 2026',
            scope: 'Point Broadband + Clearwave Fiber combined platform',
            figure: 'more than 500,000 homes and businesses with fiber',
            url: 'https://www.gtcr.com/point-broadband-and-clearwave-fiber-to-combine-creating-a-scaled-independent-fiber-platform/' },
        'i3 Broadband':  { type: 'press_release', as_of: '2026',
            scope: 'company footprint',
            figure: 'over 300,000 homes passed',
            url: 'https://www.whinfra.com/our-portfolio/i3-broadband/' },
        'Greenlight Networks':{ type: 'press_release', as_of: 'Mar 2026',
            scope: 'company footprint',
            figure: 'more than 320,000 households and businesses',
            url: 'https://fiberbroadband.org/2026/03/12/greenlight-networks-announces-expansive-fiber-internet-buildout-across-nine-northeast-pennsylvania-communities/' },
        'Hawaiian Telcom':{ type: 'press_release', as_of: 'Jan 2025',
            scope: 'statewide Hawaii FTTP network',
            figure: 'over 400,000 homes and businesses',
            url: 'https://www.businesswire.com/news/home/20250110887911/en/Hawaiian-Telcom-Partners-with-Government-Leaders-to-Announce-Landmark-%241.7-Billion-Investment-to-Transform-Hawaii-into-the-First-Fully-Fiber-Enabled-State-by-2026' },
        'Omni Fiber':    { type: 'press_release', as_of: 'Dec 2025',
            scope: 'company footprint',
            figure: 'approximately 340,000 locations by year-end',
            url: 'https://www.omnifiber.com/blog/omni-fiber-secures-200m-for-fiber-expansion/' },
    };

    var PUBLIC_REPORTED = {
        // AT&T Q4 2025 reports 32.0M consumer and business locations passed with fiber.
        'AT&T':              { fiber: 32000000,  cable: null,     dsl: null     },
        // Verizon Q4 2025 + Frontier acq. closed Jan 20 2026 — combined ~30M
        // Frontier raw FCC/operator names resolve to Verizon Fios in provider mode.
        'Verizon Fios':      { fiber: 30000000,  cable: null,     dsl: null     },
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
        // Lumos / T-Mobile-EQT JV close — 475K homes at close, 3.5M target by 2028
        'Lumos':             { fiber: 475000,    cable: null,     dsl: null     },
        // altafiber 2023 financing release — latest public footprint figure found
        'altafiber':         { fiber: 1100000,   cable: null,     dsl: null     },
        // Ezee Fiber + Tachus close — on track to exceed 600K FTTH passings by end-2025
        'Ezee Fiber':        { fiber: 600000,    cable: null,     dsl: null     },
        // Conexon Connect Sep 2025 Georgia reach disclosure
        'Conexon':           { fiber: 158000,    cable: null,     dsl: null     },
        // Bluepeak Dec 2025 South Dakota disclosure: 55K built to date, 175K+ total reach statewide.
        'Bluepeak':          { fiber: 175000,    cable: null,     dsl: null     },
        // Ripple Fiber May 2025 actual deployed passings disclosure
        'Ripple Fiber':      { fiber: 170000,    cable: null,     dsl: null     },
        // Shentel Q4 2025 earnings — 427K Glo Fiber FTTH expansion passings; 679K total broadband
        'Shentel / Glo Fiber':{ fiber: 427000,   cable: null,     dsl: null     },
        // Surf Internet PR Dec 2025 — 250K passings after "record-breaking year" (from FBA news)
        'Surf Internet':     { fiber: 250000,    cable: null,     dsl: null     },
        // C Spire PR Apr 2026 — 217,900 MS passings confirmed; total footprint (MS/AL/FL/TN) higher
        'C Spire':           { fiber: 218000,    cable: null,     dsl: null     },
        // Empire Fiber press release Apr 15 2026 — self-reported, FCC figure pending next filing
        'Empire Fiber':      { fiber: 200000,    cable: null,     dsl: null     },
        // Point Broadband + Clearwave Fiber Jan 2026 combination disclosure
        'Point Broadband':   { fiber: 500000,    cable: null,     dsl: null     },
        // i3 Broadband Wren House portfolio disclosure
        'i3 Broadband':      { fiber: 300000,    cable: null,     dsl: null     },
        // Greenlight Networks Mar 2026 company footprint disclosure
        'Greenlight Networks':{ fiber: 320000,   cable: null,     dsl: null     },
        // Hawaiian Telcom Jan 2025 statewide FTTP network disclosure
        'Hawaiian Telcom':   { fiber: 400000,    cable: null,     dsl: null     },
        // Omni Fiber Dec 2025 financing release
        'Omni Fiber':        { fiber: 340000,    cable: null,     dsl: null     },
    };

    // Returns public-reported totals for a canonical provider, or null if not available.
    function getPublicTotals(canonicalName) {
        return PUBLIC_REPORTED[canonicalName] || null;
    }

    // Returns all canonical provider names that have public-reported figures.
    function publicProviderNames() {
        return Object.keys(PUBLIC_REPORTED);
    }

    var REQUIRED_PROVIDER_NAMES = [
        'AT&T',
        'Verizon Fios',
        'Quantum Fiber',
        'Brightspeed',
        'Fidium Fiber',
        'Windstream',
        'Metronet',
        'Lumos',
        'Google Fiber',
        'Optimum',
        'Sparklight',
        'WOW!',
        'TDS Telecom',
        'altafiber',
        'Ziply Fiber',
        'GoNetspeed',
        'Shentel / Glo Fiber',
        'Allo Communications',
        'Point Broadband',
        'Conexon',
        'Sonic',
        'C Spire',
        'Astound Broadband',
        'Armstrong / Zito',
        'EverFast Fiber',
        'i3 Broadband',
        'Bluepeak',
        'Ezee Fiber',
        'Greenlight Networks',
        'Hawaiian Telcom',
        'Surf Internet',
        'Omni Fiber',
        'Dobson Fiber',
        'UTOPIA Fiber',
        'FiberFirst',
        'Ting',
        'Wyyerd Fiber',
        'Ripple Fiber',
        'EPB',
        'Empire Fiber',
        'LiveOak Fiber',
        'Hotwire',
        'Race Communications',
        'IdeaTek',
        'Carolina Connect',
        'CONXXUS',
        'IQ Fiber',
        'KUB Fiber',
        'U.S. Internet',
    ];

    var DISPLAY_NAMES = {
        'Verizon Fios': 'Verizon',
        'Quantum Fiber': 'Lumen / CenturyLink / Quantum Fiber',
        'Fidium Fiber': 'Fidium / Consolidated Communications',
        'Windstream': 'Windstream / Kinetic',
        'Optimum': 'Optimum / Altice USA',
        'Allo Communications': 'Allo',
        'Conexon': 'Conexon Connect',
        'Astound Broadband': 'Astound',
        'Armstrong / Zito': 'Armstrong',
        'EverFast Fiber': 'Everfast',
        'Ting': 'Ting Internet',
        'Empire Fiber': 'Empire Fiber / Empire Access',
        'Hotwire': 'Hotwire Communications / Fision',
    };

    function requiredProviderNames() {
        return REQUIRED_PROVIDER_NAMES.slice();
    }

    function isRequiredProvider(canonicalName) {
        return REQUIRED_PROVIDER_NAMES.indexOf(canonicalName) !== -1;
    }

    function getDisplayName(canonicalName) {
        return DISPLAY_NAMES[canonicalName] || canonicalName;
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
                'AT&T', 'Spectrum', 'Xfinity', 'Quantum Fiber',
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
                'Vyve Broadband', 'Bluepeak', 'Ting', 'Vexus Fiber',
                'Empire Fiber', 'GoNetspeed', 'Greenlight Networks', 'EverFast Fiber',
                'Ripple Fiber', 'LiveOak Fiber', 'i3 Broadband', 'Ezee Fiber',
                'UTOPIA Fiber', 'FiberFirst', 'Wyyerd Fiber', 'EPB', 'Hawaiian Telcom', 'Race Communications',
                'IQ Fiber', 'KUB Fiber', 'U.S. Internet', 'altafiber',
            ]
        },
        {
            group: 'Rural & Cooperative',
            providers: [
                'Conexon', 'Great Plains Communications', 'ARVIG', 'Nextlink',
                'Point Broadband', 'Valor Telecom', 'Nex-Tech', 'RiverStreet',
                'Surf Internet', 'Centric Fiber', 'Omni Fiber', 'Armstrong / Zito',
                'IdeaTek', 'Sonic', 'Carolina Connect', 'CONXXUS',
            ]
        }
    ];

    function normalizeProviderName(name) {
        return String(name || '')
            .trim()
            .toLowerCase()
            .replace(/[^a-z0-9&!]+/g, ' ')
            .replace(/\s+/g, ' ')
            .trim();
    }

    // Resolve a raw FCC brand name to its canonical name (or itself if unknown)
    function resolve(rawName) {
        if (!rawName) return null;
        var trimmed = rawName.trim();
        var exactAlias = ALIASES[trimmed];
        if (exactAlias) return exactAlias;

        // The live data can include Frontier legal entities and state subsidiaries.
        // Keep every Frontier variant under Verizon after the acquisition.
        if (normalizeProviderName(trimmed).indexOf('frontier') !== -1) {
            return 'Verizon Fios';
        }

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
        requiredProviderNames: requiredProviderNames,
        isRequiredProvider: isRequiredProvider,
        getDisplayName: getDisplayName,
        getSourceNote: getSourceNote,
    };

})(window);
