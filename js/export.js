// PPT export module
// Renders a proper D3/AlbersUSA map to canvas (no screenshot, no offset issues),
// then builds a 16:9 PPTX: slide 1 = map, slide 2 = key statistics.

(function() {
    'use strict';

    var MAP_W    = 1280;   // offscreen canvas width (px)
    var MAP_H    = 760;    // offscreen canvas height (px)
    var BG       = '0D0D1A';
    var TEXT_PRI = 'FFFFFF';
    var TEXT_SEC = '9999BB';
    var TEXT_DIM = '555577';
    var ACCENT   = '4D9FFF';
    var FONT     = 'Calibri';

    var LAYER_LABELS = {
        penetration:   'Fiber Penetration',
        cable:         'Cable Coverage',
        fwa:           'Fixed Wireless',
        demographic:   'Demographics',
        attractiveness:'Market Attractiveness',
        competitive:   'Competition',
        momentum:      'Build Momentum',
        terrain:       'Build Difficulty',
        broadband_gap: 'Broadband Gap',
    };

    // ── D3 map rendering ──────────────────────────────────────────────────────

    function _renderMapToCanvas(resolve, reject) {
        if (typeof d3 === 'undefined') {
            reject(new Error('D3 not loaded'));
            return;
        }
        if (!DataHandler.usCountiesTopo) {
            reject(new Error('County TopoJSON not loaded'));
            return;
        }

        var canvas = document.createElement('canvas');
        canvas.width  = MAP_W * 2;   // 2× for crisp rendering
        canvas.height = MAP_H * 2;
        var ctx = canvas.getContext('2d');
        ctx.scale(2, 2);

        // Background
        ctx.fillStyle = '#0d0d1a';
        ctx.fillRect(0, 0, MAP_W, MAP_H);

        var projection = d3.geoAlbersUsa()
            .scale(1680)
            .translate([MAP_W / 2, MAP_H / 2]);
        var pathGen = d3.geoPath().projection(projection).context(ctx);

        var topo     = DataHandler.usCountiesTopo;
        var features = topojson.feature(topo, topo.objects.counties).features;

        // Sort: draw unloaded (dark) counties first, loaded on top
        features.forEach(function(feature) {
            var rawId = feature.id != null ? String(feature.id) : '';
            var fips  = rawId.padStart(5, '0');
            var data  = DataHandler.getCountyData(fips);
            var color = MapRenderer._countyColor(data);

            ctx.beginPath();
            pathGen(feature);
            ctx.fillStyle = color;
            ctx.fill();
        });

        // County borders — single pass for perf
        features.forEach(function(feature) {
            ctx.beginPath();
            pathGen(feature);
            ctx.strokeStyle = 'rgba(13,13,26,0.8)';
            ctx.lineWidth   = 0.25;
            ctx.stroke();
        });

        resolve(canvas.toDataURL('image/png'));
    }

    // ── Stats builder ─────────────────────────────────────────────────────────

    function _buildStats(mode, layer, provider) {
        var states     = (typeof DataHandler !== 'undefined') ? DataHandler.getAllStates() : [];
        var allCounties = [];
        states.forEach(function(s) {
            var cs = DataHandler.getCountiesForState(s.code);
            if (cs) allCounties = allCounties.concat(cs);
        });
        if (!allCounties.length) return [];

        if (mode === 'provider' && provider) {
            var countyCoverage = 0, totalFiber = 0;
            allCounties.forEach(function(c) {
                var ops = c.fiber_operators || [];
                if (ops.indexOf(provider) !== -1) {
                    countyCoverage++;
                    totalFiber += (c.fiber_passings || 0);
                }
            });
            return [
                { label: 'Provider',                   value: provider },
                { label: 'States in Dataset',          value: states.length.toString() },
                { label: 'Counties with FCC Presence', value: countyCoverage.toLocaleString() },
                { label: 'FCC Fiber Passings in Map',  value: totalFiber > 0 ? totalFiber.toLocaleString() : 'N/A' },
                { label: 'Data Source',                value: 'FCC BDC Jun 2025' },
            ];
        }

        var fiberPens = allCounties.filter(function(c) { return c.fiber_penetration != null; });
        var avgPen    = fiberPens.length
            ? fiberPens.reduce(function(s, c) { return s + c.fiber_penetration; }, 0) / fiberPens.length
            : 0;
        var unserved  = allCounties.reduce(function(s, c) { return s + (c.fiber_unserved || 0); }, 0);
        var totalPop  = allCounties.reduce(function(s, c) { return s + (c.total_population || 0); }, 0);
        var highOpp   = allCounties.filter(function(c) {
            return (c.fiber_penetration || 0) < 0.20 && (c.total_population || 0) > 20000;
        }).length;
        return [
            { label: 'Counties Analyzed',                         value: allCounties.length.toLocaleString() },
            { label: 'States Covered',                            value: states.length.toString() },
            { label: 'Avg Fiber Penetration',                     value: (avgPen * 100).toFixed(1) + '%' },
            { label: 'Total Unserved Locations',                  value: unserved.toLocaleString() },
            { label: 'Total Population in Map',                   value: totalPop.toLocaleString() },
            { label: 'High-Opportunity Counties (<20% pen, 20K+ pop)', value: highOpp.toLocaleString() },
            { label: 'Data Vintage',                              value: 'FCC BDC Jun 2025 / Census ACS 2023' },
        ];
    }

    // ── PPTX slide builders ───────────────────────────────────────────────────

    function _slideTitle(mode, layer, provider, tech) {
        if (mode === 'provider' && provider) {
            var techLabel = tech === 'fiber' ? 'Fiber'
                          : tech === 'cable' ? 'Cable'
                          : tech === 'dsl'   ? 'DSL'
                          : 'All Tech';
            return provider + ' \u2014 ' + techLabel + ' Footprint';
        }
        return 'US Fiber Market \u2014 ' + (LAYER_LABELS[layer] || layer);
    }

    function _addSlide1(pptx, imgData, title, dateStr) {
        var s = pptx.addSlide();
        s.background = { color: BG };
        s.addText(title, {
            x: 0.3, y: 0.12, w: 12, h: 0.44,
            fontSize: 22, bold: true, color: TEXT_PRI, fontFace: FONT,
        });
        s.addText('FiberMap USA  \u00b7  ' + dateStr + '  \u00b7  FCC BDC Jun 2025', {
            x: 0.3, y: 0.56, w: 12, h: 0.24,
            fontSize: 10, color: TEXT_DIM, fontFace: FONT,
        });
        s.addImage({ data: imgData, x: 0.3, y: 0.87, w: 12.73, h: (MAP_H / MAP_W) * 12.73 });
    }

    function _addSlide2(pptx, title, dateStr, stats) {
        var s = pptx.addSlide();
        s.background = { color: BG };
        s.addText(title + ' \u2014 Key Statistics', {
            x: 0.3, y: 0.2, w: 12.73, h: 0.44,
            fontSize: 22, bold: true, color: TEXT_PRI, fontFace: FONT,
        });

        var rows = [[
            { text: 'Metric', options: { bold: true, color: ACCENT, fill: '111133' } },
            { text: 'Value',  options: { bold: true, color: ACCENT, fill: '111133' } },
        ]];
        stats.forEach(function(stat, i) {
            var f = i % 2 === 0 ? '111130' : '0D0D22';
            rows.push([
                { text: stat.label, options: { color: TEXT_SEC, fill: f } },
                { text: stat.value, options: { color: TEXT_PRI, bold: true, fill: f } },
            ]);
        });

        var rowH = 0.44;
        s.addTable(rows, {
            x: 0.5, y: 0.85, w: 12.33, h: rows.length * rowH,
            rowH: rowH, colW: [7, 5.33],
            border: { type: 'solid', pt: 0.5, color: '222244' },
            fontFace: FONT, fontSize: 12,
        });
        s.addText('Source: FiberMap USA  \u00b7  FCC BDC Jun 2025  \u00b7  US Census ACS 2023  \u00b7  USDA RUCC 2023', {
            x: 0.3, y: 7.15, w: 12.73, h: 0.25,
            fontSize: 9, italic: true, color: TEXT_DIM, fontFace: FONT,
        });
    }

    // ── Public API ────────────────────────────────────────────────────────────

    function exportToPPT(btnId) {
        if (typeof PptxGenJS === 'undefined') {
            alert('Export library not loaded yet. Please wait a moment and try again.');
            return;
        }

        var btn = document.getElementById(btnId || 'export-ppt-btn');
        function setLabel(txt, disabled) {
            if (btn) { btn.textContent = txt; btn.disabled = !!disabled; }
        }

        var mode     = (typeof MapRenderer !== 'undefined') ? MapRenderer.currentMode     : 'market';
        var layer    = (typeof MapRenderer !== 'undefined') ? MapRenderer.currentLayer    : 'penetration';
        var provider = (typeof MapRenderer !== 'undefined') ? MapRenderer.currentProvider : '';
        var tech     = (typeof MapRenderer !== 'undefined') ? MapRenderer.currentTech     : 'fiber';
        var title    = _slideTitle(mode, layer, provider, tech);
        var today    = new Date().toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
        var slug     = (mode === 'provider' && provider
            ? provider.replace(/[^A-Za-z0-9]+/g, '-')
            : layer) + '-' + new Date().toISOString().slice(0, 10);

        setLabel('Rendering\u2026', true);

        new Promise(function(resolve, reject) {
            _renderMapToCanvas(resolve, reject);
        }).then(function(imgData) {
            setLabel('Building PPT\u2026', true);

            var pptx = new PptxGenJS();
            pptx.layout = 'LAYOUT_WIDE';
            _addSlide1(pptx, imgData, title, today);
            var stats = _buildStats(mode, layer, provider);
            if (stats.length) _addSlide2(pptx, title, today, stats);

            setLabel('Saving\u2026', true);
            return pptx.writeFile({ fileName: 'FiberMap-' + slug + '.pptx' });
        }).then(function() {
            setLabel('Export PPT', false);
        }).catch(function(err) {
            console.error('PPT export error:', err);
            setLabel('Export PPT', false);
            alert('Export failed: ' + err.message);
        });
    }

    window.MapExport = { exportToPPT: exportToPPT };

})();
