// PPT export module
// Captures the Leaflet map and builds a 16:9 PPTX with a map slide and a stats slide.

(function() {
    'use strict';

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

    var BG        = '0D0D1A';
    var TEXT_PRI  = 'FFFFFF';
    var TEXT_SEC  = '9999BB';
    var TEXT_DIM  = '555577';
    var ACCENT    = '4D9FFF';
    var FONT      = 'Calibri';

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

    function _buildStats(mode, layer, provider) {
        var states = (typeof DataHandler !== 'undefined') ? DataHandler.getAllStates() : [];
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
                { label: 'Provider',                  value: provider },
                { label: 'States Mapped',             value: states.length.toString() },
                { label: 'Counties with FCC Presence',value: countyCoverage.toLocaleString() },
                { label: 'FCC Fiber Passings in Map', value: totalFiber > 0 ? totalFiber.toLocaleString() : 'N/A' },
                { label: 'Data Source',               value: 'FCC BDC Jun 2025' },
            ];
        }

        // Market mode
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
            { label: 'Counties Analyzed',                    value: allCounties.length.toLocaleString() },
            { label: 'States Covered',                       value: states.length.toString() },
            { label: 'Avg Fiber Penetration',                value: (avgPen * 100).toFixed(1) + '%' },
            { label: 'Total Unserved Locations',             value: unserved.toLocaleString() },
            { label: 'Total Population in Map',              value: totalPop.toLocaleString() },
            { label: 'High-Opportunity Counties (<20%, 20K+ pop)', value: highOpp.toLocaleString() },
            { label: 'Data Vintage',                         value: 'FCC BDC Jun 2025 / Census ACS 2023' },
        ];
    }

    function _addSlide1(pptx, imgData, title, dateStr) {
        var s = pptx.addSlide();
        s.background = { color: BG };

        s.addText(title, {
            x: 0.3, y: 0.12, w: 10, h: 0.42,
            fontSize: 20, bold: true, color: TEXT_PRI, fontFace: FONT,
        });
        s.addText('FiberMap USA  \u00b7  ' + dateStr, {
            x: 0.3, y: 0.54, w: 10, h: 0.25,
            fontSize: 10, color: TEXT_DIM, fontFace: FONT,
        });
        s.addImage({ data: imgData, x: 0.3, y: 0.85, w: 12.73, h: 6.4 });
    }

    function _addSlide2(pptx, title, dateStr, stats) {
        var s = pptx.addSlide();
        s.background = { color: BG };

        s.addText(title + ' \u2014 Key Statistics', {
            x: 0.3, y: 0.2, w: 12.73, h: 0.42,
            fontSize: 20, bold: true, color: TEXT_PRI, fontFace: FONT,
        });

        var rows = stats.map(function(stat, i) {
            var rowFill = i % 2 === 0 ? '111130' : '0D0D22';
            return [
                { text: stat.label, options: { color: TEXT_SEC, fill: rowFill } },
                { text: stat.value, options: { color: TEXT_PRI, bold: true, fill: rowFill } },
            ];
        });

        // Header row
        rows.unshift([
            { text: 'Metric',  options: { bold: true, color: ACCENT, fill: '111133' } },
            { text: 'Value',   options: { bold: true, color: ACCENT, fill: '111133' } },
        ]);

        var rowH = 0.42;
        s.addTable(rows, {
            x: 0.5, y: 0.85, w: 12.33,
            h: rows.length * rowH,
            rowH: rowH,
            colW: [7, 5.33],
            border: { type: 'solid', pt: 0.5, color: '222244' },
            fontFace: FONT,
            fontSize: 12,
        });

        s.addText('Source: FiberMap USA  \u00b7  FCC BDC Jun 2025  \u00b7  US Census ACS 2023  \u00b7  USDA RUCC 2023', {
            x: 0.3, y: 7.15, w: 12.73, h: 0.25,
            fontSize: 9, italic: true, color: TEXT_DIM, fontFace: FONT,
        });
    }

    function exportToPPT() {
        if (typeof html2canvas === 'undefined' || typeof PptxGenJS === 'undefined') {
            alert('Export libraries not loaded yet. Please wait a moment and try again.');
            return;
        }

        var btn = document.getElementById('export-ppt-btn');
        if (btn) { btn.textContent = 'Capturing\u2026'; btn.disabled = true; }

        var mode     = (typeof MapRenderer !== 'undefined') ? MapRenderer.currentMode     : 'market';
        var layer    = (typeof MapRenderer !== 'undefined') ? MapRenderer.currentLayer    : 'penetration';
        var provider = (typeof MapRenderer !== 'undefined') ? MapRenderer.currentProvider : '';
        var tech     = (typeof MapRenderer !== 'undefined') ? MapRenderer.currentTech     : 'fiber';

        var title   = _slideTitle(mode, layer, provider, tech);
        var today   = new Date().toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
        var fileSlug = (mode === 'provider' && provider
            ? provider.replace(/[^A-Za-z0-9]+/g, '-')
            : layer) + '-' + new Date().toISOString().slice(0, 10);

        var mapEl = document.getElementById('map');
        html2canvas(mapEl, {
            useCORS:         true,
            allowTaint:      true,
            backgroundColor: '#0d0d1a',
            scale:           1.5,
            logging:         false,
            removeContainer: true,
        }).then(function(canvas) {
            var imgData = canvas.toDataURL('image/png');

            var pptx = new PptxGenJS();
            pptx.layout = 'LAYOUT_WIDE';   // 13.33 x 7.5 in

            _addSlide1(pptx, imgData, title, today);

            var stats = _buildStats(mode, layer, provider);
            if (stats.length) _addSlide2(pptx, title, today, stats);

            if (btn) btn.textContent = 'Saving\u2026';
            return pptx.writeFile({ fileName: 'FiberMap-' + fileSlug + '.pptx' });
        }).then(function() {
            if (btn) { btn.textContent = 'Export PPT'; btn.disabled = false; }
        }).catch(function(err) {
            console.error('PPT export error:', err);
            if (btn) { btn.textContent = 'Export PPT'; btn.disabled = false; }
            alert('Export failed. Check the browser console for details.');
        });
    }

    window.MapExport = { exportToPPT: exportToPPT };

})();
