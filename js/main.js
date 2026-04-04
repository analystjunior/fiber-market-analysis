// Main application initialization
// Wrapped in IIFE to avoid global pollution

(function() {
    'use strict';

    document.addEventListener('DOMContentLoaded', async function() {
        console.log('US Fiber Market Analysis initializing...');

        // Load data
        var dataLoaded = await DataHandler.loadData();
        if (!dataLoaded) {
            var loadError = DataHandler.getLoadError();
            console.error('Failed to load data:', loadError);
            var mapEl = document.getElementById('map');
            if (mapEl) {
                var errorP = document.createElement('p');
                errorP.style.cssText = 'color: red; text-align: center; padding: 2rem;';
                errorP.textContent = 'Failed to load data. Please refresh the page.';
                mapEl.appendChild(errorP);
            }
            return;
        }

        console.log('Data loaded:', DataHandler.getAllStates().length, 'states,',
            DataHandler.getCountiesForState('NY').length, 'NY counties,',
            DataHandler.getCountiesForState('MO').length, 'MO counties');

        // Initialize components
        InfoPanel.init();
        await MapRenderer.init('map');  // loads all state county data
        TableManager.init();
        NPVCalculator.init();

        // Render table for default active state (MO)
        TableManager.renderTable();

        // Setup UI controls (buildProviderList runs after all county data is loaded)
        setupControls();

        // Setup global event handlers
        setupGlobalHandlers();

        console.log('Application initialized');
    });

    function setupControls() {
        // ── Mode switcher (By Market / By Provider) ──
        var modeMarketBtn    = document.getElementById('mode-market');
        var modeProviderBtn  = document.getElementById('mode-provider');
        var marketControls   = document.getElementById('market-controls');
        var providerPickerEl = document.getElementById('provider-picker-panel');

        function switchMode(mode) {
            var isProvider = mode === 'provider';
            modeMarketBtn.classList.toggle('active', !isProvider);
            modeMarketBtn.setAttribute('aria-pressed', isProvider ? 'false' : 'true');
            modeProviderBtn.classList.toggle('active', isProvider);
            modeProviderBtn.setAttribute('aria-pressed', isProvider ? 'true' : 'false');
            marketControls.style.display = isProvider ? 'none' : '';
            if (providerPickerEl) providerPickerEl.style.display = isProvider ? 'block' : 'none';
            MapRenderer.setMode(mode);
            InfoPanel.setMode(mode);
        }

        if (modeMarketBtn) modeMarketBtn.addEventListener('click', function() { switchMode('market'); });
        if (modeProviderBtn) modeProviderBtn.addEventListener('click', function() { switchMode('provider'); });

        // ── Provider sub-view toggle (Individual / Competition) ──
        var subviewIndividualBtn  = document.getElementById('subview-individual');
        var subviewCompetitionBtn = document.getElementById('subview-competition');
        var individualPanel       = document.getElementById('individual-panel');
        var competitionPanel      = document.getElementById('competition-panel');

        function switchSubview(subview) {
            var isCompetition = subview === 'competition';
            subviewIndividualBtn.classList.toggle('active', !isCompetition);
            subviewIndividualBtn.setAttribute('aria-pressed', isCompetition ? 'false' : 'true');
            subviewCompetitionBtn.classList.toggle('active', isCompetition);
            subviewCompetitionBtn.setAttribute('aria-pressed', isCompetition ? 'true' : 'false');
            if (individualPanel)  individualPanel.style.display  = isCompetition ? 'none' : '';
            if (competitionPanel) competitionPanel.style.display = isCompetition ? ''     : 'none';
            // Reset active selection state
            if (_activeProviderBtn) {
                _activeProviderBtn.classList.remove('active');
                _activeProviderBtn.setAttribute('aria-selected', 'false');
                _activeProviderBtn = null;
            }
            _competitionSelected = [];
            MapRenderer.setSubview(subview);
        }

        if (subviewIndividualBtn)  subviewIndividualBtn.addEventListener('click',  function() { switchSubview('individual'); });
        if (subviewCompetitionBtn) subviewCompetitionBtn.addEventListener('click', function() { switchSubview('competition'); });

        // ── Provider picker ──
        buildProviderList();
        buildCompetitionList();
        var providerSearch = document.getElementById('provider-search');
        if (providerSearch) {
            providerSearch.addEventListener('input', function() {
                var q = this.value.trim().toLowerCase();
                filterProviderList(q);
                filterCompetitionList(q);
            });
        }

        // Layer toggle buttons with ARIA support
        var toggleBtns = document.querySelectorAll('#layer-toggle .toggle-btn');
        toggleBtns.forEach(function(btn) {
            btn.addEventListener('click', function() {
                // Stop deep dive when manually selecting a layer
                MapRenderer.stopDeepDive();

                // Update active state
                toggleBtns.forEach(function(b) {
                    b.classList.remove('active');
                    b.setAttribute('aria-pressed', 'false');
                });
                btn.classList.add('active');
                btn.setAttribute('aria-pressed', 'true');
                MapRenderer.setLayer(btn.dataset.layer);
            });

            btn.addEventListener('keydown', function(e) {
                if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    btn.click();
                }
            });
        });

        // Min population filter
        var minPopSelect = document.getElementById('min-pop');
        if (minPopSelect) {
            minPopSelect.addEventListener('change', function(e) {
                MapRenderer.setFilters({ minPop: parseInt(e.target.value, 10) || 0 });
            });
        }

        // Min density filter
        var minDensitySelect = document.getElementById('min-density');
        if (minDensitySelect) {
            minDensitySelect.addEventListener('change', function(e) {
                MapRenderer.setFilters({ minDensity: parseInt(e.target.value, 10) || 0 });
            });
        }

        // Deep Dive toggle
        var deepDiveBtn = document.getElementById('deep-dive-btn');
        if (deepDiveBtn) {
            deepDiveBtn.addEventListener('click', function() {
                MapRenderer.toggleDeepDive();
            });
        }

        // Tech filter buttons (Fiber / Cable / DSL / All) in provider picker
        var techBtns = document.querySelectorAll('#tech-filter .tech-btn');
        techBtns.forEach(function(btn) {
            btn.addEventListener('click', function() {
                techBtns.forEach(function(b) {
                    b.classList.remove('active');
                    b.setAttribute('aria-pressed', 'false');
                });
                btn.classList.add('active');
                btn.setAttribute('aria-pressed', 'true');
                MapRenderer.setTech(btn.dataset.tech);
            });
        });

        // NPV Calculator trigger button
        var npvBtn = document.getElementById('open-npv-btn');
        if (npvBtn) {
            npvBtn.addEventListener('click', function() {
                var fips = InfoPanel.pinnedCounty;
                if (fips) {
                    NPVCalculator.open(fips);
                } else {
                    npvBtn.textContent = 'Click a county first';
                    setTimeout(function() {
                        npvBtn.textContent = 'Open NPV Calculator';
                    }, 2000);
                }
            });
        }
    }

    var _activeProviderBtn = null;

    function buildProviderList() {
        var container = document.getElementById('provider-list');
        if (!container) return;
        container.textContent = '';

        var computed = ProviderIndex.computeNationalTotals();

        // Resolve display values for a provider:
        // - If in PUBLIC_REPORTED: use those figures (null tech → 0 for total, "—" for display)
        // - If not in PUBLIC_REPORTED: fall back to county-computed totals
        function resolveDisplay(name) {
            var pub = ProviderIndex.getPublicTotals(name);
            if (pub) {
                var fiber = pub.fiber != null ? pub.fiber : 0;
                var cable = pub.cable != null ? pub.cable : 0;
                var dsl   = pub.dsl   != null ? pub.dsl   : 0;
                return {
                    fiber: pub.fiber,  // null preserved for "—" display
                    cable: pub.cable,
                    dsl:   pub.dsl,
                    all:   fiber + cable + dsl,
                    sortFiber: fiber,
                };
            }
            var c = computed[name] || { fiber: 0, cable: 0, dsl: 0, all: 0 };
            return {
                fiber: c.fiber || null,
                cable: c.cable || null,
                dsl:   c.dsl   || null,
                all:   c.all,
                sortFiber: c.fiber,
            };
        }

        // Union of public-reported providers + computed providers with 100K+ fiber
        var nameSet = {};
        ProviderIndex.publicProviderNames().forEach(function(n) { nameSet[n] = true; });
        Object.keys(computed).forEach(function(n) {
            if (computed[n].fiber >= 100000) nameSet[n] = true;
        });

        var providers = Object.keys(nameSet)
            .map(function(name) { return { name: name, d: resolveDisplay(name) }; })
            .filter(function(p) { return p.d.sortFiber >= 100000; })
            .sort(function(a, b) { return b.d.sortFiber - a.d.sortFiber; });

        providers.forEach(function(p) {
            var name = p.name;
            var d    = p.d;

            var btn = document.createElement('button');
            btn.className = 'provider-item';
            btn.setAttribute('role', 'option');
            btn.setAttribute('aria-selected', 'false');
            btn.dataset.provider = name;

            var nameSpan = document.createElement('span');
            nameSpan.className = 'provider-item-name';
            nameSpan.textContent = name;
            btn.appendChild(nameSpan);

            var cols = [
                { val: d.fiber, cls: 'provider-item-stat' },
                { val: d.cable, cls: 'provider-item-stat' },
                { val: d.dsl,   cls: 'provider-item-stat' },
                { val: d.all,   cls: 'provider-item-stat provider-item-stat-total' },
            ];
            cols.forEach(function(col) {
                var span = document.createElement('span');
                span.className = col.cls;
                span.textContent = ProviderIndex.formatPassings(col.val) || '—';
                btn.appendChild(span);
            });

            btn.addEventListener('click', function() {
                if (_activeProviderBtn) {
                    _activeProviderBtn.classList.remove('active');
                    _activeProviderBtn.setAttribute('aria-selected', 'false');
                }
                btn.classList.add('active');
                btn.setAttribute('aria-selected', 'true');
                _activeProviderBtn = btn;
                MapRenderer.setProvider(name);
            });
            container.appendChild(btn);
        });
    }

    function filterProviderList(query) {
        var container = document.getElementById('provider-list');
        if (!container) return;
        var items = container.querySelectorAll('.provider-item');
        items.forEach(function(item) {
            var match = !query || item.textContent.toLowerCase().indexOf(query) !== -1;
            item.style.display = match ? '' : 'none';
        });
    }

    // ── Competition view ─────────────────────────────────────────────────────

    var _competitionSelected = [];  // ordered list of selected canonical names

    function buildCompetitionList() {
        var container = document.getElementById('competition-list');
        if (!container) return;
        container.textContent = '';

        var computed = ProviderIndex.computeNationalTotals();

        // Same provider set as individual view, sorted by fiber descending
        var nameSet = {};
        ProviderIndex.publicProviderNames().forEach(function(n) { nameSet[n] = true; });
        Object.keys(computed).forEach(function(n) {
            if (computed[n].fiber >= 100000) nameSet[n] = true;
        });

        var providers = Object.keys(nameSet)
            .map(function(name) {
                var pub = ProviderIndex.getPublicTotals(name);
                var fiberVal = (pub && pub.fiber != null) ? pub.fiber : (computed[name] ? computed[name].fiber : 0);
                return { name: name, fiber: fiberVal };
            })
            .filter(function(p) { return p.fiber >= 100000; })
            .sort(function(a, b) { return b.fiber - a.fiber; });

        providers.forEach(function(p) {
            var btn = document.createElement('button');
            btn.className = 'provider-item competition-item';
            btn.setAttribute('role', 'option');
            btn.setAttribute('aria-selected', 'false');
            btn.dataset.provider = p.name;

            var dot = document.createElement('span');
            dot.className = 'competition-dot';
            btn.appendChild(dot);

            var nameSpan = document.createElement('span');
            nameSpan.className = 'provider-item-name';
            nameSpan.textContent = p.name;
            btn.appendChild(nameSpan);

            var stat = document.createElement('span');
            stat.className = 'provider-item-stat';
            stat.textContent = ProviderIndex.formatPassings(p.fiber) || '—';
            btn.appendChild(stat);

            btn.addEventListener('click', function() {
                var idx = _competitionSelected.indexOf(p.name);
                if (idx !== -1) {
                    // Deselect
                    _competitionSelected.splice(idx, 1);
                    btn.classList.remove('active');
                    btn.setAttribute('aria-selected', 'false');
                    dot.style.background = '';
                } else {
                    if (_competitionSelected.length >= 5) return;
                    _competitionSelected.push(p.name);
                    var colorIdx = _competitionSelected.length - 1;
                    btn.classList.add('active');
                    btn.setAttribute('aria-selected', 'true');
                    dot.style.background = MapRenderer.COMPETITION_PALETTE[colorIdx];
                }
                // Re-sync dot colors for all selected items (order may have changed)
                _syncCompetitionDots();
                MapRenderer.setCompetitionProviders(_competitionSelected);
                _updateCompetitionLegendPanel();
            });

            container.appendChild(btn);
        });
    }

    function _syncCompetitionDots() {
        var container = document.getElementById('competition-list');
        if (!container) return;
        container.querySelectorAll('.competition-item').forEach(function(btn) {
            var name = btn.dataset.provider;
            var idx = _competitionSelected.indexOf(name);
            var dot = btn.querySelector('.competition-dot');
            if (!dot) return;
            if (idx !== -1) {
                btn.classList.add('active');
                btn.setAttribute('aria-selected', 'true');
                dot.style.background = MapRenderer.COMPETITION_PALETTE[idx];
            } else {
                btn.classList.remove('active');
                btn.setAttribute('aria-selected', 'false');
                dot.style.background = '';
            }
        });
    }

    function _updateCompetitionLegendPanel() {
        var legend = document.getElementById('competition-legend');
        if (!legend) return;
        legend.textContent = '';
        var selected = _competitionSelected;
        if (!selected.length) return;
        selected.forEach(function(name, i) {
            var item = document.createElement('div');
            item.className = 'competition-legend-item';
            var dot = document.createElement('span');
            dot.className = 'competition-dot competition-dot-sm';
            dot.style.background = MapRenderer.COMPETITION_PALETTE[i];
            item.appendChild(dot);
            item.appendChild(document.createTextNode(name));
            legend.appendChild(item);
        });
        if (selected.length > 1) {
            var overlapItem = document.createElement('div');
            overlapItem.className = 'competition-legend-item';
            var overlapDot = document.createElement('span');
            overlapDot.className = 'competition-dot competition-dot-sm';
            overlapDot.style.background = MapRenderer.COMPETITION_OVERLAP_COLOR;
            overlapItem.appendChild(overlapDot);
            overlapItem.appendChild(document.createTextNode('Overlap'));
            legend.appendChild(overlapItem);
        }
    }

    function filterCompetitionList(query) {
        var container = document.getElementById('competition-list');
        if (!container) return;
        var items = container.querySelectorAll('.competition-item');
        items.forEach(function(item) {
            var match = !query || item.textContent.toLowerCase().indexOf(query) !== -1;
            item.style.display = match ? '' : 'none';
        });
    }

    function setupGlobalHandlers() {
        // Close pin button
        var closePinBtn = document.getElementById('close-pin-btn');
        if (closePinBtn) {
            closePinBtn.addEventListener('click', function() {
                InfoPanel.unpinCounty();
            });
            closePinBtn.addEventListener('keydown', function(e) {
                if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    InfoPanel.unpinCounty();
                }
            });
        }
    }

})();
