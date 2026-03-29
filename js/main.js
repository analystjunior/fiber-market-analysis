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

        // ── Provider picker ──
        buildProviderList();
        var providerSearch = document.getElementById('provider-search');
        if (providerSearch) {
            providerSearch.addEventListener('input', function() {
                filterProviderList(this.value.trim().toLowerCase());
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

        var totals = ProviderIndex.computeNationalTotals();

        ProviderIndex.GROUPS.forEach(function(group) {
            var groupEl = document.createElement('div');
            groupEl.className = 'provider-group';

            var label = document.createElement('div');
            label.className = 'provider-group-label';
            label.textContent = group.group;
            groupEl.appendChild(label);

            group.providers.forEach(function(name) {
                var btn = document.createElement('button');
                btn.className = 'provider-item';
                btn.setAttribute('role', 'option');
                btn.setAttribute('aria-selected', 'false');
                btn.dataset.provider = name;

                // Provider name
                var nameSpan = document.createElement('span');
                nameSpan.className = 'provider-item-name';
                nameSpan.textContent = name;
                btn.appendChild(nameSpan);

                // National passings badge
                var total = totals[name];
                var formatted = ProviderIndex.formatPassings(total);
                if (formatted) {
                    var badge = document.createElement('span');
                    badge.className = 'provider-item-passings';
                    badge.textContent = formatted;
                    btn.appendChild(badge);
                }

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
                groupEl.appendChild(btn);
            });

            container.appendChild(groupEl);
        });
    }

    function filterProviderList(query) {
        var container = document.getElementById('provider-list');
        if (!container) return;
        var groups = container.querySelectorAll('.provider-group');
        groups.forEach(function(group) {
            var items = group.querySelectorAll('.provider-item');
            var anyVisible = false;
            items.forEach(function(item) {
                var match = !query || item.textContent.toLowerCase().indexOf(query) !== -1;
                item.style.display = match ? '' : 'none';
                if (match) anyVisible = true;
            });
            group.style.display = anyVisible ? '' : 'none';
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
