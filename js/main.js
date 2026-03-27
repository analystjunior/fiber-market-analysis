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
        await MapRenderer.init('map');
        TableManager.init();
        NPVCalculator.init();

        // Render table for default active state (MO)
        TableManager.renderTable();

        // Setup UI controls
        setupControls();

        // Setup global event handlers
        setupGlobalHandlers();

        console.log('Application initialized');
    });

    function setupControls() {
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

        // Exclude NYC checkbox
        var excludeNycCheckbox = document.getElementById('exclude-nyc');
        if (excludeNycCheckbox) {
            excludeNycCheckbox.addEventListener('change', function(e) {
                MapRenderer.setFilters({ excludeNYC: e.target.checked });
            });
        }

        // Exclude STL/KC Metro checkbox
        var excludeStlKcCheckbox = document.getElementById('exclude-stlkc');
        if (excludeStlKcCheckbox) {
            excludeStlKcCheckbox.addEventListener('change', function(e) {
                MapRenderer.setFilters({ excludeSTLKC: e.target.checked });
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
