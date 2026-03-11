// NPV Calculator module
// Wrapped in IIFE to avoid global pollution

(function(global) {
    'use strict';

    var NPVCalculator = {
        _panel: null,
        _currentFips: null,
        _debounceTimer: null,

        // Default assumptions
        _defaults: {
            arpu: 65,
            takeRate: 35,
            aerialCost: 800,
            undergroundCost: 2500,
            aerialPct: 60,
            wacc: 8,
            horizon: 20
        },

        init: function() {
            this._panel = document.getElementById('npv-panel');
            if (!this._panel) return;

            var self = this;
            // Attach slider listeners
            var sliders = this._panel.querySelectorAll('input[type="range"]');
            sliders.forEach(function(slider) {
                slider.addEventListener('input', function() {
                    self._updateSliderDisplay(slider);
                    self._debouncedCalculate();
                });
            });

            // Close button
            var closeBtn = document.getElementById('npv-close-btn');
            if (closeBtn) {
                closeBtn.addEventListener('click', function() {
                    self.close();
                });
            }
        },

        open: function(fips) {
            if (!this._panel) return;
            this._currentFips = fips;
            this._panel.classList.add('open');

            // Reset sliders to defaults
            this._setSlider('npv-arpu', this._defaults.arpu);
            this._setSlider('npv-take-rate', this._defaults.takeRate);
            this._setSlider('npv-aerial-cost', this._defaults.aerialCost);
            this._setSlider('npv-ug-cost', this._defaults.undergroundCost);
            this._setSlider('npv-aerial-pct', this._defaults.aerialPct);
            this._setSlider('npv-wacc', this._defaults.wacc);
            this._setSlider('npv-horizon', this._defaults.horizon);

            this._calculate();
        },

        close: function() {
            if (!this._panel) return;
            this._panel.classList.remove('open');
            this._currentFips = null;
        },

        _setSlider: function(id, value) {
            var slider = document.getElementById(id);
            if (slider) {
                slider.value = value;
                this._updateSliderDisplay(slider);
            }
        },

        _updateSliderDisplay: function(slider) {
            var displayId = slider.id + '-val';
            var display = document.getElementById(displayId);
            if (!display) return;

            var val = parseFloat(slider.value);
            var format = slider.dataset.format || '';

            switch (format) {
                case 'currency':
                    display.textContent = '$' + val.toLocaleString('en-US');
                    break;
                case 'percent':
                    display.textContent = val + '%';
                    break;
                case 'years':
                    display.textContent = val + ' yrs';
                    break;
                default:
                    display.textContent = val;
            }
        },

        _debouncedCalculate: function() {
            var self = this;
            if (this._debounceTimer) clearTimeout(this._debounceTimer);
            this._debounceTimer = setTimeout(function() {
                self._calculate();
            }, 100);
        },

        _calculate: function() {
            if (!this._currentFips) return;
            var data = DataHandler.getCountyData(this._currentFips);
            if (!data) return;

            // Read slider values
            var arpu = this._getSliderVal('npv-arpu');
            var takeRate = this._getSliderVal('npv-take-rate') / 100;
            var aerialCost = this._getSliderVal('npv-aerial-cost');
            var ugCost = this._getSliderVal('npv-ug-cost');
            var aerialPct = this._getSliderVal('npv-aerial-pct') / 100;
            var wacc = this._getSliderVal('npv-wacc') / 100;
            var horizon = this._getSliderVal('npv-horizon');

            // Core calculations
            var homesPassed = data.fiber_unserved || 0;
            var subscribers = Math.round(homesPassed * takeRate);

            // Blended capex per passing
            var blendedCapex = aerialPct * aerialCost + (1 - aerialPct) * ugCost;
            var totalCapex = homesPassed * blendedCapex;

            // BEAD subsidy offset
            var beadSubsidy = data.bead_dollars_awarded || 0;
            var netCapex = Math.max(0, totalCapex - beadSubsidy);

            // Annual revenue & opex
            var annualRevenue = subscribers * arpu * 12;
            var annualOpex = annualRevenue * 0.40; // 40% opex ratio
            var annualFCF = annualRevenue - annualOpex;

            // NPV calculation
            var npv = -netCapex;
            var paybackYear = null;
            var cumFCF = -netCapex;

            for (var t = 1; t <= horizon; t++) {
                var discountedFCF = annualFCF / Math.pow(1 + wacc, t);
                npv += discountedFCF;
                cumFCF += annualFCF; // Undiscounted for payback
                if (paybackYear === null && cumFCF >= 0) {
                    paybackYear = t;
                }
            }

            // Display results
            this._setResult('npv-homes-passed', DataHandler.formatNumber(homesPassed));
            this._setResult('npv-subscribers', DataHandler.formatNumber(subscribers));
            this._setResult('npv-total-capex', this._formatMillions(totalCapex));
            this._setResult('npv-bead-subsidy', this._formatMillions(beadSubsidy));
            this._setResult('npv-net-capex', this._formatMillions(netCapex));
            this._setResult('npv-annual-rev', this._formatMillions(annualRevenue));
            this._setResult('npv-result', this._formatMillions(npv));
            this._setResult('npv-payback', paybackYear ? paybackYear + ' years' : '>' + horizon + ' years');

            // Color code NPV result
            var npvEl = document.getElementById('npv-result');
            if (npvEl) {
                npvEl.className = 'npv-result-value ' + (npv >= 0 ? 'positive' : 'negative');
            }

            // Update county name display
            this._setResult('npv-county-name', data.name + ' County');
        },

        _getSliderVal: function(id) {
            var slider = document.getElementById(id);
            return slider ? parseFloat(slider.value) : 0;
        },

        _setResult: function(id, text) {
            var el = document.getElementById(id);
            if (el) el.textContent = text;
        },

        _formatMillions: function(num) {
            if (!Number.isFinite(num)) return 'N/A';
            if (Math.abs(num) >= 1000000) {
                return '$' + (num / 1000000).toFixed(1) + 'M';
            }
            return '$' + num.toLocaleString('en-US', { maximumFractionDigits: 0 });
        }
    };

    global.NPVCalculator = NPVCalculator;

})(typeof window !== 'undefined' ? window : global);
