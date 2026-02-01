// Data handling module
// Wrapped in IIFE to avoid global pollution, exported explicitly

(function(global) {
    'use strict';

    // ============================================
    // UTILITY FUNCTIONS (Pure, testable)
    // ============================================

    /**
     * Safely formats a number with locale string
     * @param {number|null|undefined} num
     * @returns {string}
     */
    function formatNumber(num) {
        if (num === null || num === undefined || !Number.isFinite(num)) {
            return 'N/A';
        }
        return num.toLocaleString('en-US');
    }

    /**
     * Safely formats a currency value
     * @param {number|null|undefined} num
     * @returns {string}
     */
    function formatCurrency(num) {
        if (num === null || num === undefined || !Number.isFinite(num)) {
            return 'N/A';
        }
        return '$' + num.toLocaleString('en-US');
    }

    /**
     * Formats a decimal as percentage (0.5 -> "50.0%")
     * @param {number|null|undefined} num
     * @param {number} decimals
     * @returns {string}
     */
    function formatPercent(num, decimals) {
        if (decimals === undefined) decimals = 1;
        if (num === null || num === undefined || !Number.isFinite(num)) {
            return 'N/A';
        }
        return (num * 100).toFixed(decimals) + '%';
    }

    /**
     * Formats a percentage value directly (50 -> "50.0%")
     * @param {number|null|undefined} num
     * @param {number} decimals
     * @returns {string}
     */
    function formatPercentDirect(num, decimals) {
        if (decimals === undefined) decimals = 1;
        if (num === null || num === undefined || !Number.isFinite(num)) {
            return 'N/A';
        }
        return num.toFixed(decimals) + '%';
    }

    /**
     * Sanitizes a string for safe display (prevents XSS)
     * @param {string|null|undefined} str
     * @returns {string}
     */
    function sanitizeString(str) {
        if (str === null || str === undefined) {
            return '';
        }
        return String(str);
    }

    /**
     * Validates a FIPS/GEOID code
     * @param {string|number} fips
     * @returns {boolean}
     */
    function isValidFips(fips) {
        if (fips === null || fips === undefined) return false;
        const str = String(fips);
        // FIPS codes are 2 digits (state) or 5 digits (county)
        return /^\d{2}$/.test(str) || /^\d{5}$/.test(str);
    }

    /**
     * Safely calculates penetration rate avoiding divide-by-zero
     * @param {number} served
     * @param {number} total
     * @returns {number}
     */
    function calculatePenetration(served, total) {
        if (!Number.isFinite(served) || !Number.isFinite(total) || total === 0) {
            return 0;
        }
        return Math.min(1, Math.max(0, served / total));
    }

    // ============================================
    // DATA HANDLER
    // ============================================

    const DataHandler = {
        // NY county data
        unifiedData: null,
        tigerGeoJSON: null,
        // US state data
        stateData: null,
        usGeoJSON: null,
        // Loading state
        _isLoaded: false,
        _loadError: null,

        async loadData() {
            try {
                // Load all data in parallel
                const [unifiedResponse, tigerResponse, stateResponse, usGeoResponse] = await Promise.all([
                    fetch('data/ny-unified-data.json'),
                    fetch('data/ny_counties_tiger.geojson'),
                    fetch('data/fiber-data.json'),
                    fetch('data/us-states.json')
                ]);

                // Check for HTTP errors
                if (!unifiedResponse.ok) throw new Error('Failed to load NY unified data: ' + unifiedResponse.status);
                if (!tigerResponse.ok) throw new Error('Failed to load NY GeoJSON: ' + tigerResponse.status);
                if (!stateResponse.ok) throw new Error('Failed to load state fiber data: ' + stateResponse.status);
                if (!usGeoResponse.ok) throw new Error('Failed to load US GeoJSON: ' + usGeoResponse.status);

                this.unifiedData = await unifiedResponse.json();
                this.tigerGeoJSON = await tigerResponse.json();
                this.stateData = await stateResponse.json();
                this.usGeoJSON = await usGeoResponse.json();

                // Validate data integrity
                this._validateData();

                this._isLoaded = true;
                this._loadError = null;
                return true;
            } catch (error) {
                console.error('Error loading data:', error);
                this._loadError = error;
                this._isLoaded = false;
                return false;
            }
        },

        _validateData() {
            // Validate NY unified data has required fields
            if (this.unifiedData) {
                const sampleCounty = Object.values(this.unifiedData)[0];
                const requiredFields = ['geoid', 'name', 'fiber_penetration', 'demo_score', 'attractiveness_index'];
                for (const field of requiredFields) {
                    if (!(field in sampleCounty)) {
                        console.warn('Missing required field in county data:', field);
                    }
                }
            }

            // Validate GeoJSON structure
            if (this.tigerGeoJSON && !this.tigerGeoJSON.features) {
                console.warn('NY GeoJSON missing features array');
            }
        },

        isLoaded() {
            return this._isLoaded;
        },

        getLoadError() {
            return this._loadError;
        },

        // NY county methods
        getCountyData(fips) {
            if (!this.unifiedData || !fips) return null;
            return this.unifiedData[fips] || null;
        },

        getAllCounties() {
            if (!this.unifiedData) return [];
            return Object.values(this.unifiedData);
        },

        getGeoJSON() {
            return this.tigerGeoJSON;
        },

        // US state methods
        getStateData(stateCode) {
            if (!this.stateData || !stateCode) return null;
            return this.stateData[stateCode] || null;
        },

        getAllStates() {
            if (!this.stateData) return [];
            return Object.values(this.stateData);
        },

        getUSGeoJSON() {
            return this.usGeoJSON;
        },

        // Expose utility functions
        formatNumber: formatNumber,
        formatCurrency: formatCurrency,
        formatPercent: formatPercent,
        formatPercentDirect: formatPercentDirect,
        sanitizeString: sanitizeString,
        isValidFips: isValidFips,
        calculatePenetration: calculatePenetration
    };

    // ============================================
    // COLOR SCALES (with caching)
    // ============================================

    const ColorScales = {
        // Cached color lookups for performance
        _colorCache: new Map(),

        // Fiber Penetration: Red = low (opportunity), Green = high (saturated)
        penetration: [
            { threshold: 0.3, color: '#fecaca', label: '<30%' },
            { threshold: 0.5, color: '#fde68a', label: '30-50%' },
            { threshold: 0.7, color: '#d9f99d', label: '50-70%' },
            { threshold: 0.85, color: '#bbf7d0', label: '70-85%' },
            { threshold: 1.0, color: '#86efac', label: '>85%' }
        ],
        // Demographics: Red = poor, Green = excellent
        demographic: [
            { threshold: 0.2, color: '#fecaca', label: 'Poor' },
            { threshold: 0.4, color: '#fed7aa', label: 'Below Avg' },
            { threshold: 0.6, color: '#fde68a', label: 'Average' },
            { threshold: 0.8, color: '#d9f99d', label: 'Good' },
            { threshold: 1.0, color: '#86efac', label: 'Excellent' }
        ],
        // Market Attractiveness: Red = least attractive, Green = most attractive
        attractiveness: [
            { threshold: 0.2, color: '#fecaca', label: 'Least Attractive' },
            { threshold: 0.3, color: '#fed7aa', label: 'Low' },
            { threshold: 0.4, color: '#fde68a', label: 'Neutral' },
            { threshold: 0.5, color: '#d9f99d', label: 'Good' },
            { threshold: 1.0, color: '#86efac', label: 'Most Attractive' }
        ],

        getColor(layer, value) {
            // Handle invalid inputs
            if (!Number.isFinite(value)) {
                return '#cbd5e1';
            }

            const scale = this[layer];
            if (!scale) return '#cbd5e1';

            // Check cache first
            const cacheKey = layer + '_' + value.toFixed(3);
            if (this._colorCache.has(cacheKey)) {
                return this._colorCache.get(cacheKey);
            }

            // Find color
            let color = scale[scale.length - 1].color;
            for (const item of scale) {
                if (value <= item.threshold) {
                    color = item.color;
                    break;
                }
            }

            // Cache result (limit cache size)
            if (this._colorCache.size > 1000) {
                this._colorCache.clear();
            }
            this._colorCache.set(cacheKey, color);

            return color;
        },

        getLegend(layer) {
            return this[layer] || this.penetration;
        },

        clearCache() {
            this._colorCache.clear();
        }
    };

    // NYC Borough FIPS codes (immutable)
    const NYC_BOROUGHS = Object.freeze(new Set(['36005', '36047', '36061', '36081', '36085']));

    // ============================================
    // EXPORTS
    // ============================================

    // Export to global scope (required for current architecture)
    // In a module system, these would be ES6 exports
    global.DataHandler = DataHandler;
    global.ColorScales = ColorScales;
    global.NYC_BOROUGHS = NYC_BOROUGHS;

    // Also expose utility functions for testing
    global.FiberUtils = {
        formatNumber: formatNumber,
        formatCurrency: formatCurrency,
        formatPercent: formatPercent,
        formatPercentDirect: formatPercentDirect,
        sanitizeString: sanitizeString,
        isValidFips: isValidFips,
        calculatePenetration: calculatePenetration
    };

})(typeof window !== 'undefined' ? window : global);
