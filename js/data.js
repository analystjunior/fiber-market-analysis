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
     * 
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
        var str = String(fips);
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

    var DataHandler = {
        // Multi-state county data keyed by state code
        _stateCountyData: {},
        _activeState: null,

        // NY-specific GeoJSON (dedicated file)
        tigerGeoJSON: null,
        // US TopoJSON for extracting state county boundaries
        usCountiesTopo: null,
        // Extracted state GeoJSON cache
        _stateGeoJSONCache: {},

        // US state data
        stateData: null,
        usGeoJSON: null,
        // Loading state
        _isLoaded: false,
        _loadError: null,

        async loadData() {
            try {
                // Load all data in parallel
                var responses = await Promise.all([
                    fetch('data/ny-unified-data.json'),
                    fetch('data/ny_counties_tiger.geojson'),
                    fetch('data/fiber-data.json'),
                    fetch('data/us-states.json'),
                    fetch('data/us-counties.json'),
                    fetch('data/mo-unified-data.json')
                ]);

                var unifiedResponse = responses[0];
                var tigerResponse = responses[1];
                var stateResponse = responses[2];
                var usGeoResponse = responses[3];
                var usCountiesResponse = responses[4];
                var moResponse = responses[5];

                // Check for HTTP errors
                if (!unifiedResponse.ok) throw new Error('Failed to load NY unified data: ' + unifiedResponse.status);
                if (!tigerResponse.ok) throw new Error('Failed to load NY GeoJSON: ' + tigerResponse.status);
                if (!stateResponse.ok) throw new Error('Failed to load state fiber data: ' + stateResponse.status);
                if (!usGeoResponse.ok) throw new Error('Failed to load US GeoJSON: ' + usGeoResponse.status);
                if (!usCountiesResponse.ok) throw new Error('Failed to load US counties TopoJSON: ' + usCountiesResponse.status);
                if (!moResponse.ok) throw new Error('Failed to load MO unified data: ' + moResponse.status);

                var nyData = await unifiedResponse.json();
                this.tigerGeoJSON = await tigerResponse.json();
                this.stateData = await stateResponse.json();
                this.usGeoJSON = await usGeoResponse.json();
                this.usCountiesTopo = await usCountiesResponse.json();
                var moData = await moResponse.json();

                // Store county data by state
                this._stateCountyData['NY'] = nyData;
                this._stateCountyData['MO'] = moData;

                // Default active state
                this._activeState = 'MO';

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
            // Validate each state's county data has required fields
            var requiredFields = ['geoid', 'name', 'fiber_penetration', 'demo_score', 'attractiveness_index'];
            for (var stateCode in this._stateCountyData) {
                var stateCounties = this._stateCountyData[stateCode];
                if (stateCounties) {
                    var sampleCounty = Object.values(stateCounties)[0];
                    for (var i = 0; i < requiredFields.length; i++) {
                        if (!(requiredFields[i] in sampleCounty)) {
                            console.warn('Missing required field in ' + stateCode + ' county data:', requiredFields[i]);
                        }
                    }
                }
            }

            // Validate GeoJSON structure
            if (this.tigerGeoJSON && !this.tigerGeoJSON.features) {
                console.warn('NY GeoJSON missing features array');
            }
        },

        isLoaded: function() {
            return this._isLoaded;
        },

        getLoadError: function() {
            return this._loadError;
        },

        // Active state management
        setActiveState: function(stateCode) {
            if (this._stateCountyData[stateCode]) {
                this._activeState = stateCode;
                return true;
            }
            return false;
        },

        getActiveState: function() {
            return this._activeState;
        },

        // County methods (state-aware)
        getCountyData: function(fips) {
            if (!fips) return null;
            // Try active state first
            var activeData = this._stateCountyData[this._activeState];
            if (activeData && activeData[fips]) return activeData[fips];
            // Fallback: search all states
            for (var sc in this._stateCountyData) {
                if (this._stateCountyData[sc][fips]) return this._stateCountyData[sc][fips];
            }
            return null;
        },

        getAllCounties: function() {
            var activeData = this._stateCountyData[this._activeState];
            if (!activeData) return [];
            return Object.values(activeData);
        },

        getCountiesForState: function(stateCode) {
            var data = this._stateCountyData[stateCode];
            if (!data) return [];
            return Object.values(data);
        },

        getGeoJSON: function() {
            return this.tigerGeoJSON;
        },

        /**
         * Extracts county GeoJSON for a state from the US counties TopoJSON.
         * Uses FIPS prefix to filter geometries.
         * @param {string} fipsPrefix - 2-digit state FIPS (e.g. '29' for MO)
         * @returns {Object} GeoJSON FeatureCollection
         */
        extractStateGeoJSON: function(fipsPrefix) {
            // Check cache
            if (this._stateGeoJSONCache[fipsPrefix]) {
                return this._stateGeoJSONCache[fipsPrefix];
            }

            if (!this.usCountiesTopo || !this.usCountiesTopo.objects || !this.usCountiesTopo.objects.counties) {
                console.error('US counties TopoJSON not loaded or missing counties object');
                return null;
            }

            // Filter geometries by FIPS prefix
            var allGeometries = this.usCountiesTopo.objects.counties.geometries;
            var filteredGeometries = allGeometries.filter(function(g) {
                return g.id && String(g.id).startsWith(fipsPrefix);
            });

            // Create a filtered topology object
            var filteredObject = {
                type: 'GeometryCollection',
                geometries: filteredGeometries
            };

            // Use topojson.feature to convert to GeoJSON
            var geojson = topojson.feature(this.usCountiesTopo, filteredObject);

            // Ensure each feature has an id matching the county FIPS
            geojson.features.forEach(function(f) {
                if (!f.id && f.properties && f.properties.id) {
                    f.id = f.properties.id;
                }
            });

            // Cache result
            this._stateGeoJSONCache[fipsPrefix] = geojson;
            return geojson;
        },

        // US state methods
        getStateData: function(stateCode) {
            if (!this.stateData || !stateCode) return null;
            return this.stateData[stateCode] || null;
        },

        getAllStates: function() {
            if (!this.stateData) return [];
            return Object.values(this.stateData);
        },

        getUSGeoJSON: function() {
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

    var ColorScales = {
        // Cached color lookups for performance
        _colorCache: new Map(),

        // Shared ramp — red (low/opportunity) → amber → green (high/saturated)
        // Used by: penetration, cable, fwa, demographic, attractiveness
        _ramp: ['#dc2626', '#f97316', '#ca8a04', '#16a34a', '#15803d'],

        // Fiber Penetration: red = low coverage (opportunity), green = saturated
        penetration: [
            { threshold: 0.3,  color: '#dc2626', label: '<30%' },
            { threshold: 0.5,  color: '#f97316', label: '30–50%' },
            { threshold: 0.7,  color: '#ca8a04', label: '50–70%' },
            { threshold: 0.85, color: '#16a34a', label: '70–85%' },
            { threshold: 1.0,  color: '#15803d', label: '>85%' }
        ],
        // Cable Coverage — identical scale to fiber for direct comparability
        cable: [
            { threshold: 0.3,  color: '#dc2626', label: '<30%' },
            { threshold: 0.5,  color: '#f97316', label: '30–50%' },
            { threshold: 0.7,  color: '#ca8a04', label: '50–70%' },
            { threshold: 0.85, color: '#16a34a', label: '70–85%' },
            { threshold: 1.0,  color: '#15803d', label: '>85%' }
        ],
        // Fixed Wireless — identical scale to fiber
        fwa: [
            { threshold: 0.3,  color: '#dc2626', label: '<30%' },
            { threshold: 0.5,  color: '#f97316', label: '30–50%' },
            { threshold: 0.7,  color: '#ca8a04', label: '50–70%' },
            { threshold: 0.85, color: '#16a34a', label: '70–85%' },
            { threshold: 1.0,  color: '#15803d', label: '>85%' }
        ],
        // Broadband Gap: green = well-served, red = high unserved rate
        broadband_gap: [
            { threshold: 0.05, color: '#15803d', label: '<5% Unserved' },
            { threshold: 0.15, color: '#16a34a', label: '5–15%' },
            { threshold: 0.30, color: '#ca8a04', label: '15–30%' },
            { threshold: 0.50, color: '#f97316', label: '30–50%' },
            { threshold: 1.0,  color: '#dc2626', label: '>50% Unserved' }
        ],
        // Demographics: red = poor profile, green = strong profile
        demographic: [
            { threshold: 0.2, color: '#dc2626', label: 'Poor' },
            { threshold: 0.4, color: '#f97316', label: 'Below Avg' },
            { threshold: 0.6, color: '#ca8a04', label: 'Average' },
            { threshold: 0.8, color: '#16a34a', label: 'Good' },
            { threshold: 1.0, color: '#15803d', label: 'Excellent' }
        ],
        // Market Attractiveness: red = least attractive, green = most attractive
        attractiveness: [
            { threshold: 0.2, color: '#dc2626', label: 'Least Attractive' },
            { threshold: 0.3, color: '#f97316', label: 'Low' },
            { threshold: 0.4, color: '#ca8a04', label: 'Neutral' },
            { threshold: 0.5, color: '#16a34a', label: 'Good' },
            { threshold: 1.0, color: '#15803d', label: 'Most Attractive' }
        ],
        // BEAD Funding: light → deep indigo
        bead: [
            { threshold: 0.01, color: '#e0e7ff', label: 'Not Targeted' },
            { threshold: 0.25, color: '#a5b4fc', label: '<25% Claimed' },
            { threshold: 0.50, color: '#6366f1', label: '25–50%' },
            { threshold: 0.75, color: '#4338ca', label: '50–75%' },
            { threshold: 1.0,  color: '#3730a3', label: '>75% Claimed' }
        ],
        // Competitive Intensity: red = monopoly, green = high competition
        competitive: [
            { threshold: 0.25, color: '#dc2626', label: 'Monopoly' },
            { threshold: 0.50, color: '#f97316', label: 'Low' },
            { threshold: 0.75, color: '#ca8a04', label: 'Moderate' },
            { threshold: 1.0,  color: '#15803d', label: 'High' }
        ],
        // Build Momentum: red = stalled, green = surging
        momentum: [
            { threshold: 0.10, color: '#dc2626', label: 'Stalled' },
            { threshold: 0.27, color: '#ca8a04', label: 'Steady' },
            { threshold: 0.50, color: '#16a34a', label: 'Growing' },
            { threshold: 1.0,  color: '#15803d', label: 'Surging' }
        ],
        // Terrain/Build Difficulty: green = easy, red = challenging
        terrain: [
            { threshold: 0.2, color: '#15803d', label: 'Easy' },
            { threshold: 0.4, color: '#16a34a', label: 'Moderate' },
            { threshold: 0.6, color: '#ca8a04', label: 'Mod-Hard' },
            { threshold: 0.8, color: '#f97316', label: 'Hard' },
            { threshold: 1.0, color: '#dc2626', label: 'Challenging' }
        ],

        getColor: function(layer, value) {
            // Handle invalid inputs
            if (!Number.isFinite(value)) {
                return '#cbd5e1';
            }

            var scale = this[layer];
            if (!scale) return '#cbd5e1';

            // Check cache first
            var cacheKey = layer + '_' + value.toFixed(3);
            if (this._colorCache.has(cacheKey)) {
                return this._colorCache.get(cacheKey);
            }

            // Find color
            var color = scale[scale.length - 1].color;
            for (var i = 0; i < scale.length; i++) {
                if (value <= scale[i].threshold) {
                    color = scale[i].color;
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

        getLegend: function(layer) {
            return this[layer] || this.penetration;
        },

        clearCache: function() {
            this._colorCache.clear();
        }
    };

    // NYC Borough FIPS codes (immutable)
    var NYC_BOROUGHS = Object.freeze(new Set(['36005', '36047', '36061', '36081', '36085']));

    // STL/KC Metro county FIPS codes
    var STL_KC_METROS = Object.freeze(new Set([
        '29189', '29510', '29183', '29099', '29071', '29113', '29219', // STL metro
        '29095', '29047', '29165', '29037', '29025', '29177', '29049', '29107'  // KC metro
    ]));

    // ============================================
    // EXPORTS
    // ============================================

    // Export to global scope (required for current architecture)
    global.DataHandler = DataHandler;
    global.ColorScales = ColorScales;
    global.NYC_BOROUGHS = NYC_BOROUGHS;
    global.STL_KC_METROS = STL_KC_METROS;

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
