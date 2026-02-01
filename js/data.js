// Data handling module

const DataHandler = {
    // NY county data
    unifiedData: null,
    tigerGeoJSON: null,
    // US state data
    stateData: null,
    usGeoJSON: null,

    async loadData() {
        try {
            // Load all data in parallel
            const [unifiedResponse, tigerResponse, stateResponse, usGeoResponse] = await Promise.all([
                fetch('data/ny-unified-data.json'),
                fetch('data/ny_counties_tiger.geojson'),
                fetch('data/fiber-data.json'),
                fetch('data/us-states.json')
            ]);

            this.unifiedData = await unifiedResponse.json();
            this.tigerGeoJSON = await tigerResponse.json();
            this.stateData = await stateResponse.json();
            this.usGeoJSON = await usGeoResponse.json();

            return true;
        } catch (error) {
            console.error('Error loading data:', error);
            return false;
        }
    },

    // NY county methods
    getCountyData(fips) {
        return this.unifiedData ? this.unifiedData[fips] : null;
    },

    getAllCounties() {
        return this.unifiedData ? Object.values(this.unifiedData) : [];
    },

    getGeoJSON() {
        return this.tigerGeoJSON;
    },

    // US state methods
    getStateData(stateCode) {
        return this.stateData ? this.stateData[stateCode] : null;
    },

    getAllStates() {
        return this.stateData ? Object.values(this.stateData) : [];
    },

    getUSGeoJSON() {
        return this.usGeoJSON;
    },

    formatNumber(num) {
        if (num === null || num === undefined) return 'N/A';
        return num.toLocaleString('en-US');
    },

    formatCurrency(num) {
        if (num === null || num === undefined) return 'N/A';
        return '$' + num.toLocaleString('en-US');
    },

    formatPercent(num, decimals = 1) {
        if (num === null || num === undefined) return 'N/A';
        return (num * 100).toFixed(decimals) + '%';
    },

    formatPercentDirect(num, decimals = 1) {
        if (num === null || num === undefined) return 'N/A';
        return num.toFixed(decimals) + '%';
    }
};

// Color scales for different layers (lighter pastel colors)
const ColorScales = {
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
        const scale = this[layer];
        if (!scale) return '#cbd5e1';

        for (const item of scale) {
            if (value <= item.threshold) {
                return item.color;
            }
        }
        return scale[scale.length - 1].color;
    },

    getLegend(layer) {
        return this[layer] || this.penetration;
    }
};

// NYC Borough FIPS codes
const NYC_BOROUGHS = new Set(['36005', '36047', '36061', '36081', '36085']);
