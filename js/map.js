// Map rendering module
// Wrapped in IIFE to avoid global pollution

(function(global) {
    'use strict';

    // ============================================
    // HELPER FUNCTIONS
    // ============================================

    function formatPassings(n) {
        if (!n || n <= 0) return '—';
        if (n >= 1000000) return (n / 1000000).toFixed(1).replace(/\.0$/, '') + 'M';
        if (n >= 10000)   return Math.round(n / 1000) + 'k';
        if (n >= 1000)    return (n / 1000).toFixed(1).replace(/\.0$/, '') + 'k';
        return n.toLocaleString();
    }

    function setTextContent(selector, text) {
        var el = document.querySelector(selector);
        if (el) {
            el.textContent = DataHandler.sanitizeString(text);
        }
    }

    function setTextById(id, text) {
        var el = document.getElementById(id);
        if (el) {
            el.textContent = DataHandler.sanitizeString(text);
        }
    }

    // Allowlist of safe DOM properties for createElement
    var SAFE_ELEMENT_PROPS = {
        'className': true, 'id': true, 'tabindex': true, 'tabIndex': true,
        'role': true, 'type': true, 'htmlFor': true, 'href': true, 'src': true,
        'alt': true, 'title': true, 'placeholder': true, 'value': true,
        'disabled': true, 'checked': true, 'name': true, 'rel': true
    };

    function createElement(tag, attrs, textContent) {
        var el = document.createElement(tag);
        if (attrs) {
            var keys = Object.keys(attrs);
            for (var i = 0; i < keys.length; i++) {
                var key = keys[i];
                var value = attrs[key];
                if (key === 'className') {
                    el.className = value;
                } else if (key === 'style') {
                    el.style.cssText = value;
                } else if (key.startsWith('data-') || key.startsWith('aria-')) {
                    el.setAttribute(key, value);
                } else if (SAFE_ELEMENT_PROPS[key]) {
                    el.setAttribute(key, value);
                }
            }
        }
        if (textContent !== undefined) {
            el.textContent = String(textContent != null ? textContent : '');
        }
        return el;
    }

    // ============================================
    // FEATURED STATES CONFIG
    // ============================================

    var FEATURED_STATES = {
        AL: { label: 'Alabama',         fipsPrefix: '01' },
        AK: { label: 'Alaska',          fipsPrefix: '02' },
        AZ: { label: 'Arizona',         fipsPrefix: '04' },
        AR: { label: 'Arkansas',        fipsPrefix: '05' },
        CA: { label: 'California',      fipsPrefix: '06' },
        CO: { label: 'Colorado',        fipsPrefix: '08' },
        CT: { label: 'Connecticut',     fipsPrefix: '09' },
        DE: { label: 'Delaware',        fipsPrefix: '10' },
        DC: { label: 'Washington DC',   fipsPrefix: '11' },
        FL: { label: 'Florida',         fipsPrefix: '12' },
        GA: { label: 'Georgia',         fipsPrefix: '13' },
        HI: { label: 'Hawaii',          fipsPrefix: '15' },
        ID: { label: 'Idaho',           fipsPrefix: '16' },
        IL: { label: 'Illinois',        fipsPrefix: '17' },
        IN: { label: 'Indiana',         fipsPrefix: '18' },
        IA: { label: 'Iowa',            fipsPrefix: '19' },
        KS: { label: 'Kansas',          fipsPrefix: '20' },
        KY: { label: 'Kentucky',        fipsPrefix: '21' },
        LA: { label: 'Louisiana',       fipsPrefix: '22' },
        ME: { label: 'Maine',           fipsPrefix: '23' },
        MD: { label: 'Maryland',        fipsPrefix: '24' },
        MA: { label: 'Massachusetts',   fipsPrefix: '25' },
        MI: { label: 'Michigan',        fipsPrefix: '26' },
        MN: { label: 'Minnesota',       fipsPrefix: '27' },
        MS: { label: 'Mississippi',     fipsPrefix: '28' },
        MO: { label: 'Missouri',        fipsPrefix: '29' },
        MT: { label: 'Montana',         fipsPrefix: '30' },
        NE: { label: 'Nebraska',        fipsPrefix: '31' },
        NV: { label: 'Nevada',          fipsPrefix: '32' },
        NH: { label: 'New Hampshire',   fipsPrefix: '33' },
        NJ: { label: 'New Jersey',      fipsPrefix: '34' },
        NM: { label: 'New Mexico',      fipsPrefix: '35' },
        NY: { label: 'New York',        fipsPrefix: '36' },
        NC: { label: 'North Carolina',  fipsPrefix: '37' },
        ND: { label: 'North Dakota',    fipsPrefix: '38' },
        OH: { label: 'Ohio',            fipsPrefix: '39' },
        OK: { label: 'Oklahoma',        fipsPrefix: '40' },
        OR: { label: 'Oregon',          fipsPrefix: '41' },
        PA: { label: 'Pennsylvania',    fipsPrefix: '42' },
        RI: { label: 'Rhode Island',    fipsPrefix: '44' },
        SC: { label: 'South Carolina',  fipsPrefix: '45' },
        SD: { label: 'South Dakota',    fipsPrefix: '46' },
        TN: { label: 'Tennessee',       fipsPrefix: '47' },
        TX: { label: 'Texas',           fipsPrefix: '48' },
        UT: { label: 'Utah',            fipsPrefix: '49' },
        VT: { label: 'Vermont',         fipsPrefix: '50' },
        VA: { label: 'Virginia',        fipsPrefix: '51' },
        WA: { label: 'Washington',      fipsPrefix: '53' },
        WV: { label: 'West Virginia',   fipsPrefix: '54' },
        WI: { label: 'Wisconsin',       fipsPrefix: '55' },
        WY: { label: 'Wyoming',         fipsPrefix: '56' }
    };

    // Zoom level at which county view activates (state view below this)
    var COUNTY_ZOOM_THRESHOLD = 6;

    // ============================================
    // MAP RENDERER  (Leaflet-based, zoom-driven)
    // ============================================

    var MapRenderer = {
        _map: null,
        _stateLayer: null,
        _countyLayer: null,
        _countyLayerMap: {},   // fips → Leaflet layer
        _countyFipsList: [],   // ordered FIPS for keyboard nav
        _focusedFips: null,
        _inCountyView: false,
        currentState: 'MO',
        currentLayer: 'penetration',
        currentMode: 'market',       // 'market' | 'provider'
        currentSubview: 'individual', // 'individual' | 'competition'
        currentProvider: null,     // canonical provider name when in provider mode
        currentTech: 'fiber',      // 'fiber' | 'cable' | 'dsl' | 'all'
        competitionProviders: [],  // up to 5 canonical names for competition view
        filters: {
            minPop: 0,
            minDensity: 0,
        },
        // Deep Dive mode
        _deepDiveActive: false,
        _deepDiveTimer: null,
        _deepDiveLayers: ['penetration', 'cable', 'fwa', 'demographic', 'attractiveness', 'competitive', 'momentum', 'terrain'],
        _deepDiveIndex: 0,

        async init(containerId) {
            var container = document.getElementById(containerId);
            if (!container) {
                console.error('Map container not found:', containerId);
                return false;
            }

            this._map = L.map(containerId, {
                center: [39.5, -98.35],
                zoom: 4,
                zoomControl: false,
                attributionControl: true,
            });

            L.control.zoom({ position: 'topright' }).addTo(this._map);

            // Base tiles without labels — data fills render on top of this
            L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png', {
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
                subdomains: 'abcd',
                maxZoom: 19,
            }).addTo(this._map);

            // Labels-only tile layer rendered above all data fills
            var labelsPane = this._map.createPane('labels');
            labelsPane.style.zIndex = 650;
            labelsPane.style.pointerEvents = 'none';
            L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}{r}.png', {
                subdomains: 'abcd',
                maxZoom: 19,
                pane: 'labels',
            }).addTo(this._map);

            // Render state layer (always present)
            this._renderStateLayer();

            // Pre-load all county layers (hidden until zoom threshold)
            await this._renderAllCountyLayers();

            // Zoom-driven view switching
            var self = this;
            this._map.on('zoomend', function() {
                self._onZoomChanged();
            });

            // Set initial state based on starting zoom
            this._onZoomChanged();
            this._setupKeyboardNavigation();

            // Redraw map when container resizes (orientation change, window resize)
            var self = this;
            window.addEventListener('resize', function() {
                self._map.invalidateSize();
            });

            // Force size recalc after CSS animations/layout settle on mobile
            setTimeout(function() { self._map.invalidateSize(); }, 300);

            // Show controls and table immediately
            var controlsPanel = document.querySelector('.controls-panel');
            var tableSection = document.querySelector('.table-section');
            if (controlsPanel) controlsPanel.style.display = 'flex';
            if (tableSection) tableSection.style.display = 'block';

            return true;
        },

        // ===== ZOOM-DRIVEN VIEW SWITCHING =====

        _onZoomChanged: function() {
            // Always show county view — state-level view removed
            if (this._inCountyView) return;
            this._inCountyView = true;

            if (this._countyLayer && !this._map.hasLayer(this._countyLayer)) {
                this._countyLayer.addTo(this._map);
            }
            this.updateLegend();
        },

        // ===== STATE LAYER (always loaded) =====

        _renderStateLayer: function() {
            if (this._stateLayer) { this._map.removeLayer(this._stateLayer); this._stateLayer = null; }

            var geojson = DataHandler.getUSGeoJSON();
            if (!geojson) { console.error('US GeoJSON not loaded'); return; }

            var geoData = geojson.type === 'Topology'
                ? topojson.feature(geojson, geojson.objects.states)
                : geojson;

            this._stateLayer = L.geoJSON(geoData, {
                style: function() {
                    return {
                        fillOpacity: 0,
                        color: 'rgba(255,255,255,0.12)',
                        weight: 0.8,
                        opacity: 1
                    };
                },
                interactive: false
            }).addTo(this._map);

            setTextById('map-title', 'United States — County-Level Detail');
        },

        // ===== COUNTY LAYERS (all featured states, zoom-toggled) =====

        _renderAllCountyLayers: async function() {
            if (this._countyLayer) { this._map.removeLayer(this._countyLayer); this._countyLayer = null; }
            this._countyLayerMap = {};
            this._countyFipsList = [];

            // Lazy-load all state county data concurrently before rendering
            var stateCodes = Object.keys(FEATURED_STATES);
            await DataHandler.loadStatesData(stateCodes);

            // Merge GeoJSON features from all featured states
            var allFeatures = [];
            for (var i = 0; i < stateCodes.length; i++) {
                var sc = stateCodes[i];
                var config = FEATURED_STATES[sc];
                var geojson = sc === 'NY'
                    ? DataHandler.getGeoJSON()
                    : DataHandler.extractStateGeoJSON(config.fipsPrefix);
                if (!geojson) continue;
                var features = geojson.features || [];
                allFeatures = allFeatures.concat(features);
            }

            if (allFeatures.length === 0) return;

            var self = this;
            var combinedGeoJSON = { type: 'FeatureCollection', features: allFeatures };

            this._countyLayer = L.geoJSON(combinedGeoJSON, {
                style: function(feature) {
                    return self._countyStyle(feature, false, false);
                },
                onEachFeature: function(feature, layer) {
                    var fips = self._getFips(feature);
                    if (!fips) return;
                    self._countyLayerMap[fips] = layer;
                    self._countyFipsList.push(fips);

                    layer.on({
                        mouseover: function(e) {
                            if (!InfoPanel.pinnedCounty) {
                                if (MapRenderer.currentMode === 'provider') {
                                    InfoPanel.showProviderInfo(fips);
                                } else {
                                    InfoPanel.showCountyInfo(fips);
                                }
                            }
                            if (InfoPanel.pinnedCounty !== fips) {
                                e.target.setStyle(self._countyStyle(feature, true, false));
                                e.target.bringToFront();
                            }
                        },
                        mouseout: function(e) {
                            if (!InfoPanel.pinnedCounty) InfoPanel.hideInfo();
                            if (InfoPanel.pinnedCounty !== fips) {
                                e.target.setStyle(self._countyStyle(feature, false, false));
                            }
                        },
                        click: function() {
                            self._handleCountyClick(fips, layer, feature);
                        }
                    });
                }
            });
            // Note: NOT added to map yet — zoom controls visibility
        },

        getStateCode: function(feature) {
            if (feature.properties && feature.properties.STUSPS) return feature.properties.STUSPS;
            if (feature.properties && feature.properties.postal) return feature.properties.postal;
            var fipsToState = {
                '36': 'NY', '06': 'CA', '48': 'TX', '12': 'FL', '17': 'IL',
                '42': 'PA', '39': 'OH', '13': 'GA', '37': 'NC', '26': 'MI',
                '34': 'NJ', '51': 'VA', '53': 'WA', '04': 'AZ', '25': 'MA',
                '47': 'TN', '18': 'IN', '29': 'MO', '24': 'MD', '55': 'WI',
                '08': 'CO', '27': 'MN', '45': 'SC', '01': 'AL', '22': 'LA',
                '21': 'KY', '41': 'OR', '40': 'OK', '09': 'CT', '19': 'IA',
                '28': 'MS', '05': 'AR', '20': 'KS', '32': 'NV', '35': 'NM',
                '31': 'NE', '54': 'WV', '16': 'ID', '15': 'HI', '33': 'NH',
                '23': 'ME', '30': 'MT', '44': 'RI', '10': 'DE', '46': 'SD',
                '38': 'ND', '02': 'AK', '11': 'DC', '50': 'VT', '56': 'WY',
                '49': 'UT', '72': 'PR'
            };
            var id = feature.id ? String(feature.id).padStart(2, '0') : null;
            return id ? (fipsToState[id] || null) : null;
        },

        _getFips: function(feature) {
            var fips = feature.id
                || (feature.properties && (feature.properties.GEOID || feature.properties.geoid));
            return fips ? String(fips).padStart(5, '0') : null;
        },

        _countyStyle: function(feature, hovered, pinned) {
            var fips = this._getFips(feature);
            var data = DataHandler.getCountyData(fips);
            var color = this._countyColor(data);
            var filtered = this.isFiltered(data);

            return {
                fillColor: color,
                fillOpacity: filtered ? 0.1 : pinned ? 0.85 : hovered ? 0.8 : 0.62,
                color: pinned ? '#e0e7ff' : hovered ? '#94a3b8' : 'rgba(0,0,0,0.35)',
                weight: pinned ? 2.5 : hovered ? 1.5 : 0.5,
                opacity: 1,
            };
        },

        // Fixed palette for competition view — up to 5 providers
        COMPETITION_PALETTE: ['#60a5fa', '#fb923c', '#34d399', '#f472b6', '#a78bfa'],
        COMPETITION_OVERLAP_COLOR: '#facc15',  // bright yellow = overlap

        // Minimum share of county fiber passings for a provider to count as "present"
        COMPETITION_MIN_SHARE: 0.10,

        _competitionColor: function(data) {
            var selected = this.competitionProviders;
            if (!selected.length || !data) return '#1e293b';
            var totalFiber = data.fiber_served || 0;
            var minPassings = totalFiber > 0
                ? Math.max(1, Math.round(totalFiber * this.COMPETITION_MIN_SHARE))
                : 1;
            var present = [];
            for (var i = 0; i < selected.length; i++) {
                var passings = ProviderIndex.getPassings(data, selected[i], 'fiber');
                if (passings >= minPassings) {
                    present.push(i);
                }
            }
            if (present.length === 0) return '#1e293b';
            if (present.length === 1) return this.COMPETITION_PALETTE[present[0]];
            return this.COMPETITION_OVERLAP_COLOR;
        },

        _countyColor: function(data) {
            if (!data) return '#1e293b';

            // Provider mode
            if (this.currentMode === 'provider') {
                if (this.currentSubview === 'competition') {
                    return this._competitionColor(data);
                }
                // Individual provider view — color by depth
                if (!this.currentProvider) return '#1e293b';
                var passings = ProviderIndex.getPassings(data, this.currentProvider, this.currentTech);
                var bsls = data.total_bsls || 1;
                var ratio = passings > 0 ? Math.min(1, passings / bsls) : 0;
                var colorValue = passings > 0 ? Math.max(0.002, ratio) : 0;
                return ColorScales.getColor('provider', colorValue);
            }

            var value;
            switch (this.currentLayer) {
                case 'penetration':   value = data.fiber_penetration; break;
                case 'cable':         value = data.cable_coverage_pct; break;
                case 'fwa':           value = data.fwa_coverage_pct; break;
                case 'broadband_gap': value = data.broadband_gap_pct; break;
                case 'demographic':   value = data.demo_score; break;
                case 'attractiveness':value = data.attractiveness_index; break;
                case 'bead':
                    // Color by $/eligible location — comparable cross-county metric
                    // Typical range: $3K (dense/urban) to $40K+ (very rural)
                    if (data.bead_dollars_per_eligible_loc != null && data.bead_dollars_per_eligible_loc > 0) {
                        value = Math.min(1, data.bead_dollars_per_eligible_loc / 40000);
                    } else if (data.bead_eligible_locations === 0) {
                        value = 0; // fully served — not eligible
                    } else {
                        value = null;
                    }
                    break;
                case 'competitive':   value = data.competitive_intensity != null ? data.competitive_intensity / 3 : null; break;
                case 'momentum':
                    if (data.fiber_growth_pct != null) {
                        value = Math.min(1, Math.max(0, data.fiber_growth_pct / 30));
                    } else {
                        // Estimated proxy: fiber provider count as build-activity stand-in
                        // (0=Stalled, 1=Steady, 2=Growing, 3+=Surging). BDC delta pending.
                        var ci = data.competitive_intensity != null ? data.competitive_intensity : 0;
                        value = ci === 0 ? 0.05 : ci === 1 ? 0.22 : ci === 2 ? 0.38 : 0.65;
                    }
                    break;
                case 'terrain':       value = data.terrain_roughness; break;
                default:              value = data.fiber_penetration;
            }
            if (value == null || !Number.isFinite(value)) return '#1e293b';
            return ColorScales.getColor(this.currentLayer, value);
        },

        _handleCountyClick: function(fips, layer, feature) {
            if (InfoPanel.pinnedCounty === fips) {
                InfoPanel.unpinCounty();
            } else {
                if (InfoPanel.pinnedCounty) {
                    var prev = this._countyLayerMap[InfoPanel.pinnedCounty];
                    if (prev) prev.setStyle(this._countyStyle(prev.feature, false, false));
                }
                InfoPanel.pinCounty(fips);
                layer.setStyle(this._countyStyle(feature, false, true));
                layer.bringToFront();
            }
        },

        _refreshCountyStyle: function(fips, hovered) {
            var layer = this._countyLayerMap[fips];
            if (!layer) return;
            layer.setStyle(this._countyStyle(layer.feature, hovered || false, InfoPanel.pinnedCounty === fips));
        },

        updatePinStyles: function(pinnedFips) {
            var self = this;
            if (!this._countyLayer) return;
            this._countyLayer.eachLayer(function(layer) {
                var fips = self._getFips(layer.feature);
                layer.setStyle(self._countyStyle(layer.feature, false, fips === pinnedFips));
                if (fips === pinnedFips) layer.bringToFront();
            });
        },

        _setupKeyboardNavigation: function() {
            var self = this;
            document.addEventListener('keydown', function(e) {
                if (e.key === 'Escape' && InfoPanel.pinnedCounty) {
                    InfoPanel.unpinCounty();
                    return;
                }
                if (self._inCountyView && self._countyFipsList.length > 0) {
                    var idx = self._countyFipsList.indexOf(self._focusedFips);
                    if (e.key === 'ArrowRight' || e.key === 'ArrowDown') {
                        e.preventDefault();
                        self._focusCountyAt(Math.min(idx + 1, self._countyFipsList.length - 1));
                    } else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') {
                        e.preventDefault();
                        self._focusCountyAt(Math.max(0, idx - 1));
                    } else if ((e.key === 'Enter' || e.key === ' ') && self._focusedFips) {
                        e.preventDefault();
                        var layer = self._countyLayerMap[self._focusedFips];
                        if (layer) self._handleCountyClick(self._focusedFips, layer, layer.feature);
                    }
                }
            });
        },

        _focusCountyAt: function(idx) {
            var fips = this._countyFipsList[idx];
            if (!fips) return;
            this._focusedFips = fips;
            if (!InfoPanel.pinnedCounty) InfoPanel.showCountyInfo(fips);
            var layer = this._countyLayerMap[fips];
            if (layer) {
                this._map.panTo(layer.getBounds().getCenter(), { animate: true, duration: 0.3 });
                this._refreshCountyStyle(fips, true);
            }
        },

        // ===== SHARED METHODS =====

        setLayer: function(layer) {
            this.currentLayer = layer;
            if (this._countyLayer) {
                var self = this;
                this._countyLayer.eachLayer(function(l) {
                    l.setStyle(self._countyStyle(l.feature, false, InfoPanel.pinnedCounty === self._getFips(l.feature)));
                });
                this.updateLegend();
            }
        },

        setMode: function(mode) {
            this.currentMode = mode;
            this.currentSubview = 'individual';
            this.competitionProviders = [];
            this.stopDeepDive();

            // Force county layer on when entering provider mode
            if (mode === 'provider') {
                this._inCountyView = false; // reset so _onZoomChanged triggers
                this._onZoomChanged();
            } else {
                // Return to normal zoom-based logic
                this._inCountyView = false;
                this._onZoomChanged();
            }

            var self = this;
            if (this._countyLayer) {
                this._countyLayer.eachLayer(function(l) {
                    l.setStyle(self._countyStyle(l.feature, false, InfoPanel.pinnedCounty === self._getFips(l.feature)));
                });
            }
            if (mode === 'provider') {
                this._buildLegend('provider');
            } else {
                this.updateLegend();
            }
        },

        setSubview: function(subview) {
            this.currentSubview = subview;
            ColorScales.clearCache();
            var self = this;
            if (this._countyLayer) {
                this._countyLayer.eachLayer(function(l) {
                    l.setStyle(self._countyStyle(l.feature, false, InfoPanel.pinnedCounty === self._getFips(l.feature)));
                });
            }
            if (subview === 'competition') {
                this._buildCompetitionLegend();
            } else {
                this._buildLegend('provider');
            }
            this._updateProviderTitle();
        },

        setCompetitionProviders: function(providers) {
            this.competitionProviders = providers.slice(0, 5);
            ColorScales.clearCache();
            var self = this;
            if (this._countyLayer) {
                this._countyLayer.eachLayer(function(l) {
                    l.setStyle(self._countyStyle(l.feature, false, InfoPanel.pinnedCounty === self._getFips(l.feature)));
                });
            }
            this._buildCompetitionLegend();
            this._updateProviderTitle();
        },

        _buildCompetitionLegend: function() {
            var container = document.getElementById('legend-container');
            if (!container) return;
            container.textContent = '';
            var legend = createElement('div', { className: 'legend' });
            var selected = this.competitionProviders;

            if (!selected.length) {
                var hint = createElement('div', { className: 'legend-item' }, 'Select providers above');
                legend.appendChild(hint);
            } else {
                var self = this;
                selected.forEach(function(name, i) {
                    var el = createElement('div', { className: 'legend-item' });
                    var swatch = createElement('div', { className: 'legend-color' });
                    swatch.style.background = self.COMPETITION_PALETTE[i];
                    el.appendChild(swatch);
                    el.appendChild(createElement('span', {}, name + ' only'));
                    legend.appendChild(el);
                });
                if (selected.length > 1) {
                    var overlapEl = createElement('div', { className: 'legend-item' });
                    var overlapSwatch = createElement('div', { className: 'legend-color' });
                    overlapSwatch.style.background = this.COMPETITION_OVERLAP_COLOR;
                    overlapEl.appendChild(overlapSwatch);
                    overlapEl.appendChild(createElement('span', {}, selected.length === 2 ? 'Both overlap' : 'Any overlap'));
                    legend.appendChild(overlapEl);
                }
                var noneEl = createElement('div', { className: 'legend-item' });
                var noneSwatch = createElement('div', { className: 'legend-color' });
                noneSwatch.style.background = '#1e293b';
                noneSwatch.style.border = '1px solid rgba(255,255,255,0.15)';
                noneEl.appendChild(noneSwatch);
                noneEl.appendChild(createElement('span', {}, 'Not present'));
                legend.appendChild(noneEl);
            }
            container.appendChild(legend);
        },

        setProvider: function(canonicalName) {
            this.currentProvider = canonicalName;
            ColorScales.clearCache();
            var self = this;
            if (this._countyLayer) {
                this._countyLayer.eachLayer(function(l) {
                    l.setStyle(self._countyStyle(l.feature, false, InfoPanel.pinnedCounty === self._getFips(l.feature)));
                });
            }
            this._buildLegend('provider');
            this._updateProviderTitle();
        },

        setTech: function(techType) {
            this.currentTech = techType || 'fiber';
            ColorScales.clearCache();
            var self = this;
            if (this._countyLayer) {
                this._countyLayer.eachLayer(function(l) {
                    l.setStyle(self._countyStyle(l.feature, false, InfoPanel.pinnedCounty === self._getFips(l.feature)));
                });
            }
            // Refresh pinned panel if open
            if (InfoPanel.pinnedCounty) {
                InfoPanel.showProviderInfo(InfoPanel.pinnedCounty);
            }
            this._updateProviderTitle();
        },

        _updateProviderTitle: function() {
            var label;
            if (this.currentSubview === 'competition') {
                var sel = this.competitionProviders;
                label = sel.length > 1
                    ? sel.join(' vs ') + ' — Overlap'
                    : sel.length === 1
                        ? sel[0] + ' — select another to compare'
                        : 'Select providers to compare';
            } else {
                var techLabel = { fiber: 'Fiber', cable: 'Cable', dsl: 'DSL/Copper', all: 'All Tech' };
                label = this.currentProvider
                    ? this.currentProvider + ' — ' + (techLabel[this.currentTech] || 'Fiber') + ' Footprint'
                    : 'Select a provider';
            }
            setTextById('map-title', label);
        },

        setFilters: function(filters) {
            this.filters = Object.assign({}, this.filters, filters);
            this.applyFilters();
            TableManager.applyFilters();
        },

        applyFilters: function() {
            if (!this._countyLayer) return;
            var self = this;
            this._countyLayer.eachLayer(function(l) {
                l.setStyle(self._countyStyle(l.feature, false, InfoPanel.pinnedCounty === self._getFips(l.feature)));
            });
        },

        isFiltered: function(county) {
            if (!county) return true;
            if (this.filters.minPop > 0 && (county.population_2023 == null || county.population_2023 < this.filters.minPop)) return true;
            if (this.filters.minDensity > 0 && (county.housing_density == null || county.housing_density < this.filters.minDensity)) return true;
            return false;
        },

        _buildLegend: function(layerName, extraItems) {
            var container = document.getElementById('legend-container');
            if (!container) return;
            container.textContent = '';
            var legend = createElement('div', { className: 'legend' });
            var items = ColorScales.getLegend(layerName);
            items.forEach(function(item) {
                var el = createElement('div', { className: 'legend-item' });
                var swatch = createElement('div', { className: 'legend-color' });
                swatch.style.background = item.color;
                el.appendChild(swatch);
                el.appendChild(createElement('span', {}, item.label));
                legend.appendChild(el);
            });
            if (extraItems) {
                extraItems.forEach(function(item) {
                    var el = createElement('div', { className: 'legend-item', style: 'margin-left:1rem;' });
                    var swatch = createElement('div', { className: 'legend-color' });
                    swatch.style.background = item.color;
                    el.appendChild(swatch);
                    el.appendChild(createElement('span', {}, item.label));
                    legend.appendChild(el);
                });
            }
            // Momentum disclaimer when real BDC delta data is unavailable
            if (layerName === 'momentum') {
                var note = createElement('div', { className: 'legend-note' }, '* Estimated from fiber provider count — BDC delta pending');
                container.appendChild(note);
            }
            container.appendChild(legend);
        },

        updateLegendForUS: function() {
            this._buildLegend('penetration');
        },

        updateLegend: function() {
            if (this.currentMode === 'provider' && this.currentSubview === 'competition') {
                this._buildCompetitionLegend();
            } else {
                this._buildLegend(this.currentMode === 'provider' ? 'provider' : this.currentLayer);
            }
        },

        highlightCounty: function(fips) {
            if (!this._countyLayer) return;
            var self = this;
            this._countyLayer.eachLayer(function(l) {
                var f = self._getFips(l.feature);
                l.setStyle({ fillOpacity: f === fips ? 0.85 : 0.08 });
            });
        },

        clearHighlight: function() {
            if (!this._countyLayer) return;
            var self = this;
            this._countyLayer.eachLayer(function(l) {
                l.setStyle(self._countyStyle(l.feature, false, InfoPanel.pinnedCounty === self._getFips(l.feature)));
            });
        },

        // ===== DEEP DIVE MODE =====

        startDeepDive: function() {
            if (this._deepDiveActive) return;
            this._deepDiveActive = true;
            this._deepDiveIndex = 0;
            this._runDeepDiveCycle();
        },

        _runDeepDiveCycle: function() {
            if (!this._deepDiveActive) return;
            var self = this;

            var layer = this._deepDiveLayers[this._deepDiveIndex];
            this.setLayer(layer);

            var toggleBtns = document.querySelectorAll('#layer-toggle .toggle-btn');
            toggleBtns.forEach(function(btn) {
                var isActive = btn.dataset.layer === layer;
                btn.classList.toggle('active', isActive);
                btn.setAttribute('aria-pressed', isActive ? 'true' : 'false');
            });

            this._deepDiveIndex = (this._deepDiveIndex + 1) % this._deepDiveLayers.length;

            this._deepDiveTimer = setTimeout(function() {
                self._runDeepDiveCycle();
            }, 3000);
        },

        stopDeepDive: function() {
            this._deepDiveActive = false;
            if (this._deepDiveTimer) {
                clearTimeout(this._deepDiveTimer);
                this._deepDiveTimer = null;
            }
            var deepDiveBtn = document.getElementById('deep-dive-btn');
            if (deepDiveBtn) {
                deepDiveBtn.classList.remove('active');
                deepDiveBtn.textContent = 'Deep Dive';
            }
        },

        toggleDeepDive: function() {
            if (this._deepDiveActive) {
                this.stopDeepDive();
            } else {
                this.startDeepDive();
                var deepDiveBtn = document.getElementById('deep-dive-btn');
                if (deepDiveBtn) {
                    deepDiveBtn.classList.add('active');
                    deepDiveBtn.textContent = 'Stop Dive';
                }
            }
        }
    };

    // ============================================
    // INFO PANEL
    // ============================================

    var InfoPanel = {
        defaultEl: null,
        countyInfoEl: null,
        stateInfoEl: null,
        providerInfoEl: null,
        pinnedCounty: null,
        _mode: 'market',

        init: function() {
            this.defaultEl = document.querySelector('.default-message');
            this.countyInfoEl = document.querySelector('.county-info');
            this.stateInfoEl = document.querySelector('.state-info');
            this.providerInfoEl = document.querySelector('.provider-info');
        },

        setMode: function(mode) {
            this._mode = mode;
            this.pinnedCounty = null;
            this.hideInfo();
            if (this.defaultEl) {
                this.defaultEl.querySelector('h2').textContent =
                    mode === 'provider' ? 'Hover over a county' : 'Hover over a state';
                this.defaultEl.querySelector('p').textContent =
                    mode === 'provider' ? 'to see provider footprint' : 'to see fiber coverage statistics';
            }
        },

        pinCounty: function(fips) {
            this.pinnedCounty = fips;
            if (this._mode === 'provider') {
                this.showProviderInfo(fips);
            } else {
                this.showCountyInfo(fips);
            }
            var pinIndicator = document.querySelector('.pin-indicator');
            if (pinIndicator) pinIndicator.style.display = 'flex';
            MapRenderer.updatePinStyles(fips);
            this._announceToScreenReader('County details pinned. Press Escape to close.');
        },

        unpinCounty: function() {
            this.pinnedCounty = null;
            var pinIndicator = document.querySelector('.pin-indicator');
            if (pinIndicator) pinIndicator.style.display = 'none';
            MapRenderer.updatePinStyles(null);
            this.hideInfo();
            this._announceToScreenReader('County details closed.');
        },

        _announceToScreenReader: function(message) {
            var announcer = document.getElementById('sr-announcer') || this._createAnnouncer();
            announcer.textContent = message;
        },

        _createAnnouncer: function() {
            var announcer = createElement('div', {
                id: 'sr-announcer',
                className: 'sr-only',
                'aria-live': 'polite',
                'aria-atomic': 'true'
            });
            announcer.style.cssText = 'position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0;';
            document.body.appendChild(announcer);
            return announcer;
        },

        showStateInfo: function(stateCode) {
            var data = DataHandler.getStateData(stateCode);

            setTextContent('.state-name', data ? data.state : stateCode);

            if (data) {
                setTextById('state-housing', DataHandler.formatNumber(data.totalHousingUnits));
                setTextById('state-fiber', DataHandler.formatNumber(data.totalFiberPassings));
                setTextById('state-penetration', data.fiberPenetration.toFixed(1) + '%');

                var operatorsList = document.getElementById('state-operators-list');
                if (operatorsList) {
                    operatorsList.textContent = '';
                    if (data.operators && data.operators.length > 0) {
                        data.operators.slice(0, 6).forEach(function(op) {
                            var li = createElement('li');
                            var nameSpan = createElement('span', { className: 'operator-name' }, op.name);
                            var passingsSpan = createElement('span', { className: 'operator-passings' },
                                DataHandler.formatNumber(op.passings));
                            li.appendChild(nameSpan);
                            li.appendChild(passingsSpan);
                            operatorsList.appendChild(li);
                        });
                    }
                }
            }

            // Hint for featured states — zoom in instead of click
            var clickHint = document.querySelector('.click-hint');
            if (clickHint) {
                if (FEATURED_STATES[stateCode]) {
                    clickHint.style.display = 'block';
                    clickHint.textContent = '';
                    var strong = createElement('strong', {}, 'Zoom in to explore');
                    clickHint.appendChild(strong);
                    clickHint.appendChild(document.createTextNode(
                        ' County-level data available for ' + FEATURED_STATES[stateCode].label
                    ));
                } else {
                    clickHint.style.display = 'none';
                }
            }

            if (this.defaultEl) this.defaultEl.style.display = 'none';
            if (this.countyInfoEl) this.countyInfoEl.style.display = 'none';
            if (this.stateInfoEl) this.stateInfoEl.style.display = 'block';
        },

        showCountyInfo: function(fips) {
            var data = DataHandler.getCountyData(fips);
            if (!data) return;

            setTextContent('.county-name', data.name + ' County');

            // Score bars
            var attrScore = document.getElementById('attr-score');
            var attrValue = document.getElementById('attr-value');
            if (attrScore && attrValue) {
                var attrIndex = Number.isFinite(data.attractiveness_index) ? data.attractiveness_index : 0;
                attrScore.style.width = (attrIndex * 100) + '%';
                attrValue.textContent = attrIndex.toFixed(2);
            }

            var demoScore = document.getElementById('demo-score');
            var demoValue = document.getElementById('demo-value');
            if (demoScore && demoValue) {
                var demoScoreVal = Number.isFinite(data.demo_score) ? data.demo_score : 0;
                demoScore.style.width = (demoScoreVal * 100) + '%';
                demoValue.textContent = demoScoreVal.toFixed(2);
            }

            var penScore = document.getElementById('pen-score');
            var penValue = document.getElementById('pen-value');
            if (penScore && penValue) {
                var penVal = Number.isFinite(data.fiber_penetration) ? data.fiber_penetration : 0;
                penScore.style.width = (penVal * 100) + '%';
                penValue.textContent = (penVal * 100).toFixed(0) + '%';
            }

            // Fiber stats
            setTextById('total-bsls', DataHandler.formatNumber(data.total_bsls));
            setTextById('fiber-served', DataHandler.formatNumber(data.fiber_served));
            setTextById('fiber-unserved', DataHandler.formatNumber(data.fiber_unserved));
            setTextById('penetration-rate', DataHandler.formatPercent(data.fiber_penetration));

            // Demographics
            setTextById('population', DataHandler.formatNumber(data.population_2023));

            var popGrowthEl = document.getElementById('pop-growth');
            if (popGrowthEl) {
                if (Number.isFinite(data.pop_growth_pct)) {
                    popGrowthEl.textContent = (data.pop_growth_pct >= 0 ? '+' : '') + data.pop_growth_pct.toFixed(1) + '%';
                    popGrowthEl.className = 'stat-value ' + (data.pop_growth_pct >= 0 ? 'positive' : 'negative');
                } else {
                    popGrowthEl.textContent = 'N/A';
                    popGrowthEl.className = 'stat-value';
                }
            }

            setTextById('housing-units', DataHandler.formatNumber(data.housing_units));
            setTextById('housing-density', Number.isFinite(data.housing_density) ? DataHandler.formatNumber(data.housing_density) + '/sq mi' : 'N/A');
            setTextById('median-hhi', DataHandler.formatCurrency(data.median_hhi));
            setTextById('median-rent', DataHandler.formatCurrency(data.median_rent));
            setTextById('owner-occ', DataHandler.formatPercentDirect(data.owner_occupied_pct));
            setTextById('wfh-pct', DataHandler.formatPercentDirect(data.wfh_pct));
            setTextById('median-home-value', DataHandler.formatCurrency(data.median_home_value));

            // Operators list
            var operatorsList = document.getElementById('operators-list');
            if (operatorsList) {
                operatorsList.textContent = '';
                if (data.operators && data.operators.length > 0) {
                    var topOperators = data.operators.slice().sort(function(a, b) {
                        var aTotal = (a.fiber_passings || a.passings || 0) + (a.cable_passings || 0) + (a.dsl_passings || 0);
                        var bTotal = (b.fiber_passings || b.passings || 0) + (b.cable_passings || 0) + (b.dsl_passings || 0);
                        return bTotal - aTotal;
                    }).slice(0, 5);
                    topOperators.forEach(function(op) {
                        var fiberP = op.fiber_passings || op.passings || 0;
                        var cableP = op.cable_passings || 0;
                        var dslP   = op.dsl_passings   || 0;
                        var totalP = fiberP + cableP + dslP;
                        var li = createElement('li');
                        li.appendChild(createElement('span', { className: 'operator-name' }, op.name));
                        li.appendChild(createElement('span', { className: 'op-stat' }, formatPassings(fiberP)));
                        li.appendChild(createElement('span', { className: 'op-stat' }, formatPassings(cableP)));
                        li.appendChild(createElement('span', { className: 'op-stat' }, formatPassings(dslP)));
                        li.appendChild(createElement('span', { className: 'op-stat op-stat-total' }, formatPassings(totalP)));
                        operatorsList.appendChild(li);
                    });
                } else {
                    var noOp = createElement('li', { style: 'color: #64748b;' }, 'No operators reported');
                    operatorsList.appendChild(noOp);
                }
            }

            // BEAD Funding section
            var beadSection = document.getElementById('bead-section');
            if (beadSection) {
                if (data.bead_status != null) {
                    beadSection.style.display = '';
                    setTextById('bead-status', data.bead_status || 'N/A');
                    var beadStatusEl = document.getElementById('bead-status');
                    if (beadStatusEl) {
                        var beadClass = (data.bead_status || '').toLowerCase().replace(/\s+/g, '-');
                        beadStatusEl.className = 'stat-value bead-badge bead-' + beadClass;
                    }

                    var hasBeadMetrics = data.bead_eligible_locations != null;
                    var detailIds = ['bead-details-eligible', 'bead-details-implied', 'bead-details-per-loc', 'bead-details-allocation'];
                    detailIds.forEach(function(id) {
                        var el = document.getElementById(id);
                        if (el) el.style.display = hasBeadMetrics ? '' : 'none';
                    });
                    var beadNote = document.getElementById('bead-note');
                    if (beadNote) beadNote.style.display = 'none';

                    if (hasBeadMetrics) {
                        setTextById('bead-eligible-locs', DataHandler.formatNumber(data.bead_eligible_locations));
                        var impliedAward = data.bead_implied_county_award;
                        setTextById('bead-implied-award', impliedAward != null && impliedAward > 0
                            ? '$' + (impliedAward >= 1000000
                                ? (impliedAward / 1000000).toFixed(1) + 'M'
                                : DataHandler.formatNumber(impliedAward))
                            : 'N/A');
                        setTextById('bead-per-loc', data.bead_dollars_per_eligible_loc != null
                            ? '$' + DataHandler.formatNumber(Math.round(data.bead_dollars_per_eligible_loc))
                            : 'N/A');
                        setTextById('bead-state-alloc', data.bead_state_allocation != null
                            ? '$' + (data.bead_state_allocation / 1e9).toFixed(2) + 'B'
                            : 'N/A');
                    }
                } else {
                    beadSection.style.display = 'none';
                }
            }

            // Technology Coverage section
            var techSection = document.getElementById('tech-coverage-section');
            if (techSection) {
                if (data.cable_coverage_pct != null || data.fwa_coverage_pct != null) {
                    techSection.style.display = '';
                    setTextById('cable-coverage', FiberUtils.formatPercent(data.cable_coverage_pct));
                    setTextById('fwa-coverage', FiberUtils.formatPercent(data.fwa_coverage_pct));
                    setTextById('bb-coverage', FiberUtils.formatPercent(data.broadband_coverage_pct));
                    setTextById('bb-gap', FiberUtils.formatPercent(data.broadband_gap_pct));
                } else {
                    techSection.style.display = 'none';
                }
            }

            // Competition & Growth section
            var compSection = document.getElementById('competition-section');
            if (compSection) {
                if (data.competitive_intensity != null) {
                    compSection.style.display = '';
                    setTextById('comp-label', data.competitive_label || 'N/A');
                    setTextById('comp-providers', data.wireline_providers ? data.wireline_providers.length.toString() : 'N/A');
                    setTextById('cable-present', data.cable_present ? 'Yes' : 'No');
                    setTextById('fwa-present', data.fwa_present ? 'Yes' : 'No');
                    setTextById('total-bb-providers', data.total_broadband_providers != null ? data.total_broadband_providers.toString() : 'N/A');
                    var momLabel = TableManager._momentumLabel(data);
                    var isEstimated = data.momentum_class == null;
                    setTextById('momentum-class', momLabel + (isEstimated ? ' *' : ''));
                    var momEl = document.getElementById('momentum-class');
                    if (momEl) {
                        var momClass = momLabel.replace('*', '').trim().toLowerCase();
                        momEl.className = 'stat-value momentum-' + momClass;
                        momEl.title = isEstimated ? 'Estimated from fiber provider count — actual BDC delta pending' : '';
                    }
                } else {
                    compSection.style.display = 'none';
                }
            }

            // Build Environment section
            var buildSection = document.getElementById('build-section');
            if (buildSection) {
                if (data.terrain_roughness != null) {
                    buildSection.style.display = '';
                    setTextById('build-difficulty', data.build_difficulty || 'N/A');
                    setTextById('terrain-roughness', data.terrain_roughness != null ? data.terrain_roughness.toFixed(2) : 'N/A');
                    setTextById('elevation-mean', data.elevation_mean_ft != null ? data.elevation_mean_ft.toFixed(1) : 'N/A');
                    setTextById('cost-tier', data.construction_cost_tier || 'N/A');
                    setTextById('rucc-class', data.rural_class || 'N/A');
                    setTextById('rucc-code', data.rucc_code != null ? 'RUCC ' + data.rucc_code : 'N/A');
                } else {
                    buildSection.style.display = 'none';
                }
            }

            // NPV button — only for MO counties (fips starts with 29)
            var npvBtn = document.getElementById('open-npv-btn');
            if (npvBtn) {
                npvBtn.style.display = (fips && fips.startsWith('29')) ? '' : 'none';
            }

            if (this.defaultEl) this.defaultEl.style.display = 'none';
            if (this.stateInfoEl) this.stateInfoEl.style.display = 'none';
            if (this.countyInfoEl) this.countyInfoEl.style.display = 'block';
        },

        showProviderInfo: function(fips) {
            var data = DataHandler.getCountyData(fips);
            if (!data) return;
            var provider = MapRenderer.currentProvider;
            var tech = MapRenderer.currentTech || 'fiber';
            var bsls = data.total_bsls || 0;

            // Per-tech passings for selected provider
            var fiberP = provider ? ProviderIndex.getPassings(data, provider, 'fiber') : 0;
            var cableP = provider ? ProviderIndex.getPassings(data, provider, 'cable') : 0;
            var dslP   = provider ? ProviderIndex.getPassings(data, provider, 'dsl')   : 0;
            var activeP = tech === 'cable' ? cableP : tech === 'dsl' ? dslP : fiberP;
            var pct = bsls > 0 && activeP > 0 ? (activeP / bsls * 100).toFixed(1) + '%' : '—';

            setTextById('prov-county-name', (data.name || '') + (data.state_code ? ', ' + data.state_code : ''));
            setTextById('prov-panel-title', provider || 'Provider Footprint');
            setTextById('prov-passings', activeP > 0 ? activeP.toLocaleString() : '—');
            setTextById('prov-coverage', pct);
            setTextById('prov-total-bsls', bsls > 0 ? bsls.toLocaleString() : '—');

            // Per-tech breakdown rows
            setTextById('prov-fiber-passings', fiberP > 0 ? fiberP.toLocaleString() : '—');
            setTextById('prov-cable-passings', cableP > 0 ? cableP.toLocaleString() : '—');
            setTextById('prov-dsl-passings',   dslP   > 0 ? dslP.toLocaleString()   : '—');

            // All operators in county across any tech
            var allNames = {};
            (data.operators || []).forEach(function(op) {
                var n = ProviderIndex.resolve(op.name);
                if (n) allNames[n] = true;
            });
            setTextById('prov-competitors', Object.keys(allNames).join(', ') || '—');

            setTextById('prov-population', data.population_2023 ? data.population_2023.toLocaleString() : '—');
            var inc = data.median_hhi;
            setTextById('prov-income', inc ? '$' + Number(inc).toLocaleString() : '—');
            setTextById('prov-fiber-pct', data.fiber_penetration != null
                ? (data.fiber_penetration * 100).toFixed(1) + '%' : '—');
            setTextById('prov-market-type', data.is_metro_county ? 'Metro' : (data.rural_class || 'Rural'));

            if (this.defaultEl) this.defaultEl.style.display = 'none';
            if (this.countyInfoEl) this.countyInfoEl.style.display = 'none';
            if (this.stateInfoEl) this.stateInfoEl.style.display = 'none';
            if (this.providerInfoEl) this.providerInfoEl.style.display = 'block';
        },

        hideInfo: function() {
            if (this.defaultEl) this.defaultEl.style.display = 'block';
            if (this.countyInfoEl) this.countyInfoEl.style.display = 'none';
            if (this.stateInfoEl) this.stateInfoEl.style.display = 'none';
            if (this.providerInfoEl) this.providerInfoEl.style.display = 'none';
        },

        copyCountyData: function() {
            var fips = this.pinnedCounty;
            if (!fips) return false;
            var c = DataHandler.getCountyData(fips);
            if (!c) return false;
            var row = {
                geoid: c.geoid,
                name: c.name,
                state: c.state_code,
                attractiveness_index: c.attractiveness_index,
                demo_score: c.demo_score,
                fiber_penetration_pct: c.fiber_penetration != null ? +(c.fiber_penetration * 100).toFixed(1) : null,
                fiber_served: c.fiber_served,
                fiber_unserved: c.fiber_unserved,
                total_bsls: c.total_bsls,
                population_2023: c.population_2023,
                median_hhi: c.median_hhi,
                housing_density: c.housing_density,
                cable_present: c.cable_present,
                fwa_present: c.fwa_present,
                competitive_intensity: c.competitive_intensity,
                build_difficulty: c.build_difficulty,
                terrain_roughness: c.terrain_roughness,
                rucc_code: c.rucc_code,
                bead_eligible_locations: c.bead_eligible_locations,
                bead_implied_county_award: c.bead_implied_county_award,
                momentum: TableManager._momentumLabel(c),
            };
            var text = JSON.stringify(row, null, 2);
            if (navigator.clipboard && navigator.clipboard.writeText) {
                navigator.clipboard.writeText(text);
                return true;
            }
            return false;
        }
    };

    // ============================================
    // TABLE MANAGER
    // ============================================

    var TableManager = {
        sortColumn: 'attractiveness_index',
        sortDirection: 'desc',
        searchTerm: '',
        _searchDebounceTimer: null,
        _selectedFips: new Set(),

        init: function() {
            this.setupEventListeners();
        },

        renderTable: function() {
            var tbody = document.getElementById('county-table-body');
            if (!tbody) return;

            var counties = DataHandler.getAllCounties();

            var self = this;
            counties.sort(function(a, b) {
                var aVal = a[self.sortColumn];
                var bVal = b[self.sortColumn];

                if (aVal === null || aVal === undefined || (typeof aVal === 'number' && !Number.isFinite(aVal))) {
                    aVal = -Infinity;
                }
                if (bVal === null || bVal === undefined || (typeof bVal === 'number' && !Number.isFinite(bVal))) {
                    bVal = -Infinity;
                }

                if (typeof aVal === 'string') aVal = aVal.toLowerCase();
                if (typeof bVal === 'string') bVal = bVal.toLowerCase();

                if (aVal === bVal) return 0;
                if (self.sortDirection === 'asc') {
                    return aVal > bVal ? 1 : -1;
                } else {
                    return aVal < bVal ? 1 : -1;
                }
            });

            tbody.textContent = '';
            var self2 = self;

            counties.forEach(function(c) {
                var tr = createElement('tr', { 'data-fips': c.geoid });
                if (MapRenderer.isFiltered(c)) {
                    tr.classList.add('filtered-out');
                }

                // Checkbox cell
                var cbTd = createElement('td', { className: 'row-select-cell' });
                var cb = createElement('input', { type: 'checkbox', className: 'row-select-cb', 'data-fips': c.geoid });
                if (self2._selectedFips.has(c.geoid)) cb.checked = true;
                cb.addEventListener('change', function(e) {
                    if (e.target.checked) {
                        self2._selectedFips.add(c.geoid);
                    } else {
                        self2._selectedFips.delete(c.geoid);
                    }
                    self2._updateExportBtn();
                });
                cb.addEventListener('click', function(e) { e.stopPropagation(); });
                cbTd.appendChild(cb);
                tr.appendChild(cbTd);

                tr.appendChild(createElement('td', {}, c.name));

                var attrVal = Number.isFinite(c.attractiveness_index) ? c.attractiveness_index.toFixed(2) : 'N/A';
                tr.appendChild(createElement('td', {}, attrVal));

                var demoVal = Number.isFinite(c.demo_score) ? c.demo_score.toFixed(2) : 'N/A';
                tr.appendChild(createElement('td', {}, demoVal));

                var penVal = Number.isFinite(c.fiber_penetration) ? (c.fiber_penetration * 100).toFixed(1) + '%' : 'N/A';
                tr.appendChild(createElement('td', {}, penVal));

                tr.appendChild(createElement('td', {}, DataHandler.formatNumber(c.fiber_unserved)));
                tr.appendChild(createElement('td', {}, c.median_hhi ? DataHandler.formatCurrency(c.median_hhi) : 'N/A'));
                tr.appendChild(createElement('td', {}, DataHandler.formatNumber(c.housing_density)));
                var momLabel = self._momentumLabel(c);
                var momTd = createElement('td', { className: 'momentum-cell momentum-' + momLabel.replace('*','').toLowerCase() }, momLabel);
                tr.appendChild(momTd);

                tbody.appendChild(tr);
            });

            this._attachRowListeners(tbody);
        },

        _attachRowListeners: function(tbody) {
            var hoverTimer = null;
            tbody.querySelectorAll('tr').forEach(function(row) {
                row.addEventListener('mouseenter', function() {
                    if (hoverTimer) { clearTimeout(hoverTimer); hoverTimer = null; }
                    var fips = row.dataset.fips;
                    hoverTimer = setTimeout(function() {
                        if (!InfoPanel.pinnedCounty) {
                            InfoPanel.showCountyInfo(fips);
                            MapRenderer.highlightCounty(fips);
                        }
                    }, 60);
                });
                row.addEventListener('mouseleave', function() {
                    if (hoverTimer) { clearTimeout(hoverTimer); hoverTimer = null; }
                    if (!InfoPanel.pinnedCounty) {
                        InfoPanel.hideInfo();
                        MapRenderer.clearHighlight();
                    }
                });
                row.addEventListener('click', function() {
                    var fips = row.dataset.fips;
                    if (InfoPanel.pinnedCounty === fips) {
                        InfoPanel.unpinCounty();
                    } else {
                        InfoPanel.pinCounty(fips);
                    }
                });
                row.setAttribute('tabindex', '0');
                row.addEventListener('keydown', function(e) {
                    if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        var fips = row.dataset.fips;
                        if (InfoPanel.pinnedCounty === fips) {
                            InfoPanel.unpinCounty();
                        } else {
                            InfoPanel.pinCounty(fips);
                        }
                    }
                });
            });
        },

        setupEventListeners: function() {
            var self = this;
            document.querySelectorAll('#county-table th[data-sort]').forEach(function(th) {
                th.addEventListener('click', function() {
                    var column = th.dataset.sort;
                    if (self.sortColumn === column) {
                        self.sortDirection = self.sortDirection === 'asc' ? 'desc' : 'asc';
                    } else {
                        self.sortColumn = column;
                        self.sortDirection = 'desc';
                    }
                    document.querySelectorAll('#county-table th').forEach(function(h) {
                        h.classList.remove('sort-asc', 'sort-desc');
                    });
                    th.classList.add(self.sortDirection === 'asc' ? 'sort-asc' : 'sort-desc');
                    self.renderTable();
                });
            });

            var searchInput = document.getElementById('table-search');
            if (searchInput) {
                searchInput.addEventListener('input', function(e) {
                    if (self._searchDebounceTimer) clearTimeout(self._searchDebounceTimer);
                    self._searchDebounceTimer = setTimeout(function() {
                        self.searchTerm = e.target.value.toLowerCase();
                        self.applyFilters();
                    }, 200);
                });
            }
        },

        applyFilters: function() {
            var self = this;
            var rows = document.querySelectorAll('#county-table-body tr');
            rows.forEach(function(row) {
                var fips = row.dataset.fips;
                var county = DataHandler.getCountyData(fips);
                if (!county) { row.classList.add('filtered-out'); return; }
                var matchesSearch = !self.searchTerm || county.name.toLowerCase().indexOf(self.searchTerm) !== -1;
                var filtered = MapRenderer.isFiltered(county) || !matchesSearch;
                row.classList.toggle('filtered-out', filtered);
            });
        },

        _updateExportBtn: function() {
            var btn = document.getElementById('export-csv-btn');
            if (!btn) return;
            var n = this._selectedFips.size;
            btn.textContent = n > 0 ? 'Export Selected (' + n + ')' : 'Export CSV';
        },

        _momentumLabel: function(c) {
            if (c.momentum_class) return c.momentum_class;
            var ci = c.competitive_intensity != null ? c.competitive_intensity : 0;
            return ci === 0 ? 'Stalled*' : ci === 1 ? 'Steady*' : ci === 2 ? 'Growing*' : 'Surging*';
        },

        exportCSV: function() {
            var counties = DataHandler.getAllCounties();
            if (!counties || !counties.length) return;
            var self = this;
            var activeState = DataHandler.getActiveState() || '';

            var hasSelection = this._selectedFips.size > 0;
            var visible = counties.filter(function(c) {
                if (hasSelection) return self._selectedFips.has(c.geoid);
                if (MapRenderer.isFiltered(c)) return false;
                if (self.searchTerm && c.name.toLowerCase().indexOf(self.searchTerm) === -1) return false;
                return true;
            });

            // Sort same as current table
            visible.sort(function(a, b) {
                var aVal = a[self.sortColumn], bVal = b[self.sortColumn];
                if (aVal == null) aVal = -Infinity;
                if (bVal == null) bVal = -Infinity;
                if (typeof aVal === 'string') aVal = aVal.toLowerCase();
                if (typeof bVal === 'string') bVal = bVal.toLowerCase();
                if (aVal === bVal) return 0;
                return self.sortDirection === 'asc' ? (aVal > bVal ? 1 : -1) : (aVal < bVal ? 1 : -1);
            });

            var headers = [
                'County', 'State', 'Attractiveness Score', 'Demographics Score',
                'Penetration %', 'Unserved Locations', 'Median HHI', 'Housing Density',
                'Population', 'Fiber Providers', 'Cable Present', 'FWA Present',
                'Build Difficulty', 'Terrain Score', 'RUCC Code',
                'BEAD Eligible Locations', 'BEAD Implied Award', 'Momentum'
            ];

            function esc(v) {
                var s = v == null ? '' : String(v);
                return /[,"\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
            }

            var rows = visible.map(function(c) {
                return [
                    c.name,
                    c.state_code || activeState,
                    c.attractiveness_index != null ? c.attractiveness_index.toFixed(3) : '',
                    c.demo_score != null ? c.demo_score.toFixed(3) : '',
                    c.fiber_penetration != null ? (c.fiber_penetration * 100).toFixed(1) : '',
                    c.fiber_unserved != null ? c.fiber_unserved : '',
                    c.median_hhi != null ? c.median_hhi : '',
                    c.housing_density != null ? c.housing_density.toFixed(1) : '',
                    c.population_2023 != null ? c.population_2023 : '',
                    c.competitive_intensity != null ? c.competitive_intensity : '',
                    c.cable_present ? 'Y' : 'N',
                    c.fwa_present ? 'Y' : 'N',
                    c.build_difficulty || '',
                    c.terrain_roughness != null ? c.terrain_roughness.toFixed(3) : '',
                    c.rucc_code != null ? c.rucc_code : '',
                    c.bead_eligible_locations != null ? c.bead_eligible_locations : '',
                    c.bead_implied_county_award != null ? Math.round(c.bead_implied_county_award) : '',
                    self._momentumLabel(c)
                ].map(esc).join(',');
            });

            var csv = headers.map(esc).join(',') + '\n' + rows.join('\n');
            var date = new Date().toISOString().slice(0, 10);
            var blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
            var url = URL.createObjectURL(blob);
            var a = document.createElement('a');
            a.href = url;
            a.download = 'fiber-market-analysis-' + date + '.csv';
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
        }
    };

    // ============================================
    // EXPORTS
    // ============================================

    global.MapRenderer = MapRenderer;
    global.InfoPanel = InfoPanel;
    global.TableManager = TableManager;
    global.FEATURED_STATES = FEATURED_STATES;

})(typeof window !== 'undefined' ? window : global);
