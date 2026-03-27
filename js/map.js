// Map rendering module
// Wrapped in IIFE to avoid global pollution

(function(global) {
    'use strict';

    // ============================================
    // HELPER FUNCTIONS
    // ============================================

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
                // Silently ignore unknown properties to prevent innerHTML/prototype injection
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
        NY: { color: '#10b981', label: 'New York', fipsPrefix: '36' },
        MO: { color: '#8b5cf6', label: 'Missouri', fipsPrefix: '29' }
    };

    // ============================================
    // MAP RENDERER
    // ============================================

    var MapRenderer = {
        svg: null,
        mapGroup: null,
        projection: null,
        path: null,
        currentView: 'us', // 'us' or 'state'
        currentState: null, // 'NY', 'MO', etc.
        currentLayer: 'penetration',
        filters: {
            minPop: 0,
            minDensity: 0,
            excludeNYC: false,
            excludeSTLKC: false
        },
        _focusedCountyIndex: -1,
        _countyElements: [],
        // Deep Dive mode
        _deepDiveActive: false,
        _deepDiveTimer: null,
        _deepDiveLayers: ['penetration', 'cable', 'fwa', 'broadband_gap', 'demographic', 'attractiveness', 'competitive', 'terrain'],
        _deepDiveIndex: 0,

        async init(containerId) {
            var container = document.getElementById(containerId);
            if (!container) {
                console.error('Map container not found:', containerId);
                return false;
            }

            this.svg = d3.select('#' + containerId)
                .append('svg')
                .attr('viewBox', '0 0 960 600')
                .attr('preserveAspectRatio', 'xMidYMid meet')
                .attr('role', 'img')
                .attr('aria-label', 'Interactive map of United States showing fiber coverage');

            this.svg.append('rect')
                .attr('width', '100%')
                .attr('height', '100%')
                .attr('fill', '#f8fafc');

            this.mapGroup = this.svg.append('g').attr('class', 'map-container');

            await this.renderUSMap();
            this.updateLegendForUS();
            this._setupKeyboardNavigation();

            return true;
        },

        _setupKeyboardNavigation: function() {
            var self = this;
            document.addEventListener('keydown', function(e) {
                if (e.key === 'Escape' && InfoPanel.pinnedCounty) {
                    InfoPanel.unpinCounty();
                    return;
                }

                if (self.currentView === 'state' && self._countyElements.length > 0) {
                    if (e.key === 'ArrowRight' || e.key === 'ArrowDown') {
                        e.preventDefault();
                        self._focusNextCounty(1);
                    } else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') {
                        e.preventDefault();
                        self._focusNextCounty(-1);
                    } else if (e.key === 'Enter' || e.key === ' ') {
                        if (self._focusedCountyIndex >= 0) {
                            e.preventDefault();
                            var county = self._countyElements[self._focusedCountyIndex];
                            if (county && county.__data__) {
                                self.handleCountyClick(county.__data__);
                            }
                        }
                    }
                }
            });
        },

        _focusNextCounty: function(direction) {
            var newIndex = this._focusedCountyIndex + direction;
            if (newIndex >= 0 && newIndex < this._countyElements.length) {
                this._focusedCountyIndex = newIndex;
                var county = this._countyElements[newIndex];
                if (county && county.__data__) {
                    InfoPanel.showCountyInfo(county.__data__.id);
                    this.mapGroup.selectAll('.county').classed('keyboard-focus', false);
                    d3.select(county).classed('keyboard-focus', true);
                }
            }
        },

        // ===== US MAP VIEW =====
        async renderUSMap() {
            this.currentView = 'us';
            this.currentState = null;
            this.mapGroup.selectAll('*').remove();
            this.svg.attr('viewBox', '0 0 960 600')
                .attr('aria-label', 'Interactive map of United States showing fiber coverage by state');

            var geojson = DataHandler.getUSGeoJSON();
            if (!geojson) {
                console.error('US GeoJSON not loaded');
                return;
            }

            this.projection = d3.geoAlbersUsa()
                .scale(1200)
                .translate([480, 300]);

            this.path = d3.geoPath().projection(this.projection);

            var features;
            if (geojson.type === 'Topology') {
                features = topojson.feature(geojson, geojson.objects.states).features;
            } else {
                features = geojson.features;
            }

            var self = this;
            this.mapGroup.selectAll('.state')
                .data(features)
                .enter()
                .append('path')
                .attr('class', function(d) {
                    var stateCode = self.getStateCode(d);
                    if (FEATURED_STATES[stateCode]) {
                        return 'state featured-state featured-' + stateCode.toLowerCase();
                    }
                    return 'state';
                })
                .attr('d', this.path)
                .attr('data-state', function(d) { return self.getStateCode(d); })
                .attr('tabindex', function(d) {
                    return FEATURED_STATES[self.getStateCode(d)] ? '0' : '-1';
                })
                .attr('role', 'button')
                .attr('aria-label', function(d) {
                    var stateCode = self.getStateCode(d);
                    var data = DataHandler.getStateData(stateCode);
                    if (data) {
                        var label = data.state + ', ' + data.fiberPenetration.toFixed(1) + '% fiber penetration';
                        if (FEATURED_STATES[stateCode]) {
                            label += ' (Click to explore counties)';
                        }
                        return label;
                    }
                    return stateCode || 'Unknown state';
                })
                .each(function(d) {
                    self.updateStateColor(d3.select(this), d);
                })
                .on('mouseenter', function(event, d) { self.handleStateMouseEnter(d); })
                .on('mouseleave', function() { self.handleStateMouseLeave(); })
                .on('click', function(event, d) { self.handleStateClick(d); })
                .on('keydown', function(event, d) {
                    if (event.key === 'Enter' || event.key === ' ') {
                        event.preventDefault();
                        self.handleStateClick(d);
                    }
                });

            setTextById('map-title', 'United States - Fiber Coverage by State');
            document.querySelector('.back-btn').classList.remove('visible');
            document.querySelector('.controls-panel').style.display = 'none';
            document.querySelector('.table-section').style.display = 'none';
        },

        getStateCode: function(d) {
            if (d.properties && d.properties.STUSPS) return d.properties.STUSPS;
            if (d.properties && d.properties.postal) return d.properties.postal;
            if (d.id) {
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
                    '72': 'PR'
                };
                return fipsToState[d.id] || fipsToState[String(d.id).padStart(2, '0')];
            }
            return null;
        },

        updateStateColor: function(selection, d) {
            var stateCode = this.getStateCode(d);
            var data = DataHandler.getStateData(stateCode);
            var featured = FEATURED_STATES[stateCode];

            if (featured) {
                selection.attr('fill', featured.color);
            } else if (data) {
                var penetration = data.fiberPenetration / 100;
                var color = ColorScales.getColor('penetration', penetration);
                selection.attr('fill', color);
            } else {
                selection.attr('fill', '#cbd5e1');
            }
        },

        handleStateMouseEnter: function(d) {
            var stateCode = this.getStateCode(d);
            InfoPanel.showStateInfo(stateCode);
        },

        handleStateMouseLeave: function() {
            InfoPanel.hideInfo();
        },

        handleStateClick: function(d) {
            var stateCode = this.getStateCode(d);
            if (FEATURED_STATES[stateCode]) {
                this.drillDownToState(stateCode);
            }
        },

        // ===== GENERALIZED STATE COUNTY VIEW =====
        async drillDownToState(stateCode) {
            var config = FEATURED_STATES[stateCode];
            if (!config) {
                console.error('No config for state:', stateCode);
                return;
            }

            this.currentView = 'state';
            this.currentState = stateCode;
            DataHandler.setActiveState(stateCode);
            this.mapGroup.selectAll('*').remove();
            this._focusedCountyIndex = -1;
            this.svg.attr('viewBox', '0 0 800 600')
                .attr('aria-label', 'Interactive map of ' + config.label + ' counties showing fiber market data');

            // Get GeoJSON: NY uses dedicated file, others use extracted TopoJSON
            var geojson;
            if (stateCode === 'NY') {
                geojson = DataHandler.getGeoJSON();
            } else {
                geojson = DataHandler.extractStateGeoJSON(config.fipsPrefix);
            }

            if (!geojson) {
                console.error(stateCode + ' GeoJSON not loaded');
                return;
            }

            // Auto-fit projection to state bounds
            this.projection = d3.geoMercator()
                .fitSize([800, 600], geojson);

            this.path = d3.geoPath().projection(this.projection);

            var self = this;
            var countyPaths = this.mapGroup.selectAll('.county')
                .data(geojson.features)
                .enter()
                .append('path')
                .attr('class', 'county')
                .attr('d', this.path)
                .attr('data-fips', function(d) { return d.id; })
                .attr('tabindex', '0')
                .attr('role', 'button')
                .attr('aria-label', function(d) {
                    var data = DataHandler.getCountyData(d.id);
                    if (data) {
                        var pen = Number.isFinite(data.fiber_penetration) ?
                            (data.fiber_penetration * 100).toFixed(1) + '%' : 'N/A';
                        return data.name + ' County, ' + pen + ' fiber penetration';
                    }
                    return 'County ' + d.id;
                })
                .each(function(d) {
                    self.updateCountyColor(d3.select(this), d.id);
                })
                .on('mouseenter', function(event, d) { self.handleCountyMouseEnter(d); })
                .on('mouseleave', function() { self.handleCountyMouseLeave(); })
                .on('click', function(event, d) { self.handleCountyClick(d); })
                .on('focus', function(event, d) {
                    if (!InfoPanel.pinnedCounty) {
                        InfoPanel.showCountyInfo(d.id);
                    }
                })
                .on('blur', function() {
                    if (!InfoPanel.pinnedCounty) {
                        InfoPanel.hideInfo();
                    }
                })
                .on('keydown', function(event, d) {
                    if (event.key === 'Enter' || event.key === ' ') {
                        event.preventDefault();
                        self.handleCountyClick(d);
                    }
                });

            this._countyElements = countyPaths.nodes();

            // State outline (appended to mapGroup so it's cleaned up on navigation)
            this.mapGroup.append('path')
                .attr('class', 'state-outline')
                .datum(geojson)
                .attr('fill', 'none')
                .attr('stroke', '#475569')
                .attr('stroke-width', '2')
                .attr('d', this.path)
                .style('pointer-events', 'none');

            // Update UI
            setTextById('map-title', config.label + ' Counties');
            document.querySelector('.back-btn').classList.add('visible');
            document.querySelector('.controls-panel').style.display = 'flex';
            document.querySelector('.table-section').style.display = 'block';

            // Show/hide state-specific filters
            var excludeNyc = document.getElementById('exclude-nyc');
            var excludeStlKc = document.getElementById('exclude-stlkc');
            if (excludeNyc) {
                excludeNyc.closest('.filter-item').style.display = stateCode === 'NY' ? '' : 'none';
            }
            if (excludeStlKc) {
                excludeStlKc.closest('.filter-item').style.display = stateCode === 'MO' ? '' : 'none';
            }

            // Show/hide NPV button based on state
            var npvBtn = document.getElementById('open-npv-btn');
            if (npvBtn) {
                npvBtn.style.display = stateCode === 'MO' ? '' : 'none';
            }

            this.updateLegend();
            this.applyFilters();
            TableManager.renderTable();
        },

        updateCountyColor: function(selection, fips) {
            var data = DataHandler.getCountyData(fips);
            if (!data) {
                selection.attr('fill', '#e2e8f0');
                return;
            }

            var value;
            switch (this.currentLayer) {
                case 'penetration':
                    value = data.fiber_penetration;
                    break;
                case 'cable':
                    value = data.cable_coverage_pct != null ? data.cable_coverage_pct : null;
                    break;
                case 'fwa':
                    value = data.fwa_coverage_pct != null ? data.fwa_coverage_pct : null;
                    break;
                case 'broadband_gap':
                    value = data.broadband_gap_pct != null ? data.broadband_gap_pct : null;
                    break;
                case 'demographic':
                    value = data.demo_score;
                    break;
                case 'attractiveness':
                    value = data.attractiveness_index;
                    break;
                case 'bead':
                    value = data.bead_claimed_pct != null ? data.bead_claimed_pct : null;
                    break;
                case 'competitive':
                    value = data.competitive_intensity != null ? data.competitive_intensity / 3 : null;
                    break;
                case 'momentum':
                    value = data.fiber_growth_pct != null ? Math.min(1, Math.max(0, data.fiber_growth_pct / 30)) : null;
                    break;
                case 'terrain':
                    value = data.terrain_roughness != null ? data.terrain_roughness : null;
                    break;
                default:
                    value = data.fiber_penetration;
            }

            if (value === null || value === undefined || !Number.isFinite(value)) {
                selection.attr('fill', '#e2e8f0');
                return;
            }

            var color = ColorScales.getColor(this.currentLayer, value);
            selection.attr('fill', color);
        },

        handleCountyMouseEnter: function(d) {
            if (!InfoPanel.pinnedCounty) {
                InfoPanel.showCountyInfo(d.id);
            }
        },

        handleCountyMouseLeave: function() {
            if (!InfoPanel.pinnedCounty) {
                InfoPanel.hideInfo();
            }
        },

        handleCountyClick: function(d) {
            if (InfoPanel.pinnedCounty === d.id) {
                InfoPanel.unpinCounty();
            } else {
                InfoPanel.pinCounty(d.id);
            }
        },

        // ===== SHARED METHODS =====
        setLayer: function(layer) {
            this.currentLayer = layer;
            if (this.currentView === 'state') {
                var self = this;
                this.mapGroup.selectAll('.county').each(function(d) {
                    self.updateCountyColor(d3.select(this), d.id);
                });
                this.updateLegend();
            }
        },

        setFilters: function(filters) {
            this.filters = Object.assign({}, this.filters, filters);
            if (this.currentView === 'state') {
                this.applyFilters();
                TableManager.applyFilters();
            }
        },

        applyFilters: function() {
            var self = this;
            this.mapGroup.selectAll('.county').each(function(d) {
                var county = DataHandler.getCountyData(d.id);
                var filtered = self.isFiltered(county);
                d3.select(this).classed('filtered-out', filtered);
            });
        },

        isFiltered: function(county) {
            if (!county) return true;
            if (this.filters.excludeNYC && county.is_nyc_borough) return true;
            if (this.filters.excludeSTLKC && county.is_stl_kc_metro) return true;
            if (this.filters.minPop > 0 && (county.population_2023 == null || county.population_2023 < this.filters.minPop)) return true;
            if (this.filters.minDensity > 0 && (county.housing_density == null || county.housing_density < this.filters.minDensity)) return true;
            return false;
        },

        updateLegendForUS: function() {
            var container = document.getElementById('legend-container');
            if (!container) return;

            container.textContent = '';

            var legend = createElement('div', { className: 'legend' });

            var label = createElement('span', {
                style: 'font-size: 0.7rem; color: #64748b; margin-right: 0.5rem;'
            }, 'Fiber Penetration:');
            legend.appendChild(label);

            var legendItems = ColorScales.getLegend('penetration');
            legendItems.forEach(function(item) {
                var itemEl = createElement('div', { className: 'legend-item' });
                var colorEl = createElement('div', { className: 'legend-color' });
                colorEl.style.background = item.color;
                var labelEl = createElement('span', {}, item.label);
                itemEl.appendChild(colorEl);
                itemEl.appendChild(labelEl);
                legend.appendChild(itemEl);
            });

            // Featured state indicators
            var stateCodes = Object.keys(FEATURED_STATES);
            for (var i = 0; i < stateCodes.length; i++) {
                var sc = stateCodes[i];
                var cfg = FEATURED_STATES[sc];
                var stItem = createElement('div', {
                    className: 'legend-item',
                    style: i === 0 ? 'margin-left: 1rem;' : ''
                });
                var stColor = createElement('div', { className: 'legend-color' });
                stColor.style.background = cfg.color;
                var stLabel = createElement('span', {}, sc + ' (Click to explore)');
                stItem.appendChild(stColor);
                stItem.appendChild(stLabel);
                legend.appendChild(stItem);
            }

            container.appendChild(legend);
        },

        updateLegend: function() {
            var container = document.getElementById('legend-container');
            if (!container) return;

            container.textContent = '';

            var legend = createElement('div', { className: 'legend' });
            var legendItems = ColorScales.getLegend(this.currentLayer);

            legendItems.forEach(function(item) {
                var itemEl = createElement('div', { className: 'legend-item' });
                var colorEl = createElement('div', { className: 'legend-color' });
                colorEl.style.background = item.color;
                var labelEl = createElement('span', {}, item.label);
                itemEl.appendChild(colorEl);
                itemEl.appendChild(labelEl);
                legend.appendChild(itemEl);
            });

            container.appendChild(legend);
        },

        highlightCounty: function(fips) {
            if (this.currentView !== 'state') return;
            this.mapGroup.selectAll('.county')
                .style('opacity', function(d) { return d.id === fips ? 1 : 0.3; });
        },

        clearHighlight: function() {
            if (this.currentView !== 'state') return;
            this.mapGroup.selectAll('.county')
                .style('opacity', 1);
        },

        backToUS: function() {
            InfoPanel.pinnedCounty = null;
            var pinIndicator = document.querySelector('.pin-indicator');
            if (pinIndicator) {
                pinIndicator.style.display = 'none';
            }
            this._focusedCountyIndex = -1;
            this._countyElements = [];
            this.stopDeepDive();
            this.renderUSMap();
            this.updateLegendForUS();
            InfoPanel.hideInfo();
            // Hide NPV panel if open
            if (typeof NPVCalculator !== 'undefined' && NPVCalculator.close) {
                NPVCalculator.close();
            }
        },

        // ===== DEEP DIVE MODE =====
        startDeepDive: function() {
            if (this._deepDiveActive) return;
            this._deepDiveActive = true;
            this._deepDiveIndex = 0;

            // If on US map, drill to MO first
            if (this.currentView === 'us') {
                var self = this;
                this.drillDownToState('MO').then(function() {
                    self._runDeepDiveCycle();
                });
            } else {
                this._runDeepDiveCycle();
            }
        },

        _runDeepDiveCycle: function() {
            if (!this._deepDiveActive) return;
            var self = this;

            var layer = this._deepDiveLayers[this._deepDiveIndex];
            this.setLayer(layer);

            // Update toggle buttons to reflect current layer
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
            // Update toggle button for deep dive
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
        pinnedCounty: null,

        init: function() {
            this.defaultEl = document.querySelector('.default-message');
            this.countyInfoEl = document.querySelector('.county-info');
            this.stateInfoEl = document.querySelector('.state-info');
        },

        pinCounty: function(fips) {
            this.pinnedCounty = fips;
            this.showCountyInfo(fips);
            var pinIndicator = document.querySelector('.pin-indicator');
            if (pinIndicator) {
                pinIndicator.style.display = 'flex';
            }
            MapRenderer.mapGroup.selectAll('.county')
                .classed('pinned', function(d) { return d.id === fips; });

            this._announceToScreenReader('County details pinned. Press Escape to close.');
        },

        unpinCounty: function() {
            this.pinnedCounty = null;
            var pinIndicator = document.querySelector('.pin-indicator');
            if (pinIndicator) {
                pinIndicator.style.display = 'none';
            }
            MapRenderer.mapGroup.selectAll('.county').classed('pinned', false);
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

            // Show click hint for featured states
            var clickHint = document.querySelector('.click-hint');
            if (clickHint) {
                if (FEATURED_STATES[stateCode]) {
                    clickHint.style.display = 'block';
                    clickHint.textContent = '';
                    var strong = createElement('strong', {}, 'Click to explore');
                    clickHint.appendChild(strong);
                    clickHint.appendChild(document.createTextNode(
                        ' Detailed county-level data available for ' + FEATURED_STATES[stateCode].label
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
                        return (b.passings || 0) - (a.passings || 0);
                    }).slice(0, 5);

                    topOperators.forEach(function(op) {
                        var li = createElement('li');
                        var nameSpan = createElement('span', { className: 'operator-name' }, op.name);
                        var passingsSpan = createElement('span', { className: 'operator-passings' },
                            DataHandler.formatNumber(op.passings) + ' passings');
                        li.appendChild(nameSpan);
                        li.appendChild(passingsSpan);
                        operatorsList.appendChild(li);
                    });
                } else {
                    var li = createElement('li', { style: 'color: #64748b;' }, 'No fiber operators reported');
                    operatorsList.appendChild(li);
                }
            }

            // === NEW SECTIONS ===

            // BEAD Funding section
            var beadSection = document.getElementById('bead-section');
            if (beadSection) {
                if (data.bead_status != null) {
                    beadSection.style.display = '';
                    setTextById('bead-status', data.bead_status || 'N/A');
                    var beadStatusEl = document.getElementById('bead-status');
                    if (beadStatusEl) {
                        var validBeadClasses = { 'awarded': true, 'in-progress': true, 'pending': true, 'not-targeted': true, 'unverified': true };
                        var beadClass = (data.bead_status || '').toLowerCase().replace(/\s+/g, '-');
                        beadStatusEl.className = 'stat-value bead-badge' + (validBeadClasses[beadClass] ? ' bead-' + beadClass : '');
                    }

                    // Show detail fields only if BEAD data is verified (not "Unverified")
                    var hasBeadData = data.bead_status !== 'Unverified' && data.bead_dollars_awarded != null;
                    var detailIds = ['bead-details-dollars', 'bead-details-locations', 'bead-details-claimed', 'bead-details-awardees'];
                    detailIds.forEach(function(id) {
                        var el = document.getElementById(id);
                        if (el) el.style.display = hasBeadData ? '' : 'none';
                    });
                    var beadNote = document.getElementById('bead-note');
                    if (beadNote) beadNote.style.display = hasBeadData ? 'none' : '';

                    if (hasBeadData) {
                        setTextById('bead-dollars', DataHandler.formatCurrency(data.bead_dollars_awarded));
                        setTextById('bead-locations', DataHandler.formatNumber(data.bead_locations_covered));
                        setTextById('bead-claimed', data.bead_claimed_pct != null ? DataHandler.formatPercent(data.bead_claimed_pct) : 'N/A');
                        var awardeesList = document.getElementById('bead-awardees');
                        if (awardeesList) {
                            awardeesList.textContent = (data.bead_awardees && data.bead_awardees.length > 0) ?
                                data.bead_awardees.join(', ') : 'None';
                        }
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
                    setTextById('cable-coverage', DataHandler.formatPercent(data.cable_coverage_pct));
                    setTextById('fwa-coverage', DataHandler.formatPercent(data.fwa_coverage_pct));
                    setTextById('bb-coverage', DataHandler.formatPercent(data.broadband_coverage_pct));
                    setTextById('bb-gap', DataHandler.formatPercent(data.broadband_gap_pct));
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
                    // Momentum: show "Pending" if null (Dec 2024 data not available)
                    setTextById('momentum-class', data.momentum_class || 'Pending');
                    var momEl = document.getElementById('momentum-class');
                    if (momEl) {
                        var momClass = data.momentum_class ? data.momentum_class.toLowerCase() : 'pending';
                        momEl.className = 'stat-value momentum-' + momClass;
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

            // NPV button visibility
            var npvBtn = document.getElementById('open-npv-btn');
            if (npvBtn) {
                npvBtn.style.display = (MapRenderer.currentState === 'MO') ? '' : 'none';
            }

            if (this.defaultEl) this.defaultEl.style.display = 'none';
            if (this.stateInfoEl) this.stateInfoEl.style.display = 'none';
            if (this.countyInfoEl) this.countyInfoEl.style.display = 'block';
        },

        hideInfo: function() {
            if (this.defaultEl) this.defaultEl.style.display = 'block';
            if (this.countyInfoEl) this.countyInfoEl.style.display = 'none';
            if (this.stateInfoEl) this.stateInfoEl.style.display = 'none';
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

                // Normalize nulls
                if (aVal === null || aVal === undefined || (typeof aVal === 'number' && !Number.isFinite(aVal))) {
                    aVal = -Infinity;
                }
                if (bVal === null || bVal === undefined || (typeof bVal === 'number' && !Number.isFinite(bVal))) {
                    bVal = -Infinity;
                }

                // Case-insensitive string comparison
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

            counties.forEach(function(c) {
                var tr = createElement('tr', { 'data-fips': c.geoid });
                if (MapRenderer.isFiltered(c)) {
                    tr.classList.add('filtered-out');
                }

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

                tbody.appendChild(tr);
            });

            this._attachRowListeners(tbody);
        },

        _attachRowListeners: function(tbody) {
            tbody.querySelectorAll('tr').forEach(function(row) {
                row.addEventListener('mouseenter', function() {
                    var fips = row.dataset.fips;
                    if (!InfoPanel.pinnedCounty) {
                        InfoPanel.showCountyInfo(fips);
                        MapRenderer.highlightCounty(fips);
                    }
                });
                row.addEventListener('mouseleave', function() {
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
                    if (self._searchDebounceTimer) {
                        clearTimeout(self._searchDebounceTimer);
                    }
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
                if (!county) {
                    row.classList.add('filtered-out');
                    return;
                }
                var matchesSearch = !self.searchTerm || county.name.toLowerCase().indexOf(self.searchTerm) !== -1;
                var filtered = MapRenderer.isFiltered(county) || !matchesSearch;
                row.classList.toggle('filtered-out', filtered);
            });
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
