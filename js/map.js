// Map rendering module
// Wrapped in IIFE to avoid global pollution

(function(global) {
    'use strict';

    // ============================================
    // HELPER FUNCTIONS
    // ============================================

    /**
     * Safely sets text content (prevents XSS)
     * @param {string} selector
     * @param {string} text
     */
    function setTextContent(selector, text) {
        const el = document.querySelector(selector);
        if (el) {
            el.textContent = DataHandler.sanitizeString(text);
        }
    }

    /**
     * Safely sets text content by ID
     * @param {string} id
     * @param {string} text
     */
    function setTextById(id, text) {
        const el = document.getElementById(id);
        if (el) {
            el.textContent = DataHandler.sanitizeString(text);
        }
    }

    /**
     * Creates a DOM element safely
     * @param {string} tag
     * @param {Object} attrs
     * @param {string} textContent
     * @returns {HTMLElement}
     */
    function createElement(tag, attrs, textContent) {
        const el = document.createElement(tag);
        if (attrs) {
            for (const [key, value] of Object.entries(attrs)) {
                if (key === 'className') {
                    el.className = value;
                } else if (key.startsWith('data-')) {
                    el.setAttribute(key, value);
                } else if (key.startsWith('aria-')) {
                    el.setAttribute(key, value);
                } else {
                    el[key] = value;
                }
            }
        }
        if (textContent !== undefined) {
            el.textContent = DataHandler.sanitizeString(textContent);
        }
        return el;
    }

    // ============================================
    // MAP RENDERER
    // ============================================

    const MapRenderer = {
        svg: null,
        mapGroup: null,
        projection: null,
        path: null,
        currentView: 'us', // 'us' or 'ny'
        currentLayer: 'penetration',
        filters: {
            minPop: 0,
            minDensity: 0,
            excludeNYC: false
        },
        // Track focused county for keyboard navigation
        _focusedCountyIndex: -1,
        _countyElements: [],

        async init(containerId) {
            const container = document.getElementById(containerId);
            if (!container) {
                console.error('Map container not found:', containerId);
                return false;
            }

            // Create SVG with accessibility attributes
            this.svg = d3.select('#' + containerId)
                .append('svg')
                .attr('viewBox', '0 0 960 600')
                .attr('preserveAspectRatio', 'xMidYMid meet')
                .attr('role', 'img')
                .attr('aria-label', 'Interactive map of United States showing fiber coverage');

            // Background
            this.svg.append('rect')
                .attr('width', '100%')
                .attr('height', '100%')
                .attr('fill', '#f8fafc');

            this.mapGroup = this.svg.append('g').attr('class', 'map-container');

            // Start with US view
            await this.renderUSMap();
            this.updateLegendForUS();

            // Setup keyboard navigation
            this._setupKeyboardNavigation();

            return true;
        },

        _setupKeyboardNavigation() {
            // Global keyboard handler for map navigation
            document.addEventListener('keydown', (e) => {
                // Escape key unpins county
                if (e.key === 'Escape' && InfoPanel.pinnedCounty) {
                    InfoPanel.unpinCounty();
                    return;
                }

                // Arrow keys for county navigation (only in NY view)
                if (this.currentView === 'ny' && this._countyElements.length > 0) {
                    if (e.key === 'ArrowRight' || e.key === 'ArrowDown') {
                        e.preventDefault();
                        this._focusNextCounty(1);
                    } else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') {
                        e.preventDefault();
                        this._focusNextCounty(-1);
                    } else if (e.key === 'Enter' || e.key === ' ') {
                        if (this._focusedCountyIndex >= 0) {
                            e.preventDefault();
                            const county = this._countyElements[this._focusedCountyIndex];
                            if (county && county.__data__) {
                                this.handleCountyClick(county.__data__);
                            }
                        }
                    }
                }
            });
        },

        _focusNextCounty(direction) {
            const newIndex = this._focusedCountyIndex + direction;
            if (newIndex >= 0 && newIndex < this._countyElements.length) {
                this._focusedCountyIndex = newIndex;
                const county = this._countyElements[newIndex];
                if (county && county.__data__) {
                    InfoPanel.showCountyInfo(county.__data__.id);
                    // Visual focus indicator
                    this.mapGroup.selectAll('.county').classed('keyboard-focus', false);
                    d3.select(county).classed('keyboard-focus', true);
                }
            }
        },

        // ===== US MAP VIEW =====
        async renderUSMap() {
            this.currentView = 'us';
            this.mapGroup.selectAll('*').remove();
            this.svg.attr('viewBox', '0 0 960 600')
                .attr('aria-label', 'Interactive map of United States showing fiber coverage by state');

            const geojson = DataHandler.getUSGeoJSON();
            if (!geojson) {
                console.error('US GeoJSON not loaded');
                return;
            }

            // Use Albers USA projection for US map
            this.projection = d3.geoAlbersUsa()
                .scale(1200)
                .translate([480, 300]);

            this.path = d3.geoPath().projection(this.projection);

            // Convert TopoJSON to GeoJSON if needed
            let features;
            if (geojson.type === 'Topology') {
                features = topojson.feature(geojson, geojson.objects.states).features;
            } else {
                features = geojson.features;
            }

            // Render states
            this.mapGroup.selectAll('.state')
                .data(features)
                .enter()
                .append('path')
                .attr('class', (d) => {
                    const stateCode = this.getStateCode(d);
                    return stateCode === 'NY' ? 'state ny-state' : 'state';
                })
                .attr('d', this.path)
                .attr('data-state', (d) => this.getStateCode(d))
                .attr('tabindex', (d) => this.getStateCode(d) === 'NY' ? '0' : '-1')
                .attr('role', 'button')
                .attr('aria-label', (d) => {
                    const stateCode = this.getStateCode(d);
                    const data = DataHandler.getStateData(stateCode);
                    if (data) {
                        return data.state + ', ' + data.fiberPenetration.toFixed(1) + '% fiber penetration';
                    }
                    return stateCode || 'Unknown state';
                })
                .each((d, i, nodes) => {
                    this.updateStateColor(d3.select(nodes[i]), d);
                })
                .on('mouseenter', (event, d) => this.handleStateMouseEnter(d))
                .on('mouseleave', () => this.handleStateMouseLeave())
                .on('click', (event, d) => this.handleStateClick(d))
                .on('keydown', (event, d) => {
                    if (event.key === 'Enter' || event.key === ' ') {
                        event.preventDefault();
                        this.handleStateClick(d);
                    }
                });

            // Update UI
            setTextById('map-title', 'United States - Fiber Coverage by State');
            document.querySelector('.back-btn').classList.remove('visible');
            document.querySelector('.controls-panel').style.display = 'none';
            document.querySelector('.table-section').style.display = 'none';
        },

        getStateCode(d) {
            // Handle different data formats
            if (d.properties && d.properties.STUSPS) return d.properties.STUSPS;
            if (d.properties && d.properties.postal) return d.properties.postal;
            if (d.id) {
                // FIPS to state code mapping
                const fipsToState = {
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

        updateStateColor(selection, d) {
            const stateCode = this.getStateCode(d);
            const data = DataHandler.getStateData(stateCode);

            if (stateCode === 'NY') {
                // NY is highlighted differently
                selection.attr('fill', '#10b981');
            } else if (data) {
                // Color by fiber penetration
                const penetration = data.fiberPenetration / 100;
                const color = ColorScales.getColor('penetration', penetration);
                selection.attr('fill', color);
            } else {
                selection.attr('fill', '#cbd5e1');
            }
        },

        handleStateMouseEnter(d) {
            const stateCode = this.getStateCode(d);
            InfoPanel.showStateInfo(stateCode);
        },

        handleStateMouseLeave() {
            InfoPanel.hideInfo();
        },

        handleStateClick(d) {
            const stateCode = this.getStateCode(d);
            if (stateCode === 'NY') {
                this.drillDownToNY();
            }
        },

        // ===== NY COUNTY VIEW =====
        async drillDownToNY() {
            this.currentView = 'ny';
            this.mapGroup.selectAll('*').remove();
            this._focusedCountyIndex = -1;
            this.svg.attr('viewBox', '0 0 800 600')
                .attr('aria-label', 'Interactive map of New York State counties showing fiber market data');

            const geojson = DataHandler.getGeoJSON();
            if (!geojson) {
                console.error('NY GeoJSON not loaded');
                return;
            }

            // Calculate bounds and create projection for NY
            const bounds = d3.geoBounds(geojson);
            const center = [(bounds[0][0] + bounds[1][0]) / 2, (bounds[0][1] + bounds[1][1]) / 2];

            this.projection = d3.geoMercator()
                .center(center)
                .scale(6000)
                .translate([400, 300]);

            this.path = d3.geoPath().projection(this.projection);

            // Render counties
            const countyPaths = this.mapGroup.selectAll('.county')
                .data(geojson.features)
                .enter()
                .append('path')
                .attr('class', 'county')
                .attr('d', this.path)
                .attr('data-fips', (d) => d.id)
                .attr('tabindex', '0')
                .attr('role', 'button')
                .attr('aria-label', (d) => {
                    const data = DataHandler.getCountyData(d.id);
                    if (data) {
                        return data.name + ' County, ' +
                               (data.fiber_penetration * 100).toFixed(1) + '% fiber penetration';
                    }
                    return 'County ' + d.id;
                })
                .each((d, i, nodes) => {
                    this.updateCountyColor(d3.select(nodes[i]), d.id);
                })
                .on('mouseenter', (event, d) => this.handleCountyMouseEnter(d))
                .on('mouseleave', () => this.handleCountyMouseLeave())
                .on('click', (event, d) => this.handleCountyClick(d))
                .on('focus', (event, d) => {
                    if (!InfoPanel.pinnedCounty) {
                        InfoPanel.showCountyInfo(d.id);
                    }
                })
                .on('blur', () => {
                    if (!InfoPanel.pinnedCounty) {
                        InfoPanel.hideInfo();
                    }
                })
                .on('keydown', (event, d) => {
                    if (event.key === 'Enter' || event.key === ' ') {
                        event.preventDefault();
                        this.handleCountyClick(d);
                    }
                });

            // Store county elements for keyboard navigation
            this._countyElements = countyPaths.nodes();

            // State outline
            this.svg.append('path')
                .attr('class', 'state-outline')
                .datum(geojson)
                .attr('fill', 'none')
                .attr('stroke', '#475569')
                .attr('stroke-width', '2')
                .attr('d', this.path)
                .style('pointer-events', 'none');

            // Update UI
            setTextById('map-title', 'New York State Counties');
            document.querySelector('.back-btn').classList.add('visible');
            document.querySelector('.controls-panel').style.display = 'flex';
            document.querySelector('.table-section').style.display = 'block';

            this.updateLegend();
            this.applyFilters();
            TableManager.renderTable();
        },

        updateCountyColor(selection, fips) {
            const data = DataHandler.getCountyData(fips);
            if (!data) {
                selection.attr('fill', '#e2e8f0');
                return;
            }

            let value;
            switch (this.currentLayer) {
                case 'penetration':
                    value = data.fiber_penetration;
                    break;
                case 'demographic':
                    value = data.demo_score;
                    break;
                case 'attractiveness':
                    value = data.attractiveness_index;
                    break;
                default:
                    value = data.fiber_penetration;
            }

            // Handle missing/invalid values
            if (!Number.isFinite(value)) {
                selection.attr('fill', '#e2e8f0');
                return;
            }

            const color = ColorScales.getColor(this.currentLayer, value);
            selection.attr('fill', color);
        },

        handleCountyMouseEnter(d) {
            // Only show on hover if no county is pinned
            if (!InfoPanel.pinnedCounty) {
                InfoPanel.showCountyInfo(d.id);
            }
        },

        handleCountyMouseLeave() {
            // Only hide on mouse leave if no county is pinned
            if (!InfoPanel.pinnedCounty) {
                InfoPanel.hideInfo();
            }
        },

        handleCountyClick(d) {
            // Toggle pin on click
            if (InfoPanel.pinnedCounty === d.id) {
                // Clicking same county unpins it
                InfoPanel.unpinCounty();
            } else {
                // Pin this county
                InfoPanel.pinCounty(d.id);
            }
        },

        // ===== SHARED METHODS =====
        setLayer(layer) {
            this.currentLayer = layer;
            if (this.currentView === 'ny') {
                this.mapGroup.selectAll('.county').each((d, i, nodes) => {
                    this.updateCountyColor(d3.select(nodes[i]), d.id);
                });
                this.updateLegend();
            }
        },

        setFilters(filters) {
            this.filters = { ...this.filters, ...filters };
            if (this.currentView === 'ny') {
                this.applyFilters();
                TableManager.applyFilters();
            }
        },

        applyFilters() {
            this.mapGroup.selectAll('.county').each((d, i, nodes) => {
                const county = DataHandler.getCountyData(d.id);
                const filtered = this.isFiltered(county);
                d3.select(nodes[i]).classed('filtered-out', filtered);
            });
        },

        isFiltered(county) {
            if (!county) return true;
            if (this.filters.excludeNYC && county.is_nyc_borough) return true;
            if (county.population_2023 < this.filters.minPop) return true;
            if (county.housing_density < this.filters.minDensity) return true;
            return false;
        },

        updateLegendForUS() {
            const container = document.getElementById('legend-container');
            if (!container) return;

            // Clear existing content safely
            container.textContent = '';

            const legend = createElement('div', { className: 'legend' });

            // Label
            const label = createElement('span', {
                style: 'font-size: 0.7rem; color: #64748b; margin-right: 0.5rem;'
            }, 'Fiber Penetration:');
            legend.appendChild(label);

            // Legend items
            const legendItems = ColorScales.getLegend('penetration');
            legendItems.forEach((item) => {
                const itemEl = createElement('div', { className: 'legend-item' });
                const colorEl = createElement('div', { className: 'legend-color' });
                colorEl.style.background = item.color;
                const labelEl = createElement('span', {}, item.label);
                itemEl.appendChild(colorEl);
                itemEl.appendChild(labelEl);
                legend.appendChild(itemEl);
            });

            // NY indicator
            const nyItem = createElement('div', {
                className: 'legend-item',
                style: 'margin-left: 1rem;'
            });
            const nyColor = createElement('div', { className: 'legend-color' });
            nyColor.style.background = '#10b981';
            const nyLabel = createElement('span', {}, 'NY (Click to explore)');
            nyItem.appendChild(nyColor);
            nyItem.appendChild(nyLabel);
            legend.appendChild(nyItem);

            container.appendChild(legend);
        },

        updateLegend() {
            const container = document.getElementById('legend-container');
            if (!container) return;

            // Clear existing content safely
            container.textContent = '';

            const legend = createElement('div', { className: 'legend' });
            const legendItems = ColorScales.getLegend(this.currentLayer);

            legendItems.forEach((item) => {
                const itemEl = createElement('div', { className: 'legend-item' });
                const colorEl = createElement('div', { className: 'legend-color' });
                colorEl.style.background = item.color;
                const labelEl = createElement('span', {}, item.label);
                itemEl.appendChild(colorEl);
                itemEl.appendChild(labelEl);
                legend.appendChild(itemEl);
            });

            container.appendChild(legend);
        },

        highlightCounty(fips) {
            if (this.currentView !== 'ny') return;
            this.mapGroup.selectAll('.county')
                .style('opacity', (d) => d.id === fips ? 1 : 0.3);
        },

        clearHighlight() {
            if (this.currentView !== 'ny') return;
            this.mapGroup.selectAll('.county')
                .style('opacity', 1);
        },

        backToUS() {
            // Remove state outline before switching views
            this.svg.select('.state-outline').remove();
            // Reset any pinned county
            InfoPanel.pinnedCounty = null;
            const pinIndicator = document.querySelector('.pin-indicator');
            if (pinIndicator) {
                pinIndicator.style.display = 'none';
            }
            this._focusedCountyIndex = -1;
            this._countyElements = [];
            this.renderUSMap();
            this.updateLegendForUS();
            InfoPanel.hideInfo();
        }
    };

    // ============================================
    // INFO PANEL
    // ============================================

    const InfoPanel = {
        defaultEl: null,
        countyInfoEl: null,
        stateInfoEl: null,
        pinnedCounty: null,

        init() {
            this.defaultEl = document.querySelector('.default-message');
            this.countyInfoEl = document.querySelector('.county-info');
            this.stateInfoEl = document.querySelector('.state-info');
        },

        pinCounty(fips) {
            this.pinnedCounty = fips;
            this.showCountyInfo(fips);
            // Show close button and highlight
            const pinIndicator = document.querySelector('.pin-indicator');
            if (pinIndicator) {
                pinIndicator.style.display = 'flex';
            }
            // Highlight the pinned county on map
            MapRenderer.mapGroup.selectAll('.county')
                .classed('pinned', (d) => d.id === fips);

            // Announce to screen readers
            this._announceToScreenReader('County details pinned. Press Escape to close.');
        },

        unpinCounty() {
            this.pinnedCounty = null;
            const pinIndicator = document.querySelector('.pin-indicator');
            if (pinIndicator) {
                pinIndicator.style.display = 'none';
            }
            MapRenderer.mapGroup.selectAll('.county').classed('pinned', false);
            this.hideInfo();

            // Announce to screen readers
            this._announceToScreenReader('County details closed.');
        },

        _announceToScreenReader(message) {
            // Create a live region announcement
            const announcer = document.getElementById('sr-announcer') || this._createAnnouncer();
            announcer.textContent = message;
        },

        _createAnnouncer() {
            const announcer = createElement('div', {
                id: 'sr-announcer',
                className: 'sr-only',
                'aria-live': 'polite',
                'aria-atomic': 'true'
            });
            announcer.style.cssText = 'position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0;';
            document.body.appendChild(announcer);
            return announcer;
        },

        showStateInfo(stateCode) {
            const data = DataHandler.getStateData(stateCode);

            setTextContent('.state-name', data ? data.state : stateCode);

            if (data) {
                setTextById('state-housing', DataHandler.formatNumber(data.totalHousingUnits));
                setTextById('state-fiber', DataHandler.formatNumber(data.totalFiberPassings));
                setTextById('state-penetration', data.fiberPenetration.toFixed(1) + '%');

                // Operators list - built safely with DOM methods
                const operatorsList = document.getElementById('state-operators-list');
                if (operatorsList) {
                    operatorsList.textContent = ''; // Clear safely
                    if (data.operators && data.operators.length > 0) {
                        data.operators.slice(0, 6).forEach((op) => {
                            const li = createElement('li');
                            const nameSpan = createElement('span', { className: 'operator-name' }, op.name);
                            const passingsSpan = createElement('span', { className: 'operator-passings' },
                                DataHandler.formatNumber(op.passings));
                            li.appendChild(nameSpan);
                            li.appendChild(passingsSpan);
                            operatorsList.appendChild(li);
                        });
                    }
                }
            }

            // Show/hide click hint for NY
            const clickHint = document.querySelector('.click-hint');
            if (clickHint) {
                clickHint.style.display = stateCode === 'NY' ? 'block' : 'none';
            }

            if (this.defaultEl) this.defaultEl.style.display = 'none';
            if (this.countyInfoEl) this.countyInfoEl.style.display = 'none';
            if (this.stateInfoEl) this.stateInfoEl.style.display = 'block';
        },

        showCountyInfo(fips) {
            const data = DataHandler.getCountyData(fips);
            if (!data) return;

            // County name - safe text content
            setTextContent('.county-name', data.name + ' County');

            // Score bars - using safe numeric operations
            const attrScore = document.getElementById('attr-score');
            const attrValue = document.getElementById('attr-value');
            if (attrScore && attrValue) {
                const attrIndex = Number.isFinite(data.attractiveness_index) ? data.attractiveness_index : 0;
                attrScore.style.width = (attrIndex * 100) + '%';
                attrValue.textContent = attrIndex.toFixed(2);
            }

            const demoScore = document.getElementById('demo-score');
            const demoValue = document.getElementById('demo-value');
            if (demoScore && demoValue) {
                const demoScoreVal = Number.isFinite(data.demo_score) ? data.demo_score : 0;
                demoScore.style.width = (demoScoreVal * 100) + '%';
                demoValue.textContent = demoScoreVal.toFixed(2);
            }

            const penScore = document.getElementById('pen-score');
            const penValue = document.getElementById('pen-value');
            if (penScore && penValue) {
                const penVal = Number.isFinite(data.fiber_penetration) ? data.fiber_penetration : 0;
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

            const popGrowthEl = document.getElementById('pop-growth');
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
            setTextById('housing-density', DataHandler.formatNumber(data.housing_density) + '/sq mi');

            // Demographics & Economic
            setTextById('median-hhi', DataHandler.formatCurrency(data.median_hhi));
            setTextById('median-rent', DataHandler.formatCurrency(data.median_rent));
            setTextById('owner-occ', DataHandler.formatPercentDirect(data.owner_occupied_pct));
            setTextById('wfh-pct', DataHandler.formatPercentDirect(data.wfh_pct));
            setTextById('median-home-value', DataHandler.formatCurrency(data.median_home_value));

            // Top 5 Fiber Operators - built safely with DOM methods
            const operatorsList = document.getElementById('operators-list');
            if (operatorsList) {
                operatorsList.textContent = ''; // Clear safely

                if (data.operators && data.operators.length > 0) {
                    // Sort by passings and take top 5
                    const topOperators = [...data.operators]
                        .sort((a, b) => (b.passings || 0) - (a.passings || 0))
                        .slice(0, 5);

                    topOperators.forEach((op) => {
                        const li = createElement('li');
                        const nameSpan = createElement('span', { className: 'operator-name' }, op.name);
                        const passingsSpan = createElement('span', { className: 'operator-passings' },
                            DataHandler.formatNumber(op.passings) + ' passings');
                        li.appendChild(nameSpan);
                        li.appendChild(passingsSpan);
                        operatorsList.appendChild(li);
                    });
                } else {
                    const li = createElement('li', { style: 'color: #64748b;' }, 'No fiber operators reported');
                    operatorsList.appendChild(li);
                }
            }

            if (this.defaultEl) this.defaultEl.style.display = 'none';
            if (this.stateInfoEl) this.stateInfoEl.style.display = 'none';
            if (this.countyInfoEl) this.countyInfoEl.style.display = 'block';
        },

        hideInfo() {
            if (this.defaultEl) this.defaultEl.style.display = 'block';
            if (this.countyInfoEl) this.countyInfoEl.style.display = 'none';
            if (this.stateInfoEl) this.stateInfoEl.style.display = 'none';
        }
    };

    // ============================================
    // TABLE MANAGER
    // ============================================

    const TableManager = {
        sortColumn: 'attractiveness_index',
        sortDirection: 'desc',
        searchTerm: '',
        _searchDebounceTimer: null,

        init() {
            this.setupEventListeners();
        },

        renderTable() {
            const tbody = document.getElementById('county-table-body');
            if (!tbody) return;

            const counties = DataHandler.getAllCounties();

            // Sort
            counties.sort((a, b) => {
                let aVal = a[this.sortColumn];
                let bVal = b[this.sortColumn];

                if (typeof aVal === 'string') {
                    aVal = aVal.toLowerCase();
                    bVal = (bVal || '').toLowerCase();
                }

                if (aVal === null || aVal === undefined || (typeof aVal === 'number' && !Number.isFinite(aVal))) {
                    aVal = -Infinity;
                }
                if (bVal === null || bVal === undefined || (typeof bVal === 'number' && !Number.isFinite(bVal))) {
                    bVal = -Infinity;
                }

                if (this.sortDirection === 'asc') {
                    return aVal > bVal ? 1 : -1;
                } else {
                    return aVal < bVal ? 1 : -1;
                }
            });

            // Clear table safely
            tbody.textContent = '';

            // Build rows using DOM methods (not innerHTML)
            counties.forEach((c) => {
                const tr = createElement('tr', { 'data-fips': c.geoid });
                if (MapRenderer.isFiltered(c)) {
                    tr.classList.add('filtered-out');
                }

                // County name
                tr.appendChild(createElement('td', {}, c.name));

                // Attractiveness
                const attrVal = Number.isFinite(c.attractiveness_index) ? c.attractiveness_index.toFixed(2) : 'N/A';
                tr.appendChild(createElement('td', {}, attrVal));

                // Demographics
                const demoVal = Number.isFinite(c.demo_score) ? c.demo_score.toFixed(2) : 'N/A';
                tr.appendChild(createElement('td', {}, demoVal));

                // Penetration
                const penVal = Number.isFinite(c.fiber_penetration) ? (c.fiber_penetration * 100).toFixed(1) + '%' : 'N/A';
                tr.appendChild(createElement('td', {}, penVal));

                // Unserved
                tr.appendChild(createElement('td', {}, DataHandler.formatNumber(c.fiber_unserved)));

                // Median HHI
                tr.appendChild(createElement('td', {}, c.median_hhi ? DataHandler.formatCurrency(c.median_hhi) : 'N/A'));

                // Density
                tr.appendChild(createElement('td', {}, DataHandler.formatNumber(c.housing_density)));

                tbody.appendChild(tr);
            });

            // Add event listeners to rows
            this._attachRowListeners(tbody);
        },

        _attachRowListeners(tbody) {
            tbody.querySelectorAll('tr').forEach((row) => {
                row.addEventListener('mouseenter', () => {
                    const fips = row.dataset.fips;
                    if (!InfoPanel.pinnedCounty) {
                        InfoPanel.showCountyInfo(fips);
                        MapRenderer.highlightCounty(fips);
                    }
                });
                row.addEventListener('mouseleave', () => {
                    if (!InfoPanel.pinnedCounty) {
                        InfoPanel.hideInfo();
                        MapRenderer.clearHighlight();
                    }
                });
                row.addEventListener('click', () => {
                    const fips = row.dataset.fips;
                    if (InfoPanel.pinnedCounty === fips) {
                        InfoPanel.unpinCounty();
                    } else {
                        InfoPanel.pinCounty(fips);
                    }
                });
                // Keyboard support for rows
                row.setAttribute('tabindex', '0');
                row.addEventListener('keydown', (e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        const fips = row.dataset.fips;
                        if (InfoPanel.pinnedCounty === fips) {
                            InfoPanel.unpinCounty();
                        } else {
                            InfoPanel.pinCounty(fips);
                        }
                    }
                });
            });
        },

        setupEventListeners() {
            // Column sorting
            document.querySelectorAll('#county-table th[data-sort]').forEach((th) => {
                th.addEventListener('click', () => {
                    const column = th.dataset.sort;

                    // Update sort direction
                    if (this.sortColumn === column) {
                        this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc';
                    } else {
                        this.sortColumn = column;
                        this.sortDirection = 'desc';
                    }

                    // Update header classes
                    document.querySelectorAll('#county-table th').forEach((h) => {
                        h.classList.remove('sort-asc', 'sort-desc');
                    });
                    th.classList.add(this.sortDirection === 'asc' ? 'sort-asc' : 'sort-desc');

                    this.renderTable();
                });
            });

            // Search with debouncing
            const searchInput = document.getElementById('table-search');
            if (searchInput) {
                searchInput.addEventListener('input', (e) => {
                    // Clear previous timer
                    if (this._searchDebounceTimer) {
                        clearTimeout(this._searchDebounceTimer);
                    }
                    // Debounce search (200ms)
                    this._searchDebounceTimer = setTimeout(() => {
                        this.searchTerm = e.target.value.toLowerCase();
                        this.applyFilters();
                    }, 200);
                });
            }
        },

        applyFilters() {
            const rows = document.querySelectorAll('#county-table-body tr');
            rows.forEach((row) => {
                const fips = row.dataset.fips;
                const county = DataHandler.getCountyData(fips);
                if (!county) {
                    row.classList.add('filtered-out');
                    return;
                }
                const matchesSearch = !this.searchTerm || county.name.toLowerCase().includes(this.searchTerm);
                const filtered = MapRenderer.isFiltered(county) || !matchesSearch;
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

})(typeof window !== 'undefined' ? window : global);
