// Map rendering module

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

    async init(containerId) {
        const container = document.getElementById(containerId);
        if (!container) return false;

        // Create SVG
        this.svg = d3.select(`#${containerId}`)
            .append('svg')
            .attr('viewBox', '0 0 960 600')
            .attr('preserveAspectRatio', 'xMidYMid meet');

        // Background
        this.svg.append('rect')
            .attr('width', '100%')
            .attr('height', '100%')
            .attr('fill', '#f8fafc');

        this.mapGroup = this.svg.append('g').attr('class', 'map-container');

        // Start with US view
        await this.renderUSMap();
        this.updateLegendForUS();

        return true;
    },

    // ===== US MAP VIEW =====
    async renderUSMap() {
        this.currentView = 'us';
        this.mapGroup.selectAll('*').remove();
        this.svg.attr('viewBox', '0 0 960 600');

        const geojson = DataHandler.getUSGeoJSON();
        if (!geojson) return;

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
            .attr('class', d => {
                const stateCode = this.getStateCode(d);
                return stateCode === 'NY' ? 'state ny-state' : 'state';
            })
            .attr('d', this.path)
            .attr('data-state', d => this.getStateCode(d))
            .each((d, i, nodes) => {
                this.updateStateColor(d3.select(nodes[i]), d);
            })
            .on('mouseenter', (event, d) => this.handleStateMouseEnter(d))
            .on('mouseleave', () => this.handleStateMouseLeave())
            .on('click', (event, d) => this.handleStateClick(d));

        // Update UI
        document.getElementById('map-title').textContent = 'United States - Fiber Coverage by State';
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
        this.svg.attr('viewBox', '0 0 800 600');

        const geojson = DataHandler.getGeoJSON();
        if (!geojson) return;

        // Calculate bounds and create projection for NY
        const bounds = d3.geoBounds(geojson);
        const center = [(bounds[0][0] + bounds[1][0]) / 2, (bounds[0][1] + bounds[1][1]) / 2];

        this.projection = d3.geoMercator()
            .center(center)
            .scale(6000)
            .translate([400, 300]);

        this.path = d3.geoPath().projection(this.projection);

        // Render counties
        this.mapGroup.selectAll('.county')
            .data(geojson.features)
            .enter()
            .append('path')
            .attr('class', 'county')
            .attr('d', this.path)
            .attr('data-fips', d => d.id)
            .each((d, i, nodes) => {
                this.updateCountyColor(d3.select(nodes[i]), d.id);
            })
            .on('mouseenter', (event, d) => this.handleCountyMouseEnter(d))
            .on('mouseleave', () => this.handleCountyMouseLeave())
            .on('click', (event, d) => this.handleCountyClick(d));

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
        document.getElementById('map-title').textContent = 'New York State Counties';
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
        const legendItems = ColorScales.getLegend('penetration');

        let html = '<div class="legend">';
        html += '<span style="font-size: 0.7rem; color: #64748b; margin-right: 0.5rem;">Fiber Penetration:</span>';
        legendItems.forEach(item => {
            html += `<div class="legend-item">
                <div class="legend-color" style="background: ${item.color};"></div>
                <span>${item.label}</span>
            </div>`;
        });
        html += '<div class="legend-item" style="margin-left: 1rem;"><div class="legend-color" style="background: #10b981;"></div><span>NY (Click to explore)</span></div>';
        html += '</div>';
        container.innerHTML = html;
    },

    updateLegend() {
        const container = document.getElementById('legend-container');
        const legendItems = ColorScales.getLegend(this.currentLayer);

        let html = '<div class="legend">';
        legendItems.forEach(item => {
            html += `<div class="legend-item">
                <div class="legend-color" style="background: ${item.color};"></div>
                <span>${item.label}</span>
            </div>`;
        });
        html += '</div>';
        container.innerHTML = html;
    },

    highlightCounty(fips) {
        if (this.currentView !== 'ny') return;
        this.mapGroup.selectAll('.county')
            .style('opacity', d => d.id === fips ? 1 : 0.3);
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
        document.querySelector('.pin-indicator').style.display = 'none';
        this.renderUSMap();
        this.updateLegendForUS();
        InfoPanel.hideInfo();
    }
};

// Info Panel
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
        document.querySelector('.pin-indicator').style.display = 'flex';
        // Highlight the pinned county on map
        MapRenderer.mapGroup.selectAll('.county')
            .classed('pinned', d => d.id === fips);
    },

    unpinCounty() {
        this.pinnedCounty = null;
        document.querySelector('.pin-indicator').style.display = 'none';
        MapRenderer.mapGroup.selectAll('.county').classed('pinned', false);
        this.hideInfo();
    },

    showStateInfo(stateCode) {
        const data = DataHandler.getStateData(stateCode);

        document.querySelector('.state-name').textContent = data ? data.state : stateCode;

        if (data) {
            document.getElementById('state-housing').textContent = DataHandler.formatNumber(data.totalHousingUnits);
            document.getElementById('state-fiber').textContent = DataHandler.formatNumber(data.totalFiberPassings);
            document.getElementById('state-penetration').textContent = data.fiberPenetration.toFixed(1) + '%';

            // Operators list
            const operatorsList = document.getElementById('state-operators-list');
            operatorsList.innerHTML = '';
            if (data.operators && data.operators.length > 0) {
                data.operators.slice(0, 6).forEach(op => {
                    const li = document.createElement('li');
                    li.innerHTML = `
                        <span class="operator-name">${op.name}</span>
                        <span class="operator-passings">${DataHandler.formatNumber(op.passings)}</span>
                    `;
                    operatorsList.appendChild(li);
                });
            }
        }

        // Show/hide click hint for NY
        const clickHint = document.querySelector('.click-hint');
        if (clickHint) {
            clickHint.style.display = stateCode === 'NY' ? 'block' : 'none';
        }

        this.defaultEl.style.display = 'none';
        this.countyInfoEl.style.display = 'none';
        this.stateInfoEl.style.display = 'block';
    },

    showCountyInfo(fips) {
        const data = DataHandler.getCountyData(fips);
        if (!data) return;

        // County name
        document.querySelector('.county-name').textContent = data.name + ' County';

        // Score bars
        document.getElementById('attr-score').style.width = (data.attractiveness_index * 100) + '%';
        document.getElementById('attr-value').textContent = data.attractiveness_index.toFixed(2);

        document.getElementById('demo-score').style.width = (data.demo_score * 100) + '%';
        document.getElementById('demo-value').textContent = data.demo_score.toFixed(2);

        document.getElementById('pen-score').style.width = (data.fiber_penetration * 100) + '%';
        document.getElementById('pen-value').textContent = (data.fiber_penetration * 100).toFixed(0) + '%';

        // Fiber stats
        document.getElementById('total-bsls').textContent = DataHandler.formatNumber(data.total_bsls);
        document.getElementById('fiber-served').textContent = DataHandler.formatNumber(data.fiber_served);
        document.getElementById('fiber-unserved').textContent = DataHandler.formatNumber(data.fiber_unserved);
        document.getElementById('penetration-rate').textContent = DataHandler.formatPercent(data.fiber_penetration);

        // Demographics
        document.getElementById('population').textContent = DataHandler.formatNumber(data.population_2023);

        const popGrowthEl = document.getElementById('pop-growth');
        if (data.pop_growth_pct !== null) {
            popGrowthEl.textContent = (data.pop_growth_pct >= 0 ? '+' : '') + data.pop_growth_pct.toFixed(1) + '%';
            popGrowthEl.className = 'stat-value ' + (data.pop_growth_pct >= 0 ? 'positive' : 'negative');
        } else {
            popGrowthEl.textContent = 'N/A';
            popGrowthEl.className = 'stat-value';
        }

        document.getElementById('housing-units').textContent = DataHandler.formatNumber(data.housing_units);
        document.getElementById('housing-density').textContent = DataHandler.formatNumber(data.housing_density) + '/sq mi';

        // Demographics & Economic
        document.getElementById('median-hhi').textContent = DataHandler.formatCurrency(data.median_hhi);
        document.getElementById('median-rent').textContent = DataHandler.formatCurrency(data.median_rent);
        document.getElementById('owner-occ').textContent = DataHandler.formatPercentDirect(data.owner_occupied_pct);
        document.getElementById('wfh-pct').textContent = DataHandler.formatPercentDirect(data.wfh_pct);
        document.getElementById('median-home-value').textContent = DataHandler.formatCurrency(data.median_home_value);

        // Top 5 Fiber Operators
        const operatorsList = document.getElementById('operators-list');
        operatorsList.innerHTML = '';

        if (data.operators && data.operators.length > 0) {
            // Sort by passings and take top 5
            const topOperators = [...data.operators].sort((a, b) => b.passings - a.passings).slice(0, 5);
            topOperators.forEach(op => {
                const li = document.createElement('li');
                li.innerHTML = `
                    <span class="operator-name">${op.name}</span>
                    <span class="operator-passings">${DataHandler.formatNumber(op.passings)} passings</span>
                `;
                operatorsList.appendChild(li);
            });
        } else {
            operatorsList.innerHTML = '<li style="color: #64748b;">No fiber operators reported</li>';
        }

        this.defaultEl.style.display = 'none';
        this.stateInfoEl.style.display = 'none';
        this.countyInfoEl.style.display = 'block';
    },

    hideInfo() {
        this.defaultEl.style.display = 'block';
        this.countyInfoEl.style.display = 'none';
        if (this.stateInfoEl) this.stateInfoEl.style.display = 'none';
    }
};

// Table Manager
const TableManager = {
    sortColumn: 'attractiveness_index',
    sortDirection: 'desc',
    searchTerm: '',

    init() {
        this.setupEventListeners();
    },

    renderTable() {
        const tbody = document.getElementById('county-table-body');
        const counties = DataHandler.getAllCounties();

        // Sort
        counties.sort((a, b) => {
            let aVal = a[this.sortColumn];
            let bVal = b[this.sortColumn];

            if (typeof aVal === 'string') {
                aVal = aVal.toLowerCase();
                bVal = bVal.toLowerCase();
            }

            if (aVal === null || aVal === undefined) aVal = -Infinity;
            if (bVal === null || bVal === undefined) bVal = -Infinity;

            if (this.sortDirection === 'asc') {
                return aVal > bVal ? 1 : -1;
            } else {
                return aVal < bVal ? 1 : -1;
            }
        });

        tbody.innerHTML = counties.map(c => `
            <tr data-fips="${c.geoid}" class="${MapRenderer.isFiltered(c) ? 'filtered-out' : ''}">
                <td>${c.name}</td>
                <td>${c.attractiveness_index.toFixed(2)}</td>
                <td>${c.demo_score.toFixed(2)}</td>
                <td>${(c.fiber_penetration * 100).toFixed(1)}%</td>
                <td>${DataHandler.formatNumber(c.fiber_unserved)}</td>
                <td>${c.median_hhi ? DataHandler.formatCurrency(c.median_hhi) : 'N/A'}</td>
                <td>${DataHandler.formatNumber(c.housing_density)}</td>
            </tr>
        `).join('');

        // Row hover and click handlers
        tbody.querySelectorAll('tr').forEach(row => {
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
        });
    },

    setupEventListeners() {
        // Column sorting
        document.querySelectorAll('#county-table th[data-sort]').forEach(th => {
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
                document.querySelectorAll('#county-table th').forEach(h => {
                    h.classList.remove('sort-asc', 'sort-desc');
                });
                th.classList.add(this.sortDirection === 'asc' ? 'sort-asc' : 'sort-desc');

                this.renderTable();
            });
        });

        // Search
        document.getElementById('table-search').addEventListener('input', (e) => {
            this.searchTerm = e.target.value.toLowerCase();
            this.applyFilters();
        });
    },

    applyFilters() {
        const rows = document.querySelectorAll('#county-table-body tr');
        rows.forEach(row => {
            const fips = row.dataset.fips;
            const county = DataHandler.getCountyData(fips);
            const matchesSearch = !this.searchTerm || county.name.toLowerCase().includes(this.searchTerm);
            const filtered = MapRenderer.isFiltered(county) || !matchesSearch;
            row.classList.toggle('filtered-out', filtered);
        });
    }
};
