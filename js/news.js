/**
 * NewsPanel — loads FBA news from Supabase, renders the news tab,
 * and surfaces county-relevant articles in the county info panel.
 */
(function(global) {
    'use strict';

    var _articles    = [];    // all loaded (up to 200, last 90 days)
    var _countyIndex = {};    // geoid → [article, ...]
    var _stateIndex  = {};    // state_code → [article, ...]
    var _loaded      = false;
    var _loading     = false;
    var _stateFilter = '';
    var _searchTerm  = '';

    // ── Helpers ──────────────────────────────────────────────────

    function formatDate(iso) {
        if (!iso) return '';
        var d = new Date(iso);
        return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    }

    function buildIndexes() {
        _countyIndex = {};
        _stateIndex  = {};
        _articles.forEach(function(art) {
            (art.county_tags || []).forEach(function(g) {
                if (!_countyIndex[g]) _countyIndex[g] = [];
                _countyIndex[g].push(art);
            });
            (art.state_tags || []).forEach(function(s) {
                if (!_stateIndex[s]) _stateIndex[s] = [];
                _stateIndex[s].push(art);
            });
        });
    }

    function safeText(el, text) {
        el.textContent = text || '';
    }

    function makeLink(href, text, className) {
        var a = document.createElement('a');
        a.href = href;
        a.target = '_blank';
        a.rel = 'noopener noreferrer';
        if (className) a.className = className;
        a.textContent = text;
        return a;
    }

    // ── Article card (news tab) ───────────────────────────────────

    function renderArticleCard(art) {
        var card = document.createElement('div');
        card.className = 'news-card';

        var header = document.createElement('div');
        header.className = 'news-card-header';

        var title = makeLink(art.link, art.title, 'news-card-title');
        var date = document.createElement('span');
        date.className = 'news-card-date';
        safeText(date, formatDate(art.published_at));

        header.appendChild(title);
        header.appendChild(date);
        card.appendChild(header);

        if (art.excerpt) {
            var excerpt = document.createElement('p');
            excerpt.className = 'news-card-excerpt';
            safeText(excerpt, art.excerpt.length > 200 ? art.excerpt.slice(0, 200) + '…' : art.excerpt);
            card.appendChild(excerpt);
        }

        // Tag chips
        var tags = (art.county_tags || []).length + (art.state_tags || []).length;
        if (tags > 0) {
            var chipRow = document.createElement('div');
            chipRow.className = 'news-card-chips';
            // State chips
            (art.state_tags || []).forEach(function(s) {
                var chip = document.createElement('span');
                chip.className = 'news-chip news-chip-state';
                safeText(chip, s);
                chipRow.appendChild(chip);
            });
            // Show county chips (up to 4 then "+N more")
            var counties = art.county_tags || [];
            var shown = counties.slice(0, 4);
            shown.forEach(function(geoid) {
                var chip = document.createElement('span');
                chip.className = 'news-chip news-chip-county';
                // Resolve county name from DataHandler if loaded
                var cData = DataHandler.getCountyData(geoid);
                safeText(chip, cData ? cData.name + ', ' + (cData.state_code || '') : geoid);
                chipRow.appendChild(chip);
            });
            if (counties.length > 4) {
                var more = document.createElement('span');
                more.className = 'news-chip news-chip-more';
                safeText(more, '+' + (counties.length - 4) + ' more');
                chipRow.appendChild(more);
            }
            card.appendChild(chipRow);
        }

        return card;
    }

    // ── News tab rendering ────────────────────────────────────────

    function renderNewsList() {
        var container = document.getElementById('news-list');
        if (!container) return;
        container.textContent = '';

        var term = _searchTerm.toLowerCase();
        var filtered = _articles.filter(function(art) {
            var matchState = !_stateFilter ||
                (art.state_tags || []).indexOf(_stateFilter) !== -1;
            var matchSearch = !term ||
                art.title.toLowerCase().indexOf(term) !== -1 ||
                (art.excerpt || '').toLowerCase().indexOf(term) !== -1;
            return matchState && matchSearch;
        });

        if (filtered.length === 0) {
            var empty = document.createElement('p');
            empty.className = 'news-empty';
            safeText(empty, _loaded ? 'No articles match your filter.' : 'Loading…');
            container.appendChild(empty);
            return;
        }

        filtered.forEach(function(art) {
            container.appendChild(renderArticleCard(art));
        });
    }

    function populateStateFilter() {
        var sel = document.getElementById('news-state-filter');
        if (!sel) return;
        // Keep "All States" option, remove any previous dynamic ones
        while (sel.options.length > 1) sel.remove(1);
        var states = Object.keys(_stateIndex).sort();
        states.forEach(function(s) {
            var opt = document.createElement('option');
            opt.value = s;
            opt.textContent = s;
            sel.appendChild(opt);
        });
    }

    // ── County panel — Related News ───────────────────────────────

    function renderCountyNews(geoid) {
        var section = document.getElementById('county-news-section');
        if (!section) return;

        var articles = _countyIndex[geoid] || [];
        // Also include state-level articles for this county's state
        var county = DataHandler.getCountyData(geoid);
        var stateArts = county ? (_stateIndex[county.state_code] || []) : [];
        // Merge, deduplicate by id, prefer county-tagged first
        var seen = {};
        var combined = [];
        articles.forEach(function(a) { if (!seen[a.id]) { seen[a.id] = true; combined.push(a); } });
        stateArts.forEach(function(a) { if (!seen[a.id] && combined.length < 5) { seen[a.id] = true; combined.push(a); } });

        var list = document.getElementById('county-news-list');
        if (!list) return;
        list.textContent = '';

        if (combined.length === 0) {
            section.style.display = 'none';
            return;
        }

        section.style.display = '';
        combined.slice(0, 5).forEach(function(art) {
            var li = document.createElement('li');
            li.className = 'county-news-item';

            var link = makeLink(art.link, art.title, 'county-news-title');
            var date = document.createElement('span');
            date.className = 'county-news-date';
            safeText(date, formatDate(art.published_at));

            // Mark if it's a county-specific match vs state-level
            if ((art.county_tags || []).indexOf(geoid) !== -1) {
                var dot = document.createElement('span');
                dot.className = 'county-news-dot';
                dot.title = 'Mentions this county';
                safeText(dot, '●');
                li.appendChild(dot);
            }

            li.appendChild(link);
            li.appendChild(date);
            list.appendChild(li);
        });

        // "See all" link → switch to news tab filtered to this state
        var seeAll = document.getElementById('county-news-see-all');
        if (seeAll) {
            seeAll.onclick = function() {
                if (county) {
                    NewsPanel.openTabForState(county.state_code);
                } else {
                    NewsPanel.openTab();
                }
            };
        }
    }

    // ── Tab control ───────────────────────────────────────────────

    var NewsPanel = {
        init: function() {
            var tabBtn = document.getElementById('tab-btn-news');
            var rankBtn = document.getElementById('tab-btn-rankings');
            if (tabBtn) {
                tabBtn.addEventListener('click', function() { NewsPanel.openTab(); });
            }
            if (rankBtn) {
                rankBtn.addEventListener('click', function() { NewsPanel.closeTab(); });
            }

            var searchEl = document.getElementById('news-search');
            if (searchEl) {
                searchEl.addEventListener('input', function(e) {
                    _searchTerm = e.target.value;
                    renderNewsList();
                });
            }

            var stateEl = document.getElementById('news-state-filter');
            if (stateEl) {
                stateEl.addEventListener('change', function(e) {
                    _stateFilter = e.target.value;
                    renderNewsList();
                });
            }
        },

        load: function() {
            if (_loaded || _loading) return Promise.resolve();
            _loading = true;
            var sb = DataHandler.getSupabaseClient();
            var since = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString();
            return sb
                .from('news_articles')
                .select('id,title,link,published_at,excerpt,county_tags,state_tags')
                .gte('published_at', since)
                .order('published_at', { ascending: false })
                .limit(200)
                .then(function(result) {
                    _loading = false;
                    if (result.error) { console.error('News load error:', result.error); return; }
                    _articles = result.data || [];
                    _loaded = true;
                    buildIndexes();
                    populateStateFilter();
                    renderNewsList();
                })
                .catch(function(err) {
                    _loading = false;
                    console.error('News fetch failed:', err);
                });
        },

        openTab: function() {
            var newsSection  = document.getElementById('news-section');
            var tableSection = document.querySelector('.table-section');
            var tabNews      = document.getElementById('tab-btn-news');
            var tabRank      = document.getElementById('tab-btn-rankings');
            if (newsSection)  newsSection.style.display  = '';
            if (tableSection) tableSection.style.display = 'none';
            if (tabNews)  tabNews.classList.add('active');
            if (tabRank)  tabRank.classList.remove('active');
            // Lazy-load on first open
            if (!_loaded) {
                var container = document.getElementById('news-list');
                if (container && !container.hasChildNodes()) {
                    var loading = document.createElement('p');
                    loading.className = 'news-empty';
                    safeText(loading, 'Loading articles…');
                    container.appendChild(loading);
                }
                this.load();
            } else {
                renderNewsList();
            }
        },

        closeTab: function() {
            var newsSection  = document.getElementById('news-section');
            var tableSection = document.querySelector('.table-section');
            var tabNews      = document.getElementById('tab-btn-news');
            var tabRank      = document.getElementById('tab-btn-rankings');
            if (newsSection)  newsSection.style.display  = 'none';
            if (tableSection) tableSection.style.display = '';
            if (tabNews)  tabNews.classList.remove('active');
            if (tabRank)  tabRank.classList.add('active');
        },

        openTabForState: function(stateCode) {
            _stateFilter = stateCode || '';
            var sel = document.getElementById('news-state-filter');
            if (sel) sel.value = _stateFilter;
            this.openTab();
        },

        // Called by InfoPanel when a county is pinned
        showForCounty: function(geoid) {
            if (!_loaded) {
                this.load().then(function() { renderCountyNews(geoid); });
            } else {
                renderCountyNews(geoid);
            }
        },

        hideCountyNews: function() {
            var section = document.getElementById('county-news-section');
            if (section) section.style.display = 'none';
        }
    };

    global.NewsPanel = NewsPanel;

})(typeof window !== 'undefined' ? window : global);
