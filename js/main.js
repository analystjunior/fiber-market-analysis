// Main application initialization

document.addEventListener('DOMContentLoaded', async () => {
    console.log('US Fiber Market Analysis initializing...');

    // Load data
    const dataLoaded = await DataHandler.loadData();
    if (!dataLoaded) {
        console.error('Failed to load data');
        document.getElementById('map').innerHTML = '<p style="color: red; text-align: center; padding: 2rem;">Failed to load data</p>';
        return;
    }

    console.log('Data loaded:', DataHandler.getAllStates().length, 'states,', DataHandler.getAllCounties().length, 'NY counties');

    // Initialize components
    InfoPanel.init();
    await MapRenderer.init('map');
    TableManager.init();

    // Setup UI controls (only used in NY view)
    setupControls();

    console.log('Application initialized');
});

function setupControls() {
    // Layer toggle buttons
    document.querySelectorAll('#layer-toggle .toggle-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('#layer-toggle .toggle-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            MapRenderer.setLayer(btn.dataset.layer);
        });
    });

    // Min population filter
    document.getElementById('min-pop').addEventListener('change', (e) => {
        MapRenderer.setFilters({ minPop: parseInt(e.target.value) || 0 });
    });

    // Min density filter
    document.getElementById('min-density').addEventListener('change', (e) => {
        MapRenderer.setFilters({ minDensity: parseInt(e.target.value) || 0 });
    });

    // Exclude NYC checkbox
    document.getElementById('exclude-nyc').addEventListener('change', (e) => {
        MapRenderer.setFilters({ excludeNYC: e.target.checked });
    });
}
