/**
 * Market Metrics Dashboard - JavaScript
 * Handles API calls, chart rendering, user interactions, and cross-filtering.
 */

// State
let selectedFile = '';
let allDistricts = [];
let selectedDistricts = [];

// Chart Instances
let charts = {
    room: null,
    size: null,
    banos: null,
    tipo: null,
    barrio: null,
    zona: null,
    specialStatus: null,
    altura: null,
    terraza: null,
    garaje: null,
    trastero: null,
    estado: null
};

// Raw data storage for cross-filtering
let rawData = null;
let activeFilters = {
    district: null,
    room: null,      // { raw: number }
    size: null,      // { min: number, max: number }
    banos: null,     // { raw: number/string }
    tipo: null,      // { raw: string }
    barrio: null,    // { raw: string }
    zona: null,      // { raw: string }
    specialStatus: null, // { raw: string } (Okupado/Copropiedad/Nuda)
    altura: null,    // { raw: string }
    terraza: null,   // { raw: boolean }
    garaje: null,    // { raw: boolean }
    trastero: null,  // { raw: boolean }
    estado: null     // { raw: string }
};

// DOM Elements
const fileSelect = document.getElementById('fileSelect');
const districtGroup = document.getElementById('districtGroup');
const districtSearch = document.getElementById('districtSearch');
const districtList = document.getElementById('districtList');
const districtInfo = document.getElementById('districtInfo');
const loadBtn = document.getElementById('loadBtn');
const statsBar = document.getElementById('statsBar');
const chartsGrid = document.getElementById('chartsGrid');
const emptyState = document.getElementById('emptyState');

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadFiles();
    setupEventListeners();
});

function setupEventListeners() {
    fileSelect.addEventListener('change', onFileChange);
    districtSearch.addEventListener('input', filterDistrictList);
    loadBtn.addEventListener('click', loadAnalytics);
}

// API Calls & Basic Setup (unchanged logic)
async function loadFiles() {
    try {
        const res = await fetch('/api/files');
        const data = await res.json();
        fileSelect.innerHTML = '<option value="">Select a VENTA file...</option>';
        if (data.files && data.files.length > 0) {
            data.files.forEach(file => {
                const option = document.createElement('option');
                option.value = file.path;
                option.textContent = file.name;
                fileSelect.appendChild(option);
            });
        } else {
            fileSelect.innerHTML = '<option value="">No VENTA files found</option>';
        }
    } catch (err) {
        console.error('Error loading files:', err);
        fileSelect.innerHTML = '<option value="">Error loading files</option>';
    }
}

async function onFileChange() {
    selectedFile = fileSelect.value;
    selectedDistricts = [];
    clearAllFilters();

    if (!selectedFile) {
        districtGroup.style.display = 'none';
        loadBtn.disabled = true;
        return;
    }
    loadBtn.disabled = false;
    try {
        const res = await fetch(`/api/districts?file=${encodeURIComponent(selectedFile)}`);
        const data = await res.json();
        if (data.districts && data.districts.length > 0) {
            allDistricts = data.districts;
            renderDistrictList(allDistricts);
            districtGroup.style.display = 'block';
        }
    } catch (err) { console.error('Error loading districts:', err); }
}

function renderDistrictList(districts) {
    districtList.innerHTML = '';
    const selectAllItem = document.createElement('div');
    selectAllItem.className = 'checkbox-item';
    selectAllItem.innerHTML = `
        <input type="checkbox" id="selectAll" ${selectedDistricts.length === 0 ? 'checked' : ''}>
        <label for="selectAll"><strong>All Districts</strong></label>
    `;
    selectAllItem.querySelector('input').addEventListener('change', (e) => {
        if (e.target.checked) {
            selectedDistricts = [];
            renderDistrictList(allDistricts);
        }
        updateDistrictInfo();
    });
    districtList.appendChild(selectAllItem);
    districts.forEach((district, idx) => {
        const item = document.createElement('div');
        item.className = 'checkbox-item';
        const isChecked = selectedDistricts.includes(district);
        item.innerHTML = `
            <input type="checkbox" id="district-${idx}" ${isChecked ? 'checked' : ''}>
            <label for="district-${idx}">${district}</label>
        `;
        item.querySelector('input').addEventListener('change', (e) => {
            if (e.target.checked) {
                if (!selectedDistricts.includes(district)) selectedDistricts.push(district);
            } else {
                selectedDistricts = selectedDistricts.filter(d => d !== district);
            }
            document.getElementById('selectAll').checked = selectedDistricts.length === 0;
            updateDistrictInfo();
        });
        districtList.appendChild(item);
    });
    updateDistrictInfo();
}
function filterDistrictList() {
    const query = districtSearch.value.toLowerCase();
    renderDistrictList(allDistricts.filter(d => d.toLowerCase().includes(query)));
}
function updateDistrictInfo() {
    districtInfo.textContent = selectedDistricts.length === 0 ? 'Selected: All' : `Selected: ${selectedDistricts.length} district(s)`;
}

async function loadAnalytics() {
    loadBtn.disabled = true;
    loadBtn.innerHTML = '<span class="btn-icon">⏳</span> Loading...';
    try {
        const res = await fetch('/api/analytics', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ file: selectedFile, districts: selectedDistricts })
        });
        const data = await res.json();
        if (data.error) { alert('Error: ' + data.error); return; }
        rawData = data;
        clearAllFilters();
        emptyState.style.display = 'none';
        statsBar.style.display = 'grid';
        chartsGrid.style.display = 'grid';
        updateDashboardFromData(filterData());
    } catch (err) {
        console.error('Error loading analytics:', err);
        alert('Error loading analytics');
    } finally {
        loadBtn.disabled = false;
        loadBtn.innerHTML = '<span class="btn-icon">📈</span> Load Analytics';
    }
}

// Core Filtering
function filterData() {
    if (!rawData || !rawData.raw_properties) return null;
    let filteredProps = rawData.raw_properties;

    // Generic filter helper
    const applyFilter = (key, checkFn) => {
        if (activeFilters[key]) {
            filteredProps = filteredProps.filter(checkFn);
        }
    };

    applyFilter('district', p => p.district === activeFilters.district);
    applyFilter('room', p => p.rooms === activeFilters.room.raw);
    applyFilter('size', p => p.size >= activeFilters.size.min && p.size < activeFilters.size.max);
    applyFilter('banos', p => p.banos === activeFilters.banos.raw);
    applyFilter('tipo', p => p.tipo === activeFilters.tipo.raw);
    applyFilter('barrio', p => p.barrio === activeFilters.barrio.raw);
    applyFilter('zona', p => p.zona === activeFilters.zona.raw);
    applyFilter('altura', p => p.altura === activeFilters.altura.raw);
    applyFilter('estado', p => p.estado === activeFilters.estado.raw);

    // Boolean filters
    applyFilter('terraza', p => p.terraza === activeFilters.terraza.raw);
    applyFilter('garaje', p => p.garaje === activeFilters.garaje.raw);
    applyFilter('trastero', p => p.trastero === activeFilters.trastero.raw);

    // Special status (combined check)
    if (activeFilters.specialStatus) {
        const status = activeFilters.specialStatus.raw; // 'okupado', 'copropiedad', etc.
        if (status === 'okupado') filteredProps = filteredProps.filter(p => p.okupado);
        else if (status === 'copropiedad') filteredProps = filteredProps.filter(p => p.copropiedad);
        else if (status === 'nuda propiedad') filteredProps = filteredProps.filter(p => p.nuda_propiedad);
    }

    const metrics = calculateMetrics(filteredProps);
    metrics.filtered_properties = filteredProps; // Attach filtered properties to metrics
    return metrics;
}

// Metric Calculation
function calculateMetrics(properties) {
    const total = properties.length;

    // District Summary
    const districtCounts = {};
    properties.forEach(p => districtCounts[p.district] = (districtCounts[p.district] || 0) + 1);
    const districtSummary = Object.entries(districtCounts).map(([k, v]) => ({ _sheet: k, count: v }));

    // Categorical Helper
    const getDist = (field, labelFn = l => l) => {
        const counts = {};
        properties.forEach(p => {
            if (p[field] !== undefined) counts[p[field]] = (counts[p[field]] || 0) + 1;
        });
        return Object.entries(counts).map(([k, v]) => ({ label: labelFn(k), value: v, raw: isNaN(k) ? k : window.parseFloat(k) })); // Keep raw types somewhat sane
    };

    // Boolean Helper
    const getBoolDist = (field) => {
        let yes = 0, no = 0;
        properties.forEach(p => {
            if (p[field] === true) yes++;
            if (p[field] === false) no++;
        });
        return [
            { label: 'Sí', value: yes, raw: true },
            { label: 'No', value: no, raw: false }
        ];
    };

    // Special Status Helper
    const getSpecialDist = () => {
        let ok = 0, co = 0, nu = 0;
        properties.forEach(p => {
            if (p.okupado) ok++;
            if (p.copropiedad) co++;
            if (p.nuda_propiedad) nu++;
        });
        const res = [];
        if (ok > 0) res.push({ label: 'Okupado', value: ok, raw: 'okupado' });
        if (co > 0) res.push({ label: 'Copropiedad', value: co, raw: 'copropiedad' });
        if (nu > 0) res.push({ label: 'Nuda Propiedad', value: nu, raw: 'nuda propiedad' });
        return res;
    };

    // Distributions
    const roomDistribution = getDist('rooms', r => `${r} hab`).sort((a, b) => a.raw - b.raw);
    const banosDistribution = getDist('banos', b => `${b} baños`).sort((a, b) => a.raw - b.raw);
    const tipoDistribution = getDist('tipo');
    const barrioDistribution = getDist('barrio');
    const zonaDistribution = getDist('zona');
    const alturaDistribution = getDist('altura');
    const estadoDistribution = getDist('estado');

    const terrazaDistribution = getBoolDist('terraza');
    const garajeDistribution = getBoolDist('garaje');
    const trasteroDistribution = getBoolDist('trastero');
    const specialStatusDistribution = getSpecialDist();

    // Size Distribution
    const sizeDistribution = [];
    const binSize = 20; const maxBin = 300; const bins = {};
    for (let i = 0; i < maxBin; i += binSize) bins[i] = 0;
    bins['300+'] = 0;
    properties.forEach(p => {
        if (p.size !== undefined) {
            if (p.size >= maxBin) bins['300+']++;
            else {
                const start = Math.floor(p.size / binSize) * binSize;
                bins[start] = (bins[start] || 0) + 1;
            }
        }
    });
    Object.keys(bins).forEach(k => {
        if (bins[k] > 0) {
            if (k === '300+') sizeDistribution.push({ label: '> 300 m²', value: bins[k], min: 300, max: 99999 });
            else {
                const s = parseInt(k);
                sizeDistribution.push({ label: `${s}-${s + binSize} m²`, value: bins[k], min: s, max: s + binSize });
            }
        }
    });
    sizeDistribution.sort((a, b) => a.min - b.min);

    // Price Stats
    const prices = properties.filter(p => p.price_per_m2 !== undefined).map(p => p.price_per_m2);
    let priceStats = { min: '-', max: '-', mean: '-', median: '-' };
    if (prices.length > 0) {
        prices.sort((a, b) => a - b);
        const sum = prices.reduce((a, b) => a + b, 0);
        const mid = Math.floor(prices.length / 2);
        const median = prices.length % 2 === 0 ? (prices[mid - 1] + prices[mid]) / 2 : prices[mid];
        priceStats = {
            min: Math.min(...prices).toFixed(2),
            max: Math.max(...prices).toFixed(2),
            mean: Math.round(sum / prices.length).toLocaleString(),
            median: Math.round(median).toLocaleString()
        };
    }

    return {
        total,
        distinct_districts: Object.keys(districtCounts).length,
        district_summary: districtSummary,
        room_distribution: roomDistribution,
        size_distribution: sizeDistribution,
        price_stats: priceStats,
        banos_distribution: banosDistribution,
        tipo_distribution: tipoDistribution,
        barrio_distribution: barrioDistribution,
        zona_distribution: zonaDistribution,
        special_status_distribution: specialStatusDistribution,
        altura_distribution: alturaDistribution,
        terraza_distribution: terrazaDistribution,
        garaje_distribution: garajeDistribution,
        trastero_distribution: trasteroDistribution,
        estado_distribution: estadoDistribution,
        filtered_properties: properties
    };
}

function updateDashboardFromData(metrics) {
    if (!metrics) return;

    // Scorecards
    document.getElementById('statTotal').textContent = metrics.total.toLocaleString();
    document.getElementById('statDistricts').textContent = metrics.distinct_districts;
    const avg = metrics.price_stats.mean; document.getElementById('statPriceAvg').textContent = avg !== '-' ? `€${avg}` : '-';
    const med = metrics.price_stats.median; document.getElementById('statPriceMedian').textContent = med !== '-' ? `€${med}` : '-';

    // Update charts
    renderDistrictTable(metrics.district_summary, metrics.total);

    renderDoughnut('room', metrics.room_distribution, activeFilters.room, { raw: 'raw' });
    renderDoughnut('banos', metrics.banos_distribution, activeFilters.banos, { raw: 'raw' });
    renderDoughnut('tipo', metrics.tipo_distribution, activeFilters.tipo, { raw: 'raw' });
    renderDoughnut('barrio', metrics.barrio_distribution, activeFilters.barrio, { raw: 'raw' }, true);
    renderDoughnut('zona', metrics.zona_distribution, activeFilters.zona, { raw: 'raw' }, true);
    renderDoughnut('specialStatus', metrics.special_status_distribution, activeFilters.specialStatus, { raw: 'raw' });
    renderDoughnut('altura', metrics.altura_distribution, activeFilters.altura, { raw: 'raw' });

    renderBar('terraza', metrics.terraza_distribution, activeFilters.terraza, { raw: 'raw' });
    renderBar('garaje', metrics.garaje_distribution, activeFilters.garaje, { raw: 'raw' });
    renderBar('trastero', metrics.trastero_distribution, activeFilters.trastero, { raw: 'raw' });

    renderHistogram('size', metrics.size_distribution, activeFilters.size);
    renderHistogram('estado', metrics.estado_distribution, activeFilters.estado, true); // Render col as hist for styling

    // Render properties table
    renderPropertiesTable(metrics.filtered_properties || []);

    updateFilterDisplay();
}

// Properties Table Renderer
function renderPropertiesTable(properties) {
    const panel = document.getElementById('propertiesTablePanel');
    const tbody = document.getElementById('propertiesTableBody');
    const countSpan = document.getElementById('propertiesCount');
    const infoSpan = document.getElementById('tableInfo');

    if (!panel || !tbody) return;

    // Show panel when we have data
    if (properties.length > 0) {
        panel.style.display = 'block';
    } else {
        panel.style.display = 'none';
        return;
    }

    // Update count
    countSpan.textContent = `(${properties.length.toLocaleString()})`;

    // Limit display for performance (show first 500)
    const maxDisplay = 500;
    const displayProps = properties.slice(0, maxDisplay);

    // Clear and rebuild table
    tbody.innerHTML = '';

    // Helper function for cell value display
    const val = (v, suffix = '') => {
        if (v === undefined || v === null || v === '') return '<span class="value-na">-</span>';
        return `${v}${suffix}`;
    };

    const boolVal = (v) => {
        if (v === true) return '<span class="bool-yes">Sí</span>';
        if (v === false) return '<span class="bool-no">No</span>';
        return '<span class="value-na">-</span>';
    };

    displayProps.forEach(p => {
        const row = document.createElement('tr');

        // Title with hyperlink
        let titleCell;
        if (p.titulo && p.url) {
            titleCell = `<a href="${p.url}" target="_blank" class="property-link">${p.titulo}</a>`;
        } else if (p.titulo) {
            titleCell = p.titulo;
        } else {
            titleCell = '<span class="value-na">-</span>';
        }

        // Format price with € symbol
        let priceCell;
        if (p.precio) {
            priceCell = `€${p.precio.toLocaleString()}`;
        } else {
            priceCell = '<span class="value-na">-</span>';
        }

        row.innerHTML = `
            <td>${titleCell}</td>
            <td>${val(p.rooms)}</td>
            <td>${val(p.banos)}</td>
            <td>${val(p.size, ' m²')}</td>
            <td>${priceCell}</td>
            <td>${boolVal(p.garaje)}</td>
            <td>${boolVal(p.trastero)}</td>
            <td>${boolVal(p.terraza)}</td>
        `;
        tbody.appendChild(row);
    });

    // Update info text
    if (properties.length > maxDisplay) {
        infoSpan.textContent = `Mostrando ${maxDisplay.toLocaleString()} de ${properties.length.toLocaleString()} propiedades`;
    } else {
        infoSpan.textContent = `Mostrando ${properties.length.toLocaleString()} propiedades`;
    }
}

// Generic Renderers
function renderDoughnut(key, data, activeFilter, filterMap, hideLegendSmall = false) {
    const canvas = document.getElementById(`${key}Chart`);
    const panel = canvas.closest('.chart-panel');
    const ctx = canvas.getContext('2d');
    if (charts[key]) charts[key].destroy();

    // Hide panel if no data
    if (!data || data.length === 0) {
        if (panel) panel.style.display = 'none';
        return;
    }
    if (panel) panel.style.display = 'block';

    const baseColors = ['#fb923c', '#f97316', '#ea580c', '#22c55e', '#06b6d4', '#3b82f6', '#8b5cf6', '#d946ef', '#ec4899', '#f43f5e'];
    const bgColors = data.map((d, i) => {
        // Highlight active
        if (activeFilter && d.raw === activeFilter.raw) return baseColors[i % baseColors.length];
        if (activeFilter) return baseColors[i % baseColors.length] + '40';
        return baseColors[i % baseColors.length];
    });

    // Calculate total for percentage
    const total = data.reduce((sum, d) => sum + d.value, 0);

    charts[key] = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: data.map(d => d.label),
            datasets: [{ data: data.map(d => d.value), backgroundColor: bgColors, borderWidth: 1 }]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            onClick: (e, els) => {
                if (els.length > 0) {
                    const item = data[els[0].index];
                    activeFilters[key] = (activeFilters[key] && activeFilters[key].raw === item.raw) ? null : { raw: item.raw, label: item.label };
                    updateFilter();
                }
            },
            plugins: {
                legend: { display: false },
                datalabels: {
                    color: '#fff',
                    font: { family: 'Outfit', size: 18, weight: 700 },
                    formatter: (value, context) => {
                        const label = data[context.dataIndex].label;
                        const pct = total > 0 ? ((value / total) * 100).toFixed(1) : 0;
                        return `${label}: ${value} (${pct}%)`;
                    },
                    display: (context) => {
                        const value = context.dataset.data[context.dataIndex];
                        const pct = total > 0 ? (value / total) * 100 : 0;
                        return pct >= 5; // Hide labels for segments < 5%
                    },
                    anchor: 'center',
                    align: 'center',
                    textAlign: 'center'
                }
            }
        },
        plugins: [ChartDataLabels]
    });
}

function renderBar(key, data, activeFilter, filterMap) {
    const canvas = document.getElementById(`${key}Chart`);
    const panel = canvas.closest('.chart-panel');
    const ctx = canvas.getContext('2d');
    if (charts[key]) charts[key].destroy();

    // Hide panel if no data
    if (!data || data.length === 0) {
        if (panel) panel.style.display = 'none';
        return;
    }
    if (panel) panel.style.display = 'block';

    const bgColors = data.map(d => {
        if (activeFilter && d.raw === activeFilter.raw) return '#fb923c';
        if (activeFilter) return 'rgba(251, 146, 60, 0.3)';
        return '#fb923c';
    });

    // Calculate total for percentage
    const total = data.reduce((sum, d) => sum + d.value, 0);

    charts[key] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => d.label),
            datasets: [{ label: 'Properties', data: data.map(d => d.value), backgroundColor: bgColors, borderRadius: 4 }]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            onClick: (e, els) => {
                if (els.length > 0) {
                    const item = data[els[0].index];
                    activeFilters[key] = (activeFilters[key] && activeFilters[key].raw === item.raw) ? null : { raw: item.raw, label: item.label };
                    updateFilter();
                }
            },
            plugins: {
                legend: { display: false },
                datalabels: {
                    color: '#fff',
                    font: { family: 'Outfit', size: 16, weight: 700 },
                    formatter: (value, context) => {
                        const label = data[context.dataIndex].label;
                        const pct = total > 0 ? ((value / total) * 100).toFixed(1) : 0;
                        return `${label}: ${value} (${pct}%)`;
                    },
                    display: (context) => {
                        const value = context.dataset.data[context.dataIndex];
                        const pct = total > 0 ? (value / total) * 100 : 0;
                        return pct >= 5; // Hide labels < 5%
                    },
                    anchor: 'end',
                    align: 'top'
                }
            },
            scales: { x: { ticks: { color: '#64748b' }, grid: { display: false } }, y: { ticks: { color: '#64748b' }, grid: { color: 'rgba(255,255,255,0.05)' } } }
        },
        plugins: [ChartDataLabels]
    });
}

function renderHistogram(key, data, activeFilter, isCategorical = false) {
    const canvas = document.getElementById(`${key}Chart`);
    const panel = canvas.closest('.chart-panel');
    const ctx = canvas.getContext('2d');
    if (charts[key]) charts[key].destroy();

    // Hide panel if no data
    if (!data || data.length === 0) {
        if (panel) panel.style.display = 'none';
        return;
    }
    if (panel) panel.style.display = 'block';

    const bgColors = data.map(d => {
        if (isCategorical) {
            // Filter by raw
            if (activeFilter && d.raw === activeFilter.raw) return '#fb923c';
            if (activeFilter) return 'rgba(251, 146, 60, 0.3)';
            return '#fb923c';
        } else {
            // Filter by min/max
            if (activeFilter && d.min === activeFilter.min && d.max === activeFilter.max) return '#fb923c';
            if (activeFilter) return 'rgba(251, 146, 60, 0.3)';
            return '#fb923c';
        }
    });

    // Calculate total for percentage
    const total = data.reduce((sum, d) => sum + d.value, 0);

    charts[key] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => d.label),
            datasets: [{ label: 'Properties', data: data.map(d => d.value), backgroundColor: bgColors, borderRadius: 2, categoryPercentage: 1.0, barPercentage: 0.95 }]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            onClick: (e, els) => {
                if (els.length > 0) {
                    const item = data[els[0].index];
                    if (isCategorical) {
                        activeFilters[key] = (activeFilters[key] && activeFilters[key].raw === item.raw) ? null : { raw: item.raw, label: item.label };
                    } else {
                        activeFilters[key] = (activeFilters[key] && activeFilters[key].min === item.min) ? null : { min: item.min, max: item.max, label: item.label };
                    }
                    updateFilter();
                }
            },
            plugins: {
                legend: { display: false },
                datalabels: {
                    color: '#fff',
                    font: { family: 'Outfit', size: 15, weight: 700 },
                    formatter: (value, context) => {
                        const pct = total > 0 ? ((value / total) * 100).toFixed(1) : 0;
                        return `${value} (${pct}%)`;
                    },
                    display: (context) => {
                        const value = context.dataset.data[context.dataIndex];
                        const pct = total > 0 ? (value / total) * 100 : 0;
                        return pct >= 5; // Hide labels < 5%
                    },
                    anchor: 'end',
                    align: 'top'
                }
            },
            scales: { x: { ticks: { color: '#64748b' }, grid: { display: false } }, y: { ticks: { color: '#64748b' }, grid: { color: 'rgba(255,255,255,0.05)' } } }
        },
        plugins: [ChartDataLabels]
    });
}

function renderDistrictTable(data, total) {
    const tbody = document.getElementById('districtTableBody');
    tbody.innerHTML = '';
    data.sort((a, b) => b.count - a.count);
    data.forEach(item => {
        const pct = total > 0 ? ((item.count / total) * 100).toFixed(1) : '0.0';
        const isActive = activeFilters.district === item._sheet;
        const row = document.createElement('tr');
        row.className = isActive ? 'active-filter' : '';
        row.onclick = () => {
            activeFilters.district = (activeFilters.district === item._sheet) ? null : item._sheet;
            updateFilter();
        };
        row.innerHTML = `<td>${item._sheet}</td><td>${item.count.toLocaleString()}</td><td>${pct}%</td>`;
        tbody.appendChild(row);
    });
}

function updateFilter() {
    updateDashboardFromData(filterData());
}

function updateFilterDisplay() {
    const container = document.getElementById('filterTagsContainer');
    const indicator = document.getElementById('filterIndicator');
    if (!container || !indicator) return;
    container.innerHTML = '';
    let hasFilters = false;

    const createTag = (label, type) => {
        const tag = document.createElement('div');
        tag.className = 'filter-tag';
        tag.innerHTML = `<span>${label}</span><span class="filter-tag-remove" onclick="removeFilter('${type}')">×</span>`;
        container.appendChild(tag);
        hasFilters = true;
    };

    if (activeFilters.district) createTag(`District: ${activeFilters.district}`, 'district');
    if (activeFilters.room) createTag(`Rooms: ${activeFilters.room.raw}`, 'room');
    if (activeFilters.size) createTag(`Size: ${activeFilters.size.min}-${activeFilters.size.max}`, 'size');
    if (activeFilters.banos) createTag(`Baños: ${activeFilters.banos.raw}`, 'banos');
    if (activeFilters.tipo) createTag(`Type: ${activeFilters.tipo.raw}`, 'tipo');
    if (activeFilters.barrio) createTag(`Barrio: ${activeFilters.barrio.raw}`, 'barrio');
    if (activeFilters.zona) createTag(`Zona: ${activeFilters.zona.raw}`, 'zona');
    if (activeFilters.specialStatus) createTag(activeFilters.specialStatus.raw, 'specialStatus');
    if (activeFilters.altura) createTag(`Height: ${activeFilters.altura.raw}`, 'altura');
    if (activeFilters.terraza) createTag(`Terrace: ${activeFilters.terraza.raw ? 'Yes' : 'No'}`, 'terraza');
    if (activeFilters.garaje) createTag(`Garage: ${activeFilters.garaje.raw ? 'Yes' : 'No'}`, 'garaje');
    if (activeFilters.trastero) createTag(`Storage: ${activeFilters.trastero.raw ? 'Yes' : 'No'}`, 'trastero');
    if (activeFilters.estado) createTag(`State: ${activeFilters.estado.raw}`, 'estado');

    indicator.style.display = hasFilters ? 'flex' : 'none';
}

function removeFilter(type) {
    activeFilters[type] = null;
    updateFilter();
}
window.removeFilter = removeFilter; // Export global

function clearAllFilters() {
    Object.keys(activeFilters).forEach(k => activeFilters[k] = null);
    updateFilterDisplay();
    if (rawData) updateDashboardFromData(calculateMetrics(rawData.raw_properties));
}
window.clearAllFilters = clearAllFilters;
