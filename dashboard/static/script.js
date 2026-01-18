/**
 * Market Metrics Dashboard - JavaScript
 * Handles API calls, chart rendering, user interactions, and cross-filtering.
 */

// State
let selectedFile = '';
let allDistricts = [];
let selectedDistricts = [];
let roomChart = null;
let sizeChart = null;

// Raw data storage for cross-filtering
let rawData = null; // Contains: total, district_summary, room_distribution, size_distribution, price_stats, raw_properties
let activeFilters = {
    room: null,      // { raw: number }
    size: null,      // { min: number, max: number }
    district: null   // string (district name)
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

// API Calls
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

    // Load districts for the selected file
    try {
        const res = await fetch(`/api/districts?file=${encodeURIComponent(selectedFile)}`);
        const data = await res.json();

        if (data.districts && data.districts.length > 0) {
            allDistricts = data.districts;
            renderDistrictList(allDistricts);
            districtGroup.style.display = 'block';
        }
    } catch (err) {
        console.error('Error loading districts:', err);
    }
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
                if (!selectedDistricts.includes(district)) {
                    selectedDistricts.push(district);
                }
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
    const filtered = allDistricts.filter(d => d.toLowerCase().includes(query));
    renderDistrictList(filtered);
}

function updateDistrictInfo() {
    if (selectedDistricts.length === 0) {
        districtInfo.textContent = 'Selected: All';
    } else {
        districtInfo.textContent = `Selected: ${selectedDistricts.length} district(s)`;
    }
}

async function loadAnalytics() {
    loadBtn.disabled = true;
    loadBtn.innerHTML = '<span class="btn-icon">⏳</span> Loading...';

    try {
        const res = await fetch('/api/analytics', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                file: selectedFile,
                districts: selectedDistricts
            })
        });

        const data = await res.json();

        if (data.error) {
            alert('Error: ' + data.error);
            return;
        }

        rawData = data;
        clearAllFilters();

        emptyState.style.display = 'none';
        statsBar.style.display = 'grid';
        chartsGrid.style.display = 'grid';

        // Initial render (no filters active)
        updateDashboardFromData(filterData());

    } catch (err) {
        console.error('Error loading analytics:', err);
        alert('Error loading analytics');
    } finally {
        loadBtn.disabled = false;
        loadBtn.innerHTML = '<span class="btn-icon">📈</span> Load Analytics';
    }
}

// Core filtering logic
function filterData() {
    if (!rawData || !rawData.raw_properties) return null;

    // Start with all loaded properties
    let filteredProps = rawData.raw_properties;

    // Apply Room Filter
    if (activeFilters.room) {
        filteredProps = filteredProps.filter(p => p.rooms === activeFilters.room.raw);
    }

    // Apply Size Filter
    if (activeFilters.size) {
        filteredProps = filteredProps.filter(p =>
            p.size >= activeFilters.size.min && p.size < activeFilters.size.max
        );
    }

    // Apply District Filter (from chart click)
    if (activeFilters.district) {
        filteredProps = filteredProps.filter(p => p.district === activeFilters.district);
    }

    return calculateMetrics(filteredProps);
}

// Calculate all metrics from a subset of properties
function calculateMetrics(properties) {
    const total = properties.length;

    // 1. District Summary
    const districtCounts = {};
    properties.forEach(p => {
        districtCounts[p.district] = (districtCounts[p.district] || 0) + 1;
    });
    const districtSummary = Object.entries(districtCounts).map(([name, count]) => ({
        _sheet: name,
        count: count
    }));

    // 2. Room Distribution
    const roomCounts = {};
    properties.forEach(p => {
        if (p.rooms !== undefined) {
            roomCounts[p.rooms] = (roomCounts[p.rooms] || 0) + 1;
        }
    });
    const roomDistribution = Object.keys(roomCounts).sort((a, b) => a - b).map(r => ({
        label: `${r} hab`,
        value: roomCounts[r],
        raw: parseInt(r)
    }));

    // 3. Size Distribution
    // Use fixed bins for consistency (20m2 steps up to 300, then 300+)
    const sizeDistribution = [];
    const binSize = 20;
    const maxBin = 300;
    const bins = {};

    // Initialize bins
    for (let i = 0; i < maxBin; i += binSize) {
        bins[i] = 0;
    }
    bins['300+'] = 0;

    properties.forEach(p => {
        if (p.size !== undefined) {
            if (p.size >= maxBin) {
                bins['300+']++;
            } else {
                const binStart = Math.floor(p.size / binSize) * binSize;
                bins[binStart] = (bins[binStart] || 0) + 1;
            }
        }
    });

    // Convert to array
    Object.keys(bins).forEach(k => {
        const count = bins[k];
        if (count > 0) {
            if (k === '300+') {
                sizeDistribution.push({
                    label: '> 300 m²',
                    value: count,
                    min: 300,
                    max: 99999
                });
            } else {
                const start = parseInt(k);
                sizeDistribution.push({
                    label: `${start}-${start + binSize} m²`,
                    value: count,
                    min: start,
                    max: start + binSize
                });
            }
        }
    });
    // Sort by min size
    sizeDistribution.sort((a, b) => a.min - b.min);

    // 4. Price Stats
    const prices = properties
        .filter(p => p.price_per_m2 !== undefined)
        .map(p => p.price_per_m2);

    let priceStats = { min: '-', max: '-', mean: '-', median: '-' };
    if (prices.length > 0) {
        prices.sort((a, b) => a - b);
        const sum = prices.reduce((a, b) => a + b, 0);
        const mean = sum / prices.length;
        const middle = Math.floor(prices.length / 2);
        const median = prices.length % 2 === 0
            ? (prices[middle - 1] + prices[middle]) / 2
            : prices[middle];

        priceStats = {
            min: Math.min(...prices).toFixed(2),
            max: Math.max(...prices).toFixed(2),
            mean: Math.round(mean).toLocaleString(),
            median: Math.round(median).toLocaleString()
        };
    }

    return {
        total,
        district_summary: districtSummary,
        room_distribution: roomDistribution,
        size_distribution: sizeDistribution,
        price_stats: priceStats,
        distinct_districts: Object.keys(districtCounts).length
    };
}

function updateDashboardFromData(metrics) {
    if (!metrics) return;

    // Update Scorecards
    document.getElementById('statTotal').textContent = metrics.total.toLocaleString();
    document.getElementById('statDistricts').textContent = metrics.distinct_districts;

    const avgLabel = typeof metrics.price_stats.mean === 'number'
        ? metrics.price_stats.mean.toLocaleString()
        : metrics.price_stats.mean;
    document.getElementById('statPriceAvg').textContent = avgLabel !== '-' ? `€${avgLabel}` : '-';

    const medLabel = typeof metrics.price_stats.median === 'number'
        ? metrics.price_stats.median.toLocaleString()
        : metrics.price_stats.median;
    document.getElementById('statPriceMedian').textContent = medLabel !== '-' ? `€${medLabel}` : '-';

    // Render Charts
    renderDistrictTable(metrics.district_summary, metrics.total);
    renderRoomChart(metrics.room_distribution);
    renderSizeChart(metrics.size_distribution);

    // Update Indicator
    updateFilterDisplay();
}

function applyFilter(type, value) {
    // Toggle logic
    if (activeFilters[type] && JSON.stringify(activeFilters[type]) === JSON.stringify(value)) {
        activeFilters[type] = null;
    } else {
        activeFilters[type] = value;
    }

    const newMetrics = filterData();
    updateDashboardFromData(newMetrics);
}

function updateFilterDisplay() {
    const container = document.getElementById('filterTagsContainer');
    const indicator = document.getElementById('filterIndicator');

    if (!container || !indicator) return;

    container.innerHTML = '';
    let hasFilters = false;

    // Helper to create tag
    const createTag = (label, type) => {
        const tag = document.createElement('div');
        tag.className = 'filter-tag';
        tag.innerHTML = `
            <span>${label}</span>
            <span class="filter-tag-remove" onclick="removeFilter('${type}')">×</span>
        `;
        container.appendChild(tag);
        hasFilters = true;
    };

    if (activeFilters.district) {
        createTag(`District: ${activeFilters.district}`, 'district');
    }
    if (activeFilters.room) {
        createTag(`Rooms: ${activeFilters.room.raw}`, 'room');
    }
    if (activeFilters.size) {
        createTag(`Size: ${activeFilters.size.min}-${activeFilters.size.max} m²`, 'size');
    }

    if (hasFilters) {
        indicator.style.display = 'flex';
    } else {
        indicator.style.display = 'none';
    }
}

function removeFilter(type) {
    activeFilters[type] = null;
    const newMetrics = filterData();
    updateDashboardFromData(newMetrics);

    // Also update UI selection states (like unchecking checkbox or chart highlight)
    // For charts, re-rendering handles highlighting update.
    // For district table, we need to ensure the checkbox list (if used) matches.
    // But since the chart-based filters are visual, re-render covers it.
}
window.removeFilter = removeFilter;

function clearAllFilters() {
    activeFilters = { room: null, size: null, district: null };
    updateFilterDisplay();
    if (rawData) {
        // Reset to full data
        updateDashboardFromData(calculateMetrics(rawData.raw_properties));
    }
}
window.clearAllFilters = clearAllFilters;

// --- Chart Rendering ---

function renderDistrictTable(data, total) {
    const tbody = document.getElementById('districtTableBody');
    tbody.innerHTML = '';

    // Sort by count desc
    data.sort((a, b) => b.count - a.count);

    data.forEach(item => {
        const pct = total > 0 ? ((item.count / total) * 100).toFixed(1) : '0.0';
        const isActive = activeFilters.district === item._sheet;

        const row = document.createElement('tr');
        row.className = isActive ? 'active-filter' : '';
        row.onclick = () => applyFilter('district', item._sheet);

        row.innerHTML = `
            <td>${item._sheet}</td>
            <td>${item.count.toLocaleString()}</td>
            <td>${pct}%</td>
        `;
        tbody.appendChild(row);
    });
}

function renderRoomChart(data) {
    const ctx = document.getElementById('roomChart').getContext('2d');
    if (roomChart) roomChart.destroy();

    if (data.length === 0) return;

    const baseColors = [
        '#6366f1', '#8b5cf6', '#a855f7', '#d946ef', '#ec4899',
        '#f43f5e', '#ef4444', '#f97316', '#eab308', '#22c55e'
    ];

    roomChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: data.map(d => d.label),
            datasets: [{
                data: data.map(d => d.value),
                backgroundColor: baseColors.slice(0, data.length),
                borderColor: '#1e1e3c',
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            onClick: (evt, els) => {
                if (els.length > 0) {
                    const idx = els[0].index;
                    applyFilter('room', { raw: data[idx].raw });
                }
            },
            plugins: {
                legend: {
                    position: 'top',
                    labels: {
                        color: '#a0a0c0',
                        font: { family: 'Outfit', size: 14, weight: '500' },
                        padding: 20,
                        usePointStyle: true
                    }
                },
                tooltip: {
                    cornerRadius: 8,
                    padding: 12,
                    titleFont: { family: 'Outfit', size: 14 },
                    bodyFont: { family: 'Outfit', size: 13 },
                    callbacks: {
                        label: (ctx) => {
                            const val = ctx.raw;
                            const total = ctx.dataset.data.reduce((a, b) => a + b, 0);
                            const pct = ((val / total) * 100).toFixed(1);
                            return ` ${ctx.label}: ${val} (${pct}%)`;
                        }
                    }
                }
            },
            layout: {
                padding: { top: 10, bottom: 10 }
            }
        }
    });
}

function renderSizeChart(data) {
    const ctx = document.getElementById('sizeChart').getContext('2d');
    if (sizeChart) sizeChart.destroy();

    if (data.length === 0) return;

    sizeChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => d.label),
            datasets: [{
                label: 'Properties',
                data: data.map(d => d.value),
                backgroundColor: 'rgba(99, 102, 241, 0.8)',
                borderColor: '#6366f1',
                borderWidth: 1,
                borderRadius: 4,
                hoverBackgroundColor: '#8b5cf6'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            onClick: (evt, els) => {
                if (els.length > 0) {
                    const idx = els[0].index;
                    const item = data[idx];
                    applyFilter('size', { min: item.min, max: item.max });
                }
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    align: 'end',
                    labels: {
                        color: '#a0a0c0',
                        font: { family: 'Outfit', size: 13 },
                        boxWidth: 12
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(15, 15, 35, 0.9)',
                    titleColor: '#fff',
                    bodyColor: '#a0a0c0',
                    cornerRadius: 8,
                    padding: 10,
                    titleFont: { family: 'Outfit', size: 13 },
                    bodyFont: { family: 'Outfit', size: 13 }
                }
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: {
                        color: '#6a6a8a',
                        font: { family: 'Outfit', size: 11 },
                        maxRotation: 45,
                        minRotation: 45
                    }
                },
                y: {
                    grid: { color: 'rgba(255, 255, 255, 0.05)' },
                    ticks: {
                        color: '#6a6a8a',
                        font: { family: 'Outfit', size: 11 }
                    }
                }
            }
        }
    });
}
