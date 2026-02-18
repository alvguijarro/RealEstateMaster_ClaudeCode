function init() {
    // Setup file refresh button
    const refreshBtn = document.getElementById('btnRefreshFiles');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', loadFiles);
    }

    loadFiles();
    // Removed loadResults() from init to keep tables empty on start
    setupFilters();
    setupAnalyze();
    setupSorting();
    startHeartbeat();
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    // DOM already loaded
    init();
}

function startHeartbeat() {
    setInterval(() => {
        fetch('/heartbeat', { method: 'POST' })
            .catch(e => console.log('Heartbeat failed', e));
    }, 3000); // Send every 3 seconds
}

async function loadFiles() {
    const btnRefresh = document.getElementById('btnRefreshFiles');
    if (btnRefresh) btnRefresh.classList.add('loading');

    try {
        const response = await fetch('/list-files');
        const files = await response.json();

        // Sort by modified date (newest first)
        files.sort((a, b) => new Date(b.last_modified) - new Date(a.last_modified));

        const ventaSelect = document.getElementById('ventaFile');
        const alquilerSelect = document.getElementById('alquilerFile');

        ventaSelect.innerHTML = '<option value="">-- Seleccionar archivo --</option>';
        alquilerSelect.innerHTML = '<option value="">-- Seleccionar archivo --</option>';

        let ventaSelected = false;
        let alquilerSelected = false;

        files.forEach(file => {
            const dateStr = `[${file.last_modified}]`;

            // Venta Option
            const optV = document.createElement('option');
            optV.value = file.filename;
            optV.innerHTML = `${file.filename} &nbsp;&nbsp; ${dateStr}`;
            ventaSelect.appendChild(optV);

            // Auto-select newest 'venta' file
            if (!ventaSelected && file.type === 'venta') {
                optV.selected = true;
                ventaSelected = true;
            }

            // Alquiler Option
            const optA = document.createElement('option');
            optA.value = file.filename;
            optA.innerHTML = `${file.filename} &nbsp;&nbsp; ${dateStr}`;
            alquilerSelect.appendChild(optA);

            // Auto-select newest 'alquiler' file
            if (!alquilerSelected && file.type === 'alquiler') {
                optA.selected = true;
                alquilerSelected = true;
            }
        });

        // Add event listeners for validation
        ventaSelect.addEventListener('change', validateFileSelection);
        alquilerSelect.addEventListener('change', validateFileSelection);

        // Initial validation
        validateFileSelection();

    } catch (error) {
        console.error('Error loading files:', error);
    } finally {
        if (btnRefresh) btnRefresh.classList.remove('loading');
    }
}

function validateFileSelection() {
    const vVal = document.getElementById('ventaFile').value;
    const aVal = document.getElementById('alquilerFile').value;

    // Warning if Venta file doesn't contain "venta" (case insensitive)
    const vWarning = document.getElementById('ventaWarning');
    if (vVal && !vVal.toLowerCase().includes('venta')) {
        vWarning.style.display = 'block';
    } else {
        vWarning.style.display = 'none';
    }

    // Warning if Alquiler file doesn't contain "alquiler" (case insensitive)
    const aWarning = document.getElementById('alquilerWarning');
    if (aVal && !aVal.toLowerCase().includes('alquiler')) {
        aWarning.style.display = 'block';
    } else {
        aWarning.style.display = 'none';
    }
    validateAnalyzeButton();
}


function setupFilters() {
    const filterGroups = {
        'filter-ascensor': { type: 'exclusive' },
        'filter-garaje': { type: 'exclusive' },
        'filter-terraza': { type: 'exclusive' },
        'filter-estado': { type: 'standard' },
        'filter-tipo': { type: 'standard' },
        'filter-altura': { type: 'standard' },
        'filter-habs': { type: 'standard' },
        'filter-banos': { type: 'standard' },
        'filter-especial': { type: 'optional' }
    };

    const pills = document.querySelectorAll('.filter-pill');
    pills.forEach(pill => {
        pill.addEventListener('click', (e) => {
            const container = pill.parentElement;
            const containerId = container.id;
            const groupConfig = filterGroups[containerId];

            if (!groupConfig) {
                pill.classList.toggle('active');
                return;
            }

            if (groupConfig.type === 'exclusive') {
                pill.classList.add('active');
                Array.from(container.children).forEach(sibling => {
                    if (sibling !== pill) sibling.classList.remove('active');
                });
            }
            else if (groupConfig.type === 'standard') {
                if (pill.classList.contains('active')) {
                    const activeCount = container.querySelectorAll('.filter-pill.active').length;
                    if (activeCount > 1) {
                        pill.classList.remove('active');
                    } else {
                        console.log("Cannot deselect last option");
                    }
                } else {
                    pill.classList.add('active');
                }
            } else {
                pill.classList.toggle('active');
            }
            validateAnalyzeButton();
        });
    });
    validateAnalyzeButton();
}

function validateAnalyzeButton() {
    const btn = document.getElementById('btnAnalyze');
    const isRunning = document.getElementById('btnText').textContent !== "COMENZAR ANALISIS";

    // Do not modify button state if analysis is running
    if (isRunning) return;

    const vVal = document.getElementById('ventaFile').value;
    const aVal = document.getElementById('alquilerFile').value;

    // Strict validation: files must exist and contain specific keywords
    const vValid = vVal && vVal.toLowerCase().includes('venta');
    const aValid = aVal && aVal.toLowerCase().includes('alquiler');

    if (vValid && aValid) {
        btn.disabled = false;
        btn.style.opacity = '1';
        btn.style.cursor = 'pointer';
    } else {
        btn.disabled = true;
        btn.style.opacity = '0.5';
        btn.style.cursor = 'not-allowed';
    }
}

function getSelectedValues(containerId) {
    const container = document.getElementById(containerId);
    const actives = container.querySelectorAll('.filter-pill.active');
    const values = [];
    actives.forEach(el => {
        const val = el.dataset.value;
        values.push(isNaN(val) ? val : parseInt(val));
    });
    return values;
}

function getSelectedTextValues(containerId) {
    const container = document.getElementById(containerId);
    const actives = container.querySelectorAll('.filter-pill.active');
    const values = [];
    actives.forEach(el => {
        values.push(el.innerText);
    });
    return values;
}

async function setupAnalyze() {
    const btn = document.getElementById('btnAnalyze');

    btn.addEventListener('click', async () => {
        const ventaFile = document.getElementById('ventaFile').value;
        const alquilerFile = document.getElementById('alquilerFile').value;

        if (!ventaFile || !alquilerFile) {
            alert("Por favor seleccione los archivos de origen.");
            return;
        }

        const filters = {
            'estado': getSelectedValues('filter-estado'),
            'tipo': getSelectedValues('filter-tipo'),
            'altura': getSelectedValues('filter-altura'),
            'ascensor': getSelectedTextValues('filter-ascensor'),
            'garaje': getSelectedTextValues('filter-garaje'),
            'terraza': getSelectedTextValues('filter-terraza'),
            'include_especial': getSelectedTextValues('filter-especial'),
            'habs': getSelectedValues('filter-habs'),
            'banos': getSelectedValues('filter-banos'),
            'price_min': document.getElementById('filter-price-min').value,
            'price_max': document.getElementById('filter-price-max').value
        };

        const payload = {
            'venta_file': ventaFile,
            'alquiler_file': alquilerFile,
            'filters': filters
        };

        btn.disabled = true;
        document.getElementById('btnText').textContent = "ANALIZANDO...";
        document.getElementById('btnLoader').classList.remove('hidden');
        document.getElementById('resultsArea').style.display = 'block';
        const terminal = document.getElementById('logTerminal');
        terminal.innerHTML = '<div class="log-line log-info">> Starting analysis...</div>';

        try {
            const res = await fetch('/analyze', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            if (!res.ok) throw new Error("Failed to start analysis");

            pollLogs();

        } catch (e) {
            alert("Error: " + e.message);
            resetBtn();
        }
    });
}

async function pollLogs() {
    const term = document.getElementById('logTerminal');
    let lastIndex = 0;

    const interval = setInterval(async () => {
        try {
            const res = await fetch(`/stream?since=${lastIndex}`);
            const data = await res.json();

            if (data.logs && data.logs.length > 0) {
                lastIndex += data.logs.length;
                data.logs.forEach(line => {
                    const div = document.createElement('div');
                    div.className = 'log-line';
                    div.textContent = line;
                    if (line.includes('ERROR')) div.classList.add('log-error');
                    if (line.includes('RESULT')) div.classList.add('log-success');
                    if (line.includes('PHASE')) div.classList.add('log-highlight');
                    term.appendChild(div);
                });
                term.scrollTop = term.scrollHeight;
            }

            if (data.status === 'done' || data.status === 'error') {
                clearInterval(interval);
                resetBtn();
                if (data.status === 'done') {
                    // Fetch final results from API
                    await loadResults();

                    document.getElementById('resultsArea').classList.remove('hidden');

                    const logDiv = document.getElementById('logTerminal');
                    logDiv.scrollTop = logDiv.scrollHeight;

                    // --- AUTO SCROLL TO RESULTS ---
                    setTimeout(() => {
                        const resultsArea = document.getElementById('resultsArea');
                        if (resultsArea) {
                            resultsArea.scrollIntoView({ behavior: 'smooth', block: 'start' });
                        }
                    }, 500);
                }
            }
        } catch (e) {
            console.error("Polling error", e);
        }
    }, 500);
}

// Global state for results and sorting
let currentResults = [];
let currentSort = { col: 'Puntuación', dir: -1 }; // -1: descending

// Global variable to store current analysis province
let currentAnalysisProvince = null;

async function loadResults() {
    try {
        const res = await fetch('/api/results');
        const response = await res.json();

        if (response.error) {
            console.error("API Error:", response.error);
            return;
        }

        if (response.data && response.data.opportunities) {
            // New Format
            currentResults = response.data.opportunities;
            currentTop100 = response.data.top_100 || [];
        } else if (Array.isArray(response.data)) {
            // Old Format fallback
            currentResults = response.data;
            currentTop100 = [];
        } else {
            currentResults = [];
            currentTop100 = [];
        }

        // Extract Province/City from filename (Salidas/resultado_Valencia_...)
        currentAnalysisProvince = null;
        if (response.file) {
            const parts = response.file.split('_');
            if (parts.length >= 2) {
                // parts[1] is typically the city name
                currentAnalysisProvince = parts[1];
            }
        }

        renderResults();

        document.getElementById('resultCount').textContent = `${currentResults.length} Oportunidades encontradas`;
        document.getElementById('resultsArea').style.display = 'block';
        document.getElementById('opportunitiesCard').style.display = 'block';
        document.getElementById('top100Card').style.display = 'block';

    } catch (e) {
        console.error("Error loading results", e);
    }
}

// Full Map of 50 Provinces to Calculator Code
function getComunidadByProvince(prov) {
    if (!prov) return null;
    const p = prov.toLowerCase().trim();

    const map = {
        // Andalucía (0.07)
        'almería': 'andalucia', 'almeria': 'andalucia',
        'cádiz': 'andalucia', 'cadiz': 'andalucia',
        'córdoba': 'andalucia', 'cordoba': 'andalucia',
        'granada': 'andalucia',
        'huelva': 'andalucia',
        'jaén': 'andalucia', 'jaen': 'andalucia',
        'málaga': 'andalucia', 'malaga': 'andalucia',
        'sevilla': 'andalucia',

        // Aragón (0.08)
        'huesca': 'aragon',
        'teruel': 'aragon',
        'zaragoza': 'aragon',

        // Asturias (0.08)
        'asturias': 'asturias', 'oviedo': 'asturias', 'gijón': 'asturias', 'gijon': 'asturias',

        // Baleares (0.08)
        'baleares': 'baleares', 'mallorca': 'baleares', 'menorca': 'baleares', 'ibiza': 'baleares', 'palma': 'baleares',

        // Canarias (0.065)
        'las palmas': 'canarias', 'canarias': 'canarias', 'tenerife': 'canarias',
        'santa cruz de tenerife': 'canarias',

        // Cantabria (0.10)
        'cantabria': 'cantabria', 'santander': 'cantabria',

        // Castilla - La Mancha (0.09)
        'albacete': 'castillamancha',
        'ciudad real': 'castillamancha',
        'cuenca': 'castillamancha',
        'guadalajara': 'castillamancha',
        'toledo': 'castillamancha',

        // Castilla León (0.08)
        'ávila': 'castillaleon', 'avila': 'castillaleon',
        'burgos': 'castillaleon',
        'león': 'castillaleon', 'leon': 'castillaleon',
        'palencia': 'castillaleon',
        'salamanca': 'castillaleon',
        'segovia': 'castillaleon',
        'soria': 'castillaleon',
        'valladolid': 'castillaleon',
        'zamora': 'castillaleon',

        // Cataluña (0.10)
        'barcelona': 'cataluna',
        'girona': 'cataluna', 'gerona': 'cataluna',
        'lleida': 'cataluna', 'lérida': 'cataluna',
        'tarragona': 'cataluna',

        // Ceuta (0.06)
        'ceuta': 'ceuta',

        // Madrid (0.06)
        'madrid': 'madrid',

        // Comunidad Valenciana (0.10)
        'alicante': 'valenciana', 'alacant': 'valenciana',
        'castellón': 'valenciana', 'castellon': 'valenciana',
        'valencia': 'valenciana', 'valència': 'valenciana',

        // Extremadura (0.08)
        'badajoz': 'extremadura',
        'cáceres': 'extremadura', 'caceres': 'extremadura',

        // Galicia (0.08)
        'a coruña': 'galicia', 'la coruña': 'galicia', 'coruña': 'galicia',
        'lugo': 'galicia',
        'ourense': 'galicia', 'orense': 'galicia',
        'pontevedra': 'galicia',

        // La Rioja (0.07)
        'la rioja': 'rioja', 'rioja': 'rioja', 'logroño': 'rioja',

        // Melilla (0.06)
        'melilla': 'melilla',

        // Murcia (0.08)
        'murcia': 'murcia',

        // Navarra (0.06)
        'navarra': 'navarra', 'pamplona': 'navarra',

        // País Vasco (0.04)
        'álava': 'paisvasco', 'alava': 'paisvasco', 'araba': 'paisvasco', 'vitoria': 'paisvasco',
        'guipúzcoa': 'paisvasco', 'gipuzkoa': 'paisvasco', 'san sebastián': 'paisvasco', 'donostia': 'paisvasco',
        'vizcaya': 'paisvasco', 'bizkaia': 'paisvasco', 'bilbao': 'paisvasco'
    };

    // Partial Match Check
    for (const key in map) {
        if (p.includes(key)) {
            return map[key];
        }
    }

    return null;
}

// --- Global state for Top 100 ---
let currentTop100 = [];

function renderResults() {
    renderTable('opportunitiesBody', currentResults, currentSort, true);
    renderTable('top100Body', currentTop100, currentSortTop100, false);
}

function renderTable(tbodyId, data, sortConfig, isMainTable) {
    const tbody = document.getElementById(tbodyId);
    if (!tbody) return;
    tbody.innerHTML = '';

    const { col, dir } = sortConfig;
    const sorted = [...data].sort((a, b) => {
        let valA = a[col];
        let valB = b[col];
        if (col === 'Propiedad') {
            valA = a['Propiedad'] || a['Distrito'];
            valB = b['Propiedad'] || b['Distrito'];
        }
        if (valA === undefined) valA = '';
        if (valB === undefined) valB = '';
        if (typeof valA === 'string') valA = valA.toLowerCase();
        if (typeof valB === 'string') valB = valB.toLowerCase();
        if (valA < valB) return -1 * dir;
        if (valA > valB) return 1 * dir;
        return 0;
    });

    sorted.slice(0, isMainTable ? 50 : 100).forEach((opp, arrIdx) => {
        const tr = document.createElement('tr');
        const titulo = opp.Propiedad || opp.Distrito;
        const url = opp.URL || '#';
        const precio = Number(opp.Precio).toLocaleString();

        // Use renta_rango if available, otherwise format from estimada
        const rentaRango = opp.renta_rango || opp.Renta_Rango ||
            (opp['Renta_estimada/mes'] ? `${Number(opp['Renta_estimada/mes']).toLocaleString()}€` : '-');

        const rentabBruta = opp['Rentabilidad_Bruta_%'] || 0;
        const rentab = (rentabBruta * 100).toFixed(2);

        // Precision with color coding
        const precision = opp.precision || opp.Precision || 0;
        let precisionColor = '#888';
        if (precision >= 80) precisionColor = 'var(--success-color)';
        else if (precision >= 50) precisionColor = '#f0ad4e';
        else precisionColor = '#d9534f';

        const punt = opp.Puntuación || opp.score || 0;
        const formattedPunt = typeof punt === 'number' ? punt.toFixed(0) : punt;

        // Refs link
        const refsLink = `<a href="#" onclick="loadReferencias(${arrIdx}); return false;" style="color:var(--text-muted); font-size:0.9em;">Ver Refs</a>`;

        // Calculate Button
        const calcBtn = `
                <button class="btn-calc" onclick="openCalc(${opp.Precio}, ${opp['Renta_estimada/mes'] || 0}, 'madrid')" 
                    style="background:rgba(59, 130, 246, 0.2); border:1px solid #3b82f6; color:#60a5fa; border-radius:4px; padding:2px 8px; cursor:pointer; font-size:0.8rem;">
                    Calcular
                </button>`;

        tr.innerHTML = `
            <td style="font-weight: 500; max-width: 250px; overflow: hidden; text-overflow: ellipsis; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; white-space: normal; line-height: 1.2;" title="${titulo}">
                <a href="${url}" target="_blank" class="link-icon" style="color: var(--accent-color); text-decoration: none;">${titulo}</a>
            </td>
            <td>${opp.Distrito}</td>
            <td>${opp.m2}</td>
            <td>${precio}€</td>
            <td style="font-weight: 500;">${rentaRango}</td>
            <td>
                <span style="color: ${rentab > 7 ? 'var(--success-color)' : 'inherit'}">
                    ${rentab}%
                </span>
            </td>
            <td>
                <span style="color: ${precisionColor}; font-weight: 500;">
                    ${precision.toFixed ? precision.toFixed(0) : precision}%
                </span>
            </td>
            <td><span class="score-badge">${formattedPunt}</span></td>
            <td>${isMainTable ? refsLink : '-'}</td>
            <td>${calcBtn}</td>
        `;
        tbody.appendChild(tr);
    });

    if (isMainTable) {
        setupConsultarLinks();
        setupDistrictReport();
    }
}


// Sort state for Top 100
let currentSortTop100 = { col: 'Rentabilidad_Bruta_%', dir: -1 };

function setupSorting() {
    // Main Table Sorting
    document.querySelectorAll('#opportunitiesBody').forEach(() => { // Hack to scope logic 
        document.querySelectorAll('.results-table th[data-sort]').forEach(th => {
            // ... existing logic ...
        });
    });

    // Existing setup logic modified to support generalized sorting 
    document.querySelectorAll('.results-table th[data-sort]').forEach(th => {
        th.addEventListener('click', () => {
            // Check if inside top 100 table or main table by checking parent
            const isTop100 = th.closest('#top100Header') || th.closest('#top100Content');
            // Actually the click handler is inline in HTML for Top 100, so this listener is just for main table
            // We can keep this for Main Table
            if (isTop100) return;

            const col = th.dataset.sort;
            if (currentSort.col === col) {
                currentSort.dir *= -1;
            } else {
                currentSort.col = col;
                if (['Puntuación', 'Rentabilidad_Bruta_%', 'Descuento_%'].includes(col)) {
                    currentSort.dir = -1;
                } else {
                    currentSort.dir = 1;
                }
            }
            renderResults();
            updateSortHeaders();
        });
    });

    // Initial Header State
    updateSortHeaders();
}

// Inline Sort Handler for Top 100
window.sortTable = function (table, col) {
    if (table === 'top100') {
        if (currentSortTop100.col === col) {
            currentSortTop100.dir *= -1;
        } else {
            currentSortTop100.col = col;
            currentSortTop100.dir = -1; // Default desc
        }
        renderResults(); // Re-render both
        // update headers for top 100? (Visual feedback not strictly requested but good)
    }
};



function updateSortHeaders() {
    document.querySelectorAll('.results-table th[data-sort]').forEach(th => {
        const col = th.dataset.sort;
        // Clean text by removing existing arrows
        let text = th.innerText.replace(/[↕↑↓]/g, '').trim();

        if (currentSort.col === col) {
            th.classList.add('sort-active');
            th.innerText = `${text} ${currentSort.dir === 1 ? '↑' : '↓'}`;
        } else {
            th.classList.remove('sort-active');
            th.innerText = text; // Just text, no arrow for inactive
        }
    });

    const dlBtn = document.getElementById('btnDownload');
    if (dlBtn) {
        dlBtn.addEventListener('click', () => {
            window.location.href = '/download-results';
        });
    }
}

function updateSortHeaders() {
    // Main Table
    document.querySelectorAll('.results-table:not(#top100Table) th[data-sort]').forEach(th => {
        const col = th.dataset.sort;
        let text = th.innerText.replace(/[↕↑↓]/g, '').trim();
        if (currentSort.col === col) {
            th.classList.add('sort-active');
            th.innerText = `${text} ${currentSort.dir === 1 ? '↑' : '↓'}`;
        } else {
            th.classList.remove('sort-active');
            th.innerText = text;
        }
    });

    // Top 100 Table
    document.querySelectorAll('#top100Table th[data-sort]').forEach(th => {
        const col = th.dataset.sort;
        let text = th.innerText.replace(/[↕↑↓]/g, '').trim();
        if (currentSortTop100.col === col) {
            th.classList.add('sort-active');
            th.innerText = `${text} ${currentSortTop100.dir === 1 ? '↑' : '↓'}`;
        } else {
            th.classList.remove('sort-active');
            th.innerText = text;
        }
    });
}

// Store sorted results for Referencias lookup AND District Report
let sortedResults = [];
let availableDistrictReports = {};
let batchDistrictsToAnalyze = [];

function setupConsultarLinks() {
    const { col, dir } = currentSort;
    sortedResults = [...currentResults].sort((a, b) => {
        let valA = a[col];
        let valB = b[col];
        if (col === 'Propiedad') {
            valA = a['Propiedad'] || a['Distrito'];
            valB = b['Propiedad'] || b['Distrito'];
        }
        if (valA === undefined) valA = '';
        if (valB === undefined) valB = '';
        if (typeof valA === 'string') valA = valA.toLowerCase();
        if (typeof valB === 'string') valB = valB.toLowerCase();
        if (valA < valB) return -1 * dir;
        if (valA > valB) return 1 * dir;
        return 0;
    }).slice(0, 50);

    document.querySelectorAll('.consultar-link').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();

            // Highlight selected row
            const allRows = document.querySelectorAll('#opportunitiesBody tr');
            allRows.forEach(tr => tr.style.backgroundColor = '');
            const row = link.closest('tr');
            if (row) row.style.backgroundColor = 'rgba(140, 29, 100, 0.15)';

            const idx = parseInt(link.dataset.idx);
            loadReferencias(idx);

            // Scroll to references
            const refSection = document.getElementById('referenciasSection');
            refSection.style.display = 'block';
            refSection.scrollIntoView({ behavior: 'smooth' });
        });
    });
}

function setupDistrictReport() {
    const btnGen = document.getElementById('btnGenerateDistrictReport');
    if (btnGen) {
        // Clone to remove old listeners
        const newBtn = btnGen.cloneNode(true);
        btnGen.parentNode.replaceChild(newBtn, btnGen);

        newBtn.addEventListener('click', () => {
            if (!sortedResults || sortedResults.length === 0) {
                alert("No hay resultados para analizar.");
                return;
            }

            // Extract Unique Districts maintaining order
            const uniqueDistricts = [];
            const seen = new Set();
            for (const res of sortedResults) {
                if (res.Distrito && !seen.has(res.Distrito)) {
                    seen.add(res.Distrito);
                    uniqueDistricts.push(res.Distrito);
                }
            }

            if (uniqueDistricts.length === 0) {
                alert("No se encontraron distritos válidos.");
                return;
            }

            // Populate Modal Dropdown
            const modal = document.getElementById('reportModal');
            const select = document.getElementById('districtModalSelect');
            const btnModalGen = document.getElementById('btnGenerateReport');
            const reportOutput = document.getElementById('reportOutput');

            select.innerHTML = '';
            uniqueDistricts.forEach(dist => {
                const opt = document.createElement('option');
                opt.value = dist;
                opt.textContent = dist;
                select.appendChild(opt);
            });

            document.querySelector('.modal-content h2').textContent = "Selecciona Distrito para Deep Research";

            // Reset UI
            reportOutput.innerHTML = '';
            reportOutput.classList.add('hidden');
            btnModalGen.style.display = 'block';
            document.querySelector('.form-group').style.display = 'block';
            btnModalGen.disabled = false;
            btnModalGen.textContent = "EJECUTAR DEEP RESEARCH 🚀";

            modal.classList.remove('hidden');
        });
    }
}

// ... existing showDistrictReports ...
// ... existing renderDistrictContent ...
// ... existing modal interaction ...
function showDistrictReports(districts) {
    const section = document.getElementById('districtReportSection');
    const content = document.getElementById('districtReportContent');

    section.classList.remove('hidden');

    // Just render the first district in the list (usually only one is generated per run)
    if (districts.length > 0) {
        renderDistrictContent(districts[0]);
    } else {
        content.innerHTML = "No se pudieron generar informes.";
    }

    section.scrollIntoView({ behavior: 'smooth' });
}

function renderDistrictContent(distName) {
    const content = document.getElementById('districtReportContent');
    const markdown = availableDistrictReports[distName];
    if (markdown) {
        content.innerHTML = marked.parse(markdown);
    }
}

// ... existing showDistrictReports ...
// ... existing renderDistrictContent ...

function setupModalHandlers() {
    const modal = document.getElementById('reportModal');
    const closeBtn = document.querySelector('.close-modal');
    const generateBtn = document.getElementById('btnGenerateReport');

    if (closeBtn) {
        closeBtn.onclick = () => {
            modal.classList.add('hidden');
        };
    }

    window.onclick = (e) => {
        if (e.target == modal) {
            modal.classList.add('hidden');
        }
    }

    if (generateBtn) {
        // Generate button handler (Unified Logic with Dropdown)
        generateBtn.onclick = async () => {
            // 1. Get Selected District
            const select = document.getElementById('districtModalSelect');
            const selectedDistrict = select.value;

            if (!selectedDistrict) {
                alert("Selecciona un distrito.");
                return;
            }

            // 2. Close the modal
            if (modal) modal.classList.add('hidden');

            // 3. Determine Location Context (City/Province) from filename
            let city = "Madrid";
            let province = "Madrid";
            const ventaFile = document.getElementById('ventaFile') ? document.getElementById('ventaFile').value : '';

            if (ventaFile) {
                if (ventaFile.startsWith('API_BATCH_')) {
                    const parts = ventaFile.split('_');
                    if (parts.length >= 3) {
                        city = parts[2];
                        province = parts[2];
                    }
                } else {
                    const parts = ventaFile.split('_');
                    if (parts.length >= 2) {
                        city = parts[1];
                        if (city !== 'Madrid') province = city;
                    }
                }
            }

            // 4. Construct context-aware district name
            const fullDistrictName = `${selectedDistrict} (${city}, ${province})`;

            console.log(`Starting Unified Deep Research for: ${fullDistrictName}`);

            // 5. Execute Deep Research
            await executeDeepResearch(fullDistrictName);
        };
    }
}

// Initial Setup
document.addEventListener('DOMContentLoaded', () => {
    setupModalHandlers();

    // Accordion Logic
    const accHeader = document.getElementById('top100Header');
    const accContent = document.getElementById('top100Content');
    const accIcon = accHeader.querySelector('.accordion-icon');

    if (accHeader) {
        accHeader.addEventListener('click', () => {
            if (accContent.style.maxHeight === '0px' || accContent.style.maxHeight === '') {
                // Expand
                accContent.style.maxHeight = '600px'; // Adequate height
                accIcon.style.transform = 'rotate(180deg)';
            } else {
                // Collapse
                accContent.style.maxHeight = '0px';
                accIcon.style.transform = 'rotate(0deg)';
            }
        });
    }
});


// ============================================================================
// DEEP RESEARCH - Uses Google CSE + Gemini for comprehensive market research
// ============================================================================
async function executeDeepResearch(distrito) {
    const section = document.getElementById('districtReportSection');
    const content = document.getElementById('districtReportContent');

    section.classList.remove('hidden');
    content.innerHTML = `
        <div class="deep-research-loading">
            <div class="spinner"></div>
            <h3>🔬 Ejecutando Deep Research para: ${distrito}</h3>
            <p>Buscando en Google (21 consultas) + Sintetizando con Gemini...</p>
            <p style="color: #888; font-size: 0.9rem;">Esto puede tardar 30-60 segundos</p>
        </div>
    `;
    section.scrollIntoView({ behavior: 'smooth' });

    try {
        const res = await fetch('/api/deep-research', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ distrito: distrito })
        });

        const data = await res.json();

        if (data.error) {
            content.innerHTML = `<div class="error-message">❌ Error: ${data.error}</div>`;
            return;
        }

        // Store in available reports
        availableDistrictReports[distrito] = data.report;

        // Render markdown
        content.innerHTML = marked.parse(data.report);

    } catch (e) {
        content.innerHTML = `<div class="error-message">❌ Error de conexión: ${e.message}</div>`;
    }
}

// Setup Deep Research button if it exists
// function setupDeepResearchButton() { ... } // Removed as unified into Generate Report button

// Initialize Deep Research on page load
document.addEventListener('DOMContentLoaded', () => {
    // setupDeepResearchButton(); // Removed
});

function loadReferencias(idx) {
    const opp = sortedResults[idx];
    if (!opp || !opp.comparables || opp.comparables.length === 0) {
        return;
    }

    const section = document.getElementById('referenciasSection');
    const title = document.getElementById('referenciasTitle');
    const tbody = document.getElementById('referenciasBody');

    // Show aggregate precision in title
    const aggPrecision = opp.precision || opp.Precision || 0;
    title.textContent = `Propiedades de alquiler similares a: ${opp.Propiedad || opp.Distrito} (Precisión: ${aggPrecision.toFixed ? aggPrecision.toFixed(0) : aggPrecision}%)`;
    tbody.innerHTML = '';

    const vM2 = opp.m2 || 0;
    const vPrice = opp.Precio || 0;
    const vPriceM2 = vM2 > 0 ? (vPrice / vM2) : 0;
    const vGaraje = opp.garaje ? 'Sí' : 'No';
    const vTerraza = opp.terraza ? 'Sí' : 'No';

    const trVenta = document.createElement('tr');
    // Same highlight color as selected row in Top Oportunidades
    trVenta.style.backgroundColor = 'rgba(140, 29, 100, 0.15)';
    trVenta.style.borderBottom = '2px solid var(--accent-color)';

    trVenta.innerHTML = `
        <td style="font-weight: 600; color: var(--accent-color);">
            <a href="${opp.URL}" target="_blank" style="color: var(--accent-color);">★ ${opp.Propiedad || 'Propiedad Principal'} (VENTA)</a>
        </td>
        <td>${opp.habs || '-'}</td>
        <td>${opp.banos || '-'}</td>
        <td>${vM2}m²</td>
        <td>${opp.Distrito}</td>
        <td>${vGaraje}</td>
        <td>${vTerraza}</td>
        <td style="font-weight: 600;">${Math.round(vPriceM2).toLocaleString()} €/m²</td>
        <td style="font-weight: 600;">${Number(vPrice).toLocaleString()}€ (Venta)</td>
    `;
    tbody.appendChild(trVenta);

    opp.comparables.forEach(ref => {
        const tr = document.createElement('tr');
        const titulo = ref.titulo || ref.Titulo || `${ref.Distrito} - ${ref['m2 construidos']}m²`;
        const garaje = ref.garaje ? 'Sí' : 'No';
        const terraza = ref.terraza ? 'Sí' : 'No';
        const precio = Number(ref.price).toLocaleString();
        const precioM2 = ref.precio_m2 ? Math.round(ref.precio_m2).toLocaleString() : '-';

        // Precision per comparable with color coding
        const precision = ref.precision || 0;
        let precisionColor = '#888';
        if (precision >= 80) precisionColor = 'var(--success-color)';
        else if (precision >= 50) precisionColor = '#f0ad4e';
        else precisionColor = '#d9534f';

        tr.innerHTML = `
            <td style="font-weight: 500;">
                <a href="${ref.URL}" target="_blank" style="color: var(--text-primary); text-decoration: none; hover: underline;">${titulo}</a>
            </td>
            <td>${ref.habs}</td>
            <td>${ref.banos}</td>
            <td>${ref['m2 construidos']}m²</td>
            <td>${ref.Distrito}</td>
            <td>${garaje}</td>
            <td>${terraza}</td>
            <td>${precioM2} €/m²</td>
            <td>${precio}€</td>
        `;
        tbody.appendChild(tr);
    });

    section.style.display = 'block';
    section.scrollIntoView({ behavior: 'smooth' });
}

function resetBtn() {
    const btn = document.getElementById('btnAnalyze');
    btn.disabled = false;
    document.getElementById('btnText').textContent = "COMENZAR ANALISIS";
    document.getElementById('btnLoader').classList.add('hidden');
}

// Global function for Calculate Button
window.openCalc = function (price, rent, comunidad) {
    comunidad = comunidad || 'madrid';
    // Use postMessage for cross-origin communication
    if (window.parent) {
        window.parent.postMessage({
            action: 'openCalculator',
            data: { price: price, rent: rent, comunidad: comunidad }
        }, '*');
    } else {
        // Fallback for standalone mode
        window.open(`/calculator?price=${price}&rent=${rent}&comunidad=${comunidad}`, '_blank');
    }
};

/* OVERRIDE: Support for 'sale' and 'rent' keywords */
function validateFileSelection() {
    const vVal = document.getElementById('ventaFile').value;
    const aVal = document.getElementById('alquilerFile').value;

    const vWarning = document.getElementById('ventaWarning');
    if (vVal && !vVal.toLowerCase().includes('venta') && !vVal.toLowerCase().includes('sale')) {
        vWarning.style.display = 'block';
    } else {
        vWarning.style.display = 'none';
    }

    const aWarning = document.getElementById('alquilerWarning');
    if (aVal && !aVal.toLowerCase().includes('alquiler') && !aVal.toLowerCase().includes('rent')) {
        aWarning.style.display = 'block';
    } else {
        aWarning.style.display = 'none';
    }
    validateAnalyzeButton();
}

function validateAnalyzeButton() {
    const btn = document.getElementById('btnAnalyze');
    const textEl = document.getElementById('btnText');
    const isRunning = textEl && textEl.textContent !== 'COMENZAR ANALISIS';

    if (isRunning) return;

    const vVal = document.getElementById('ventaFile').value;
    const aVal = document.getElementById('alquilerFile').value;

    const vValid = vVal && (vVal.toLowerCase().includes('venta') || vVal.toLowerCase().includes('sale'));
    const aValid = aVal && (aVal.toLowerCase().includes('alquiler') || aVal.toLowerCase().includes('rent'));

    if (vValid && aValid) {
        btn.disabled = false;
        btn.style.opacity = '1';
        btn.style.cursor = 'pointer';
    } else {
        btn.disabled = true;
        btn.style.opacity = '0.5';
        btn.style.cursor = 'not-allowed';
    }
}

