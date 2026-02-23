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

        let newestVenta = null;
        let newestAlquiler = null;

        files.forEach(file => {
            const dateStr = `[${file.last_modified}]`;

            // Venta Option
            const optV = document.createElement('option');
            optV.value = file.filename;
            optV.innerHTML = `${file.filename} &nbsp;&nbsp; ${dateStr}`;
            ventaSelect.appendChild(optV);

            // Keep track of newest files
            if (!newestVenta && file.type === 'venta') newestVenta = file.filename;
            if (!newestAlquiler && file.type === 'alquiler') newestAlquiler = file.filename;

            // Alquiler Option
            const optA = document.createElement('option');
            optA.value = file.filename;
            optA.innerHTML = `${file.filename} &nbsp;&nbsp; ${dateStr}`;
            alquilerSelect.appendChild(optA);
        });

        // --- Initial Selection Logic ---
        if (newestVenta) {
            ventaSelect.value = newestVenta;
            // Try to find matching alquiler
            const prefix = extractFilePrefix(newestVenta);
            const match = Array.from(alquilerSelect.options).find(opt => opt.value.startsWith(prefix) && opt.value.toLowerCase().includes('alquiler'));
            if (match) {
                alquilerSelect.value = match.value;
            } else if (newestAlquiler) {
                alquilerSelect.value = newestAlquiler;
            }
        } else if (newestAlquiler) {
            alquilerSelect.value = newestAlquiler;
        }

        // Add event listeners for validation and SYNC
        ventaSelect.addEventListener('change', (e) => validateFileSelection('venta'));
        alquilerSelect.addEventListener('change', (e) => validateFileSelection('alquiler'));

        // Initial validation
        validateFileSelection();

    } catch (error) {
        console.error('Error loading files:', error);
    } finally {
        if (btnRefresh) btnRefresh.classList.remove('loading');
    }
}

function extractFilePrefix(filename) {
    if (!filename) return "";
    // Remove _venta.xlsx or _alquiler.xlsx (case insensitive)
    return filename.replace(/_(venta|alquiler)\.xlsx$/i, "");
}

function validateFileSelection(changedSource) {
    const vSelect = document.getElementById('ventaFile');
    const aSelect = document.getElementById('alquilerFile');
    const vVal = vSelect.value;
    const aVal = aSelect.value;

    // --- Sync Logic ---
    if (changedSource === 'venta' && vVal) {
        const prefix = extractFilePrefix(vVal);
        const match = Array.from(aSelect.options).find(opt => opt.value.startsWith(prefix) && opt.value.toLowerCase().includes('alquiler'));
        if (match) aSelect.value = match.value;
    } else if (changedSource === 'alquiler' && aVal) {
        const prefix = extractFilePrefix(aVal);
        const match = Array.from(vSelect.options).find(opt => opt.value.startsWith(prefix) && opt.value.toLowerCase().includes('venta'));
        if (match) vSelect.value = match.value;
    }

    // Refresh values after potential sync
    const finalV = vSelect.value;
    const finalA = aSelect.value;

    // 1. Generic VENTA/ALQUILER keyword warnings (yellowish/orange)
    const vWarning = document.getElementById('ventaWarning');
    if (finalV && !finalV.toLowerCase().includes('venta')) {
        vWarning.style.display = 'block';
    } else {
        vWarning.style.display = 'none';
    }

    const aWarning = document.getElementById('alquilerWarning');
    if (finalA && !finalA.toLowerCase().includes('alquiler')) {
        aWarning.style.display = 'block';
    } else {
        aWarning.style.display = 'none';
    }

    // 2. Sync Errors (Red) - Check if they actually match
    const vSyncError = document.getElementById('ventaSyncError');
    const aSyncError = document.getElementById('alquilerSyncError');

    const prefixV = extractFilePrefix(finalV);
    const prefixA = extractFilePrefix(finalA);

    if (finalV && finalA) {
        if (prefixV !== prefixA) {
            // If they don't match, show error on the one that WASN'T just changed? 
            // Or just on both if they are different. The user said: 
            // "indindicando 'Por favor, selecciona el fichero correcto' en el desplegable donde no se ha encontrado automáticamente"
            // This implies if I change V to X, and A doesn't have an X counterpart, A shows error.
            if (changedSource === 'venta') {
                aSyncError.style.display = 'flex';
                vSyncError.style.display = 'none';
            } else if (changedSource === 'alquiler') {
                vSyncError.style.display = 'flex';
                aSyncError.style.display = 'none';
            } else {
                // Initial load or unknown
                vSyncError.style.display = prefixV !== prefixA ? 'flex' : 'none';
                aSyncError.style.display = prefixV !== prefixA ? 'flex' : 'none';
            }
        } else {
            vSyncError.style.display = 'none';
            aSyncError.style.display = 'none';
        }
    } else {
        vSyncError.style.display = 'none';
        aSyncError.style.display = 'none';
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

        // Extract Province/City from analysis data or filename
        currentAnalysisProvince = null;
        // Primary: use Provincia from analysis results
        if (currentResults.length > 0) {
            const firstWithProv = currentResults.find(r => r.Provincia && r.Provincia.trim());
            if (firstWithProv) {
                currentAnalysisProvince = firstWithProv.Provincia.trim();
            }
        }
        // Fallback: parse from filename
        if (!currentAnalysisProvince && response.file) {
            const parts = response.file.split('_');
            if (parts.length >= 2) {
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
            valA = a['Propiedad'];
            valB = b['Propiedad'];
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

        // Refs link - Now enabled for both tables using URL as unique ID
        const refsLink = `<a href="#" onclick="loadReferenciasByUrl('${url}'); return false;" style="color:var(--text-muted); font-size:0.9em;">Ver Refs</a>`;

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
            <td>${refsLink}</td>
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
let sortedOpps = [];
let sortedTop100 = [];
let availableDistrictReports = {};
let batchDistrictsToAnalyze = [];

function setupConsultarLinks() {
    // This is now legacy since we use loadReferenciasByUrl directly in the onclick
    // But we still need to calculate sortedOpps for the District Report logic which uses it
    const { col, dir } = currentSort;
    sortedOpps = [...currentResults].sort((a, b) => {
        let valA = a[col];
        let valB = b[col];
        if (col === 'Propiedad') {
            valA = (a['Propiedad'] || '').toLowerCase();
            valB = (b['Propiedad'] || '').toLowerCase();
        }
        if (valA === undefined || valA === null) valA = '';
        if (valB === undefined || valB === null) valB = '';
        if (typeof valA === 'string') valA = valA.toLowerCase();
        if (typeof valB === 'string') valB = valB.toLowerCase();
        if (valA < valB) return -1 * dir;
        if (valA > valB) return 1 * dir;
        return 0;
    }).slice(0, 50);
}

function setupDistrictReport() {
    const btnGen = document.getElementById('btnGenerateDistrictReport');
    if (btnGen) {
        // Clone to remove old listeners
        const newBtn = btnGen.cloneNode(true);
        btnGen.parentNode.replaceChild(newBtn, btnGen);

        newBtn.addEventListener('click', () => {
            if (!sortedOpps || sortedOpps.length === 0) {
                alert("No hay resultados para analizar.");
                return;
            }

            // Extract Unique Districts maintaining order
            const uniqueDistricts = [];
            const seen = new Set();
            for (const res of sortedOpps) {
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

            // 3. Determine Location Context (City/Province) from analysis data
            let city = "";
            let province = "";

            // Look up the real Ciudad and Provincia from the analysis results
            const matchingResult = currentResults.find(r => r.Distrito === selectedDistrict);
            if (matchingResult) {
                city = matchingResult.Ciudad || "";
                province = matchingResult.Provincia || "";
            }

            // Fallback: try to extract from filename if analysis data is missing
            if (!city && !province) {
                const ventaFile = document.getElementById('ventaFile') ? document.getElementById('ventaFile').value : '';
                if (ventaFile) {
                    const parts = ventaFile.replace('.xlsx', '').split('_');
                    if (parts.length >= 2) {
                        city = parts[1].replace(/-/g, ' ');
                        province = city;
                    }
                }
            }

            // 4. Build display name for UI
            let displayName = selectedDistrict;
            if (city && province && city !== province) {
                displayName = `${selectedDistrict}, ${city} (${province})`;
            } else if (city) {
                displayName = `${selectedDistrict}, ${city}`;
            }

            console.log(`Starting Unified Deep Research for: ${displayName}`);

            // 5. Execute Deep Research with structured location data
            await executeDeepResearch({
                distrito: selectedDistrict,
                ciudad: city,
                provincia: province,
                displayName: displayName
            });
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
async function executeDeepResearch(locationData) {
    // locationData can be a string (legacy) or an object { distrito, ciudad, provincia, displayName }
    let distrito, ciudad, provincia, displayName;
    if (typeof locationData === 'string') {
        distrito = locationData;
        ciudad = '';
        provincia = '';
        displayName = locationData;
    } else {
        distrito = locationData.distrito;
        ciudad = locationData.ciudad || '';
        provincia = locationData.provincia || '';
        displayName = locationData.displayName || distrito;
    }

    const section = document.getElementById('districtReportSection');
    const content = document.getElementById('districtReportContent');

    section.classList.remove('hidden');
    content.innerHTML = `
        <div class="deep-research-loading">
            <div class="spinner"></div>
            <h3>🔬 Ejecutando Deep Research para: ${displayName}</h3>
            <p>Buscando en Google (21 consultas) + Sintetizando con Gemini...</p>
            <p style="color: #888; font-size: 0.9rem;">Esto puede tardar 30-60 segundos</p>
        </div>
    `;
    section.scrollIntoView({ behavior: 'smooth' });

    try {
        const res = await fetch('/api/deep-research', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ distrito, ciudad, provincia })
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

function loadReferenciasByUrl(targetUrl) {
    // Search in both collections
    let opp = currentResults.find(o => o.URL === targetUrl);
    if (!opp) opp = currentTop100.find(o => o.URL === targetUrl);

    if (!opp || !opp.comparables || opp.comparables.length === 0) {
        alert("No se encontraron comparables para esta vivienda.");
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

