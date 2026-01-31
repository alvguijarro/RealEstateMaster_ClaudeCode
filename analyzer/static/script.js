document.addEventListener('DOMContentLoaded', () => {
    // Setup file refresh button
    const refreshBtn = document.getElementById('btnRefreshFiles');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', loadFiles);
    }

    loadFiles();
    setupFilters();
    setupAnalyze();
    setupSorting();
    startHeartbeat();
});

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
        document.getElementById('resultsArea').classList.remove('hidden');
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
                    loadResults();
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

async function loadResults() {
    try {
        const res = await fetch('/api/results');
        const response = await res.json();

        if (response.error) {
            console.error("API Error:", response.error);
            return;
        }

        currentResults = response.data || [];
        renderResults();

        document.getElementById('resultCount').textContent = `${currentResults.length} Oportunidades encontradas`;

    } catch (e) {
        console.error("Error loading results", e);
    }
}

function renderResults() {
    const tbody = document.getElementById('opportunitiesBody');
    tbody.innerHTML = '';

    const { col, dir } = currentSort;
    const sorted = [...currentResults].sort((a, b) => {
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

    sorted.slice(0, 50).forEach((opp, arrIdx) => {
        const tr = document.createElement('tr');
        const titulo = opp.Propiedad || opp.Distrito;
        const url = opp.URL || '#';
        const precio = Number(opp.Precio).toLocaleString();

        // Use renta_rango if available, otherwise format from estimada
        const rentaRango = opp.renta_rango || opp.Renta_Rango ||
            (opp['Renta_estimada/mes'] ? `${Number(opp['Renta_estimada/mes']).toLocaleString()}€` : '-');

        const rentab = (opp['Rentabilidad_Bruta_%'] * 100).toFixed(2);

        // Precision with color coding
        const precision = opp.precision || opp.Precision || 0;
        let precisionColor = '#888';
        if (precision >= 80) precisionColor = 'var(--success-color)';
        else if (precision >= 50) precisionColor = '#f0ad4e';
        else precisionColor = '#d9534f';

        const punt = opp['Puntuación'];

        const hasRefs = opp.comparables && opp.comparables.length > 0;
        const refsLink = hasRefs
            ? `<a href="#" class="link-icon consultar-link" data-idx="${arrIdx}" style="color: var(--accent-color);">Consultar</a>`
            : '<span style="color: #888;">-</span>';

        // Map distrito/ciudad to Comunidad Autónoma code for calculator
        const distrito = (opp.Distrito || '').toLowerCase();
        let comunidad = 'madrid'; // Default
        // Simple mapping based on common cities
        if (distrito.includes('barcelona') || distrito.includes('tarragona') || distrito.includes('girona') || distrito.includes('lleida')) comunidad = 'cataluna';
        else if (distrito.includes('valencia') || distrito.includes('alicante') || distrito.includes('castellón')) comunidad = 'valenciana';
        else if (distrito.includes('sevilla') || distrito.includes('málaga') || distrito.includes('córdoba') || distrito.includes('granada') || distrito.includes('cádiz')) comunidad = 'andalucia';
        else if (distrito.includes('bilbao') || distrito.includes('san sebastián') || distrito.includes('vitoria')) comunidad = 'paisvasco';
        else if (distrito.includes('zaragoza') || distrito.includes('huesca') || distrito.includes('teruel')) comunidad = 'aragon';
        else if (distrito.includes('toledo') || distrito.includes('ciudad real') || distrito.includes('guadalajara') || distrito.includes('cuenca') || distrito.includes('albacete')) comunidad = 'castillamancha';
        else if (distrito.includes('valladolid') || distrito.includes('salamanca') || distrito.includes('león') || distrito.includes('burgos') || distrito.includes('segovia')) comunidad = 'castillaleon';
        else if (distrito.includes('palma') || distrito.includes('mallorca') || distrito.includes('ibiza') || distrito.includes('menorca')) comunidad = 'baleares';
        else if (distrito.includes('las palmas') || distrito.includes('tenerife') || distrito.includes('canarias')) comunidad = 'canarias';
        else if (distrito.includes('murcia') || distrito.includes('cartagena')) comunidad = 'murcia';
        else if (distrito.includes('oviedo') || distrito.includes('gijón') || distrito.includes('asturias')) comunidad = 'asturias';
        else if (distrito.includes('a coruña') || distrito.includes('vigo') || distrito.includes('santiago') || distrito.includes('galicia')) comunidad = 'galicia';
        else if (distrito.includes('pamplona') || distrito.includes('navarra')) comunidad = 'navarra';
        else if (distrito.includes('santander') || distrito.includes('cantabria')) comunidad = 'cantabria';
        else if (distrito.includes('logroño') || distrito.includes('rioja')) comunidad = 'rioja';
        else if (distrito.includes('mérida') || distrito.includes('badajoz') || distrito.includes('cáceres') || distrito.includes('extremadura')) comunidad = 'extremadura';
        else if (distrito.includes('ceuta')) comunidad = 'ceuta';
        else if (distrito.includes('melilla')) comunidad = 'melilla';

        const calcBtn = `
            <button class="btn-calc" onclick="openCalc(${opp.Precio}, ${opp['Renta_estimada/mes'] || 0}, '${comunidad}')" 
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
            <td><span class="score-badge">${punt}</span></td>
            <td>${refsLink}</td>
            <td>${calcBtn}</td>
        `;
        tbody.appendChild(tr);
    });

    setupConsultarLinks();
    setupDistrictReport();
}

function setupSorting() {
    document.querySelectorAll('.results-table th[data-sort]').forEach(th => {
        th.addEventListener('click', () => {
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

            const districts = [];
            const seen = new Set();

            // Extract Top 3 Unique Districts
            for (const res of sortedResults) {
                if (res.Distrito && !seen.has(res.Distrito)) {
                    seen.add(res.Distrito);
                    districts.push(res.Distrito);
                    if (districts.length >= 3) break;
                }
            }

            if (districts.length === 0) {
                alert("No se encontraron distritos válidos.");
                return;
            }

            // Store for batch processing
            batchDistrictsToAnalyze = districts;

            // Open Modal
            const modal = document.getElementById('reportModal');
            const reportOutput = document.getElementById('reportOutput');
            const promptInput = document.getElementById('promptInput');
            const btnModalGen = document.getElementById('btnGenerateReport');

            document.querySelector('.modal-content h2').textContent = `Generar Informe para: ${districts.join(', ')}`;

            // Default Prompt Template
            promptInput.value = "Genera un análisis de inversión detallado centrado en rentabilidad y riesgos.";

            // Reset UI
            reportOutput.innerHTML = '';
            reportOutput.classList.add('hidden');
            btnModalGen.style.display = 'block';
            document.querySelector('.form-group').style.display = 'block';
            btnModalGen.disabled = false;
            btnModalGen.textContent = "GENERAR INFORMES";

            modal.classList.remove('hidden');
        });
    }
}

function showDistrictReports(districts) {
    const section = document.getElementById('districtReportSection');
    const select = document.getElementById('districtSelect');
    const content = document.getElementById('districtReportContent');

    section.classList.remove('hidden');
    select.innerHTML = '';

    districts.forEach(dist => {
        if (availableDistrictReports[dist]) {
            const opt = document.createElement('option');
            opt.value = dist;
            opt.textContent = dist;
            select.appendChild(opt);
        }
    });

    if (select.options.length > 0) {
        select.selectedIndex = 0;
        renderDistrictContent(select.value);
    } else {
        content.innerHTML = "No se pudieron generar informes.";
    }

    select.onchange = () => {
        renderDistrictContent(select.value);
    };

    section.scrollIntoView({ behavior: 'smooth' });
}

function renderDistrictContent(distName) {
    const content = document.getElementById('districtReportContent');
    const markdown = availableDistrictReports[distName];
    if (markdown) {
        content.innerHTML = marked.parse(markdown);
    }
}

// Modal Interaction
const modal = document.getElementById('reportModal');
const closeBtn = document.querySelector('.close-modal');
const generateBtn = document.getElementById('btnGenerateReport');
const reportOutput = document.getElementById('reportOutput');

closeBtn.onclick = () => {
    modal.classList.add('hidden');
};
window.onclick = (e) => {
    if (e.target == modal) {
        modal.classList.add('hidden');
    }
}

generateBtn.onclick = async () => {
    const userPrompt = document.getElementById('promptInput').value;

    // Switch to Processing Mode
    generateBtn.disabled = true;
    generateBtn.textContent = "PROCESANDO DISTRITOS...";
    reportOutput.innerHTML = '';
    reportOutput.classList.remove('hidden');

    availableDistrictReports = {};
    const districts = batchDistrictsToAnalyze;

    for (let i = 0; i < districts.length; i++) {
        const dist = districts[i];
        reportOutput.innerHTML += `<div class="log-line">> Generando informe para <strong>${dist}</strong> (${i + 1}/${districts.length})...</div>`;

        try {
            // Combine User Prompt + District Context
            const fullPrompt = `${userPrompt}\n\n[CONTEXTO]\nDistrito: ${dist}\nProvincia: Madrid\nComunidad Autónoma: Madrid`;

            const res = await fetch('/api/generate-report', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ prompt: fullPrompt })
            });

            const data = await res.json();
            if (data.error) throw new Error(data.error);

            availableDistrictReports[dist] = data.report;
            reportOutput.innerHTML += `<div class="log-line log-success">√ Completado.</div>`;

        } catch (e) {
            reportOutput.innerHTML += `<div class="log-line log-error">X Error: ${e.message}</div>`;
        }
    }

    reportOutput.innerHTML += `<div class="log-line log-highlight">¡Proceso finalizado! Mostrando resultados...</div>`;

    setTimeout(() => {
        modal.classList.add('hidden');
        showDistrictReports(districts);
        generateBtn.disabled = false;
        generateBtn.textContent = "GENERAR INFORMES";
    }, 1500);
};

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
function setupDeepResearchButton() {
    const btn = document.getElementById('btnDeepResearch');
    if (!btn) return;

    btn.addEventListener('click', () => {
        if (!sortedResults || sortedResults.length === 0) {
            alert("Primero ejecuta un análisis para obtener resultados.");
            return;
        }

        // Get the top district from results
        const topDistrict = sortedResults[0]?.Distrito;
        if (!topDistrict) {
            alert("No se encontró un distrito válido.");
            return;
        }

        // Try to extract city from filename
        let city = "Madrid";
        const ventaFile = document.getElementById('ventaFile').value;
        if (ventaFile) {
            // Expected: idealista_City_venta.xlsx
            const parts = ventaFile.split('_');
            if (parts.length >= 2) {
                city = parts[1];
            }
        }

        // Format: "Distrito Name (City, Province)"
        // Assuming Madrid as province since app is Madrid-centric or consistent
        const defaultPrompt = `Distrito ${topDistrict} (${city}, Madrid)`;

        // Prompt user to confirm or modify
        const distrito = prompt(
            "🔬 Deep Research\n\nEste proceso ejecutará 21 búsquedas en Google y sintetizará un informe completo con Gemini.\n\nDistrito a investigar:",
            defaultPrompt
        );

        if (distrito) {
            executeDeepResearch(distrito);
        }
    });
}

// Initialize Deep Research on page load
document.addEventListener('DOMContentLoaded', () => {
    setupDeepResearchButton();
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
    if (window.parent && window.parent.openCalculator) {
        window.parent.openCalculator({ price: price, rent: rent, comunidad: comunidad });
    } else {
        // Fallback for standalone mode
        window.open(`/calculator?price=${price}&rent=${rent}&comunidad=${comunidad}`, '_blank');
    }
};
