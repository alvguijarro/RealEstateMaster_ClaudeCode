/**
 * Idealista Scraper - Frontend JavaScript
 * Handles WebSocket communication and UI updates
 */

// DOM Elements
const seedUrlInput = document.getElementById('seedUrl');
const outputDirDisplay = document.getElementById('outputDirDisplay');
const startBtn = document.getElementById('startBtn');
const pauseBtn = document.getElementById('pauseBtn');
const stopBtn = document.getElementById('stopBtn');
const dualModeBtn = document.getElementById('dualModeBtn');
const fastBtn = document.getElementById('fastBtn');
const stealthBtn = document.getElementById('stealthBtn');
const statusBadge = document.getElementById('statusBadge');
const logsContainer = document.getElementById('logsContainer');
const clearLogsBtn = document.getElementById('clearLogsBtn');
const tableHeader = document.getElementById('tableHeader');
const tableBody = document.getElementById('tableBody');
const resultsCount = document.getElementById('resultsCount');
const downloadBtn = document.getElementById('downloadBtn');
const emptyState = document.getElementById('emptyState');
const statCurrentPage = document.getElementById('statCurrentPage');
const statTotalPages = document.getElementById('statTotalPages');
const statCurrentProps = document.getElementById('statCurrentProps');
const statTotalProps = document.getElementById('statTotalProps');
const statTime = document.getElementById('statTime');
const statMode = document.getElementById('statMode');
const historyBody = document.getElementById('historyBody');
const historyEmptyState = document.getElementById('historyEmptyState');
const clearHistoryBtn = document.getElementById('clearHistoryBtn');

// State
let currentMode = 'fast';
let isPaused = false;
let isRunning = false;
let startTime = null;
let timerInterval = null;
let properties = [];
let socket = null;
let isUpdateMode = false;
let audioCtx = null;
let autoScrollEnabled = true;

// Batch Global State
let batchPriorEnriched = 0;
let maxEnrichedInCurrentFile = 0;
let batchStartTime = null;

// Column definitions (matches ORDERED_BASE in Python for consistency)
const COLUMNS_STANDARD = [
    'Titulo', 'price', 'old price', 'price change %', 'Ubicacion',
    'actualizado hace',
    'm2 construidos', 'm2 utiles', 'precio por m2', 'Num plantas', 'habs', 'banos',
    'Terraza', 'Garaje', 'Armarios', 'Trastero', 'Calefaccion',
    'tipo', 'parcela', 'ascensor', 'orientacion', 'altura',
    'construido en', 'jardin', 'piscina', 'aire acond',
    'Calle', 'Barrio', 'Distrito', 'Zona', 'Ciudad', 'Provincia',
    'Consumo 1', 'Consumo 2', 'Emisiones 1', 'Emisiones 2',
    'estado', 'gastos comunidad',
    'okupado', 'Copropiedad', 'con inquilino', 'nuda propiedad', 'ces. remate',
    'Descripcion',
    'URL'
];

// Column definitions for room rentals (habitaciones)
const COLUMNS_HABITACIONES = [
    'Titulo', 'price', 'old price', 'price change %', 'Ubicacion',
    'actualizado hace',
    'habs', 'm2_habs', 'banos',
    'Terraza', 'Garaje', 'Armarios', 'Trastero', 'Calefaccion',
    'ascensor', 'orientacion', 'altura',
    'jardin', 'piscina', 'aire acond',
    'Calle', 'Barrio', 'Distrito', 'Zona', 'Ciudad', 'Provincia',
    'estado',
    'tipo anunciante', 'nombre anunciante',
    'Descripcion',
    'Fecha Scraping',
    'URL',
    'Anuncio activo', 'Baja anuncio', 'Comunidad Autonoma'
];

// Current active columns (will be set based on seed URL)
let currentColumns = COLUMNS_STANDARD;


// Initialize
document.addEventListener('DOMContentLoaded', () => {
    initializeSocket();
    initializeUI();
    buildTableHeader();
    loadDefaultConfig();
    loadHistory();
    checkResumeState();  // Check if there's a saved session to resume
    loadExcelFiles();    // Load Excel files for URL update dropdown
});

// URL Update Elements
const updateExcelSelect = document.getElementById('updateExcelFile');
const updateUrlsBtn = document.getElementById('updateUrlsBtn');
const resumeUpdateBtn = document.getElementById('resumeUpdateBtn');
const worksheetSelectorGroup = document.getElementById('worksheetSelectorGroup');
const worksheetSearch = document.getElementById('worksheetSearch');
const worksheetList = document.getElementById('worksheetList');
const worksheetSelectionInfo = document.getElementById('worksheetSelectionInfo');

// Track available worksheets and selection state
let availableWorksheets = [];
let selectedWorksheets = new Set();

async function loadExcelFiles() {
    try {
        const response = await fetch('/api/excel-files');
        const data = await response.json();

        if (updateExcelSelect) {
            updateExcelSelect.innerHTML = '<option value="">Selecciona un archivo...</option>';
            if (data.files && data.files.length > 0) {
                data.files.forEach(file => {
                    const option = document.createElement('option');
                    option.value = file.path;
                    const countDisplay = (file.count !== undefined && file.count !== null) ? file.count : '?';
                    const dateStr = file.mtime ? ` - [${formatMtime(file.mtime)}]` : '';
                    option.textContent = `${file.name} (${countDisplay} props)${dateStr}`;
                    updateExcelSelect.appendChild(option);
                });
            } else {
                updateExcelSelect.innerHTML = '<option value="">[No hay ficheros con propiedades]</option>';
            }
        }
    } catch (error) {
        console.error('Error loading Excel files:', error);
        if (updateExcelSelect) {
            updateExcelSelect.innerHTML = '<option value="">Error al cargar archivos</option>';
        }
    }
}

// Load worksheets when file is selected
async function loadWorksheets(filePath) {
    if (!filePath || !worksheetList || !worksheetSelectorGroup) return;

    try {
        const response = await fetch(`/api/excel-worksheets?file=${encodeURIComponent(filePath)}`);
        const data = await response.json();

        if (data.sheets && data.sheets.length > 0) {
            availableWorksheets = data.sheets;
            selectedWorksheets = new Set(data.sheets); // Select all by default
            renderWorksheetList();
            updateSelectionInfo();
            worksheetSelectorGroup.style.display = 'block';
        } else {
            worksheetSelectorGroup.style.display = 'none';
        }
    } catch (error) {
        console.error('Error loading worksheets:', error);
        worksheetSelectorGroup.style.display = 'none';
    }
}

function renderWorksheetList(filter = '') {
    if (!worksheetList) return;

    const filterLower = filter.toLowerCase();
    worksheetList.innerHTML = '';

    // "Todos" checkbox
    const todosItem = createCheckboxItem('__TODOS__', 'Todos (seleccionar todas)',
        selectedWorksheets.size === availableWorksheets.length);
    worksheetList.appendChild(todosItem);

    // Divider
    const divider = document.createElement('div');
    divider.style.cssText = 'border-top: 1px solid var(--border); margin: 4px 0;';
    worksheetList.appendChild(divider);

    // Individual sheets
    availableWorksheets.forEach(sheet => {
        if (filter && !sheet.toLowerCase().includes(filterLower)) return;
        const item = createCheckboxItem(sheet, sheet, selectedWorksheets.has(sheet));
        worksheetList.appendChild(item);
    });
}

function createCheckboxItem(value, label, checked) {
    const item = document.createElement('label');
    item.style.cssText = 'display: flex; align-items: center; padding: 6px 10px; cursor: pointer; gap: 8px;';
    item.onmouseover = () => item.style.background = 'rgba(140, 29, 100, 0.1)';
    item.onmouseout = () => item.style.background = 'transparent';

    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.value = value;
    checkbox.checked = checked;
    checkbox.style.cssText = 'cursor: pointer; width: 16px; height: 16px;';
    checkbox.addEventListener('change', () => handleCheckboxChange(value, checkbox.checked));

    const text = document.createElement('span');
    text.textContent = label;
    text.style.cssText = 'font-size: 13px; color: var(--text-primary);';

    item.appendChild(checkbox);
    item.appendChild(text);
    return item;
}

function handleCheckboxChange(value, checked) {
    if (value === '__TODOS__') {
        // Toggle all
        if (checked) {
            selectedWorksheets = new Set(availableWorksheets);
        } else {
            selectedWorksheets.clear();
        }
    } else {
        // Toggle individual sheet
        if (checked) {
            selectedWorksheets.add(value);
        } else {
            selectedWorksheets.delete(value);
        }
    }

    renderWorksheetList(worksheetSearch ? worksheetSearch.value : '');
    updateSelectionInfo();
}

function updateSelectionInfo() {
    if (!worksheetSelectionInfo) return;
    const count = selectedWorksheets.size;
    const total = availableWorksheets.length;
    worksheetSelectionInfo.textContent = count === total
        ? `Seleccionadas: Todas (${total})`
        : `Seleccionadas: ${count} de ${total}`;
}

function getSelectedSheets() {
    // If all selected, return empty array (means all)
    if (selectedWorksheets.size === availableWorksheets.length) {
        return [];
    }
    return Array.from(selectedWorksheets);
}

// TABS & API DASHBOARD LOGIC
// ==========================================

let allProvinces = [];

// Load Provinces
async function loadProvinces() {
    try {
        const response = await fetch('/api/provinces');
        const data = await response.json();
        const select = document.getElementById('apiProvinces');

        if (select && data.provinces) {
            allProvinces = data.provinces;
            renderProvinces(allProvinces);
        }
    } catch (e) {
        console.error("Error loading provinces", e);
    }
}

function renderProvinces(list) {
    const select = document.getElementById('apiProvinces');
    if (!select) return;
    const currentSelected = new Set(Array.from(select.selectedOptions).map(o => o.value));

    select.innerHTML = '';
    list.forEach(p => {
        const opt = document.createElement('option');
        opt.value = p.id;
        opt.textContent = p.name;
        if (currentSelected.has(p.id)) opt.selected = true;
        select.appendChild(opt);
    });
    updateProvinceCount();
}

function updateProvinceCount() {
    const select = document.getElementById('apiProvinces');
    const countDisplay = document.getElementById('selectedCount');
    if (select && countDisplay) {
        const count = Array.from(select.selectedOptions).length;
        countDisplay.textContent = `${count} seleccionadas. Vacío = TODAS.`;
    }
}

function selectAllProvinces() {
    const select = document.getElementById('apiProvinces');
    if (!select) return;
    Array.from(select.options).forEach(opt => opt.selected = true);
    updateProvinceCount();
}

// Load Enrichment Files (Optimized)
async function loadEnrichFiles() {
    try {
        const response = await fetch('/api/salidas-files?limit=100');
        const data = await response.json();
        const select = document.getElementById('enrichFileSelect');

        if (select && data.files) {
            select.innerHTML = '';
            // Newest first is already handled by server, we show a 'Recientes' label
            if (data.files.length === 0) {
                select.innerHTML = '<option value="">Sin archivos en salidas/</option>';
                return;
            }

            data.files.forEach(f => {
                const opt = document.createElement('option');
                opt.value = f.path; // Use full path for the server
                opt.textContent = f.name;
                select.appendChild(opt);
            });
        }
    } catch (e) {
        console.error("Error loading enrich files", e);
    }
}

// Tab Switching
document.addEventListener('DOMContentLoaded', () => {
    loadProvinces();
    loadEnrichFiles();

    // Province Search Listener
    const provSearch = document.getElementById('provinceSearch');
    if (provSearch) {
        provSearch.addEventListener('input', () => {
            const term = provSearch.value.toLowerCase();
            const filtered = allProvinces.filter(p => p.name.toLowerCase().includes(term));
            renderProvinces(filtered);
        });
    }

    // Province Selection Listener
    const provSelect = document.getElementById('apiProvinces');
    if (provSelect) {
        provSelect.addEventListener('change', updateProvinceCount);
    }

    const tabBtns = document.querySelectorAll('.tab-btn');
    if (tabBtns.length > 0) {
        tabBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                // Remove active class from all buttons
                tabBtns.forEach(b => b.classList.remove('active'));
                // Add active to clicked
                btn.classList.add('active');

                // Hide all panes
                document.querySelectorAll('.tab-pane').forEach(p => p.style.display = 'none');

                // Show target pane
                const targetId = `tab-${btn.dataset.tab}`;
                const targetPane = document.getElementById(targetId);
                if (targetPane) {
                    targetPane.style.display = 'contents';
                }
            });
        });
    }
});

// Run API Task (Exposed globally for onclick)
window.runApiTask = async function (endpoint, operation) {
    // Check if scraper is running (optional, but good practice)
    if (isRunning) {
        if (!confirm("El scraper principal parece estar activo. ¿Seguro que quieres lanzar esta tarea en paralelo?")) {
            return;
        }
    }

    // UI Feedback
    addLog('INFO', `⏳ Solicitando Tarea: ${endpoint} ${operation ? '(' + operation + ')' : ''}...`);

    try {
        const body = {};
        if (operation) body.operation = operation;

        if (endpoint === 'batch-scan') {
            const select = document.getElementById('apiProvinces');
            if (select && select.selectedOptions.length > 0) {
                const selected = Array.from(select.selectedOptions).map(opt => opt.value);
                body.provinces = selected;
                // Add info log
                addLog('INFO', `🎯 Filtrando por ${selected.length} provincias seleccionadas.`);
            }
        }

        if (endpoint === 'enrich') {
            const select = document.getElementById('enrichFileSelect');
            if (select && select.value) {
                body.file_path = select.value;
                const fileName = select.options[select.selectedIndex].text;
                addLog('INFO', `🎯 Enriqueciendo: ${fileName}`);
            }
        }

        const response = await fetch(`/api/${endpoint}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });

        const data = await response.json();

        if (response.ok) {
            addLog('OK', `✅ Tarea iniciada: ${data.task}`);
            addLog('INFO', 'Sigue el progreso en este log...');
        } else {
            addLog('ERR', `❌ Error: ${data.message}`);
            alert(`Error: ${data.message}`);
        }
    } catch (e) {
        addLog('ERR', `❌ Error de conexión: ${e.message}`);
    }
};


// File selection change listener
if (updateExcelSelect) {
    updateExcelSelect.addEventListener('change', (e) => {
        const filePath = e.target.value;
        if (!filePath) {
            // Clear and hide the worksheet selector when no file is selected
            if (worksheetSelectorGroup) {
                worksheetSelectorGroup.style.display = 'none';
            }
            if (worksheetList) {
                worksheetList.innerHTML = '';
            }
            availableWorksheets = [];
            selectedWorksheets.clear();
            if (worksheetSelectionInfo) {
                worksheetSelectionInfo.textContent = '';
            }
            if (worksheetSearch) {
                worksheetSearch.value = '';
            }
        } else {
            loadWorksheets(filePath);
            checkUpdateState(filePath);
        }
    });
}

// Check if a checkpoint exists for the file
async function checkUpdateState(filePath) {
    if (!resumeUpdateBtn) return;

    // Hide by default
    resumeUpdateBtn.style.display = 'none';

    try {
        const response = await fetch('/api/update/check-state', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ excel_file: filePath })
        });
        const data = await response.json();

        if (data.can_resume) {
            resumeUpdateBtn.style.display = 'block';
            resumeUpdateBtn.innerHTML = `<span class="btn-icon">▶</span> Reanudar (${data.current_index}/${data.total})`;
            resumeUpdateBtn.title = `Reanudar desde propiedad ${data.current_index + 1}`;
        }
    } catch (error) {
        console.error('Error checking state:', error);
    }
}

// Search filter listener
if (worksheetSearch) {
    worksheetSearch.addEventListener('input', (e) => {
        renderWorksheetList(e.target.value);
    });
}

async function startUrlUpdate(resume = false) {
    const excelFile = updateExcelSelect ? updateExcelSelect.value : '';

    if (!excelFile) {
        addLog('ERR', 'Por favor, selecciona un archivo Excel');
        return;
    }

    // Get selected sheets using new checkbox-based selection
    const selectedSheets = getSelectedSheets();

    addLog('INFO', `Iniciando actualización de URLs desde: ${excelFile}`);
    updateUrlsBtn.disabled = true;
    updateUrlsBtn.innerHTML = '<span class="btn-icon">⏳</span> Actualizando...';

    // Set UI state for update mode
    isUpdateMode = true;
    isRunning = true;
    isPaused = false;
    startBtn.disabled = true;
    startBtn.title = "No disponible durante actualización";
    pauseBtn.disabled = false;
    stopBtn.disabled = false;
    if (resumeBtn) resumeBtn.style.display = 'none';

    try {
        const response = await fetch('/api/update-urls', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ excel_file: excelFile, sheets: selectedSheets, resume: resume })
        });

        const data = await response.json();

        if (!response.ok) {
            addLog('ERR', data.error || 'Error al iniciar actualización');
            resetUIState();
        } else {
            addLog('OK', 'Actualización de URLs iniciada');
        }
    } catch (error) {
        addLog('ERR', `Error de conexión: ${error.message}`);
        resetUIState();
    }
}

// Add event listener for URL update button
if (updateUrlsBtn) {
    updateUrlsBtn.addEventListener('click', () => startUrlUpdate(false));
}

if (resumeUpdateBtn) {
    resumeUpdateBtn.addEventListener('click', () => startUrlUpdate(true));
}

// API Import Handler
const startApiImportBtn = document.getElementById('startApiImportBtn');

if (startApiImportBtn) {
    startApiImportBtn.addEventListener('click', async () => {
        const locationId = document.getElementById('apiLocationId').value.trim();
        const operation = document.getElementById('apiOperation').value;
        const maxPages = document.getElementById('apiMaxPages').value;

        if (!locationId) {
            addLog('ERR', 'Por favor, introduce un Location ID válido (ej: 0-EU-ES-45)');
            return;
        }

        addLog('INFO', `Iniciando importación API para ${locationId}...`);

        // Reset state matches startScraping
        properties = [];
        tableBody.innerHTML = '';
        emptyState.style.display = 'block';
        downloadBtn.style.display = 'none';

        // Disable buttons
        startApiImportBtn.disabled = true;
        if (startBtn) startBtn.disabled = true;

        try {
            const response = await fetch('/api/import-api', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    location_id: locationId,
                    operation: operation,
                    max_pages: maxPages
                })
            });

            const data = await response.json();
            if (!response.ok) {
                addLog('ERR', data.error || 'Error al iniciar API import');
                startApiImportBtn.disabled = false;
                if (startBtn) startBtn.disabled = false;
            } else {
                startTimer(); // Track time
                // Status updates will come via socket
            }
        } catch (e) {
            addLog('ERR', 'Error de conexión: ' + e.message);
            startApiImportBtn.disabled = false;
            if (startBtn) startBtn.disabled = false;
        }
    });
}

async function loadDefaultConfig() {
    try {
        const response = await fetch('/api/config');
        const data = await response.json();
        if (data.default_output_dir && outputDirDisplay) {
            outputDirDisplay.textContent = data.default_output_dir;
        }
    } catch (error) {
        console.error('Error loading config:', error);
    }
}

function initializeSocket() {
    socket = io();

    socket.on('connect', () => {
        addLog('INFO', 'Conectado al servidor');
        updateServerButtons(true);
    });

    socket.on('disconnect', () => {
        addLog('WARN', 'Desconectado del servidor');
        updateServerButtons(false);
    });

    socket.on('log', (data) => {
        addLog(data.level, data.message);
    });

    socket.on('property_scraped', (data) => {
        addProperty(data);
    });

    socket.on('status_change', (data) => {
        handleStatusChange(data);
    });

    socket.on('history_update', (entry) => {
        addHistoryRow(entry);
        historyEmptyState.style.display = 'none';
    });

    socket.on('progress_update', (data) => {
        // Enrichment Mode (Detailed)
        if (data.excel_file) {
            // Update Max for this file
            if (data.current_properties > maxEnrichedInCurrentFile) {
                maxEnrichedInCurrentFile = data.current_properties;
            }

            // Update Scorecards (Global Batch Count)
            const globalCurrent = batchPriorEnriched + data.current_properties;
            const globalTotal = batchPriorEnriched + data.total_properties; // Running total estimate

            statCurrentProps.textContent = globalCurrent;
            statTotalProps.textContent = globalTotal;

            // Update Batch Progress Box
            if (batchProgressText) {
                batchProgressText.innerHTML = `Enriqueciendo '${data.excel_file}'\nDistrito: '${data.sheet_name || 'Generando...'}'\nProgreso: ${data.current_properties} / ${data.total_properties}`;
            }
        } else {
            // Standard Scraper Mode / API Import
            statCurrentPage.textContent = String(data.current_page || 0).padStart(2, '0');
            statTotalPages.textContent = String(data.total_pages || 0).padStart(2, '0');
            statCurrentProps.textContent = data.current_properties || 0;
            statTotalProps.textContent = data.total_properties || 0;
        }
    });

    socket.on('browser_closed', (data) => {
        addLog('WARN', 'El navegador fue cerrado. Scraping pausado.');
        showBrowserClosedModal();
    });

    // Initialize Batch Listeners if function exists (it's defined at bottom)
    if (typeof setupBatchSocketListeners === 'function') {
        setupBatchSocketListeners();
    }
}

// Browser closed modal
const browserClosedModal = document.getElementById('browserClosedModal');
const modalResumeBtn = document.getElementById('modalResumeBtn');
const modalStopBtn = document.getElementById('modalStopBtn');

function showBrowserClosedModal() {
    browserClosedModal.style.display = 'flex';
}

function hideBrowserClosedModal() {
    browserClosedModal.style.display = 'none';
}

// Modal button handlers
if (modalResumeBtn) {
    modalResumeBtn.addEventListener('click', async () => {
        hideBrowserClosedModal();
        addLog('INFO', 'Reanudando scraping...');
        try {
            await fetch('/api/resume', { method: 'POST' });
        } catch (error) {
            addLog('ERR', 'Error al reanudar: ' + error.message);
        }
    });
}

if (modalStopBtn) {
    modalStopBtn.addEventListener('click', async () => {
        hideBrowserClosedModal();
        addLog('INFO', 'Deteniendo y guardando datos...');
        try {
            await fetch('/api/stop', { method: 'POST' });
        } catch (error) {
            addLog('ERR', 'Error al detener: ' + error.message);
        }
    });
}

function initializeUI() {
    // Mode toggle - only Fast and Stealth
    fastBtn.addEventListener('click', () => selectMode('fast'));
    if (stealthBtn) {
        stealthBtn.addEventListener('click', () => selectMode('stealth'));
    }

    // Action buttons
    startBtn.addEventListener('click', () => startScraping(false));
    if (dualModeBtn) {
        dualModeBtn.addEventListener('click', () => startScraping(true));
    }
    pauseBtn.addEventListener('click', togglePause);
    stopBtn.addEventListener('click', stopScraping);
    clearLogsBtn.addEventListener('click', clearLogs);
    clearHistoryBtn.addEventListener('click', clearHistory);

    // Auto-scroll toggle for logs
    const toggleAutoScrollBtn = document.getElementById('toggleAutoScrollBtn');
    if (toggleAutoScrollBtn) {
        toggleAutoScrollBtn.addEventListener('click', toggleAutoScroll);
    }

    // Enter key to start
    seedUrlInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter' && !isRunning) {
            startScraping(false);
        }
    });

    // URL validation for Dual Mode
    seedUrlInput.addEventListener('input', validateDualMode);
}

function validateDualMode() {
    if (!dualModeBtn) return;

    const url = seedUrlInput.value.trim();
    const hasPagina = url.includes('/pagina-');
    const isIdealista = url.includes('idealista.com');

    // Enable only if it's idealista URL AND does NOT have /pagina-
    if (isIdealista && !hasPagina && url.length > 0) {
        dualModeBtn.disabled = false;
        dualModeBtn.style.cursor = 'pointer';
        dualModeBtn.title = "Iniciar scraping secuencial (Alquiler + Venta)";
    } else {
        dualModeBtn.disabled = true;
        dualModeBtn.style.cursor = 'not-allowed';
        if (hasPagina) {
            dualModeBtn.title = "No disponible para URLs paginadas específico (contiene /pagina-X)";
        } else {
            dualModeBtn.title = "Introduce una URL válida de Idealista";
        }
    }
}

function selectMode(mode) {
    currentMode = mode;
    fastBtn.classList.toggle('active', mode === 'fast');
    if (stealthBtn) {
        stealthBtn.classList.toggle('active', mode === 'stealth');
    }
    const modeNames = {
        'fast': 'Fast',
        'stealth': 'Stealth'
    };
    statMode.textContent = modeNames[mode] || mode;

    // Hot-swap: Notify server if scraper exists
    if (isRunning || isPaused) {
        fetch('/api/set_mode', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ mode: mode })
        }).then(response => {
            if (response.ok) {
                addLog('INFO', `Modo cambiado dinámicamente a: ${modeNames[mode]}`);
            } else {
                addLog('ERR', 'Error al cambiar modo');
            }
        });
    }
}

function buildTableHeader() {
    tableHeader.innerHTML = currentColumns.map(col =>
        `<th>${escapeHtml(col)}</th>`
    ).join('');
}

function addLog(level, message) {
    const now = new Date();
    const time = now.toLocaleTimeString('es-ES', { hour12: false });

    const entry = document.createElement('div');
    entry.className = `log-entry log-${level.toLowerCase()}`;
    entry.innerHTML = `
        <span class="log-time">${time}</span>
        <span class="log-message">${escapeHtml(message)}</span>
    `;

    logsContainer.appendChild(entry);

    // Only auto-scroll if enabled
    if (autoScrollEnabled) {
        logsContainer.scrollTop = logsContainer.scrollHeight;
    }

    // Keep only last 500 entries
    while (logsContainer.children.length > 500) {
        logsContainer.removeChild(logsContainer.firstChild);
    }
}

function clearLogs() {
    logsContainer.innerHTML = '';
    addLog('INFO', 'Logs limpiados');
}

function toggleAutoScroll() {
    autoScrollEnabled = !autoScrollEnabled;
    const btn = document.getElementById('toggleAutoScrollBtn');
    if (btn) {
        if (autoScrollEnabled) {
            btn.textContent = '⏸️';
            btn.title = 'Pausar auto-scroll';
            // Jump to bottom when re-enabling
            logsContainer.scrollTop = logsContainer.scrollHeight;
        } else {
            btn.textContent = '▶️';
            btn.title = 'Reanudar auto-scroll';
        }
    }
}

function addProperty(data) {
    properties.push(data);

    // Update results count (for table display only)
    resultsCount.textContent = `${properties.length} propiedades`;

    // Hide empty state
    emptyState.style.display = 'none';

    // Add table row
    const newFields = new Set(data._new_fields || []);

    const row = document.createElement('tr');
    row.innerHTML = currentColumns.map(col => {
        let value = data[col];
        if (value === null || value === undefined) {
            value = '';
        } else if (typeof value === 'number') {
            if (col === 'price change %') {
                value = (value * 100).toFixed(1) + '%';
            } else {
                value = value.toLocaleString('es-ES');
            }
        }

        // Highlight style
        const isNew = newFields.has(col);
        const style = isNew ? 'color: #4ade80; font-weight: 500;' : '';

        // Make URL clickable
        if (col === 'URL' && value) {
            return `<td><a href="${escapeHtml(value)}" target="_blank" style="color: var(--primary);">${escapeHtml(value)}</a></td>`;
        }
        return `<td style="${style}" title="${escapeHtml(String(value))}">${escapeHtml(String(value))}</td>`;
    }).join('');

    tableBody.appendChild(row);
}

function handleStatusChange(data) {
    const status = data.status;

    // Update badge
    statusBadge.className = `status-badge ${status}`;
    const statusTexts = {
        'idle': 'Inactivo',
        'running': 'Ejecutando',
        'paused': 'Pausado',
        'stopping': 'Deteniendo...',
        'completed': 'Completado',
        'error': 'Error',
        'stopped': 'Detenido',
        'captcha': 'CAPTCHA detectado',
        'resting': 'Descansando...'
    };
    statusBadge.querySelector('.status-text').textContent = statusTexts[status] || status;

    // Update buttons based on status
    if (status === 'running') {
        isRunning = true;
        isPaused = false;
        startBtn.disabled = true;
        if (dualModeBtn) dualModeBtn.disabled = true;
        pauseBtn.disabled = false;
        stopBtn.disabled = false;
        pauseBtn.innerHTML = '<span class="btn-icon">⏸</span> Pausar';
        seedUrlInput.disabled = true;
        if (startApiImportBtn) startApiImportBtn.disabled = true;

        // Mode switching is now allowed during execution (hot-swap)
        fastBtn.style.pointerEvents = 'auto';
        fastBtn.style.opacity = '1';
        if (stealthBtn) {
            stealthBtn.style.pointerEvents = 'auto';
            stealthBtn.style.opacity = '1';
        }
    } else if (status === 'paused') {
        isPaused = true;
        pauseBtn.innerHTML = '<span class="btn-icon">▶</span> Reanudar';
        pauseBtn.disabled = false;

        // Enable mode switching when paused
        fastBtn.style.pointerEvents = 'auto';
        fastBtn.style.opacity = '1';
        if (stealthBtn) {
            stealthBtn.style.pointerEvents = 'auto';
            stealthBtn.style.opacity = '1';
        }
    } else if (status === 'captcha') {
        isPaused = true;
        // Auto-resume logic: Backend will resume automatically, so we just show status
        pauseBtn.disabled = true; // Disable resume button during CAPTCHA
        pauseBtn.innerHTML = '<span class="btn-icon">⏱️</span> Esperando resolución...';
        pauseBtn.classList.add('btn-warning');

        // Play alarm sound
        playAlarm();
    } else if (status === 'stopping') {
        // UI should disable actions while stopping
        pauseBtn.disabled = true;
        stopBtn.disabled = true;
        stopBtn.innerHTML = '<span class="btn-icon">⏳</span> Deteniendo...';
    } else if (status === 'completed' || status === 'stopped' || status === 'error') {
        // TERMINAL STATUSES
        if (isUpdateMode) {
            resetUIState();
        } else {
            // Restore original Stop button text if it was changed to "Deteniendo..."
            stopBtn.innerHTML = '<span class="btn-icon">⏹</span> Detener';

            // Comprehensive reset
            resetUIState();

            // Check if we can resume (e.g. if stopped manually)
            // Delay slightly to ensure backend file write is finished
            setTimeout(checkResumeState, 1000);

            // Show download button if file is available
            if (data.file) {
                downloadBtn.style.display = 'inline-flex';
                downloadBtn.href = '/api/download';
                addLog('OK', `Archivo guardado: ${data.file}`);
            }
        }
    }
}

async function startScraping(isDualMode = false) {
    // Initialize Audio Context on user gesture to prevent blocking
    if (!audioCtx) {
        const AudioContext = window.AudioContext || window.webkitAudioContext;
        if (AudioContext) {
            audioCtx = new AudioContext();
        }
    }
    if (audioCtx && audioCtx.state === 'suspended') {
        await audioCtx.resume();
    }

    const seedUrl = seedUrlInput.value.trim();

    if (!seedUrl) {
        addLog('ERR', 'Por favor, introduce una URL de Idealista');
        seedUrlInput.focus();
        return;
    }

    if (!seedUrl.includes('idealista.com')) {
        addLog('ERR', 'La URL debe ser de idealista.com');
        return;
    }

    // Reset state
    properties = [];
    tableBody.innerHTML = '';
    emptyState.style.display = 'block';
    downloadBtn.style.display = 'none';

    // Reset progress counters
    statCurrentPage.textContent = '0';
    statTotalPages.textContent = '0';
    statCurrentProps.textContent = '0';
    statTotalProps.textContent = '0';
    resultsCount.textContent = '0 propiedades';

    statTotalProps.textContent = '0';
    resultsCount.textContent = '0 propiedades';

    // Disable Update controls
    if (updateUrlsBtn) updateUrlsBtn.disabled = true;
    if (startApiImportBtn) startApiImportBtn.disabled = true;

    // Detect habitacion mode and switch columns
    if (seedUrl.toLowerCase().includes('habitacion')) {
        currentColumns = COLUMNS_HABITACIONES;
        addLog('INFO', 'Modo habitaciones detectado - usando columnas específicas');
    } else {
        currentColumns = COLUMNS_STANDARD;
    }
    buildTableHeader();

    addLog('INFO', `Iniciando scraping en modo ${currentMode.toUpperCase()}...`);

    try {
        const response = await fetch('/api/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                seed_url: seedUrl,
                mode: currentMode,
                dual_mode: isDualMode
            })
        });

        const data = await response.json();

        if (!response.ok) {
            addLog('ERR', data.error || 'Error al iniciar');
            return;
        }

        startTimer();
        const modeNames = {
            'fast': 'Fast',
            'stealth': 'Stealth'
        };
        statMode.textContent = modeNames[currentMode] || currentMode;

    } catch (error) {
        addLog('ERR', `Error de conexión: ${error.message}`);
    }
}

async function togglePause() {
    let endpoint;
    if (isUpdateMode) {
        endpoint = isPaused ? '/api/update/resume' : '/api/update/pause';
    } else {
        endpoint = isPaused ? '/api/resume' : '/api/pause';
    }

    try {
        const response = await fetch(endpoint, { method: 'POST' });
        const data = await response.json();

        if (!response.ok) {
            addLog('ERR', data.error || 'Error');
        }
    } catch (error) {
        addLog('ERR', `Error: ${error.message}`);
    }
}

async function stopScraping() {
    addLog('INFO', 'Deteniendo...');
    const endpoint = isUpdateMode ? '/api/update/stop' : '/api/stop';

    try {
        const response = await fetch(endpoint, { method: 'POST' });
        const data = await response.json();

        if (!response.ok) {
            addLog('ERR', data.error || 'Error al detener');
        }
    } catch (error) {
        addLog('ERR', `Error: ${error.message}`);
    }
}

function resetUIState() {
    isRunning = false;
    isPaused = false;
    isUpdateMode = false;
    startBtn.disabled = false;
    startBtn.title = "";
    pauseBtn.disabled = true;
    stopBtn.disabled = true;

    if (updateUrlsBtn) {
        updateUrlsBtn.innerHTML = '<span class="btn-icon">🔄</span> Actualizar URLs';
        updateUrlsBtn.disabled = false;
    }

    stopTimer();
}

function startTimer() {
    startTime = Date.now();
    timerInterval = setInterval(updateTimer, 1000);
    updateTimer();
}

function stopTimer() {
    if (timerInterval) {
        clearInterval(timerInterval);
        timerInterval = null;
    }
}

function updateTimer() {
    let start = startTime;
    // Prefer batch timer if active (and override standard timer)
    if (batchStartTime) {
        start = batchStartTime;
    }

    if (!start) return;

    const elapsed = Math.floor((Date.now() - start) / 1000);
    const days = Math.floor(elapsed / 86400);
    const hours = Math.floor((elapsed % 86400) / 3600);
    const minutes = Math.floor((elapsed % 3600) / 60).toString().padStart(2, '0');
    const seconds = (elapsed % 60).toString().padStart(2, '0');

    // Build format conditionally - only show days/hours when > 0
    let timeStr = `${minutes}m:${seconds}s`;
    if (hours > 0 || days > 0) {
        timeStr = `${hours.toString().padStart(2, '0')}h:${timeStr}`;
    }
    if (days > 0) {
        timeStr = `${days}d:${timeStr}`;
    }
    // Update both timers if they exist (batch one might be different element later, but for now reuse statTime)
    if (statTime) statTime.textContent = timeStr;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// History Functions
// History Functions
async function loadHistory() {
    const tableBody = document.getElementById('historyBody');
    const emptyState = document.getElementById('historyEmptyState');
    if (!tableBody || !emptyState) return;

    try {
        // Use the file list API instead of database history for Enrichment History
        const response = await fetch('/api/salidas-files?limit=50');
        const data = await response.json();

        // Filter only _updated.xlsx files (Completed enrichments)
        const historyFiles = (data.files || []).filter(f => f.name.endsWith('_updated.xlsx'));

        tableBody.innerHTML = '';

        if (historyFiles.length > 0) {
            emptyState.style.display = 'none';
            historyFiles.forEach(file => {
                const row = document.createElement('tr');
                const dateStr = formatMtime(file.mtime);
                const count = file.count !== undefined ? file.count : '?';

                row.innerHTML = `
                    <td>${dateStr}</td>
                    <td title="${file.name}" style="max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">${file.name}</td>
                    <td>${count}</td>
                    <td><a href="/api/download?file=${file.name}" class="btn-xs">📥 Excel</a></td>
                `;
                tableBody.appendChild(row);
            });
        } else {
            emptyState.style.display = 'block';
        }
    } catch (error) {
        console.error('Error loading history:', error);
    }
}

function addHistoryRow(entry, prepend = true) {
    const row = document.createElement('tr');

    // Format timestamp
    const date = new Date(entry.timestamp);
    const formattedDate = date.toLocaleString('es-ES', {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });

    // Make file link clickable with file:// protocol
    const filePath = entry.output_file ? entry.output_file.replace(/\\/g, '/') : '';
    const fileUrl = filePath ? `file:///${filePath}` : '#';

    row.innerHTML = `
        <td>${escapeHtml(formattedDate)}</td>
        <td title="${escapeHtml(entry.seed_url || '')}">
            <a href="${escapeHtml(entry.seed_url || '#')}" target="_blank" style="color: var(--primary);">
                ${escapeHtml(truncateUrl(entry.seed_url || ''))}
            </a>
        </td>
        <td>${entry.properties_count || 0}</td>
        <td>
            ${entry.filename ? `<a href="${escapeHtml(fileUrl)}" class="file-link" title="${escapeHtml(entry.output_file || '')}">${escapeHtml(entry.filename)}</a>` : 'N/A'}
        </td>
    `;

    if (prepend) {
        historyBody.insertBefore(row, historyBody.firstChild);
    } else {
        historyBody.appendChild(row);
    }
}

function truncateUrl(url) {
    if (!url) return '';
    // Remove protocol
    let short = url.replace(/^https?:\/\//, '');
    // Truncate if too long
    if (short.length > 50) {
        short = short.substring(0, 47) + '...';
    }
    return short;
}

async function clearHistory() {
    if (!confirm('¿Borrar todo el historial de scrapes?')) return;

    try {
        await fetch('/api/history/clear', { method: 'POST' });
        historyBody.innerHTML = '';
        historyEmptyState.style.display = 'block';
        addLog('INFO', 'Historial limpiado');
    } catch (error) {
        addLog('ERR', `Error limpiando historial: ${error.message}`);
    }
}

// Resume state management
let savedResumeState = null;
const resumeBtn = document.getElementById('resumeBtn');

async function checkResumeState() {
    try {
        const response = await fetch('/api/resume-state');
        const data = await response.json();

        if (data.has_state && data.state) {
            savedResumeState = data.state;
            // Show resume button
            if (resumeBtn) {
                resumeBtn.style.display = 'inline-flex';
                // Show URL and count in tooltip
                resumeBtn.title = `URL: ${data.state.seed_url}\nDesde página: ${data.state.current_page}`;
            }
            // Pre-fill seed URL if not already filled
            if (!seedUrlInput.value && data.state.seed_url) {
                seedUrlInput.value = data.state.seed_url;
            }
            console.log('Resume state available:', data.state);
        } else {
            if (resumeBtn) resumeBtn.style.display = 'none';
        }
    } catch (error) {
        console.error('Error checking resume state:', error);
    }
}

async function resumeScraping() {
    // Initialize Audio Context on user gesture
    if (!audioCtx) {
        const AudioContext = window.AudioContext || window.webkitAudioContext;
        if (AudioContext) {
            audioCtx = new AudioContext();
        }
    }
    if (audioCtx && audioCtx.state === 'suspended') {
        await audioCtx.resume();
    }

    if (!savedResumeState) {
        addLog('ERR', 'No hay sesión guardada para reanudar');
        return;
    }

    // Use saved state to start scraping
    const seedUrl = savedResumeState.seed_url;
    const mode = savedResumeState.mode || currentMode;
    const outputDir = savedResumeState.output_dir;

    // Build resume URL with page number
    const pageNum = savedResumeState.current_page || 1;
    let resumeUrl = seedUrl;

    // Update URL to start from saved page
    if (pageNum > 1) {
        // Remove existing pagina- if present
        resumeUrl = resumeUrl.replace(/\/pagina-\d+/, '');
        // Add the correct page
        if (resumeUrl.includes('/areas/')) {
            resumeUrl = resumeUrl.replace(/\/?$/, `/pagina-${pageNum}`);
        } else {
            resumeUrl = resumeUrl.replace(/\/?$/, `/pagina-${pageNum}.htm`);
        }
    }

    addLog('INFO', `Reanudando desde página ${pageNum}...`);

    try {
        const response = await fetch('/api/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                seed_url: resumeUrl,
                mode: mode,
                output_dir: outputDir
            })
        });

        const data = await response.json();
        if (data.error) {
            addLog('ERR', `Error: ${data.error}`);
            return;
        }

        // Clear saved state after successful resume start
        await fetch('/api/clear-state', { method: 'POST' });
        if (resumeBtn) resumeBtn.style.display = 'none';
        savedResumeState = null;

        // Update UI state
        isRunning = true;
        isPaused = false;
        startTime = Date.now();
        startTimer();

        startBtn.disabled = true;
        pauseBtn.disabled = false;
        stopBtn.disabled = false;
        seedUrlInput.disabled = true;

        addLog('OK', `Scraping reanudado en modo ${mode}`);
    } catch (error) {
        addLog('ERR', `Error reanudando: ${error.message}`);
    }
}

// Add event listener for resume button
if (resumeBtn) {
    resumeBtn.addEventListener('click', resumeScraping);
}

// Alarm Sound using Web Audio API
function playAlarm() {
    if (!audioCtx) return;

    const ctx = audioCtx;

    // Function to play a single beep
    const playBeep = (freq, type, duration, startTime) => {
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();

        osc.type = type;
        osc.frequency.value = freq;

        osc.connect(gain);
        gain.connect(ctx.destination);

        osc.start(startTime);

        // Envelope to avoid clicking
        gain.gain.setValueAtTime(0, startTime);
        gain.gain.linearRampToValueAtTime(0.5, startTime + 0.05);
        gain.gain.linearRampToValueAtTime(0.5, startTime + duration - 0.05);
        gain.gain.linearRampToValueAtTime(0, startTime + duration);

        osc.stop(startTime + duration);
    };

    // Play a pattern: Beep... Beep... Beep...
    const now = ctx.currentTime;
    playBeep(880, 'square', 0.2, now);       // A5
    playBeep(880, 'square', 0.2, now + 0.4);
    playBeep(880, 'square', 0.2, now + 0.8);

    // Play a lower tone confirmation
    playBeep(440, 'sine', 0.4, now + 1.2);
}

// Server Control Buttons
// startServerBtn removed (useless)
const stopServerBtn = document.getElementById('stopServerBtn');
const restartServerBtn = document.getElementById('restartServerBtn');

function updateServerButtons(isConnected) {
    // If connected, server is running -> Start disabled
    // If disconnected, server is stopped -> Start enabled (status)
    // If connected, server is running -> Stop/Restart enabled
    // If disconnected, server is stopped -> Stop/Restart disabled
    if (stopServerBtn) stopServerBtn.disabled = !isConnected;
    if (restartServerBtn) restartServerBtn.disabled = !isConnected;
}



if (stopServerBtn) {
    stopServerBtn.addEventListener('click', async () => {
        if (!confirm('¿Seguro que quieres parar todos los servicios?')) return;
        try {
            stopServerBtn.disabled = true;
            stopServerBtn.innerHTML = '<span class="btn-icon">⏳</span> Parando...';
            const response = await fetch('/api/server/stop', { method: 'POST' });
            const data = await response.json();
            if (response.ok) {
                addLog('INFO', 'Servidor parándose...');
            } else {
                addLog('ERR', data.error || 'Error al parar servidor');
            }
        } catch (err) {
            addLog('ERR', 'Error de conexión al parar servidor');
        } finally {
            stopServerBtn.disabled = false;
            stopServerBtn.innerHTML = '<span class="btn-icon">⏹</span> Parar';
        }
    });
}

if (restartServerBtn) {
    restartServerBtn.addEventListener('click', async () => {
        if (!confirm('¿Seguro que quieres reiniciar todos los servicios?')) return;
        try {
            restartServerBtn.disabled = true;
            restartServerBtn.innerHTML = '<span class="btn-icon">⏳</span> Reiniciando...';
            const response = await fetch('/api/server/restart', { method: 'POST' });
            const data = await response.json();
            if (response.ok) {
                addLog('INFO', 'Servidor reiniciándose...');
            } else {
                addLog('ERR', data.error || 'Error al reiniciar servidor');
            }
        } catch (err) {
            addLog('ERR', 'Error de conexión al reiniciar servidor');
        } finally {
            restartServerBtn.disabled = false;
            restartServerBtn.innerHTML = '<span class="btn-icon">🔄</span> Reiniciar';
        }
    });
}
// =============================================================================
// BATCH ENRICHMENT LOGIC
// =============================================================================

const batchStartBtn = document.getElementById('batchStartBtn');
const batchPauseBtn = document.getElementById('batchPauseBtn');
const batchResumeBtn = document.getElementById('batchResumeBtn');
const batchStopBtn = document.getElementById('batchStopBtn');
// const batchFileList = document.getElementById('batchFileList'); // Removed, now split
const batchPendingList = document.getElementById('batchPendingList');
const batchCompletedList = document.getElementById('batchCompletedList');
const batchProgressText = document.getElementById('batchProgressText');
const batchCurrentFile = document.getElementById('batchCurrentFile');
const batchSelectedCount = document.getElementById('batchSelectedCount');
const clearBatchLogsBtn = document.getElementById('clearBatchLogsBtn');

if (clearBatchLogsBtn) {
    clearBatchLogsBtn.addEventListener('click', () => {
        clearLogs();
        addLog('INFO', 'Logs de lote limpiados.');
    });
}

let batchFiles = []; // Stores file objects {path, name}

/**
 * Formats Unix timestamp to DD/MM HH:mm
 */
function formatMtime(mtime) {
    if (!mtime) return '';
    const d = new Date(mtime * 1000);
    const day = String(d.getDate()).padStart(2, '0');
    const month = String(d.getMonth() + 1).padStart(2, '0');
    const hours = String(d.getHours()).padStart(2, '0');
    const minutes = String(d.getMinutes()).padStart(2, '0');
    return `${day}/${month} ${hours}:${minutes}`;
}

// Load files for Batch List
async function loadBatchFiles() {
    if (!batchPendingList || !batchCompletedList) return;

    try {
        const response = await fetch('/api/salidas-files?limit=100');
        const data = await response.json();

        batchPendingList.innerHTML = '';
        batchCompletedList.innerHTML = '';
        batchFiles = data.files || [];

        if (batchFiles.length === 0) {
            batchPendingList.innerHTML = '<div style="text-align:center; padding:20px; color:var(--text-muted);">No hay archivos</div>';
            return;
        }

        let pendingCount = 0;
        let completedCount = 0;

        batchFiles.forEach((f, index) => {
            const dateStr = formatMtime(f.mtime);
            const isCompleted = f.name.endsWith('_updated.xlsx');

            const item = document.createElement('div');
            item.className = 'batch-file-item';

            // Common Style
            item.style.cssText = 'display:flex; align-items:center; justify-content:space-between; padding:6px 10px; background:rgba(255,255,255,0.02); border-bottom:1px solid rgba(255,255,255,0.05);';

            if (isCompleted) {
                // RIGHT COLUMN: Read-only, No Checkbox
                // Add green tint
                item.style.background = 'rgba(16, 185, 129, 0.05)';
                item.style.borderLeft = '3px solid #10b981';

                item.innerHTML = `
                    <div style="flex:1; overflow:hidden; display:flex; flex-direction:column;">
                        <span style="font-size:0.85rem; color: #a7f3d0;" title="${f.name}">${f.name}</span>
                    </div>
                    <span style="font-size:0.75rem; color:var(--text-muted); font-family:monospace;">${dateStr}</span>
                `;
                batchCompletedList.appendChild(item);
                completedCount++;
            } else {
                // LEFT COLUMN: Pending, Selectable
                // If it's a Partial, add yellow tint
                const isPartial = f.name.includes('_partial');
                if (isPartial) {
                    item.style.borderLeft = '3px solid #f59e0b';
                    item.style.background = 'rgba(245, 158, 11, 0.05)';
                } else {
                    // Standard color for source files
                }

                item.innerHTML = `
                    <div style="display:flex; align-items:center; flex:1; overflow:hidden;">
                        <input type="checkbox" id="bf-${index}" value="${f.path}" style="margin-right:10px; cursor:pointer;">
                        <label for="bf-${index}" style="cursor:pointer; flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; font-size:0.85rem;" title="${f.name}">${f.name}</label>
                    </div>
                    <span style="font-size:0.7rem; color:var(--text-muted); margin-left:10px; font-family:monospace; white-space:nowrap;">${dateStr}</span>
                `;
                batchPendingList.appendChild(item);

                // Add change listener
                const cb = item.querySelector('input');
                cb.addEventListener('change', updateBatchCount);
                pendingCount++;
            }
        });

        if (pendingCount === 0) batchPendingList.innerHTML = '<div style="text-align:center; padding:20px; color:var(--text-muted);">No hay archivos pendientes</div>';
        if (completedCount === 0) batchCompletedList.innerHTML = '<div style="text-align:center; padding:20px; color:var(--text-muted);">-- Vacío --</div>';

        updateBatchCount();

    } catch (e) {
        console.error("Error loading batch files", e);
        if (batchPendingList) batchPendingList.innerHTML = `<div style="color:var(--danger)">Error: ${e.message}</div>`;
    }
}


function updateBatchCount() {
    if (!batchSelectedCount || !batchPendingList) return;
    const count = batchPendingList.querySelectorAll('input[type="checkbox"]:checked').length;
    batchSelectedCount.textContent = `${count} archivos seleccionados`;
    validateBatchButton();
}

function toggleBatchFiles(selectAll) {
    if (!batchPendingList) return;
    const cbs = batchPendingList.querySelectorAll('input[type="checkbox"]');
    cbs.forEach(cb => cb.checked = selectAll);
    updateBatchCount();
}

function validateBatchButton() {
    if (!batchStartBtn) return;

    // Check Status Text cleanly
    // If text says "Inactivo" (with any whitespace), we allow enabling if selection > 0
    const statusText = batchProgressText.textContent.trim();
    if (batchStartBtn.disabled && statusText !== "Inactivo" && statusText !== "") return;

    if (!batchPendingList) return;
    const count = batchPendingList.querySelectorAll('input[type="checkbox"]:checked').length;
    if (count > 0) {
        batchStartBtn.disabled = false;
        batchStartBtn.style.opacity = '1';
        batchStartBtn.style.cursor = 'pointer';
    } else {
        batchStartBtn.disabled = true;
        batchStartBtn.style.opacity = '0.5';
        batchStartBtn.style.cursor = 'not-allowed';
    }
}

// Batch Actions
if (batchStartBtn) {
    batchStartBtn.addEventListener('click', async () => {
        if (!batchPendingList) return;
        const checked = Array.from(batchPendingList.querySelectorAll('input[type="checkbox"]:checked')).map(cb => cb.value);

        if (checked.length === 0) {
            alert("Por favor, selecciona al menos un archivo.");
            return;
        }

        setBatchUIState('running');
        addLog('INFO', `Iniciando lote con ${checked.length} archivos...`);

        try {
            const res = await fetch('/api/batch/start', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ files: checked })
            });

            // Reset Batch Global Vars
            batchPriorEnriched = 0;
            maxEnrichedInCurrentFile = 0;
            batchStartTime = Date.now(); // Start Timer

            if (statCurrentProps) statCurrentProps.textContent = '0';
            if (statTotalProps) statTotalProps.textContent = '0';
            // if (statCurrentEnriched) statCurrentEnriched.textContent = '0';
            // if (statTotalEnriched) statTotalEnriched.textContent = '0';

            const data = await res.json();

            if (!res.ok) {
                addLog('ERR', `Error al iniciar lote: ${data.error}`);
                setBatchUIState('idle');
            }
        } catch (e) {
            addLog('ERR', `Error de conexión: ${e.message}`);
            setBatchUIState('idle');
        }
    });
}

if (batchStopBtn) {
    batchStopBtn.addEventListener('click', async () => {
        if (!confirm("¿Seguro que quieres detener todo el lote?")) return;

        addLog('WARN', 'Deteniendo lote...');
        try {
            await fetch('/api/batch/stop', { method: 'POST' });
            setBatchUIState('idle');
        } catch (e) { console.error(e); }
    });
}

if (batchPauseBtn) {
    batchPauseBtn.addEventListener('click', async () => {
        // We reuse the update urls pause endpoint
        addLog('INFO', 'Pausando proceso actual...');
        try {
            await fetch('/api/update/pause', { method: 'POST' });
            setBatchUIState('paused'); // Toggle buttons
        } catch (e) { console.error(e); }
    });
}

if (batchResumeBtn) {
    batchResumeBtn.addEventListener('click', async () => {
        addLog('INFO', 'Reanudando proceso...');
        try {
            await fetch('/api/update/resume', { method: 'POST' });
            setBatchUIState('running');
        } catch (e) { console.error(e); }
    });
}

function setBatchUIState(state) {
    // states: idle, running, paused
    if (!batchStartBtn) return;

    if (state === 'running') {
        batchStartBtn.disabled = true;
        batchStopBtn.disabled = false;
        batchPauseBtn.disabled = false;
        batchPauseBtn.style.display = 'inline-block';
        batchResumeBtn.style.display = 'none';
        batchProgressText.textContent = "Ejecutando...";
        batchProgressText.style.color = "var(--success)";
    } else if (state === 'paused') {
        batchStartBtn.disabled = true;
        batchStopBtn.disabled = false;
        batchPauseBtn.style.display = 'none';
        batchResumeBtn.style.display = 'inline-block';
        batchProgressText.textContent = "Pausado";
        batchProgressText.style.color = "var(--warning)";
    } else {
        batchStartBtn.disabled = false;
        batchStopBtn.disabled = true;
        batchPauseBtn.disabled = true;
        batchPauseBtn.style.display = 'inline-block';
        batchResumeBtn.style.display = 'none';
        batchProgressText.textContent = "Inactivo";
        batchProgressText.style.color = "var(--text-muted)";
        batchCurrentFile.textContent = "-";
    }
}

// Initialize on Load
document.addEventListener('DOMContentLoaded', () => {
    loadBatchFiles();

    // Check initial status
    fetch('/api/batch/status')
        .then(r => r.json())
        .then(data => {
            if (data.is_running) {
                setBatchUIState('running');
                // Check if paused? 
                // We'd need to check 'update_paused.flag' endpoint but for now assume running
                if (data.current_idx >= 0) {
                    batchProgressText.textContent = `Procesando archivo ${data.current_idx + 1} de ${data.total}`;
                    if (data.current_file) {
                        batchCurrentFile.textContent = data.current_file.split(/[\\/]/).pop();
                    }
                }
            }
        })
        .catch(e => console.log("Batch status check failed", e));

    // Listen to Batch Events (add to initializeSocket if global socket exists)
    if (socket) {
        setupBatchSocketListeners();
    }
});

function setupBatchSocketListeners() {
    socket.on('batch_progress', (data) => {
        // Accumulate previous file's max
        batchPriorEnriched += maxEnrichedInCurrentFile;
        maxEnrichedInCurrentFile = 0; // Reset for new file

        // Update Files Scorecard (Provincias)
        statCurrentPage.textContent = String(data.current).padStart(2, '0');
        statTotalPages.textContent = String(data.total).padStart(2, '0');

        // Update Text
        batchProgressText.innerHTML = `Iniciando archivo...`;
        if (batchCurrentFile) batchCurrentFile.style.display = 'none'; // We use the big box now

        setBatchUIState('running'); // Ensure UI is in sync
    });

    socket.on('property_scraped', (data) => {
        // 1. Update Counters
        const current = parseInt(statCurrentProps.textContent || '0') + 1;
        statCurrentProps.textContent = current;

        const total = parseInt(statTotalProps.textContent || '0') + 1;
        statTotalProps.textContent = total;

        // Track per-file max for batch logic
        if (current > maxEnrichedInCurrentFile) maxEnrichedInCurrentFile = current;

        // 2. Add Row to Table
        // Use standard addResultRow but ensure it handles dynamic columns if needed
        addResultRow(data);

        // 3. Update Results Count text
        if (resultsCount) resultsCount.textContent = `${total} propiedades`;

        // 4. Hide empty state
        if (emptyState) emptyState.style.display = 'none';

        // 5. Ensure "Running" state
        setBatchUIState('running');
    });

    // Handle detailed progress updates (for Scorecards and Text)
    socket.on('progress', (data) => {
        // data = { current_properties, total_properties, current_page, total_pages, sheet_name, excel_file }

        // Update Property Counters
        statCurrentProps.textContent = data.current_properties;
        statTotalProps.textContent = data.total_properties;

        // Update Text
        if (data.excel_file) {
            batchProgressText.innerHTML = `
                Procesando: <strong>${data.excel_file}</strong><br>
                <span style="font-size:0.9em; color:var(--text-muted)">
                   Hoja: ${data.sheet_name || '?'} | Progreso: ${data.current_properties}/${data.total_properties}
                </span>
            `;
        }
    });

    socket.on('batch_completed', (data) => {
        addLog('OK', `Lote completado: ${data.completed}/${data.total} archivos.`);
        setBatchUIState('idle');
        alert("Batch Enrichment Completed!");
        batchStartTime = null; // Stop timer
    });

    socket.on('batch_stopped', () => {
        setBatchUIState('idle');
        batchStartTime = null; // Stop timer
    });

    // Also listen for legacy status if paused manually
    socket.on('status_change', (data) => {
        if (data.status === 'paused') setBatchUIState('paused');
        if (data.status === 'running') setBatchUIState('running');
    });
}

// Hook into existing initializeSocket if possible, or just run it if socket is already there
// Since we appended this code, socket might be null if DOMContentLoaded fires early?
// No, DOMContentLoaded in this block handles it.
// BUT, the MAIN `initializeSocket` is called in main DOMContentLoaded.
// The `socket` variable is global.
// We can just add the listener registration to the original `initializeSocket` function 
// OR simpler: check interval?
// Better: Override or Extend.
// Since we can't easily edit the middle of file, we use the fact that `socket` is global.
// We just need to make sure we attach listeners ONCE.

const originalInitSocket = window.initializeSocket;
// Actually we can't override the internal logic easily if it's not exposed.
// But `initializeSocket` IS defined in global scope in the file above.
// AND we are appending to the file.
// So we can redefine it? No, that's risky.
// We can just watch for socket connection.

// Since `socket` is global, we can just add listeners if it exists.
// If it connects later, we might miss it.
// Wait, `socket` is initialized in `initializeSocket`.
// We can modify `initializeSocket` in the file above to call `setupBatchSocketListeners`.
// OR we just use `socket.on('connect')` here if socket is already created?
// `socket` is null initially.

// Let's modify the ORIGINAL `initializeSocket` to call `setupBatchSocketListeners`.
// I'll make a separate tool call to inject the call inside `initializeSocket`.


// ============ PERIODIC LOW-COST SCRAPER MODULE ============
// UI Elements
const periodicStartBtn = document.getElementById('periodicStartBtn');
const periodicPauseBtn = document.getElementById('periodicPauseBtn');
const periodicResumeBtn = document.getElementById('periodicResumeBtn');
const periodicStopBtn = document.getElementById('periodicStopBtn');
const periodicLogContainer = document.getElementById('periodicLogContainer');
const periodicResultsBody = document.getElementById('periodicResultsBody');
const periodicMainStatus = document.getElementById('periodicMainStatus');

// Stats Elements
const statProv = document.getElementById('periodicStatProvinces');
const statSucc = document.getElementById('periodicStatSuccess');
const statErr = document.getElementById('periodicStatErrors');

let periodicLogs = [];

if (periodicStartBtn) {
    // START
    periodicStartBtn.addEventListener('click', async () => {
        setPeriodicState('running');
        addPeriodicLog("Iniciando escaneo nacional...", 'info');

        try {
            const res = await fetch('/api/periodic-lowcost/start', { method: 'POST' });
            const data = await res.json();
            if (!res.ok) throw new Error(data.error || 'Error desconocido');
        } catch (e) {
            addPeriodicLog("Error al iniciar: " + e.message, 'error');
            setPeriodicState('idle');
        }
    });

    // STOP
    periodicStopBtn.addEventListener('click', async () => {
        if (!confirm("¿Seguro que quieres detener el escaneo?")) return;
        setPeriodicState('idle'); // Optimistic UI update
        try {
            await fetch('/api/periodic-lowcost/stop', { method: 'POST' });
            addPeriodicLog("Proceso detenido por el usuario.", 'warning');
        } catch (e) {
            console.error(e);
        }
    });

    // PAUSE/RESUME (Future implementation if supported by backend)
    periodicPauseBtn.addEventListener('click', () => alert("Pausa no implementada aún en backend."));
    periodicResumeBtn.addEventListener('click', () => alert("Reanudación no implementada aún en backend."));
}

function setPeriodicState(state) {
    if (!periodicStartBtn) return;

    if (state === 'running') {
        periodicStartBtn.disabled = true;
        periodicStartBtn.style.display = 'none';
        periodicStopBtn.disabled = false;
        periodicStopBtn.style.display = 'inline-flex';

        periodicMainStatus.textContent = "EJECUTANDO";
        periodicMainStatus.className = "status-badge-inline status-active";
        periodicMainStatus.style.background = "var(--green)";
        periodicMainStatus.style.color = "#000";
    } else {
        periodicStartBtn.disabled = false;
        periodicStartBtn.style.display = 'inline-flex';
        periodicStopBtn.disabled = true;
        periodicStopBtn.style.display = 'none';

        periodicMainStatus.textContent = "INACTIVO";
        periodicMainStatus.className = "status-badge-inline";
        periodicMainStatus.style.background = "var(--bg-card)";
        periodicMainStatus.style.color = "var(--text-muted)";
    }
}

function addPeriodicLog(msg, type = 'info') {
    if (!periodicLogContainer) return;

    const div = document.createElement('div');
    div.className = 'log-entry';
    div.style.fontFamily = "Consolas, monospace";
    div.style.marginBottom = "4px";

    const time = new Date().toLocaleTimeString();

    if (type === 'error') div.style.color = '#ff6b6b';
    else if (type === 'warning') div.style.color = '#f1c40f';
    else if (type === 'success') div.style.color = '#2ecc71';
    else div.style.color = '#8b949e'; // Default gray

    // Parse progress to update stats
    if (msg.includes("Processing:")) {
        // "[5/52] Processing: Madrid"
        try {
            const parts = msg.match(/\[(\d+)\/(\d+)\]/);
            if (parts) {
                statProv.textContent = `${parts[1]}/${parts[2]}`;
            }
        } catch (e) { }
    }

    div.innerText = `[${time}] ${msg}`;
    periodicLogContainer.appendChild(div);
    periodicLogContainer.scrollTop = periodicLogContainer.scrollHeight;
}

function setupPeriodicSocketListeners() {
    if (!socket) return;

    socket.on('periodic_log', (data) => {
        let type = 'info';
        if (data.level === 'ERR') type = 'error';
        if (data.level === 'WARN') type = 'warning';
        if (data.level === 'OK') type = 'success';
        addPeriodicLog(data.message, type);
    });

    socket.on('periodic_status', (data) => {
        setPeriodicState(data.status === 'running' || data.status === 'started' ? 'running' : 'idle');
    });
}

// Hook into initialization if socket exists, or wait for DOMContentLoaded
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        if (socket) setupPeriodicSocketListeners();
    });
} else {
    // Already loaded
    if (socket) setupPeriodicSocketListeners();
}
