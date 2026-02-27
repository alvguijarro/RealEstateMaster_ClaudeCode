/**
 * Idealista Scraper - Frontend JavaScript
 * Handles WebSocket communication and UI updates
 */

// Centralized Scraper State Management
function updateScraperState(active, modeTitle = null) {
    isRunning = active;
    isPaused = false; // Reset pause state on new state change unless specified

    // Update Global Buttons
    if (startBtn) {
        startBtn.disabled = active;
        if (active) {
            startBtn.classList.add('disabled');
            startBtn.title = "Scraper en curso...";
        } else {
            startBtn.classList.remove('disabled');
            startBtn.title = "Iniciar Scraping";
        }
    }

    // Resume button only shows if paused, handled separately
    if (resumeBtn) resumeBtn.style.display = 'none';

    // Pause/Stop buttons enabled when active
    if (pauseBtn) pauseBtn.disabled = !active;
    if (stopBtn) stopBtn.disabled = !active;

    // Batch Buttons (Split)
    const updateBatchBtn = (btnId) => {
        const btn = document.getElementById(btnId);
        if (btn) {
            btn.disabled = active;
            if (active) {
                btn.title = "Scraper en curso...";
                btn.classList.add('disabled');
            } else {
                btn.title = "";
                btn.classList.remove('disabled');
            }
        }
    };
    updateBatchBtn('startBatchVentaBtn');
    updateBatchBtn('startBatchAlquilerBtn');

    if (!active) {
        // Re-validate to see if they should be enabled based on selection
        validateProvinceBatchButtons();
    }


    // URL Update Button
    if (updateUrlsBtn) updateUrlsBtn.disabled = active;

    // Status Badge
    if (statusBadge) {
        const dot = statusBadge.querySelector('.status-dot');
        const text = statusBadge.querySelector('.status-text');

        if (active) {
            statusBadge.className = 'status-badge active';
            text.textContent = modeTitle || 'Scraping Activo';
        } else {
            statusBadge.className = 'status-badge';
            text.textContent = 'Inactivo';
        }
    }
}



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

// DOM Elements - Consolidated & Safe
const getEl = (id) => document.getElementById(id);

const seedUrlInput = getEl('seedUrl');
const outputDirDisplay = getEl('outputDirDisplay');
const startBtn = getEl('startBtn');
const pauseBtn = getEl('pauseBtn');
const stopBtn = getEl('stopBtn');
const dualModeBtn = getEl('dualModeBtn');
const fastBtn = getEl('fastBtn');
const stealthBtn = getEl('stealthBtn');
const statusBadge = getEl('statusBadge');
const logsContainer = getEl('logsContainer');
const clearLogsBtn = getEl('clearLogsBtn');
const tableHeader = getEl('tableHeader');
const tableBody = getEl('tableBody');
const resultsCount = getEl('resultsCount');
const downloadBtn = getEl('downloadBtn');
const emptyState = getEl('emptyState');
const statCurrentPage = getEl('statCurrentPage');
const statTotalPages = getEl('statTotalPages');
const statCurrentProps = getEl('statCurrentProps');
const statTotalProps = getEl('statTotalProps');
const statTime = getEl('statTime');
const statMode = getEl('statMode');
const historyBody = getEl('historyBody');
const historyEmptyState = getEl('historyEmptyState');
const clearHistoryBtn = getEl('clearHistoryBtn');
const rotateVpnBtn = getEl('rotateVpnBtn');
const resumeBtn = getEl('resumeBtn');
const stopServerBtn = getEl('stopServerBtn');
const restartServerBtn = getEl('restartServerBtn');
const updateExcelSelect = getEl('updateExcelFile');
const updateUrlsBtn = getEl('updateUrlsBtn');
const resumeUpdateBtn = getEl('resumeUpdateBtn');
const worksheetSelectorGroup = getEl('worksheetSelectorGroup');

// Current active columns (will be set based on seed URL)
let currentColumns = COLUMNS_STANDARD;


// Multi-Province Scraper State
let allProvincesList = [];
let selectedVenta = new Set();
let selectedAlquiler = new Set();
let isBatchMode = false;
// Lookup map: slug -> {venta_url, alquiler_url}
let provinceUrls = {};
let isBatchFileManual = false;

// Initialize
document.addEventListener('DOMContentLoaded', async () => {
    console.log('[App] Initializing application...');

    // 1. Core Logic Setup
    try {
        initializeSocket();
    } catch (e) {
        console.error('[App] Error in initializeSocket:', e);
    }

    // 2. UI Event Listeners (Garded)
    try {
        initializeUI();
    } catch (e) {
        console.error('[App] Error in initializeUI:', e);
    }

    // 3. Data Presentation Setup
    try {
        buildTableHeader();
    } catch (e) {
        console.error('[App] Error in buildTableHeader:', e);
    }

    // 4. Load Data from API
    loadDefaultConfig();
    loadHistory();
    checkResumeState();

    // Concurrent data loading
    Promise.all([
        loadExcelFiles(),
        loadBQFiles(),
        loadProvincesList(),
        loadBatchDestinationFiles(),
        loadBatchFiles()
    ]).then(() => {
        console.log('[App] Data loading completed.');
        setupMultiSelectUI();
    }).catch(err => {
        console.error('[App] Error during concurrent data load:', err);
    });

    // UI Refresh Buttons
    const btnRefreshProvinces = document.getElementById('btnRefreshProvinces');
    if (btnRefreshProvinces) {
        btnRefreshProvinces.addEventListener('click', async () => {
            btnRefreshProvinces.classList.add('loading');
            await loadProvincesList();
            setTimeout(() => btnRefreshProvinces.classList.remove('loading'), 500);
        });
    }

    const btnRefreshExcelFiles = document.getElementById('btnRefreshExcelFiles');
    if (btnRefreshExcelFiles) {
        btnRefreshExcelFiles.addEventListener('click', async () => {
            btnRefreshExcelFiles.classList.add('loading');
            await loadExcelFiles();
            setTimeout(() => btnRefreshExcelFiles.classList.remove('loading'), 500);
        });
    }
});

// URL Update Elements
const worksheetSearch = document.getElementById('worksheetSearch');
const worksheetList = document.getElementById('worksheetList');
const worksheetSelectionInfo = document.getElementById('worksheetSelectionInfo');

// Track available worksheets and selection state
let availableWorksheets = [];
let selectedWorksheets = new Set();

async function loadExcelFiles() {
    try {
        const response = await fetch('/api/salidas-files');
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

// Load BigQuery Files
async function loadBQFiles() {
    try {
        const response = await fetch('/api/salidas-files?limit=100');
        const data = await response.json();
        const select = document.getElementById('bqFiles');

        if (select && data.files) {
            select.innerHTML = '';
            if (data.files.length === 0) {
                select.innerHTML = '<option value="">Sin archivos en salidas/</option>';
                return;
            }

            data.files.forEach(f => {
                const opt = document.createElement('option');
                opt.value = f.path;
                // Add friendly name with date
                const dateStr = f.mtime ? ` [${formatMtime(f.mtime)}]` : '';
                opt.textContent = `${f.name}${dateStr}`;
                select.appendChild(opt);
            });
        }
    } catch (e) {
        console.error("Error loading BigQuery files", e);
    }
}

// Province helpers removed as they are no longer needed for BigQuery flow

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
    // BigQuery Refresh Button
    const btnRefreshBQFiles = document.getElementById('btnRefreshBQFiles');
    if (btnRefreshBQFiles) {
        btnRefreshBQFiles.addEventListener('click', async () => {
            btnRefreshBQFiles.classList.add('loading');
            await loadBQFiles();
            setTimeout(() => btnRefreshBQFiles.classList.remove('loading'), 500);
        });
    }

    // BigQuery Upload Button
    const uploadBQBtn = document.getElementById('uploadBQBtn');
    if (uploadBQBtn) {
        uploadBQBtn.addEventListener('click', async () => {
            const select = document.getElementById('bqFiles');
            const selectedFiles = Array.from(select.selectedOptions).map(opt => opt.value);

            if (selectedFiles.length === 0) {
                alert("Por favor, selecciona al menos un archivo.");
                return;
            }

            if (!confirm(`¿Estás seguro de que quieres subir ${selectedFiles.length} archivo(s) a BigQuery?`)) {
                return;
            }

            uploadBQBtn.disabled = true;
            uploadBQBtn.innerHTML = '🚀 SUBIENDO...';
            addLog('INFO', `Iniciando subida de ${selectedFiles.length} archivos a BigQuery...`);

            try {
                const response = await fetch('/api/save-to-bigquery', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ file_paths: selectedFiles })
                });

                const data = await response.json();
                if (response.ok) {
                    addLog('OK', `✅ Éxito: ${data.message}`);
                    alert(`Éxito: ${data.message}`);
                } else {
                    addLog('ERR', `❌ Error: ${data.error}`);
                    alert(`Error: ${data.error}`);
                }
            } catch (e) {
                addLog('ERR', `❌ Error de conexión: ${e.message}`);
            } finally {
                uploadBQBtn.disabled = false;
                uploadBQBtn.innerHTML = '🚀 SUBIR A BIGQUERY';
            }
        });
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

                // If switching to enricher, refresh files
                if (btn.dataset.tab === 'enricher') {
                    loadBatchFiles();
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
    isBatchMode = false;
    isUpdateMode = true;
    const excelFile = updateExcelSelect ? updateExcelSelect.value : '';

    if (!excelFile) {
        addLog('ERR', 'Por favor, selecciona un archivo Excel');
        return;
    }

    // Get selected sheets using new checkbox-based selection
    const selectedSheets = getSelectedSheets();

    addLog('INFO', `Iniciando actualización de URLs desde: ${excelFile}`);

    // Centralized State Update
    updateScraperState(true, 'Actualizando URLs');
    isUpdateMode = true;

    if (updateUrlsBtn) updateUrlsBtn.innerHTML = '<span class="btn-icon">⏳</span> Actualizando...';

    try {
        const response = await fetch('/api/update-urls', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ excel_file: excelFile, sheets: selectedSheets, resume: resume })
        });

        const data = await response.json();

        if (!response.ok) {
            addLog('ERR', data.error || 'Error al iniciar actualización');
            updateScraperState(false); // Reset on error
            if (updateUrlsBtn) updateUrlsBtn.innerHTML = '<span class="btn-icon">🔄</span> Actualizar URLs';
        } else {
            addLog('OK', 'Actualización de URLs iniciada');
            startTime = Date.now();
            startTimer();
        }
    } catch (error) {
        addLog('ERR', `Error de conexión: ${error.message}`);
        updateScraperState(false); // Reset on error
        if (updateUrlsBtn) updateUrlsBtn.innerHTML = '<span class="btn-icon">🔄</span> Actualizar URLs';
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

    let firstConnect = true;

    socket.on('connect', () => {
        if (!firstConnect) {
            addLog('INFO', 'Conexión restablecida con el servidor');
        }
        firstConnect = false;
        console.log('Socket.io connected');
        updateServerButtons(true);
        syncStatus();
    });

    socket.on('disconnect', () => {
        addLog('WARN', 'Desconectado del servidor');
        updateServerButtons(false);
        resetUIState(); // Ensure timer stops on disconnect
    });

    socket.on('log', (data) => {
        addLog(data.level, data.message);

        // Dynamic Province Name Detection
        // Match Pattern: 🚀 [1/52] Madrid (0-EU-ES-28) ...
        // Match Pattern: 🚀 [1/52] Processing: Madrid (sale/rent) ...
        const provinceMatch = data.message.match(/🚀\s\[\d+\/\d+\]\s(?:Processing:\s)?(.*?)(?:\s\(.*\)|$)/);
        if (provinceMatch && provinceMatch[1]) {
            const provinceBadge = document.getElementById('currentProvince');
            if (provinceBadge) {
                provinceBadge.textContent = `: ${provinceMatch[1].trim()}`;
            }
        }
    });

    socket.on('property_scraped', (data) => {
        addProperty(data);
    });

    socket.on('status_change', (data) => {
        handleStatusChange(data);
    });

    socket.on('history_update', (entry) => {
        addHistoryRow(entry);
        if (historyEmptyState) historyEmptyState.style.display = 'none';
    });

    socket.on('progress_update', (data) => {
        handleProgressUpdate(data);
    });

    socket.on('progress', (data) => {
        handleProgressUpdate(data);
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

function handleProgressUpdate(data) {
    // Enrichment Mode (Detailed)
    if (data.excel_file) {
        // Update Max for this file
        if (data.current_properties > maxEnrichedInCurrentFile) {
            maxEnrichedInCurrentFile = data.current_properties;
        }

        // Update Scorecards (Global Batch Count)
        const globalCurrent = parseInt(batchPriorEnriched || 0) + parseInt(data.current_properties || 0);
        const globalTotal = parseInt(batchPriorEnriched || 0) + parseInt(data.total_properties || 0); // Running total estimate

        if (statCurrentProps) statCurrentProps.textContent = globalCurrent;
        if (statTotalProps) statTotalProps.textContent = globalTotal;

        // Sync main stats for URL update feedback
        if (statCurrentPage) statCurrentPage.textContent = String(data.current_page || 1).padStart(2, '0');
        if (statTotalPages) statTotalPages.textContent = String(data.total_pages || 1).padStart(2, '0');

        // Update Batch Progress Box
        if (batchProgressText) {
            batchProgressText.innerHTML = `Enriqueciendo '${data.excel_file}'\nDistrito: '${data.sheet_name || 'Generando...'}'\nProgreso: ${data.current_properties} / ${data.total_properties}`;
        }
    } else {
        // Standard Scraper Mode / API Import
        if (statCurrentPage) statCurrentPage.textContent = String(data.current_page || 0).padStart(2, '0');
        if (statTotalPages) statTotalPages.textContent = String(data.total_pages || 0).padStart(2, '0');
        if (statCurrentProps) statCurrentProps.textContent = data.current_properties || 0;
        if (statTotalProps) statTotalProps.textContent = data.total_properties || 0;
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
    if (fastBtn) fastBtn.addEventListener('click', () => selectMode('fast'));
    if (stealthBtn) {
        stealthBtn.addEventListener('click', () => selectMode('stealth'));
    }

    // Action buttons
    if (startBtn) startBtn.addEventListener('click', () => startScraping(false));
    if (dualModeBtn) {
        dualModeBtn.addEventListener('click', () => startScraping(true));
    }
    if (pauseBtn) pauseBtn.addEventListener('click', togglePause);
    if (stopBtn) stopBtn.addEventListener('click', stopScraping);
    if (clearLogsBtn) clearLogsBtn.addEventListener('click', clearLogs);

    // New Batch Scraping Button (Province Panel)
    const startBatchBtnEl = document.getElementById('startBatchBtn');
    if (startBatchBtnEl) {
        startBatchBtnEl.addEventListener('click', startBatchFromProvinces);
    }
    if (clearHistoryBtn) clearHistoryBtn.addEventListener('click', clearHistory);

    // Auto-scroll toggle for logs
    const pauseLogBtn = document.getElementById('pauseLogBtn');
    if (pauseLogBtn) {
        pauseLogBtn.addEventListener('click', toggleAutoScroll);
    }

    // NordVPN Rotate Button
    if (rotateVpnBtn) {
        rotateVpnBtn.addEventListener('click', manualVpnRotate);
    }

    // URL validation for Dual Mode and Start Button
    if (seedUrlInput) {
        seedUrlInput.addEventListener('input', () => {
            validateDualMode();
            validateStartButton();
        });

        // Enter key to start
        seedUrlInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter' && !isRunning) {
                startScraping(false);
            }
        });
    }

    // Initial validation
    validateStartButton();
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
    if (!tableHeader) return;
    tableHeader.innerHTML = currentColumns.map(col =>
        `<th>${escapeHtml(col)}</th>`
    ).join('');
}

function addLog(level, message) {
    if (!logsContainer) return;
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
    const btn = document.getElementById('pauseLogBtn');
    if (btn) {
        if (autoScrollEnabled) {
            btn.innerHTML = '⏸';
            btn.title = 'Pausar auto-scroll';
            btn.classList.remove('is-paused');
            if (logsContainer) {
                logsContainer.classList.remove('paused');
                // Jump to bottom when re-enabling
                logsContainer.scrollTop = logsContainer.scrollHeight;
            }
        } else {
            btn.innerHTML = '▶';
            btn.title = 'Reanudar auto-scroll';
            btn.classList.add('is-paused');
            if (logsContainer) {
                logsContainer.classList.add('paused');
            }
        }
    }
}

async function addProperty(property) {
    if (properties.some(p => p.URL === property.URL)) return;

    properties.push(property);
    if (resultsCount) resultsCount.textContent = `${properties.length} props`;
    if (emptyState) emptyState.style.display = 'none';

    // Assuming renderPropertyRow is a new function that handles rendering
    // the row based on the property and currentColumns.
    // The original logic for creating the row is moved into this new function.
    // For now, we'll keep the original row creation logic here,
    // as renderPropertyRow is not defined in the provided context.
    const newFields = new Set(property._new_fields || []);

    const row = document.createElement('tr');
    row.innerHTML = currentColumns.map(col => {
        let value = property[col]; // Changed 'data' to 'property'
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

    // Detect Batch Mode from server status
    if (data.mode === 'batch') {
        isBatchMode = true;
    }

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

        // Restore Progress Counters from sync data
        if (data.current_page) {
            if (statCurrentPage) statCurrentPage.textContent = data.current_page;
            if (statTotalPages && data.total_pages) statTotalPages.textContent = data.total_pages;
        }
        if (data.properties_count || data.total_properties) {
            if (statCurrentProps) statCurrentProps.textContent = data.properties_count || 0;
            if (statTotalProps && data.total_properties) statTotalProps.textContent = data.total_properties;
        }

        // Restore Timer from sync data
        if (data.start_time && !timerInterval) {
            startTime = data.start_time * 1000;
            if (data.mode === 'batch' || data.task_mode === 'enrichment') {
                batchStartTime = startTime;
            }
            timerInterval = setInterval(updateTimer, 1000);
            updateTimer();
        }

        // Mode Specific UI
        if (data.task_mode === 'enrichment' || data.mode === 'batch') {
            setBatchUIState('running');
        } else {
            updateScraperState(true, data.mode === 'update_urls' ? 'Actualizando URLs' : 'Scraping Activo');
        }

        if (startBtn) startBtn.disabled = true;
        if (dualModeBtn) dualModeBtn.disabled = true;
        if (pauseBtn) pauseBtn.disabled = false;
        if (stopBtn) stopBtn.disabled = false;
        if (pauseBtn) pauseBtn.innerHTML = '<span class="btn-icon">⏸</span> Pausar';
        if (seedUrlInput) seedUrlInput.disabled = true;
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
    } else if (status === 'blocked' || status === 'resting') {
        // Transitional active states (Identity rotation or rest)
        isRunning = true;
        isPaused = false;
        startBtn.disabled = true;
        pauseBtn.disabled = false;
        stopBtn.disabled = false;
        if (dualModeBtn) dualModeBtn.disabled = true;
        pauseBtn.innerHTML = '<span class="btn-icon">⏸</span> Pausar';
        seedUrlInput.disabled = true;
    } else if (status === 'completed' || status === 'stopped' || status === 'idle') {
        isRunning = false;
        isPaused = false;

        // Reset enrichment UI if it was active
        if (isUpdateMode || isBatchMode) {
            setBatchUIState('idle');
        }

        updateScraperState(false);
        if (startBtn) startBtn.disabled = false;
        if (dualModeBtn) dualModeBtn.disabled = false;
        if (pauseBtn) pauseBtn.disabled = true;
        if (stopBtn) stopBtn.disabled = true;
        seedUrlInput.disabled = false;
        if (startApiImportBtn) startApiImportBtn.disabled = false;

        if ((status === 'completed' || status === 'stopped' || status === 'idle') && isBatchMode) {
            isBatchMode = false;
            if (status === 'completed') {
                addLog('OK', '✅ LOTE COMPLETADO: Todos los destinos han sido procesados.');
            }
        }

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
    } else if (status === 'error') {
        isRunning = false;
        isPaused = false;
        startBtn.disabled = false;
        pauseBtn.disabled = true;
        stopBtn.disabled = true;
        seedUrlInput.disabled = false;
        if (data.error) {
            addLog('ERR', `Error del scraper: ${data.error}`);
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

    // NOTE: Batch mode is now handled exclusively by startBatchFromProvinces()
    // triggered via the dedicated "Iniciar scraping de provincias" button.

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

    // Centralized State Update
    isBatchMode = false;
    isUpdateMode = false;
    updateScraperState(true, `Scraping ${isDualMode ? 'Dual' : currentMode.toUpperCase()}`);

    try {
        const response = await fetch('/api/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                seed_url: seedUrl,
                mode: currentMode,
                dual_mode: isDualMode,
                use_vpn: useVpnToggle ? useVpnToggle.checked : false
            })
        });

        const data = await response.json();

        if (!response.ok) {
            addLog('ERR', data.error || 'Error al iniciar');
            updateScraperState(false); // Reset on error
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
        updateScraperState(false); // Reset on error
    }
}

async function togglePause() {
    let endpoint;
    if (isBatchMode) {
        endpoint = isPaused ? '/api/batch/resume' : '/api/batch/pause';
    } else if (isUpdateMode) {
        endpoint = isPaused ? '/api/update/resume' : '/api/update/pause';
    } else {
        endpoint = isPaused ? '/api/resume' : '/api/pause';
    }

    try {
        const response = await fetch(endpoint, { method: 'POST' });
        const data = await response.json();

        if (!response.ok) {
            addLog('ERR', data.error || 'Error');
        } else {
            // Immediate UI update based on new state
            isPaused = !isPaused;
            if (isPaused) {
                if (pauseBtn) {
                    pauseBtn.innerHTML = '<span class="btn-icon">▶</span> Reanudar';
                    pauseBtn.classList.remove('btn-warning');
                }
                statusBadge.querySelector('.status-text').textContent = 'Pausado';
                statusBadge.className = 'status-badge paused';
                addLog('INFO', 'Scraping pausado.');
            } else {
                if (pauseBtn) {
                    pauseBtn.innerHTML = '<span class="btn-icon">⏸</span> Pausar';
                }
                statusBadge.querySelector('.status-text').textContent = 'Ejecutando';
                statusBadge.className = 'status-badge running';
                addLog('OK', 'Scraping reanudado.');
            }
        }
    } catch (error) {
        addLog('ERR', `Error: ${error.message}`);
    }
}

async function stopScraping() {
    addLog('INFO', 'Deteniendo...');

    // Disable stop button immediately to prevent double clicks
    if (stopBtn) {
        stopBtn.disabled = true;
        stopBtn.innerHTML = '<span class="btn-icon">⏳</span> Deteniendo...';
    }

    let endpoint;
    if (isBatchMode) {
        endpoint = '/api/batch/stop';
    } else if (isUpdateMode) {
        endpoint = '/api/update/stop';
    } else {
        endpoint = '/api/stop';
    }

    try {
        const response = await fetch(endpoint, { method: 'POST' });
        const data = await response.json();

        if (!response.ok) {
            addLog('ERR', data.error || 'Error al detener');
            // Re-enable if failed
            if (stopBtn) {
                stopBtn.disabled = false;
                stopBtn.innerHTML = '<span class="btn-icon">⏹</span> Detener';
            }
        } else {
            // Success - Server will send 'stopped' status or similar
            // But we can force UI update to be sure
            updateScraperState(false);
            if (stopBtn) stopBtn.innerHTML = '<span class="btn-icon">⏹</span> Detener';
        }
    } catch (error) {
        addLog('ERR', `Error: ${error.message}`);
        if (stopBtn) {
            stopBtn.disabled = false;
            stopBtn.innerHTML = '<span class="btn-icon">⏹</span> Detener';
        }
    }
}

function resetUIState() {
    isRunning = false;
    isPaused = false;
    isUpdateMode = false;
    validateStartButton();
    if (startBtn) startBtn.title = "";
    if (pauseBtn) pauseBtn.disabled = true;
    if (stopBtn) stopBtn.disabled = true;

    if (updateUrlsBtn) {
        updateUrlsBtn.innerHTML = '<span class="btn-icon">🔄</span> Actualizar URLs';
        updateUrlsBtn.disabled = false;
    }

    stopTimer();

    // Clear province badge
    const provinceBadge = document.getElementById('currentProvince');
    if (provinceBadge) provinceBadge.textContent = '';
}

function stopTimer() {
    if (timerInterval) {
        clearInterval(timerInterval);
        timerInterval = null;
    }
}

function startTimer() {
    startTime = Date.now();
    timerInterval = setInterval(updateTimer, 1000);
    updateTimer();
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
        // Centralized State Update
        updateScraperState(true, `Scraping Reanudado in ${mode}`);
        startTime = Date.now();
        startTimer();
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
// startServerBtn and restartServerBtn are defined globally at the top

function updateServerButtons(isConnected) {
    if (stopServerBtn) stopServerBtn.disabled = !isConnected;
    if (restartServerBtn) restartServerBtn.disabled = !isConnected;

    // Also disable scraper action buttons if disconnected
    if (!isConnected) {
        if (pauseBtn) pauseBtn.disabled = true;
        if (stopBtn) stopBtn.disabled = true;
    }
}

async function syncStatus() {
    try {
        const response = await fetch("/api/status");
        const data = await response.json();
        handleStatusChange(data);
    } catch (e) {
        console.error("Error syncing status:", e);
    }
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
            stopServerBtn.innerHTML = '<span class="btn-icon">⏹</span> Detener Serv.';
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
    validateEnrichmentBatchButton();
}

function toggleBatchFiles(selectAll) {
    if (!batchPendingList) return;
    const cbs = batchPendingList.querySelectorAll('input[type="checkbox"]');
    cbs.forEach(cb => cb.checked = selectAll);
    updateBatchCount();
}


/**
 * Validation for "Iniciar scraping de provincias" button
 * Checks if at least one province/zone is selected in the new Tree View
 */
/**
 * Validation for "Iniciar scraping de provincias" buttons (Split)
 * Checks if at least one province/zone is selected for the specific type
 */
function validateProvinceBatchButtons() {
    const validate = (type) => {
        const btn = document.getElementById(type === 'venta' ? 'startBatchVentaBtn' : 'startBatchAlquilerBtn');
        if (!btn) return;

        // Check if running
        if (isRunning || isPaused) {
            btn.disabled = true;
            btn.style.opacity = '0.5';
            btn.style.cursor = 'not-allowed';
            return;
        }

        // Check Count
        const list = document.getElementById(type === 'venta' ? 'listVenta' : 'listAlquiler');
        let count = 0;
        if (list) {
            const provCbs = list.querySelectorAll(`.prov-cb-${type}`);
            provCbs.forEach(cb => {
                if (cb.checked || cb.indeterminate) count++;
            });
        }

        if (count > 0) {
            btn.disabled = false;
            btn.style.opacity = '1';
            btn.style.cursor = 'pointer';
            btn.title = "";
        } else {
            btn.disabled = true;
            btn.style.opacity = '0.5';
            btn.style.cursor = 'not-allowed';
            btn.title = "Selecciona al menos una provincia o zona.";
        }
    };

    validate('venta');
    validate('alquiler');
}


/**
 * Validation for "Iniciar Lote" button (Excel Enrichment)
 */
function validateEnrichmentBatchButton() {
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
            } else {
                startTimer();
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
        // ONLY set these if we are actually in enrichment batch mode (the one that uses batchStartBtn)
        // If we are in provincial batch mode, isBatchMode is already true and should stay true.
        if (!isBatchMode) {
            isUpdateMode = true;
        }
        updateScraperState(true, isBatchMode ? 'Scraping Batch' : 'Enriquecimiento por Lotes');
        batchStartBtn.disabled = true;
        batchStopBtn.disabled = false;
        batchPauseBtn.disabled = false;
        batchPauseBtn.style.display = 'inline-block';
        batchResumeBtn.style.display = 'none';
        batchProgressText.textContent = "Ejecutando...";
        batchProgressText.style.color = "var(--success)";
    } else if (state === 'paused') {
        // Keep global state active but maybe indicate pause differently?
        // Ideally global state 'active' keeps start disabled.
        // But paused means we can stop or resume.
        // updateScraperState(true) is correct because we are in a session.
        batchStartBtn.disabled = true;
        batchStopBtn.disabled = false;
        batchPauseBtn.style.display = 'none';
        batchResumeBtn.style.display = 'inline-block';
        batchProgressText.textContent = "Pausado";
        batchProgressText.style.color = "var(--warning)";
    } else {
        updateScraperState(false);
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

        // Use addProperty to update UI and internal state
        addProperty(data);

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
        batchStartTime = null;
        stopTimer();
    });

    socket.on('batch_stopped', () => {
        setBatchUIState('idle');
        batchStartTime = null;
        stopTimer();
    });

    // Also listen for legacy status if paused manually
    socket.on('status_change', (data) => {
        // Only trigger Batch UI updates if we are in one of the batch/update modes
        // and NOT in standard manual scraping.
        if (isBatchMode || isUpdateMode) {
            if (data.status === 'paused') setBatchUIState('paused');
            if (data.status === 'running') setBatchUIState('running');
        }
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
// Tab Switching Logic
document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        // Remove active class from all buttons and panes
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab-pane').forEach(p => p.style.display = 'none');

        // Activate clicked
        btn.classList.add('active');
        const tabId = btn.getAttribute('data-tab');
        const pane = document.getElementById(`tab-${tabId}`);
        if (pane) {
            pane.style.display = 'block';
        }
    });
});

// Periodic Controls
const runPeriodicBtn = document.getElementById('runPeriodicBtn');
const stopPeriodicBtn = document.getElementById('stopPeriodicBtn');
const periodicStatusText = document.getElementById('periodicStatusText');
const periodicStatusIcon = document.getElementById('periodicStatusIcon');
const periodicLogsContainer = document.getElementById('periodicLogsContainer');
const periodicResultsBody = document.getElementById('periodicResultsBody');
const clearPeriodicLogsBtn = document.getElementById('clearPeriodicLogsBtn');

// Add specific styles if needed
if (periodicLogsContainer) {
    periodicLogsContainer.style.fontFamily = "'Consolas', monospace";
    periodicLogsContainer.style.fontSize = "0.85rem";
}

if (clearPeriodicLogsBtn) {
    clearPeriodicLogsBtn.addEventListener('click', () => {
        if (periodicLogsContainer) periodicLogsContainer.innerHTML = '';
    });
}

function addPeriodicLog(msg) {
    if (!periodicLogsContainer) return;
    const now = new Date().toLocaleTimeString('es-ES');
    const div = document.createElement('div');
    div.className = 'log-entry';
    div.innerHTML = `<span class="log-time">${now}</span> <span class="log-message">${msg}</span>`;
    periodicLogsContainer.appendChild(div);
    periodicLogsContainer.scrollTop = periodicLogsContainer.scrollHeight;
}

function updatePeriodicUI(status) {
    if (!periodicStatusText) return;

    if (status === 'running') {
        periodicStatusText.textContent = 'En Ejecución';
        periodicStatusText.style.color = 'var(--success)';
        periodicStatusIcon.textContent = '🔄';
        periodicStatusIcon.className = 'spin'; // Add css spin if available
        if (runPeriodicBtn) {
            runPeriodicBtn.disabled = true;
            runPeriodicBtn.innerHTML = '<span class="btn-icon">⏳</span> Ejecutando...';
        }
        if (stopPeriodicBtn) stopPeriodicBtn.disabled = false;

    } else if (status === 'paused') {
        periodicStatusText.textContent = 'Pausado';
        periodicStatusText.style.color = 'var(--warning)';
        periodicStatusIcon.textContent = '⏸';
        if (runPeriodicBtn) {
            runPeriodicBtn.disabled = false;
            runPeriodicBtn.innerHTML = '<span class="btn-icon">▶</span> Reanudar';
        }

    } else if (status === 'completed') {
        periodicStatusText.textContent = 'Completado';
        periodicStatusText.style.color = 'var(--text-main)';
        periodicStatusIcon.textContent = '✅';
        resetPeriodicButtons();

    } else {
        periodicStatusText.textContent = 'Inactivo';
        periodicStatusText.style.color = 'var(--text-muted)';
        periodicStatusIcon.textContent = '⏸';
        resetPeriodicButtons();
    }
}

function resetPeriodicButtons() {
    if (runPeriodicBtn) {
        runPeriodicBtn.disabled = false;
        runPeriodicBtn.innerHTML = '<span class="btn-icon">🚀</span> Iniciar Escaneo';
    }
    if (stopPeriodicBtn) {
        stopPeriodicBtn.disabled = true;
    }
}

// Button Listeners
if (runPeriodicBtn) {
    runPeriodicBtn.addEventListener('click', async () => {
        // Check if we are resuming or starting
        const isResuming = runPeriodicBtn.innerHTML.includes('Reanudar');
        const endpoint = isResuming ? '/api/periodic-lowcost/resume' : '/api/periodic-lowcost/start';

        try {
            await fetch(endpoint, { method: 'POST' });
        } catch (e) {
            addPeriodicLog("Error de conexión: " + e.message);
        }
    });
}

if (stopPeriodicBtn) {
    stopPeriodicBtn.addEventListener('click', async () => {
        if (confirm('¿Seguro que quieres detener el escaneo mensual?')) {
            try {
                await fetch('/api/periodic-lowcost/stop', { method: 'POST' });
            } catch (e) {
                addPeriodicLog("Error al detener: " + e.message);
            }
        }
    });
}

// Socket Listeners for Periodic
// (These should technically be inside initializeSocket, but we can append them safely)
if (typeof socket !== 'undefined' && socket) {
    socket.on('periodic_log', (data) => {
        addPeriodicLog(data.message);
    });

    socket.on('periodic_table_update', (data) => {
        // data = { province: 'Madrid', status: 'Completado' }
        if (!periodicResultsBody) return;

        // Find existing row
        let row = Array.from(periodicResultsBody.children).find(r => r.getAttribute('data-prov') === data.province);

        if (!row) {
            // Remove empty state if present
            if (periodicResultsBody.querySelector('.empty-state')) {
                periodicResultsBody.innerHTML = '';
            }
            row = document.createElement('tr');
            row.setAttribute('data-prov', data.province);
            row.innerHTML = `
                <td>${data.province}</td>
                <td class="status-cell">${data.status}</td>
                <td>--</td>
                <td>--</td>
            `;
            periodicResultsBody.insertBefore(row, periodicResultsBody.firstChild);
        } else {
            row.querySelector('.status-cell').textContent = data.status;
        }
    });
} else {
    // Retry attaching if socket not ready
    setTimeout(() => {
        if (typeof socket !== 'undefined' && socket) {
            socket.on('periodic_log', (data) => addPeriodicLog(data.message));
        }
    }, 2000);
}

// Poll initial status
async function checkPeriodicStatus() {
    try {
        const res = await fetch('/api/periodic-lowcost/status');
        const data = await res.json();
        updatePeriodicUI(data.status);
    } catch (e) { }
}

setInterval(checkPeriodicStatus, 5000);
checkPeriodicStatus();

/* Multi-Province Helpers */

async function loadProvincesList() {
    try {
        const res = await fetch('/api/provinces-list');
        const data = await res.json();
        if (data.provinces) {
            allProvincesList = data.provinces.sort((a, b) => a.name.localeCompare(b.name));
            // Build lookup map for verified URLs
            provinceUrls = {};
            allProvincesList.forEach(p => {
                // Generate slug if missing
                if (!p.slug && p.url_venta) {
                    try {
                        const parts = p.url_venta.split('/').filter(s => s.length > 0);
                        // https://www.idealista.com/venta-viviendas/madrid-provincia/
                        // [https:, www.idealista.com, venta-viviendas, madrid-provincia]
                        // Last part is the slug
                        p.slug = parts[parts.length - 1];
                    } catch (e) { p.slug = p.id; }
                }

                if (p.slug) {
                    provinceUrls[p.slug] = {
                        venta_url: p.url_venta,
                        alquiler_url: p.url_alquiler
                    };
                }
            });
            populateDropdown('listVenta', 'venta');
            populateDropdown('listAlquiler', 'alquiler');
        }
    } catch (e) { console.error(e); }
}


function populateDropdown(listId, type) {
    const list = document.getElementById(listId);
    if (!list) return;
    list.innerHTML = '';

    // Select All option
    const allDiv = document.createElement('div');
    allDiv.className = 'dropdown-item';
    allDiv.innerHTML = '<input type="checkbox" class="select-all-' + type + '"> <strong>Todos</strong>';

    allDiv.onclick = (e) => {
        if (e.target.tagName !== 'INPUT' && !e.target.classList.contains('toggle-zones-btn')) {
            const cb = allDiv.querySelector('input');
            cb.checked = !cb.checked;
            toggleAll(type, cb.checked);
        } else if (e.target.tagName === 'INPUT') {
            toggleAll(type, e.target.checked);
        }
    };
    list.appendChild(allDiv);

    allProvincesList.forEach(p => {
        const hasZones = p.zones && p.zones.length > 0;

        // Container for Province + Zones
        const container = document.createElement('div');
        container.className = 'province-group';

        // Province Row
        const item = document.createElement('div');
        item.className = 'dropdown-item';

        let toggleHtml = '';
        if (hasZones) {
            toggleHtml = `<button class="toggle-zones-btn" title="Ver ${p.zones.length} zonas">▼</button>`;
        }

        item.innerHTML = `
            <div class="province-row">
                <input type="checkbox" value="${p.id}" data-slug="${p.slug}" data-type="${type}" class="prov-cb-${type}">
                <span class="prov-name">${escapeHtml(p.name)}</span>
                ${toggleHtml}
            </div>
        `;

        // Event Listeners
        const checkbox = item.querySelector('input');
        const toggleBtn = item.querySelector('.toggle-zones-btn');

        // Toggle Zones visibility
        if (toggleBtn) {
            toggleBtn.onclick = (e) => {
                e.stopPropagation();
                const subContainer = container.querySelector('.sub-zones-container');
                if (subContainer) {
                    subContainer.classList.toggle('expanded');
                    toggleBtn.textContent = subContainer.classList.contains('expanded') ? '▲' : '▼';
                }
            };
        }

        // Checkbox Logic
        checkbox.addEventListener('change', (e) => {
            handleProvinceChange(p, type, e.target.checked, container);
        });

        item.addEventListener('click', (e) => {
            if (e.target !== checkbox && e.target !== toggleBtn) {
                checkbox.checked = !checkbox.checked;
                handleProvinceChange(p, type, checkbox.checked, container);
            }
        });

        container.appendChild(item);

        // Sub-zones Container
        if (hasZones) {
            const subContainer = document.createElement('div');
            subContainer.className = 'sub-zones-container';

            p.zones.forEach(zone => {
                const zItem = document.createElement('div');
                zItem.className = 'zone-item';
                zItem.innerHTML = `
                    <input type="checkbox" value="${zone.id}" data-href="${zone.href}" data-parent="${p.id}" class="zone-cb-${type}">
                    <span>${escapeHtml(zone.name)}</span>
                `;

                const zCheckbox = zItem.querySelector('input');
                zCheckbox.addEventListener('change', () => {
                    handleZoneChange(p, type, container);
                });

                zItem.addEventListener('click', (e) => {
                    if (e.target !== zCheckbox) {
                        zCheckbox.checked = !zCheckbox.checked;
                        handleZoneChange(p, type, container);
                    }
                });

                subContainer.appendChild(zItem);
            });
            container.appendChild(subContainer);
        }

        list.appendChild(container);
    });
}

function toggleAll(type, checked) {
    const list = document.getElementById(type === 'venta' ? 'listVenta' : 'listAlquiler');
    // Select all province checkboxes
    const provCbs = list.querySelectorAll(`.prov-cb-${type}`);
    provCbs.forEach(cb => {
        // Find parent container to check visibility
        const container = cb.closest('.province-group');
        if (container.style.display === 'none') return; // Skip if hidden by search filter

        cb.checked = checked;
        cb.indeterminate = false;

        // Find all zone checkboxes in container and check them
        const zoneCbs = container.querySelectorAll(`.zone-cb-${type}`);
        zoneCbs.forEach(zcb => zcb.checked = checked);
    });

    updateSelectionUI();
}

/**
 * Handle Province Checkbox Change (Cascade Down)
 */
function handleProvinceChange(province, type, checked, container) {
    // 1. Select/Deselect all children zones
    const zoneCbs = container.querySelectorAll(`.zone-cb-${type}`);
    zoneCbs.forEach(cb => {
        cb.checked = checked;
    });

    // 2. Update visual state of province checkbox (remove indeterminate)
    const provCb = container.querySelector(`.prov-cb-${type}`);
    if (provCb) {
        provCb.checked = checked;
        provCb.indeterminate = false;
    }

    updateSelectionUI();
}

/**
 * Handle Zone Checkbox Change (Bubble Up)
 */
function handleZoneChange(province, type, container) {
    const zoneCbs = Array.from(container.querySelectorAll(`.zone-cb-${type}`));
    const provCb = container.querySelector(`.prov-cb-${type}`);

    const allChecked = zoneCbs.every(cb => cb.checked);
    const someChecked = zoneCbs.some(cb => cb.checked);

    if (allChecked) {
        provCb.checked = true;
        provCb.indeterminate = false;
    } else if (someChecked) {
        provCb.checked = false;
        provCb.indeterminate = true;
    } else {
        provCb.checked = false;
        provCb.indeterminate = false;
    }

    updateSelectionUI();
}

function updateSelectionUI() {
    // Count selected provinces (Full or Partial)
    // We iterate over the DOM to see what is checked/indeterminate

    const countSelection = (type) => {
        const list = document.getElementById(type === 'venta' ? 'listVenta' : 'listAlquiler');
        if (!list) return 0;
        const provCbs = list.querySelectorAll(`.prov-cb-${type}`);
        let count = 0;
        provCbs.forEach(cb => {
            if (cb.checked || cb.indeterminate) count++;
        });
        return count;
    };

    const vCount = countSelection('venta');
    const aCount = countSelection('alquiler');

    const textV = document.getElementById('selectedTextVenta');
    const textA = document.getElementById('selectedTextAlquiler');
    if (textV) textV.textContent = vCount > 0 ? vCount + ' provincias (o zonas)' : 'Selecciona provincias...';
    if (textA) textA.textContent = aCount > 0 ? aCount + ' provincias (o zonas)' : 'Selecciona provincias...';

    // Update batch mode flag and validate the dedicated batch button
    const total = vCount + aCount;
    isBatchMode = total > 0;

    // Also update the hidden sets (selectedVenta/selectedAlquiler) if we still use them?
    // Actually, we'll traverse the DOM when starting the batch, so we don't need to maintain the Set strictly.
    // But for backward compatibility with simple logic:
    // ...

    validateProvinceBatchButtons();

    // Trigger Auto-Select File
    if (typeof autoSelectBatchFile === 'function') {
        autoSelectBatchFile();
    }
}

// Global startBatchFromProvinces function (needs to be defined or updated)
// Global startBatchFromProvinces function (Split by Type)
window.startBatchFromProvinces = async function (type) {
    if (!type) {
        console.error("Type (venta|alquiler) is required for batch start.");
        return;
    }

    // Traverse DOM to build URL list for specific type
    const urls = [];

    const collectUrls = (targetType) => {
        const list = document.getElementById(targetType === 'venta' ? 'listVenta' : 'listAlquiler');
        if (!list) return;

        // Find all Province Groups
        const groups = list.querySelectorAll('.province-group');
        groups.forEach(group => {
            const provCb = group.querySelector(`.prov-cb-${targetType}`);
            const zoneCbs = group.querySelectorAll(`.zone-cb-${targetType}`); // All zones
            const checkedZones = Array.from(zoneCbs).filter(z => z.checked);

            // Logic: Full province check takes precedence
            const isFullProvince = provCb.checked && !provCb.indeterminate;
            const explicitAllZones = (zoneCbs.length > 0 && checkedZones.length === zoneCbs.length);

            if (isFullProvince || explicitAllZones) {
                // MODIFIED: Prioritize Province URL as requested
                // If the user selects the full province (or all zones explicitly), we send the Province Seed URL.
                const pSlug = provCb.dataset.slug;
                let addedProvinceUrl = false;

                if (provinceUrls[pSlug]) {
                    const url = targetType === 'venta' ? provinceUrls[pSlug].venta_url : provinceUrls[pSlug].alquiler_url;
                    if (url) {
                        urls.push(url);
                        addedProvinceUrl = true;
                    }
                }

                // fallback to zones if province URL not found (shouldn't happen with verified data)
                if (!addedProvinceUrl && zoneCbs.length > 0) {
                    zoneCbs.forEach(z => {
                        urls.push(getZoneUrl(z, targetType));
                    });
                }
            } else {
                // Partial selection
                checkedZones.forEach(z => {
                    urls.push(getZoneUrl(z, targetType));
                });
            }
        });
    };

    const getZoneUrl = (checkbox, targetType) => {
        const href = checkbox.dataset.href;
        let finalHref = href;
        if (targetType === 'alquiler' && finalHref.includes('/venta-viviendas/')) {
            finalHref = finalHref.replace('/venta-viviendas/', '/alquiler-viviendas/');
        } else if (targetType === 'venta' && finalHref.includes('/alquiler-viviendas/')) {
            finalHref = finalHref.replace('/alquiler-viviendas/', '/venta-viviendas/');
        }
        return `https://www.idealista.com${finalHref}`;
    };


    console.clear();
    console.log(`[BATCH] Building URL list for ${type}...`);

    collectUrls(type);

    if (urls.length === 0) {
        alert("Por favor, selecciona al menos una provincia o zona.");
        return;
    }

    // Price Limit Logic
    let finalUrls = urls;
    const priceCheckIdx = type === 'venta' ? 'checkPriceVenta' : 'checkPriceAlquiler';
    const priceInputIdx = type === 'venta' ? 'priceVenta' : 'priceAlquiler';

    const usePriceLimit = document.getElementById(priceCheckIdx)?.checked;
    const priceValue = document.getElementById(priceInputIdx)?.value;

    if (usePriceLimit && priceValue) {
        addLog('INFO', `Aplicando filtro de precio: Max ${priceValue}€`);
        finalUrls = urls.map(u => {
            // Remove trailing slash to append safely
            let base = u.endsWith('/') ? u : u + '/';
            // standard idealista structure: .../provincia/ OR .../zona/
            // params go after. e.g. .../provincia/con-precio-hasta_300000/
            return `${base}con-precio-hasta_${priceValue}/`;
        });
    }

    addLog('INFO', `🚀 Iniciando lote ${type.toUpperCase()} con ${finalUrls.length} destinos.`);
    finalUrls.forEach((url, i) => {
        console.log(`[BATCH] #${i + 1}: ${url}`);
    });


    // Set global flags correctly
    isBatchMode = true;
    isUpdateMode = false;

    // Trigger UI Update disabling buttons
    updateScraperState(true, `Scraping Batch ${type.toUpperCase()}`);

    // Disable the OTHER start button explicitly (redundant with updateScraperState but safe)
    const otherType = type === 'venta' ? 'alquiler' : 'venta';
    const otherBtn = document.getElementById(otherType === 'venta' ? 'startBatchVentaBtn' : 'startBatchAlquilerBtn');
    if (otherBtn) otherBtn.disabled = true;


    // Get target file
    let targetFile = null;
    const fileSelect = document.getElementById('batchDestinationFile');
    if (fileSelect && fileSelect.value) {
        targetFile = fileSelect.value;
    }

    // Call API
    try {
        const response = await fetch('/api/start-batch', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                urls: finalUrls,
                mode: 'fast',
                expand: false,
                smart_enrichment: true,
                target_file: targetFile,
                province_name: urls.length === 1 ? 'SingleProvince' : 'MultiProvince', // Naive, server can handle
                operation_type: type
            })
        });

        const data = await response.json();
        if (response.ok) {
            addLog('OK', `Lote iniciado con PID ${data.pid}`);
            batchStartTime = Date.now();
            startTimer();
        } else {
            addLog('ERR', `Error: ${data.error}`);
            updateScraperState(false); // Reset if failed
        }
    } catch (e) {
        addLog('ERR', `Error de conexión: ${e.message}`);
        updateScraperState(false);
    }
};

// Bind new buttons
const bindBatchButtons = () => {
    const btnVenta = document.getElementById('startBatchVentaBtn');
    const btnAlquiler = document.getElementById('startBatchAlquilerBtn');

    if (btnVenta) {
        btnVenta.onclick = () => startBatchFromProvinces('venta');
    }
    if (btnAlquiler) {
        btnAlquiler.onclick = () => startBatchFromProvinces('alquiler');
    }
};
// Call immediately in case DOM is ready, and also ensure called on load
bindBatchButtons();
document.addEventListener('DOMContentLoaded', bindBatchButtons);



/**
 * Validates if the "Start Scraping" button should be enabled.
 * Only checks the manual URL input; province selection is handled by the dedicated batch button.
 */
function validateStartButton() {
    if (isRunning || isPaused) return; // Don't enable if already running/paused

    const url = seedUrlInput.value.trim();
    const hasValidUrl = url.includes('idealista.com/') &&
        (url.includes('/alquiler-') || url.includes('/venta-') || url.includes('/habitacion-'));

    if (hasValidUrl) {
        startBtn.disabled = false;
        startBtn.title = "";
    } else {
        startBtn.disabled = true;
        startBtn.title = "Introduce una URL válida de Idealista.";
    }
}

/**
 * Validates if the dedicated "Iniciar scraping de provincias" button should be enabled.
 * Enabled if at least one province is selected.
 */
function validateBatchButton() {
    validateProvinceBatchButtons();
}

/**
 * Starts batch scraping from the selected provinces using Fast mode.
 * Triggered by the "Iniciar scraping de provincias" button.
 */


function setupMultiSelectUI() {
    console.log('[MultiSelect] Setting up dropdown UI...');

    ['Venta', 'Alquiler'].forEach(type => {
        const trigger = document.getElementById('trigger' + type);
        const overlay = document.getElementById('dropdown' + type);
        const search = document.getElementById('search' + type);

        console.log(`[MultiSelect] ${type}: trigger=${!!trigger}, overlay=${!!overlay}`);

        if (trigger && overlay) {
            // Avoid adding multiple listeners if called multiple times
            if (trigger.dataset.listenerAttached === 'true') {
                console.log(`[MultiSelect] Listener already attached for ${type}`);
                return;
            }

            trigger.addEventListener('click', function (e) {
                e.preventDefault();
                e.stopPropagation();
                console.log(`[MultiSelect] ${type} trigger clicked!`);

                // Close other dropdowns
                document.querySelectorAll('.dropdown-overlay').forEach(el => {
                    if (el !== overlay) el.classList.remove('active');
                });

                // Toggle this dropdown
                overlay.classList.toggle('active');
                console.log(`[MultiSelect] ${type} overlay active: ${overlay.classList.contains('active')}`);
            });

            trigger.dataset.listenerAttached = 'true';
        }

        if (search) {
            // Remove old listener if needed? checking dataset is enough
            if (search.dataset.listenerAttached === 'true') return;

            search.addEventListener('input', (e) => {
                const term = e.target.value.toLowerCase();
                const list = document.getElementById('list' + type);
                if (list) {
                    const items = list.querySelectorAll('.province-group, .dropdown-item:not(.province-group)');
                    // We need to filter province groups somewhat intelligently
                    // Simple search: hide non-matching province groups?
                    // Better: filter provinces by name.
                    const groups = list.querySelectorAll('.province-group');
                    groups.forEach(group => {
                        const nameSpan = group.querySelector('.prov-name');
                        const text = nameSpan ? nameSpan.textContent.toLowerCase() : '';
                        const visible = text.includes(term);
                        group.style.display = visible ? 'block' : 'none';
                    });
                }
            });
            search.dataset.listenerAttached = 'true';
        }
    });

    // Close on click outside - but not on dropdown content
    // Use a named function to allow removal if needed, or just check flag
    if (!document.body.dataset.dropdownCloserAttached) {
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.custom-dropdown-group')) {
                document.querySelectorAll('.dropdown-overlay').forEach(el => el.classList.remove('active'));
            }
        });
        document.body.dataset.dropdownCloserAttached = 'true';
    }

    console.log('[MultiSelect] Setup complete.');
}

// Global toggle function for fallback onclick usage
window.toggleDropdown = function (type) {
    const overlay = document.getElementById('dropdown' + type);
    if (overlay) {
        // Close others
        document.querySelectorAll('.dropdown-overlay').forEach(el => {
            if (el !== overlay) el.classList.remove('active');
        });
        overlay.classList.toggle('active');
    }
};



/* BATCH DESTINATION FILE SELECTOR */
/* BATCH DESTINATION FILE SELECTOR */
const batchDestInput = document.getElementById('batchDestinationFile');
const batchDestTrigger = document.getElementById('triggerBatchFile');
const batchDestText = document.getElementById('selectedTextBatchFile');
const batchDestOverlay = document.getElementById('dropdownBatchFile');
const batchDestList = document.getElementById('listBatchFile');
const batchDestSearch = document.getElementById('searchBatchFile');

let availableExcelFiles = [];

async function loadBatchDestinationFiles() {
    if (!batchDestList) return;

    try {
        const response = await fetch('/api/salidas-files?limit=200');
        const data = await response.json();

        // Filter only .xlsx files
        availableExcelFiles = (data.files || []).filter(f => f.name.toLowerCase().endsWith('.xlsx'));

        // Render List
        renderBatchFiles();

        // Default to empty (Crear nuevo) if no value set
        if (!batchDestInput.value) {
            selectBatchFile('');
        }

        // Trigger auto-select AFTER loading files
        if (typeof autoSelectBatchFile === 'function') {
            autoSelectBatchFile();
        }

    } catch (e) {
        console.error("Error loading batch destination files:", e);
    }
}

function renderBatchFiles(filterText = '') {
    if (!batchDestList) return;
    batchDestList.innerHTML = '';

    const filter = filterText.toLowerCase();

    // Always show "New File" option matching search or if search is empty
    if ('(crear nuevo archivo automático)'.includes(filter) || filter === '') {
        const defItem = document.createElement('div');
        defItem.className = 'dropdown-item';
        defItem.textContent = '(Crear nuevo archivo automático)';
        defItem.dataset.value = '';
        defItem.onclick = () => {
            selectBatchFile('', true); // Manual selection of "New File"
            toggleBatchDropdown(false);
        };
        batchDestList.appendChild(defItem);
    }

    availableExcelFiles.forEach(file => {
        if (file.name.toLowerCase().includes(filter)) {
            const item = document.createElement('div');
            item.className = 'dropdown-item';

            // Format time safely
            let timeStr = '';
            if (typeof formatMtime === 'function') {
                timeStr = formatMtime(file.mtime);
            } else {
                timeStr = new Date(file.mtime * 1000).toLocaleString();
            }

            // Flex layout for item
            item.innerHTML = `
                <div style="display:flex; justify-content:space-between; width:100%;">
                    <span>${file.name}</span>
                    <span style="font-size:0.8em; color:#888; margin-left:10px;">${timeStr}</span>
                </div>
            `;
            item.dataset.value = file.name;
            item.onclick = () => {
                selectBatchFile(file.name, true); // Manual selection
                toggleBatchDropdown(false);
            };
            batchDestList.appendChild(item);
        }
    });
}

function selectBatchFile(filename, isManual = false) {
    if (isManual) isBatchFileManual = true;

    if (batchDestInput) batchDestInput.value = filename;
    if (batchDestText) {
        batchDestText.textContent = filename || '(Crear nuevo archivo automático)';
        if (filename) {
            batchDestText.style.color = '#fff'; // Highlight
            batchDestText.style.fontWeight = '500';
        } else {
            batchDestText.style.color = 'var(--text-secondary)';
            batchDestText.style.fontWeight = 'normal';
        }
    }
}

function toggleBatchDropdown(forceState) {
    if (!batchDestOverlay) return;

    // Close other dropdowns first
    if (!forceState && !batchDestOverlay.classList.contains('active')) {
        document.querySelectorAll('.dropdown-overlay').forEach(el => el.classList.remove('active'));
    }

    if (typeof forceState === 'boolean') {
        if (forceState) batchDestOverlay.classList.add('active');
        else batchDestOverlay.classList.remove('active');
    } else {
        batchDestOverlay.classList.toggle('active');
    }
}

// Bind Events
if (batchDestTrigger) {
    batchDestTrigger.addEventListener('click', (e) => {
        e.stopPropagation();
        toggleBatchDropdown();
    });
}

if (batchDestSearch) {
    batchDestSearch.addEventListener('input', (e) => {
        renderBatchFiles(e.target.value);
    });
    // Prevent closing when clicking search
    batchDestSearch.addEventListener('click', (e) => e.stopPropagation());
}

// Auto-select file based on partial province selection
// This is called from updateSelectionUI
function autoSelectBatchFile() {
    if (!batchDestInput) return;

    // Determine dominant province and type
    const listVenta = document.getElementById('listVenta');
    const listAlquiler = document.getElementById('listAlquiler');

    let targetProvince = null;
    let targetType = null;

    // Count selected in Venta
    let checkedVenta = [];
    if (listVenta) {
        checkedVenta = Array.from(listVenta.querySelectorAll('.prov-cb-venta:checked'));
    }

    // Count selected in Alquiler
    let checkedAlquiler = [];
    if (listAlquiler) {
        checkedAlquiler = Array.from(listAlquiler.querySelectorAll('.prov-cb-alquiler:checked'));
    }

    // Reset manual flag if selection is cleared completely
    if (checkedVenta.length === 0 && checkedAlquiler.length === 0) {
        isBatchFileManual = false;
        selectBatchFile('');
        return;
    }

    // If already manual, don't overwrite
    if (isBatchFileManual) return;

    // Logic: Only auto-select if EXACTLY ONE province is selected across BOTH lists
    // This avoids confusing behavior when multiple provinces are selected
    if (checkedVenta.length === 1 && checkedAlquiler.length === 0) {
        targetProvince = checkedVenta[0].dataset.name;
        targetType = 'venta';
    } else if (checkedAlquiler.length === 1 && checkedVenta.length === 0) {
        targetProvince = checkedAlquiler[0].dataset.name;
        targetType = 'alquiler';
    }

    if (targetProvince && targetType) {
        // Robust matching: remove accents AND non-alphanumeric chars
        const normalizeForMatch = (s) => normalizeString(s).toLowerCase().replace(/[^a-z0-9]+/g, '');
        const cleanProv = normalizeForMatch(targetProvince);
        const typeKeywords = targetType === 'venta' ? ['venta', 'sale'] : ['alquiler', 'rent'];

        console.log(`[AutoSelect] Looking for match: Prov=${cleanProv}, Type=${targetType}`);

        const bestMatch = availableExcelFiles.find(f => {
            const cleanName = normalizeForMatch(f.name);
            const hasProv = cleanName.includes(cleanProv);
            // Check if name contains type keyword
            const hasType = typeKeywords.some(k => f.name.toLowerCase().includes(k));
            return hasProv && hasType;
        });

        if (bestMatch) {
            console.log(`[AutoSelect] Found match for ${targetProvince} (${targetType}): ${bestMatch.name}`);
            selectBatchFile(bestMatch.name);
        } else {
            // If no match, reset to "Create New" only if it was NOT manually set?
            // Safer: only reset if it was already an auto-selected one or empty
            selectBatchFile('');
        }
    } else {
        // If 0 or >1 provinces selected, default to Create New (unless user already picked one manually)
        // But we'll leave it as is if it's already set.
    }
}

/**
 * Toggle Help for Province Update Process
 */
function toggleProvinceHelp() {
    const help = document.getElementById('provinceHelpText');
    if (!help) return;

    if (help.style.display === 'none') {
        help.style.display = 'block';
    } else {
        help.style.display = 'none';
    }
}

// Helper for normalization
function normalizeString(str) {
    return str.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
}

// Format timestamp (mtime)
function formatMtime(mtime) {
    if (!mtime) return '';
    const date = new Date(mtime * 1000);
    const day = String(date.getDate()).padStart(2, '0');
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const mins = String(date.getMinutes()).padStart(2, '0');
    return `${day}/${month} ${hours}:${mins}`;
}

// Initial Load replaced by DOMContentLoaded at top
