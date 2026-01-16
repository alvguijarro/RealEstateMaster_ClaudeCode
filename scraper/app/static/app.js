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

// Column definitions (matches ORDERED_BASE in Python for consistency)
const COLUMNS = [
    'Titulo', 'price', 'old price', 'price change %', 'Ubicacion',
    'actualizado hace',
    'm2 construidos', 'm2 utiles', 'precio por m2', 'Num plantas', 'habs', 'banos',
    'Terraza', 'Garaje', 'Armarios', 'Trastero', 'Calefaccion',
    'tipo', 'parcela', 'ascensor', 'orientacion', 'altura',
    'construido en', 'jardin', 'piscina', 'aire acond',
    'Calle', 'Barrio', 'Distrito', 'Zona', 'Ciudad', 'Provincia',
    'Consumo 1', 'Consumo 2', 'Emisiones 1', 'Emisiones 2',
    'estado', 'gastos comunidad',
    'okupado', 'Copropiedad', 'con inquilino', 'nuda propiedad',
    'Descripción',
    'URL'
];

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
                    option.textContent = `${file.name} (${countDisplay} propiedades)`;
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
        }
    });
}

// Search filter listener
if (worksheetSearch) {
    worksheetSearch.addEventListener('input', (e) => {
        renderWorksheetList(e.target.value);
    });
}

async function startUrlUpdate() {
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
            body: JSON.stringify({ excel_file: excelFile, sheets: selectedSheets })
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
    updateUrlsBtn.addEventListener('click', startUrlUpdate);
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
    });

    socket.on('disconnect', () => {
        addLog('WARN', 'Desconectado del servidor');
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
        // Update page progress (e.g., "02 / 68")
        statCurrentPage.textContent = String(data.current_page || 0).padStart(2, '0');
        statTotalPages.textContent = String(data.total_pages || 0).padStart(2, '0');

        // Update property progress (e.g., "980 / 2055")
        statCurrentProps.textContent = data.current_properties || 0;
        statTotalProps.textContent = data.total_properties || 0;
    });

    socket.on('browser_closed', (data) => {
        addLog('WARN', 'El navegador fue cerrado. Scraping pausado.');
        showBrowserClosedModal();
    });
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
    startBtn.addEventListener('click', startScraping);
    pauseBtn.addEventListener('click', togglePause);
    stopBtn.addEventListener('click', stopScraping);
    clearLogsBtn.addEventListener('click', clearLogs);
    clearHistoryBtn.addEventListener('click', clearHistory);

    // Enter key to start
    seedUrlInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter' && !isRunning) {
            startScraping();
        }
    });
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
}

function buildTableHeader() {
    tableHeader.innerHTML = COLUMNS.map(col =>
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
    logsContainer.scrollTop = logsContainer.scrollHeight;

    // Keep only last 500 entries
    while (logsContainer.children.length > 500) {
        logsContainer.removeChild(logsContainer.firstChild);
    }
}

function clearLogs() {
    logsContainer.innerHTML = '';
    addLog('INFO', 'Logs limpiados');
}

function addProperty(data) {
    properties.push(data);

    // Update results count (for table display only)
    resultsCount.textContent = `${properties.length} propiedades`;

    // Hide empty state
    emptyState.style.display = 'none';

    // Add table row
    const row = document.createElement('tr');
    row.innerHTML = COLUMNS.map(col => {
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
        // Make URL clickable
        if (col === 'URL' && value) {
            return `<td><a href="${escapeHtml(value)}" target="_blank" style="color: var(--primary);">${escapeHtml(value)}</a></td>`;
        }
        return `<td title="${escapeHtml(String(value))}">${escapeHtml(String(value))}</td>`;
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
        'captcha': 'CAPTCHA detectado',
        'resting': 'Descansando...'
    };
    statusBadge.querySelector('.status-text').textContent = statusTexts[status] || status;

    // Update buttons based on status
    if (status === 'running') {
        isRunning = true;
        isPaused = false;
        startBtn.disabled = true;
        pauseBtn.disabled = false;
        stopBtn.disabled = false;
        pauseBtn.innerHTML = '<span class="btn-icon">⏸</span> Pausar';
        seedUrlInput.disabled = true;
    } else if (status === 'paused') {
        isPaused = true;
        pauseBtn.innerHTML = '<span class="btn-icon">▶</span> Reanudar';
    } else if (status === 'captcha') {
        isPaused = true;
        pauseBtn.disabled = false;
        pauseBtn.innerHTML = '<span class="btn-icon">▶</span> Reanudar Despúes de CAPTCHA';
        // Make it obvious
        pauseBtn.classList.add('btn-warning');

        // Play alarm sound
        playAlarm();
    } else if (status === 'completed' || status === 'stopped' || status === 'stopping') {
        if (isUpdateMode) {
            resetUIState();
            // Add log about completion if needed, handled by server events usually
        } else {
            isRunning = false;
            isPaused = false;
            startBtn.disabled = false;
            pauseBtn.disabled = true;
            stopBtn.disabled = true;
            pauseBtn.classList.remove('btn-warning');
            seedUrlInput.disabled = false;
            stopTimer();

            // Show download button
            if (data.file) {
                downloadBtn.style.display = 'inline-flex';
                downloadBtn.href = '/api/download';
                addLog('OK', `Archivo guardado: ${data.file}`);
            }
            // Re-enable update button
            if (updateUrlsBtn) updateUrlsBtn.disabled = false;
        }
    } else if (status === 'error') {
        if (isUpdateMode) {
            resetUIState();
        } else {
            isRunning = false;
            startBtn.disabled = false;
            pauseBtn.disabled = true;
            stopBtn.disabled = true;
            seedUrlInput.disabled = false;
            stopTimer();
            // Re-enable update button
            if (updateUrlsBtn) updateUrlsBtn.disabled = false;
        }
    }
}

async function startScraping() {
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

    addLog('INFO', `Iniciando scraping en modo ${currentMode.toUpperCase()}...`);

    try {
        const response = await fetch('/api/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                seed_url: seedUrl,
                mode: currentMode
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
    if (!startTime) return;

    const elapsed = Math.floor((Date.now() - startTime) / 1000);
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
    statTime.textContent = timeStr;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// History Functions
async function loadHistory() {
    try {
        const response = await fetch('/api/history');
        const data = await response.json();

        if (data.history && data.history.length > 0) {
            historyEmptyState.style.display = 'none';
            data.history.forEach(entry => addHistoryRow(entry, false));
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
                resumeBtn.title = `Reanudar desde página ${data.state.current_page} (${data.state.scraped_count} propiedades guardadas)`;
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
        outputDirInput.disabled = true;

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
