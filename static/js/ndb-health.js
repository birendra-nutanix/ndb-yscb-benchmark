/**
 * NDB Health Dashboard JavaScript
 * Handles all interactions for the NDB Health monitoring dashboard
 */

// Global NDB connection info
let ndbHealthConnection = null;

/**
 * Connect to NDB and load health dashboard data
 */
async function connectNDBHealth() {
    const ip = document.getElementById('ndbHealthIp').value;
    const port = document.getElementById('ndbHealthPort').value;
    const username = document.getElementById('ndbHealthUsername').value;
    const password = document.getElementById('ndbHealthPassword').value;
    
    // Validation
    if (!ip || !username || !password) {
        alert('Please fill in all required fields (IP, Username, Password)');
        return;
    }
    
    // Show loading status
    const statusDiv = document.getElementById('ndb-health-connection-status');
    statusDiv.style.display = 'block';
    statusDiv.className = 'alert alert-info';
    statusDiv.innerHTML = '<i class="bi bi-hourglass-split"></i> Connecting to NDB...';
    
    try {
        // Store connection info
        ndbHealthConnection = {
            ip: ip,
            port: port,
            username: username,
            password: password
        };
        
        // Test connection by trying to fetch overview
        const response = await fetch('/api/ndb-health/overview', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(ndbHealthConnection)
        });
        
        if (!response.ok) {
            throw new Error(`Connection failed: ${response.statusText}`);
        }
        
        const data = await response.json();
        
        // Show success
        statusDiv.className = 'alert alert-success';
        statusDiv.innerHTML = `
            <i class="bi bi-check-circle"></i> Connected successfully! 
            Found ${data.total || 0} databases.
        `;
        
        // Show dashboard content
        document.getElementById('ndb-health-content').style.display = 'block';
        
        // Load all dashboard data
        await loadNDBHealthDashboard();
        
    } catch (error) {
        console.error('Error connecting to NDB:', error);
        statusDiv.className = 'alert alert-danger';
        statusDiv.innerHTML = `
            <i class="bi bi-x-circle"></i> Connection failed: ${error.message}
            <br><small>Please check your NDB IP, credentials, and network connectivity.</small>
        `;
        
        // Hide dashboard content
        document.getElementById('ndb-health-content').style.display = 'none';
    }
}

/**
 * Dashboard navigation - Switch between YCSB Generator and NDB Health
 */
function showDashboard(dashboardName) {
    console.log('showDashboard called with:', dashboardName);
    
    // Hide all dashboards
    document.querySelectorAll('.dashboard-content').forEach(dash => {
        dash.style.display = 'none';
    });
    
    // Remove active class from all nav links
    document.querySelectorAll('#sidebar .nav-link').forEach(link => {
        link.classList.remove('active');
    });
    
    // Show selected dashboard
    const dashboardId = `dashboard-${dashboardName}`;
    console.log('Looking for element:', dashboardId);
    const dashboardElement = document.getElementById(dashboardId);
    
    if (dashboardElement) {
        console.log('Found dashboard element, showing it');
        dashboardElement.style.display = 'block';
    } else {
        console.error('Dashboard element not found:', dashboardId);
    }
    
    // Add active class to selected nav link
    const navLink = document.getElementById(`nav-${dashboardName}`);
    if (navLink) {
        navLink.classList.add('active');
    }
    
    // Load dashboard data only if already connected
    if (dashboardName === 'ndb-health' && ndbHealthConnection) {
        loadNDBHealthDashboard();
    }
}

/**
 * Load complete NDB Health Dashboard
 */
async function loadNDBHealthDashboard() {
    if (!ndbHealthConnection) {
        console.log('NDB Health Dashboard: Waiting for connection...');
        return;
    }
    
    try {
        // Load all components
        await loadOverview();
        await loadAlerts();
        await loadOperations();
    } catch (error) {
        console.error('Error loading NDB health dashboard:', error);
        // Don't show alert, just log the error
        console.error('Please connect to NDB first using the connection form.');
    }
}

/**
 * Load database overview statistics
 */
async function loadOverview() {
    if (!ndbHealthConnection) {
        console.log('Overview: No NDB connection established yet');
        return;
    }
    
    try {
        const response = await fetch('/api/ndb-health/overview', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(ndbHealthConnection)
        });
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        
        // Update summary cards
        document.getElementById('total-databases').textContent = data.summary?.total || 0;
        document.getElementById('ready-databases').textContent = data.summary?.ready || 0;
        document.getElementById('not-ready-databases').textContent = data.summary?.not_ready || 0;
        
        // Update PostgreSQL
        const postgres = data.by_engine?.postgres || {};
        document.getElementById('postgres-total').textContent = postgres.total || 0;
        document.getElementById('postgres-ready').textContent = postgres.ready || 0;
        document.getElementById('postgres-si').textContent = postgres.by_type?.si || 0;
        document.getElementById('postgres-ha').textContent = postgres.by_type?.patroni || 0;
        document.getElementById('postgres-clone').textContent = postgres.by_type?.clone || 0;
        
        // Update MySQL
        const mysql = data.by_engine?.mysql || {};
        document.getElementById('mysql-total').textContent = mysql.total || 0;
        document.getElementById('mysql-ready').textContent = mysql.ready || 0;
        document.getElementById('mysql-si').textContent = mysql.by_type?.si || 0;
        document.getElementById('mysql-innodb-cluster').textContent = mysql.by_type?.innodb_cluster || 0;
        document.getElementById('mysql-clone').textContent = mysql.by_type?.clone || 0;
        
        // Update MariaDB
        const mariadb = data.by_engine?.mariadb || {};
        const mariadbTotalEl = document.getElementById('mariadb-total');
        if (mariadbTotalEl) {
            mariadbTotalEl.textContent = mariadb.total || 0;
            document.getElementById('mariadb-ready').textContent = mariadb.ready || 0;
            document.getElementById('mariadb-si').textContent = mariadb.by_type?.si || 0;
            document.getElementById('mariadb-innodb-cluster').textContent = mariadb.by_type?.innodb_cluster || 0;
            document.getElementById('mariadb-clone').textContent = mariadb.by_type?.clone || 0;
        }
        
        // Update Oracle
        const oracle = data.by_engine?.oracle || {};
        document.getElementById('oracle-total').textContent = oracle.total || 0;
        document.getElementById('oracle-ready').textContent = oracle.ready || 0;
        document.getElementById('oracle-si').textContent = oracle.by_type?.si || 0;
        document.getElementById('oracle-rac').textContent = oracle.by_type?.rac || 0;
        document.getElementById('oracle-clone').textContent = oracle.by_type?.clone || 0;
        
        // Update MS SQL
        const mssql = data.by_engine?.mssql || {};
        document.getElementById('mssql-total').textContent = mssql.total || 0;
        document.getElementById('mssql-ready').textContent = mssql.ready || 0;
        document.getElementById('mssql-si').textContent = mssql.by_type?.si || 0;
        document.getElementById('mssql-aag').textContent = mssql.by_type?.aag || 0;
        document.getElementById('mssql-clone').textContent = mssql.by_type?.clone || 0;
        
        // Update MongoDB
        const mongodb = data.by_engine?.mongodb || {};
        document.getElementById('mongodb-total').textContent = mongodb.total || 0;
        document.getElementById('mongodb-ready').textContent = mongodb.ready || 0;
        document.getElementById('mongodb-si').textContent = mongodb.by_type?.si || 0;
        document.getElementById('mongodb-replicaset').textContent = mongodb.by_type?.replicaset || 0;
        document.getElementById('mongodb-shard').textContent = mongodb.by_type?.shard_replica_set || 0;
        document.getElementById('mongodb-clone').textContent = mongodb.by_type?.clone || 0;
        
    } catch (error) {
        console.error('Error loading overview:', error);
        // Set all to 'Error'
        document.getElementById('total-databases').textContent = 'Error';
        document.getElementById('ready-databases').textContent = 'Error';
        document.getElementById('not-ready-databases').textContent = 'Error';
        
        // Set engine cards to 0 on error
        ['postgres', 'mysql', 'mariadb', 'oracle', 'mssql', 'mongodb'].forEach(engine => {
            const totalEl = document.getElementById(`${engine}-total`);
            const readyEl = document.getElementById(`${engine}-ready`);
            if (totalEl) totalEl.textContent = '0';
            if (readyEl) readyEl.textContent = '0';
        });
    }
}

// Global state for alerts pagination
let allAlertsData = [];
let currentAlertsPage = 1;
const ALERTS_PER_PAGE = 5;

/**
 * Render a specific page of alerts
 */
function renderAlertsPage(page) {
    const tbody = document.getElementById('alerts-tbody');
    const paginationInfo = document.getElementById('alerts-pagination-info');
    const paginationNav = document.getElementById('alerts-pagination');
    
    if (!allAlertsData || allAlertsData.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="5" class="text-center text-muted">
                    <i class="bi bi-check-circle text-success"></i> No alerts found
                </td>
            </tr>
        `;
        if (paginationInfo) paginationInfo.textContent = 'Showing 0 alerts';
        if (paginationNav) paginationNav.innerHTML = '';
        return;
    }
    
    const totalPages = Math.ceil(allAlertsData.length / ALERTS_PER_PAGE);
    
    // Ensure page is within bounds
    if (page < 1) page = 1;
    if (page > totalPages) page = totalPages;
    
    currentAlertsPage = page;
    
    const startIndex = (page - 1) * ALERTS_PER_PAGE;
    const endIndex = Math.min(startIndex + ALERTS_PER_PAGE, allAlertsData.length);
    const pageData = allAlertsData.slice(startIndex, endIndex);
    
    // Render table rows
    tbody.innerHTML = pageData.map(alert => {
        const severityVal = alert.severity ? alert.severity.toUpperCase() : '';
        const severityClass = {
            'CRITICAL': 'danger',
            'WARNING': 'warning',
            'INFO': 'info'
        }[severityVal] || 'secondary';
        
        const statusClass = alert.status === 'ACTIVE' ? 'danger' : 'secondary';
        
        return `
            <tr>
                <td><span class="badge bg-${severityClass}">${alert.severity}</span></td>
                <td>${alert.message || 'N/A'}</td>
                <td>${formatDate(alert.dateCreated)}</td>
                <td>${alert.entityName || 'N/A'}</td>
                <td><span class="badge bg-${statusClass}">${alert.status || 'N/A'}</span></td>
            </tr>
        `;
    }).join('');
    
    // Update pagination info
    if (paginationInfo) {
        paginationInfo.textContent = `Showing ${startIndex + 1} to ${endIndex} of ${allAlertsData.length} alerts`;
    }
    
    // Render pagination controls
    if (paginationNav) {
        let paginationHTML = '';
        
        // Previous button
        paginationHTML += `
            <li class="page-item ${page === 1 ? 'disabled' : ''}">
                <a class="page-link" href="#" onclick="event.preventDefault(); renderAlertsPage(${page - 1})">Previous</a>
            </li>
        `;
        
        // Page numbers
        const maxVisiblePages = 5;
        let startPage = Math.max(1, page - Math.floor(maxVisiblePages / 2));
        let endPage = Math.min(totalPages, startPage + maxVisiblePages - 1);
        
        if (endPage - startPage + 1 < maxVisiblePages) {
            startPage = Math.max(1, endPage - maxVisiblePages + 1);
        }
        
        if (startPage > 1) {
            paginationHTML += `
                <li class="page-item"><a class="page-link" href="#" onclick="event.preventDefault(); renderAlertsPage(1)">1</a></li>
                ${startPage > 2 ? '<li class="page-item disabled"><span class="page-link">...</span></li>' : ''}
            `;
        }
        
        for (let i = startPage; i <= endPage; i++) {
            paginationHTML += `
                <li class="page-item ${i === page ? 'active' : ''}">
                    <a class="page-link" href="#" onclick="event.preventDefault(); renderAlertsPage(${i})">${i}</a>
                </li>
            `;
        }
        
        if (endPage < totalPages) {
            paginationHTML += `
                ${endPage < totalPages - 1 ? '<li class="page-item disabled"><span class="page-link">...</span></li>' : ''}
                <li class="page-item"><a class="page-link" href="#" onclick="event.preventDefault(); renderAlertsPage(${totalPages})">${totalPages}</a></li>
            `;
        }
        
        // Next button
        paginationHTML += `
            <li class="page-item ${page === totalPages ? 'disabled' : ''}">
                <a class="page-link" href="#" onclick="event.preventDefault(); renderAlertsPage(${page + 1})">Next</a>
            </li>
        `;
        
        paginationNav.innerHTML = paginationHTML;
    }
}

/**
 * Load alerts with time filter
 */
async function loadAlerts() {
    if (!ndbHealthConnection) {
        console.log('Alerts: No NDB connection established yet');
        return;
    }
    
    const days = document.getElementById('alerts-time-filter').value;
    const tbody = document.getElementById('alerts-tbody');
    
    // Show loading
    tbody.innerHTML = `
        <tr>
            <td colspan="5" class="text-center">
                <div class="spinner-border spinner-border-sm" role="status">
                    <span class="visually-hidden">Loading...</span>
                </div>
                Loading alerts for last ${days} day(s)...
            </td>
        </tr>
    `;
    
    try {
        const response = await fetch(`/api/ndb-health/alerts?days=${days}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(ndbHealthConnection)
        });
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        
        // Count by severity
        const criticalCount = data.filter(a => a.severity && a.severity.toUpperCase() === 'CRITICAL').length;
        const warningCount = data.filter(a => a.severity && a.severity.toUpperCase() === 'WARNING').length;
        const infoCount = data.filter(a => a.severity && a.severity.toUpperCase() === 'INFO').length;
        
        // Update summary counts
        document.getElementById('alert-critical-count').textContent = criticalCount;
        document.getElementById('alert-warning-count').textContent = warningCount;
        document.getElementById('alert-info-count').textContent = infoCount;
        document.getElementById('alert-total-count').textContent = data.length;
        
        // Store data globally and render first page
        allAlertsData = data;
        renderAlertsPage(1);
        
    } catch (error) {
        console.error('Error loading alerts:', error);
        tbody.innerHTML = `
            <tr>
                <td colspan="5" class="text-center text-danger">
                    <i class="bi bi-exclamation-triangle"></i> Error loading alerts: ${error.message}
                </td>
            </tr>
        `;
        
        // Reset counts
        document.getElementById('alert-critical-count').textContent = '0';
        document.getElementById('alert-warning-count').textContent = '0';
        document.getElementById('alert-info-count').textContent = '0';
        document.getElementById('alert-total-count').textContent = '0';
    }
}

/**
 * Load operations with time filter
 */
async function loadOperations() {
    if (!ndbHealthConnection) {
        console.log('Operations: No NDB connection established yet');
        return;
    }
    
    const days = document.getElementById('operations-time-filter').value;
    
    try {
        const response = await fetch(`/api/ndb-health/operations?days=${days}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(ndbHealthConnection)
        });
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        
        // Update summary counts
        document.getElementById('operations-success-count').textContent = data.total_successful || 0;
        document.getElementById('operations-failed-count').textContent = data.total_failed || 0;
        document.getElementById('operations-total-count').textContent = 
            (data.total_successful || 0) + (data.total_failed || 0);
        
        // Update Operations by Engine UI
        renderOperationsByEngine(data.operations_by_engine || {});
        
    } catch (error) {
        console.error('Error loading operations:', error);
        
        // Reset counts
        document.getElementById('operations-success-count').textContent = 'Error';
        document.getElementById('operations-failed-count').textContent = 'Error';
        document.getElementById('operations-total-count').textContent = 'Error';
        
        // Show error
        document.getElementById('operations-by-engine-container').innerHTML = `
            <div class="alert alert-danger">
                <i class="bi bi-exclamation-triangle"></i> Error loading operations data
            </div>
        `;
    }
}

/**
 * Render operations grouped by engine and deployment type
 */
function renderOperationsByEngine(operationsByEngine) {
    const container = document.getElementById('operations-by-engine-container');
    const engines = Object.keys(operationsByEngine);
    
    if (engines.length === 0) {
        container.innerHTML = `
            <div class="alert alert-info">
                <i class="bi bi-info-circle"></i> No operations found for the selected time period.
            </div>
        `;
        return;
    }
    
    // Engine icons mapping
    const engineIcons = {
        'postgres': 'bi-database',
        'mysql': 'bi-database',
        'mariadb': 'bi-database',
        'oracle': 'bi-database',
        'mssql': 'bi-window',
        'mongodb': 'bi-diagram-3',
        'unknown': 'bi-question-circle'
    };
    
    let html = '<div class="row">';
    
    engines.forEach((engine, index) => {
        const engineData = operationsByEngine[engine];
        const deploymentTypes = Object.keys(engineData);
        
        // Calculate totals for this engine
        let engineSuccess = 0;
        let engineFailed = 0;
        
        deploymentTypes.forEach(dt => {
            const dtData = engineData[dt];
            Object.values(dtData.successful).forEach(count => engineSuccess += count);
            Object.values(dtData.failed).forEach(count => engineFailed += count);
        });
        
        const engineIcon = engineIcons[engine.toLowerCase()] || 'bi-database';
        const engineName = engine.charAt(0).toUpperCase() + engine.slice(1);
        
        html += `
            <div class="col-12 mb-4">
                <div class="card border-primary shadow-sm">
                    <div class="card-header bg-primary text-white d-flex justify-content-between align-items-center">
                        <div>
                            <i class="bi ${engineIcon} me-2"></i>
                            <strong class="me-3">${engineName}</strong>
                        </div>
                        <div>
                            <span class="badge bg-success rounded-pill me-2">${engineSuccess} Success</span>
                            <span class="badge bg-danger rounded-pill">${engineFailed} Failed</span>
                        </div>
                    </div>
                    <div class="card-body bg-light">
                        <div class="row">
        `;
        
        // Render each deployment type (SI, HA, etc)
        deploymentTypes.forEach(dt => {
            const dtData = engineData[dt];
            const dtName = dt.toUpperCase();
            
            // Calculate totals for this deployment type
            let dtSuccess = 0;
            let dtFailed = 0;
            Object.values(dtData.successful).forEach(count => dtSuccess += count);
            Object.values(dtData.failed).forEach(count => dtFailed += count);
            
            html += `
                <div class="col-md-6 col-lg-4 mb-3">
                    <div class="card h-100 border-secondary shadow-sm">
                        <div class="card-header bg-white d-flex justify-content-between align-items-center py-2">
                            <h6 class="mb-0 text-dark">${dtName}</h6>
                            <div>
                                <span class="badge bg-success" title="Successful">${dtSuccess}</span>
                                <span class="badge bg-danger" title="Failed">${dtFailed}</span>
                            </div>
                        </div>
                        <div class="card-body p-0">
                            <ul class="list-group list-group-flush">
            `;
            
            // Render operations for this deployment type
            const allOpTypes = new Set([
                ...Object.keys(dtData.successful),
                ...Object.keys(dtData.failed)
            ]);
            
            if (allOpTypes.size === 0) {
                html += `<li class="list-group-item text-muted text-center py-2"><small>No operations</small></li>`;
            } else {
                Array.from(allOpTypes).sort().forEach(opType => {
                    const sCount = dtData.successful[opType] || 0;
                    const fCount = dtData.failed[opType] || 0;
                    
                    html += `
                        <li class="list-group-item d-flex justify-content-between align-items-center py-2">
                            <span class="text-truncate" style="max-width: 70%;" title="${opType}">
                                <small>${opType}</small>
                            </span>
                            <div>
                                ${sCount > 0 ? `<span class="badge bg-success rounded-pill">${sCount}</span>` : ''}
                                ${fCount > 0 ? `<span class="badge bg-danger rounded-pill ms-1">${fCount}</span>` : ''}
                            </div>
                        </li>
                    `;
                });
            }
            
            html += `
                            </ul>
                        </div>
                    </div>
                </div>
            `;
        });
        
        html += `
                        </div>
                    </div>
                </div>
            </div>
        `;
    });
    
    html += '</div>';
    container.innerHTML = html;
}

/**
 * Helper function to format dates
 */
function formatDate(dateString) {
    if (!dateString) return 'N/A';
    
    try {
        const date = new Date(dateString);
        return date.toLocaleString();
    } catch (error) {
        return dateString;
    }
}

/**
 * Sync operations to InfluxDB
 */
async function syncToInfluxDB() {
    if (!ndbHealthConnection) {
        alert('Please connect to NDB first');
        return;
    }

    const btn = document.getElementById('btn-sync-influx');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Syncing...';

    try {
        const response = await fetch('/api/ndb-health/sync-influxdb', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                ndb_connection: ndbHealthConnection,
                days: 30
            })
        });

        const data = await response.json();
        if (data.task_id) {
            pollSyncStatus(data.task_id, btn);
        } else {
            throw new Error("No task ID returned");
        }
    } catch (error) {
        console.error("Sync error:", error);
        showToast('Failed to start sync: ' + error.message, 'danger');
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-cloud-upload"></i> Sync to InfluxDB';
    }
}

async function pollSyncStatus(taskId, btn) {
    try {
        const response = await fetch(`/api/ndb-health/sync-status/${taskId}`);
        const data = await response.json();

        if (data.status === 'completed') {
            showToast(data.message, 'success');
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-cloud-upload"></i> Sync to InfluxDB';
        } else if (data.status === 'failed') {
            showToast(data.message, 'danger');
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-cloud-upload"></i> Sync to InfluxDB';
        } else {
            // Still running
            btn.innerHTML = `<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> ${data.progress}% - ${data.message}`;
            setTimeout(() => pollSyncStatus(taskId, btn), 2000);
        }
    } catch (error) {
        console.error("Poll error:", error);
        showToast('Error checking sync status', 'danger');
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-cloud-upload"></i> Sync to InfluxDB';
    }
}

function showToast(message, type) {
    const toastContainer = document.getElementById('toast-container');
    if (!toastContainer) {
        const container = document.createElement('div');
        container.id = 'toast-container';
        container.className = 'toast-container position-fixed bottom-0 end-0 p-3';
        container.style.zIndex = '1055';
        document.body.appendChild(container);
    }
    
    const toastId = 'toast-' + Date.now();
    const toastHtml = `
        <div id="${toastId}" class="toast align-items-center text-white bg-${type} border-0" role="alert" aria-live="assertive" aria-atomic="true">
            <div class="d-flex">
                <div class="toast-body">
                    ${message}
                </div>
                <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>
            </div>
        </div>
    `;
    
    document.getElementById('toast-container').insertAdjacentHTML('beforeend', toastHtml);
    const toastElement = document.getElementById(toastId);
    const toast = new bootstrap.Toast(toastElement, { delay: 5000 });
    toast.show();
    
    toastElement.addEventListener('hidden.bs.toast', function () {
        toastElement.remove();
    });
}

// Make functions available globally
window.showDashboard = showDashboard;
window.connectNDBHealth = connectNDBHealth;
window.loadAlerts = loadAlerts;
window.loadOperations = loadOperations;
window.syncToInfluxDB = syncToInfluxDB;

/**
 * Initialize dashboard on page load
 * Note: Dashboard visibility is now controlled server-side via active_dashboard variable
 * No need to call showDashboard() on load
 */
document.addEventListener('DOMContentLoaded', function() {
    console.log('DOM loaded, dashboard initialized server-side');
    // Dashboard visibility is controlled by server-side rendering
    // The showDashboard() function is kept for backward compatibility but not used for initial load
});
