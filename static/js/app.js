/**
 * NDB YCSB Benchmark Generator - Frontend JavaScript
 */

// State management
let validatedDatabases = null;
let isValidated = false;
let isDbCredsValidated = false;
let selectedEnginesWithDatabases = {};

// DOM Elements
const validateBtn = document.getElementById('validateBtn');
const validateDbCredsBtn = document.getElementById('validateDbCredsBtn');
const generateBtn = document.getElementById('generateBtn');
const benchmarkForm = document.getElementById('benchmarkForm');
const validationStatus = document.getElementById('validationStatus');
const dbCredValidationStatus = document.getElementById('dbCredValidationStatus');
const databasesList = document.getElementById('databasesList');
const databasesContent = document.getElementById('databasesContent');
const dbCredentialsSection = document.getElementById('dbCredentialsSection');
const dbCredentialsContent = document.getElementById('dbCredentialsContent');
const resultsSection = document.getElementById('resultsSection');
const downloadLink = document.getElementById('downloadLink');
const scriptName = document.getElementById('scriptName');
const scriptNameCmd = document.getElementById('scriptNameCmd');

// Event Listeners
if (validateBtn) validateBtn.addEventListener('click', validateNDBConnection);
if (validateDbCredsBtn) validateDbCredsBtn.addEventListener('click', validateDatabaseCredentials);
if (benchmarkForm) benchmarkForm.addEventListener('submit', generateScript);

// Remote transfer toggle
const enableRemoteTransfer = document.getElementById('enableRemoteTransfer');
const remoteTransferFields = document.getElementById('remoteTransferFields');
const testSshBtn = document.getElementById('testSshBtn');

if (enableRemoteTransfer) {
    enableRemoteTransfer.addEventListener('change', function() {
        remoteTransferFields.style.display = this.checked ? 'block' : 'none';
        updateGenerateButtonText();
    });
}

if (testSshBtn) {
    testSshBtn.addEventListener('click', testSSHConnection);
}

function updateGenerateButtonText() {
    const hint = document.getElementById('generateBtnHint');
    if (enableRemoteTransfer && hint) {
        if (enableRemoteTransfer.checked) {
            hint.textContent = 'Generate and transfer to remote cluster';
        } else {
            hint.textContent = 'Generate and optionally transfer to remote cluster';
        }
    }
}

/**
 * Update benchmark mode indicator based on operation count and timeout values
 */
function updateBenchmarkModeIndicator() {
    const operationCount = parseInt(document.getElementById('operationCount').value) || 0;
    const timeout = parseInt(document.getElementById('timeout').value) || 0;
    const indicator = document.getElementById('benchmarkModeIndicator');
    const modeText = document.getElementById('benchmarkModeText');
    
    if (!indicator || !modeText) return; // Elements not yet loaded
    
    // Determine mode and set appropriate message
    if (operationCount === 0 && timeout === 0) {
        // Invalid mode
        indicator.className = 'alert alert-danger';
        indicator.style.display = 'block';
        modeText.innerHTML = `
            <strong>❌ Invalid Configuration</strong><br>
            At least one limit must be set. Either Operation Count or Timeout must be greater than 0.
        `;
    } else if (operationCount === 0 && timeout > 0) {
        // Time-based mode
        indicator.className = 'alert alert-success';
        indicator.style.display = 'block';
        const hours = Math.floor(timeout / 3600);
        const minutes = Math.floor((timeout % 3600) / 60);
        const timeStr = hours > 0 ? `${hours}h ${minutes}m` : `${minutes}m`;
        modeText.innerHTML = `
            <strong>⏱️ Time-Based Benchmark</strong><br>
            Will run for <strong>${timeStr}</strong> (${timeout.toLocaleString()} seconds) with unlimited operations.
            YCSB will perform as many operations as possible within the time limit.
        `;
    } else if (operationCount > 0 && timeout === 0) {
        // Count-based mode
        indicator.className = 'alert alert-success';
        indicator.style.display = 'block';
        modeText.innerHTML = `
            <strong>🔢 Count-Based Benchmark</strong><br>
            Will run until <strong>${operationCount.toLocaleString()} operations</strong> complete, regardless of time.
            YCSB will run as long as needed to complete all operations.
        `;
    } else if (operationCount > 0 && timeout > 0) {
        // Dual-limit mode
        indicator.className = 'alert alert-info';
        indicator.style.display = 'block';
        const hours = Math.floor(timeout / 3600);
        const minutes = Math.floor((timeout % 3600) / 60);
        const timeStr = hours > 0 ? `${hours}h ${minutes}m` : `${minutes}m`;
        modeText.innerHTML = `
            <strong>⚖️ Dual-Limit Benchmark</strong><br>
            Will stop when <strong>either</strong> limit is reached first:<br>
            • <strong>${operationCount.toLocaleString()} operations</strong>, OR<br>
            • <strong>${timeStr}</strong> (${timeout.toLocaleString()} seconds)<br>
            <small class="text-muted">Whichever completes first will end the benchmark phase.</small>
        `;
    }
}

// Add event listeners for real-time updates
document.addEventListener('DOMContentLoaded', function() {
    const operationCountField = document.getElementById('operationCount');
    const timeoutField = document.getElementById('timeout');
    
    if (operationCountField) {
        operationCountField.addEventListener('input', updateBenchmarkModeIndicator);
    }
    if (timeoutField) {
        timeoutField.addEventListener('input', updateBenchmarkModeIndicator);
    }
    
    // Initialize on page load
    updateBenchmarkModeIndicator();
});

// Engine checkbox listeners - show/hide type options
document.querySelectorAll('.engine-checkbox').forEach(checkbox => {
    checkbox.addEventListener('change', function() {
        const engine = this.value; // e.g., 'postgresql', 'mongodb', 'mysql', 'oracle', 'mssql'
        
        // Map engine value to div ID (postgresql -> postgres, others stay the same)
        const engineId = engine === 'postgresql' ? 'postgres' : engine;
        const typesDiv = document.getElementById(`${engineId}Types`);
        
        if (typesDiv) {
            typesDiv.style.display = this.checked ? 'block' : 'none';
            
            // Uncheck all type checkboxes if engine unchecked
            if (!this.checked) {
                typesDiv.querySelectorAll('input[type="checkbox"]').forEach(cb => {
                    cb.checked = false;
                });
            }
        }
        
        updateValidateButton();
        resetValidation();
    });
});

// Type checkbox listeners - update validate button
document.querySelectorAll('[id$="SI"], [id$="HA"], [id$="RS"], [id$="Sharded"], [id$="SIHA"], [id$="RAC"], [id$="AAG"]').forEach(checkbox => {
    checkbox.addEventListener('change', function() {
        updateValidateButton();
        resetValidation();
    });
});

/**
 * Validate NDB connection and fetch databases
 */
async function validateNDBConnection() {
    // Get form values
    const ndbIp = document.getElementById('ndbIp').value.trim();
    const ndbPort = parseInt(document.getElementById('ndbPort').value);
    const ndbUsername = document.getElementById('ndbUsername').value.trim();
    const ndbPassword = document.getElementById('ndbPassword').value;
    const verifySsl = document.getElementById('verifySsl').checked;

    // Validate inputs
    if (!ndbIp || !ndbUsername || !ndbPassword) {
        showValidationStatus('error', 'Please fill in all NDB connection fields');
        return;
    }

    // Get selected engine types
    const engineSelections = getSelectedEngineTypes();
    if (engineSelections.length === 0) {
        showValidationStatus('error', 'Please select at least one database engine and type');
        return;
    }

    // Show loading state
    validateBtn.disabled = true;
    validateBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Validating...';
    showValidationStatus('info', 'Connecting to NDB...');

    try {
        const response = await fetch('/api/validate-ndb-with-types', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                ndb_connection: {
                    ip: ndbIp,
                    port: ndbPort,
                    username: ndbUsername,
                    password: ndbPassword,
                    verify_ssl: verifySsl
                },
                engine_selections: engineSelections
            })
        });

        const data = await response.json();

        if (data.success) {
            isValidated = true;
            validatedDatabases = data.databases;
            selectedEnginesWithDatabases = data.databases;
            showValidationStatus('success', data.message);
            displayDatabasesByType(data.databases);
            
            // Get unique engines for credential fields from the databases response
            const engines = Object.keys(data.databases);
            console.log('Engines found:', engines); // Debug log
            displayDatabaseCredentialFields(engines);
        } else {
            isValidated = false;
            validatedDatabases = null;
            selectedEnginesWithDatabases = {};
            showValidationStatus('error', data.message);
            hideDatabases();
            hideDbCredentials();
        }
    } catch (error) {
        isValidated = false;
        validatedDatabases = null;
        selectedEnginesWithDatabases = {};
        showValidationStatus('error', `Connection error: ${error.message}`);
        hideDatabases();
        hideDbCredentials();
    } finally {
        validateBtn.disabled = false;
        validateBtn.innerHTML = '<i class="bi bi-check-circle"></i> Validate NDB Connection & Fetch Databases';
    }
}

/**
 * Validate database credentials
 */
async function validateDatabaseCredentials() {
    // Check if NDB validation was done
    if (!isValidated || !validatedDatabases) {
        showDbCredValidationStatus('error', 'Please validate NDB connection first');
        return;
    }

    // Check if any databases are selected
    if (Object.keys(selectedEnginesWithDatabases).length === 0) {
        showDbCredValidationStatus('error', 'Please select at least one database to validate');
        return;
    }

    // Get database credentials
    const dbCredentials = {};
    const engines = Object.keys(selectedEnginesWithDatabases);
    
    for (const engine of engines) {
        const username = document.getElementById(`${engine}Username`)?.value.trim();
        const password = document.getElementById(`${engine}Password`)?.value;
        
        if (!username || !password) {
            showDbCredValidationStatus('error', `Please fill in credentials for ${engine}`);
            return;
        }
        
        dbCredentials[engine] = { username, password };
    }

    // Show loading state
    validateDbCredsBtn.disabled = true;
    validateDbCredsBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Validating...';
    showDbCredValidationStatus('info', 'Testing database connections...');

    try {
        const response = await fetch('/api/validate-db-credentials', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                databases: selectedEnginesWithDatabases,
                db_credentials: dbCredentials,
                test_connectivity_only: false
            })
        });

        const data = await response.json();

        if (data.success) {
            isDbCredsValidated = true;
            showDbCredValidationStatus('success', data.message);
            
            // Show detailed results
            displayDbCredValidationResults(data.results);
        } else {
            isDbCredsValidated = false;
            showDbCredValidationStatus('error', data.message);
            
            // Show detailed results if available
            if (data.results) {
                displayDbCredValidationResults(data.results);
            }
        }
    } catch (error) {
        isDbCredsValidated = false;
        showDbCredValidationStatus('error', `Validation error: ${error.message}`);
    } finally {
        validateDbCredsBtn.disabled = false;
        validateDbCredsBtn.innerHTML = '<i class="bi bi-shield-check"></i> Validate Database Credentials';
    }
}

/**
 * Display database credential validation results
 */
function displayDbCredValidationResults(results) {
    let html = '<div class="mt-3"><h6>Validation Results:</h6>';
    
    for (const [engine, engineResults] of Object.entries(results)) {
        html += `<div class="card mb-2">
            <div class="card-header bg-light">
                <strong>${engine.toUpperCase()}</strong>
            </div>
            <div class="card-body p-2">
                <div class="table-responsive">
                    <table class="table table-sm mb-0">
                        <thead>
                            <tr>
                                <th>Database</th>
                                <th>Host:Port</th>
                                <th>Status</th>
                                <th>Message</th>
                            </tr>
                        </thead>
                        <tbody>`;
        
        for (const result of engineResults) {
            const statusIcon = result.success 
                ? '<i class="bi bi-check-circle-fill text-success"></i>' 
                : '<i class="bi bi-x-circle-fill text-danger"></i>';
            const statusClass = result.success ? 'text-success' : 'text-danger';
            
            html += `
                <tr>
                    <td>${result.db_name}</td>
                    <td>${result.host}:${result.port}</td>
                    <td>${statusIcon}</td>
                    <td class="${statusClass}">${result.message}</td>
                </tr>`;
        }
        
        html += `
                        </tbody>
                    </table>
                </div>
            </div>
        </div>`;
    }
    
    html += '</div>';
    
    // Append results after credentials content
    const existingResults = document.getElementById('dbCredValidationResults');
    if (existingResults) {
        existingResults.innerHTML = html;
    } else {
        const resultsDiv = document.createElement('div');
        resultsDiv.id = 'dbCredValidationResults';
        resultsDiv.innerHTML = html;
        
        // Find the button row and insert before it
        const buttonRow = validateDbCredsBtn.closest('.row');
        buttonRow.parentNode.insertBefore(resultsDiv, buttonRow);
    }
}

/**
 * Show database credential validation status
 */
function showDbCredValidationStatus(type, message) {
    if (!message) {
        dbCredValidationStatus.innerHTML = '';
        return;
    }
    
    let icon = '';
    let className = '';
    
    switch(type) {
        case 'success':
            icon = '<i class="bi bi-check-circle-fill text-success"></i>';
            className = 'text-success';
            break;
        case 'error':
            icon = '<i class="bi bi-x-circle-fill text-danger"></i>';
            className = 'text-danger';
            break;
        case 'info':
            icon = '<i class="bi bi-info-circle-fill text-info"></i>';
            className = 'text-info';
            break;
    }
    
    dbCredValidationStatus.innerHTML = `${icon} <span class="${className}">${message}</span>`;
}

/**
 * Generate YCSB benchmark script
 */
async function generateScript(event) {
    event.preventDefault();

    // Validate that NDB connection was validated
    if (!isValidated) {
        alert('Please validate NDB connection first');
        return;
    }
    
    // Validate that database credentials were validated
    if (!isDbCredsValidated) {
        alert('Please validate database credentials first');
        return;
    }

    // Get form values
    const ndbIp = document.getElementById('ndbIp').value.trim();
    const ndbPort = parseInt(document.getElementById('ndbPort').value);
    const ndbUsername = document.getElementById('ndbUsername').value.trim();
    const ndbPassword = document.getElementById('ndbPassword').value;
    const verifySsl = document.getElementById('verifySsl').checked;

    const engineSelections = getSelectedEngineTypes();
    if (engineSelections.length === 0) {
        alert('Please select at least one database engine and type');
        return;
    }
    
    // Add selected databases to engineSelections
    let hasSelectedDatabases = false;
    for (const selection of engineSelections) {
        const engine = selection.engine;
        const selectedDbsForEngine = [];
        
        for (const type of selection.types) {
            const checkedBoxes = document.querySelectorAll(`.db-checkbox[data-engine="${engine}"][data-type="${type}"]:checked`);
            checkedBoxes.forEach(cb => selectedDbsForEngine.push(cb.value));
        }
        
        if (selectedDbsForEngine.length > 0) {
            selection.selected_databases = selectedDbsForEngine;
            hasSelectedDatabases = true;
        }
    }
    
    if (!hasSelectedDatabases) {
        alert('Please select at least one database instance to benchmark');
        return;
    }

    // Get database credentials for each engine
    const dbCredentials = {};
    for (const selection of engineSelections) {
        if (!selection.selected_databases || selection.selected_databases.length === 0) {
            continue; // Skip engines with no selected databases
        }
        
        const engine = selection.engine;
        const username = document.getElementById(`${engine}Username`)?.value;
        const password = document.getElementById(`${engine}Password`)?.value;
        
        if (!username || !password) {
            alert(`Please provide credentials for ${engine.toUpperCase()}`);
            return;
        }
        
        dbCredentials[engine] = {
            username: username,
            password: password
        };
    }

    // Get YCSB parameters
    const recordCount = parseInt(document.getElementById('recordCount').value);
    const operationCount = parseInt(document.getElementById('operationCount').value);
    const timeout = parseInt(document.getElementById('timeout').value);
    
    // Validate at least one limit is set
    if (operationCount === 0 && timeout === 0) {
        alert('❌ Invalid Configuration\n\nEither Operation Count or Timeout must be greater than 0.\nYou cannot set both to 0.\n\nPlease set at least one limit for the benchmark.');
        return;
    }
    
    // Validate Java Integer limits (only if operationCount > 0)
    const MAX_JAVA_INT = 2147483647;
    if (recordCount > MAX_JAVA_INT) {
        alert(`Record Count exceeds Java Integer limit (${MAX_JAVA_INT.toLocaleString()}).\n\nFor large datasets, use a lower record count and increase the Timeout value instead.`);
        return;
    }
    if (operationCount > 0 && operationCount > MAX_JAVA_INT) {
        alert(`Operation Count exceeds Java Integer limit (${MAX_JAVA_INT.toLocaleString()}).\n\nFor long-running benchmarks, use a lower operation count and increase the Timeout value instead.`);
        return;
    }
    
    const ycsbParams = {
        phase: document.getElementById('phase').value,
        workload_type: document.getElementById('workloadType').value,
        record_count: recordCount,
        operation_count: operationCount,
        thread_count: parseInt(document.getElementById('threadCount').value),
        load_target_throughput: parseInt(document.getElementById('loadTargetThroughput').value),
        run_target_throughput: parseInt(document.getElementById('runTargetThroughput').value),
        retry_limit: parseInt(document.getElementById('retryLimit').value),
        retry_interval: parseInt(document.getElementById('retryInterval').value),
        timeout: timeout
    };

    // Add optional proportions if specified
    const readProportion = document.getElementById('readProportion').value;
    const updateProportion = document.getElementById('updateProportion').value;
    const insertProportion = document.getElementById('insertProportion').value;
    const scanProportion = document.getElementById('scanProportion').value;

    // Validate proportions sum to 1.0 if any are specified
    const proportions = [];
    if (readProportion) {
        const val = parseFloat(readProportion);
        ycsbParams.read_proportion = val;
        proportions.push(val);
    }
    if (updateProportion) {
        const val = parseFloat(updateProportion);
        ycsbParams.update_proportion = val;
        proportions.push(val);
    }
    if (insertProportion) {
        const val = parseFloat(insertProportion);
        ycsbParams.insert_proportion = val;
        proportions.push(val);
    }
    if (scanProportion) {
        const val = parseFloat(scanProportion);
        ycsbParams.scan_proportion = val;
        proportions.push(val);
    }
    
    // If any proportions are specified, validate they sum to 1.0
    if (proportions.length > 0) {
        const sum = proportions.reduce((a, b) => a + b, 0);
        if (Math.abs(sum - 1.0) > 0.001) {
            alert(`❌ Invalid Workload Proportions\n\nProportions must sum to 1.0\nCurrent sum: ${sum.toFixed(3)}\n\nPlease adjust your read/update/insert/scan proportions so they total exactly 1.0`);
            return;
        }
    }

    // Get remote transfer configuration
    let remoteTransfer = null;
    if (enableRemoteTransfer.checked) {
        const remoteHost = document.getElementById('remoteHost').value.trim();
        const remoteUsername = document.getElementById('remoteUsername').value.trim();
        const remotePassword = document.getElementById('remotePassword').value;
        const remoteFolder = document.getElementById('remoteFolder').value.trim();
        const remotePort = parseInt(document.getElementById('remotePort').value);
        
        if (!remoteHost || !remoteUsername || !remotePassword) {
            alert('Please fill in all required remote transfer fields (Host, Username, Password)');
            return;
        }
        
        remoteTransfer = {
            enabled: true,
            host: remoteHost,
            username: remoteUsername,
            password: remotePassword,
            target_folder: remoteFolder,
            port: remotePort
        };
    }

    // Show loading state
    generateBtn.disabled = true;
    const loadingText = remoteTransfer ? 'Generating & Transferring...' : 'Generating Script...';
    generateBtn.innerHTML = `<span class="spinner-border spinner-border-sm me-2"></span>${loadingText}`;

    try {
        const requestBody = {
            ndb_connection: {
                ip: ndbIp,
                port: ndbPort,
                username: ndbUsername,
                password: ndbPassword,
                verify_ssl: verifySsl
            },
            engine_selections: engineSelections,
            db_credentials: dbCredentials,
            ycsb_params: ycsbParams
        };
        
        if (remoteTransfer) {
            requestBody.remote_transfer = remoteTransfer;
        }
        
        const response = await fetch('/api/generate-script', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestBody)
        });

        const data = await response.json();

        if (data.success) {
            // Show results section
            scriptName.textContent = data.script_name;
            scriptNameCmd.textContent = data.script_name;
            downloadLink.href = `/api/download-script/${data.script_id}/${data.script_name}`;
            resultsSection.style.display = 'block';
            
            // Show remote transfer status if applicable
            if (data.remote_transfer_success !== null) {
                const transferAlert = document.createElement('div');
                transferAlert.className = data.remote_transfer_success ? 'alert alert-success mt-3' : 'alert alert-warning mt-3';
                transferAlert.innerHTML = `
                    <h6><i class="bi bi-${data.remote_transfer_success ? 'check-circle' : 'exclamation-triangle'}"></i> Remote Transfer ${data.remote_transfer_success ? 'Successful' : 'Failed'}</h6>
                    <p class="mb-0">${data.remote_transfer_message}</p>
                `;
                
                // Insert after the success alert
                const successAlert = resultsSection.querySelector('.alert-success');
                if (successAlert) {
                    successAlert.parentNode.insertBefore(transferAlert, successAlert.nextSibling);
                }
            }
            
            // Scroll to results
            resultsSection.scrollIntoView({ behavior: 'smooth' });
        } else {
            alert(`Script generation failed: ${data.message}`);
        }
    } catch (error) {
        alert(`Error generating script: ${error.message}`);
    } finally {
        generateBtn.disabled = false;
        generateBtn.innerHTML = '<i class="bi bi-file-earmark-code"></i> Generate YCSB Script';
    }
}

/**
 * Test SSH connection to remote host
 */
async function testSSHConnection() {
    const host = document.getElementById('remoteHost').value.trim();
    const username = document.getElementById('remoteUsername').value.trim();
    const password = document.getElementById('remotePassword').value;
    const port = parseInt(document.getElementById('remotePort').value);
    
    if (!host || !username || !password) {
        showSSHTestResult(false, 'Please fill in all required fields (Host, Username, Password)');
        return;
    }
    
    testSshBtn.disabled = true;
    testSshBtn.innerHTML = '<i class="bi bi-hourglass-split"></i> Testing...';
    
    try {
        const response = await fetch('/api/test-ssh-connection', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                enabled: true,
                host: host,
                username: username,
                password: password,
                port: port,
                target_folder: '/tmp'
            })
        });
        
        const data = await response.json();
        showSSHTestResult(data.success, data.message);
        
    } catch (error) {
        showSSHTestResult(false, `Connection test error: ${error.message}`);
    } finally {
        testSshBtn.disabled = false;
        testSshBtn.innerHTML = '<i class="bi bi-plug"></i> Test Connection';
    }
}

/**
 * Show SSH test result
 */
function showSSHTestResult(success, message) {
    const resultDiv = document.getElementById('sshTestResult');
    resultDiv.style.display = 'block';
    resultDiv.className = success ? 'alert alert-success' : 'alert alert-danger';
    resultDiv.innerHTML = `<i class="bi bi-${success ? 'check-circle' : 'x-circle'}"></i> ${message}`;
}

/**
 * Get selected database engines
 */
function getSelectedEngines() {
    const checkboxes = document.querySelectorAll('.engine-checkbox:checked');
    return Array.from(checkboxes).map(cb => cb.value);
}

/**
 * Get selected engine types
 */
function getSelectedEngineTypes() {
    const selections = [];
    
    // PostgreSQL
    if (document.getElementById('enginePostgres')?.checked) {
        const types = [];
        if (document.getElementById('postgresSI')?.checked) types.push('si');
        if (document.getElementById('postgresHA')?.checked) types.push('ha');
        if (types.length > 0) {
            selections.push({ engine: 'postgresql', types: types });
        }
    }
    
    // MongoDB
    if (document.getElementById('engineMongodb')?.checked) {
        const types = [];
        if (document.getElementById('mongodbSI')?.checked) types.push('si');
        if (document.getElementById('mongodbRS')?.checked) types.push('replicaset');
        if (document.getElementById('mongodbSharded')?.checked) types.push('sharded');
        if (types.length > 0) {
            selections.push({ engine: 'mongodb', types: types });
        }
    }
    
    // MySQL
    if (document.getElementById('engineMysql')?.checked) {
        const types = [];
        if (document.getElementById('mysqlSI')?.checked) types.push('si');
        if (document.getElementById('mysqlHA')?.checked) types.push('ha');
        if (types.length > 0) {
            selections.push({ engine: 'mysql', types: types });
        }
    }
    
    // Oracle
    if (document.getElementById('engineOracle')?.checked) {
        const types = [];
        if (document.getElementById('oracleSI')?.checked) types.push('si');
        if (document.getElementById('oracleSIHA')?.checked) types.push('siha');
        if (document.getElementById('oracleRAC')?.checked) types.push('rac');
        if (types.length > 0) {
            selections.push({ engine: 'oracle', types: types });
        }
    }
    
    // MS SQL Server
    if (document.getElementById('engineMssql')?.checked) {
        const types = [];
        if (document.getElementById('mssqlSI')?.checked) types.push('si');
        if (document.getElementById('mssqlAAG')?.checked) types.push('aag');
        if (types.length > 0) {
            selections.push({ engine: 'mssql', types: types });
        }
    }
    
    return selections;
}

/**
 * Update validate button state
 */
function updateValidateButton() {
    const selections = getSelectedEngineTypes();
    const hasSelections = selections.length > 0;
    validateBtn.disabled = !hasSelections;
}

/**
 * Reset validation state
 */
function resetValidation() {
    isValidated = false;
    isDbCredsValidated = false;
    validatedDatabases = null;
    selectedEnginesWithDatabases = {};
    hideDatabases();
    hideDbCredentials();
    validationStatus.innerHTML = '';
    if (dbCredValidationStatus) {
        dbCredValidationStatus.innerHTML = '';
    }
}

/**
 * Display validation status message
 */
function showValidationStatus(type, message) {
    const icons = {
        success: 'bi-check-circle-fill text-success',
        error: 'bi-x-circle-fill text-danger',
        info: 'bi-info-circle-fill text-primary'
    };

    validationStatus.innerHTML = `
        <i class="bi ${icons[type]} me-2"></i>
        <span class="${type === 'success' ? 'text-success' : type === 'error' ? 'text-danger' : 'text-primary'}">
            ${message}
        </span>
    `;
}

/**
 * Display available databases grouped by type
 */
function displayDatabasesByType(databases) {
    let html = '';
    let totalDatabases = 0;

    const typeLabels = {
        'si': 'Single Instance',
        'ha': 'High Availability',
        'replicaset': 'Replica Set',
        'sharded': 'Sharded Cluster',
        'siha': 'SI with HA',
        'rac': 'Real Application Clusters',
        'aag': 'Always On Availability Group'
    };

    for (const [engine, typeGroups] of Object.entries(databases)) {
        for (const [type, dbList] of Object.entries(typeGroups)) {
            if (dbList.length > 0) {
                totalDatabases += dbList.length;
                const typeLabel = typeLabels[type] || type.toUpperCase();
                
                html += `
                    <div class="mb-3">
                        <h6 class="text-primary">
                            <i class="bi bi-database-fill"></i> ${engine.toUpperCase()} - ${typeLabel}
                            <span class="badge bg-primary">${dbList.length} instance(s)</span>
                        </h6>
                        <div class="table-responsive">
                            <table class="table table-sm table-striped">
                                <thead>
                                    <tr>
                                        <th>
                                            <div class="form-check">
                                                <input class="form-check-input select-all-dbs" type="checkbox" data-engine="${engine}" data-type="${type}" checked>
                                            </div>
                                        </th>
                                        <th>Name</th>
                                        <th>Status</th>
                                        <th>Version</th>
                                        <th>Primary IP</th>
                                        <th>Port</th>
                                        <th>Cluster</th>
                                    </tr>
                                </thead>
                                <tbody>
                `;

                for (const db of dbList) {
                    const statusBadge = db.status === 'READY' ? 'badge bg-success' : 'badge bg-warning';
                    const clusterBadge = db.is_cluster ? '<span class="badge bg-info">Cluster</span>' : '<span class="badge bg-secondary">Single</span>';
                    
                    html += `
                        <tr>
                            <td>
                                <div class="form-check">
                                    <input class="form-check-input db-checkbox" type="checkbox" value="${db.id}" data-engine="${engine}" data-type="${type}" checked>
                                </div>
                            </td>
                            <td>${db.name}</td>
                            <td><span class="${statusBadge}">${db.status}</span></td>
                            <td>${db.engine_version}</td>
                            <td><strong>${db.primary_ip}</strong></td>
                            <td>${db.port}</td>
                            <td>${clusterBadge}</td>
                        </tr>
                    `;
                }

                html += `
                                </tbody>
                            </table>
                        </div>
                    </div>
                `;
            }
        }
    }

    if (html === '') {
        html = '<p class="text-muted">No databases found for selected engine types.</p>';
    } else {
        html = `<div class="alert alert-info"><strong>Total: ${totalDatabases} database(s) found</strong> - All will be benchmarked in parallel</div>` + html;
    }

    databasesContent.innerHTML = html;
    databasesList.style.display = 'block';
    
    // Add event listeners for checkboxes
    document.querySelectorAll('.select-all-dbs').forEach(selectAll => {
        selectAll.addEventListener('change', function() {
            const engine = this.dataset.engine;
            const type = this.dataset.type;
            const isChecked = this.checked;
            
            document.querySelectorAll(`.db-checkbox[data-engine="${engine}"][data-type="${type}"]`).forEach(cb => {
                cb.checked = isChecked;
            });
            
            updateSelectedDatabases();
        });
    });
    
    document.querySelectorAll('.db-checkbox').forEach(cb => {
        cb.addEventListener('change', function() {
            const engine = this.dataset.engine;
            const type = this.dataset.type;
            
            const allChecked = Array.from(document.querySelectorAll(`.db-checkbox[data-engine="${engine}"][data-type="${type}"]`))
                .every(c => c.checked);
                
            const selectAll = document.querySelector(`.select-all-dbs[data-engine="${engine}"][data-type="${type}"]`);
            if (selectAll) {
                selectAll.checked = allChecked;
            }
            
            updateSelectedDatabases();
        });
    });
    
    // Initialize selected databases
    updateSelectedDatabases();
}

/**
 * Update selected databases and UI state based on checkboxes
 */
function updateSelectedDatabases() {
    if (!validatedDatabases) return;
    
    // Clone validatedDatabases structure to store only selected ones
    selectedEnginesWithDatabases = {};
    let hasSelectedDatabases = false;
    const enginesWithSelections = new Set();
    
    for (const [engine, typeGroups] of Object.entries(validatedDatabases)) {
        for (const [type, dbList] of Object.entries(typeGroups)) {
            // Find checked checkboxes for this engine and type
            const checkedBoxes = Array.from(document.querySelectorAll(`.db-checkbox[data-engine="${engine}"][data-type="${type}"]:checked`));
            const checkedIds = new Set(checkedBoxes.map(cb => cb.value));
            
            if (checkedIds.size > 0) {
                hasSelectedDatabases = true;
                enginesWithSelections.add(engine);
                
                if (!selectedEnginesWithDatabases[engine]) {
                    selectedEnginesWithDatabases[engine] = {};
                }
                
                // Filter original dbList to only include selected ones
                selectedEnginesWithDatabases[engine][type] = dbList.filter(db => checkedIds.has(db.id));
            }
        }
    }
    
    // Update UI based on selections
    if (hasSelectedDatabases) {
        displayDatabaseCredentialFields(Array.from(enginesWithSelections));
    } else {
        hideDbCredentials();
    }
    
    // Reset credential validation state if selections changed
    if (isDbCredsValidated) {
        isDbCredsValidated = false;
        showDbCredValidationStatus('', '');
    }
}

/**
 * Hide databases list
 */
function hideDatabases() {
    databasesList.style.display = 'none';
    databasesContent.innerHTML = '';
}

/**
 * Display database credential input fields
 */
function displayDatabaseCredentialFields(engines) {
    console.log('displayDatabaseCredentialFields called with:', engines); // Debug log
    
    if (!engines || engines.length === 0) {
        console.log('No engines, hiding credentials section'); // Debug log
        hideDbCredentials();
        return;
    }

    let html = '';
    
    const engineLabels = {
        'postgresql': 'PostgreSQL',
        'mongodb': 'MongoDB',
        'mssql': 'MS SQL Server',
        'oracle': 'Oracle',
        'mysql': 'MySQL'
    };

    for (const engine of engines) {
        const label = engineLabels[engine] || engine.toUpperCase();
        html += `
            <div class="card mb-3">
                <div class="card-header bg-light">
                    <h6 class="mb-0"><i class="bi bi-database-fill"></i> ${label} Credentials</h6>
                </div>
                <div class="card-body">
                    <div class="row">
                        <div class="col-md-6">
                            <div class="mb-3">
                                <label for="${engine}Username" class="form-label">Username *</label>
                                <input type="text" class="form-control" id="${engine}Username" 
                                       placeholder="Database username" required>
                                <small class="form-text text-muted">Will be prompted when running the script</small>
                            </div>
                        </div>
                        <div class="col-md-6">
                            <div class="mb-3">
                                <label for="${engine}Password" class="form-label">Password *</label>
                                <input type="password" class="form-control" id="${engine}Password" 
                                       placeholder="Database password" required>
                                <small class="form-text text-muted">Stored securely in generated script</small>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    dbCredentialsContent.innerHTML = html;
    dbCredentialsSection.style.display = 'block';
    
    // Add input listeners to enable validate button when all fields filled
    addCredentialInputListeners();
    
    console.log('Credentials section displayed'); // Debug log
}

/**
 * Add input listeners to credential fields
 */
function addCredentialInputListeners() {
    const credentialInputs = dbCredentialsContent.querySelectorAll('input');
    credentialInputs.forEach(input => {
        input.addEventListener('input', checkCredentialFieldsFilled);
    });
    checkCredentialFieldsFilled();
}

/**
 * Check if all credential fields are filled
 */
function checkCredentialFieldsFilled() {
    const credentialInputs = dbCredentialsContent.querySelectorAll('input');
    const allFilled = Array.from(credentialInputs).every(input => input.value.trim() !== '');
    validateDbCredsBtn.disabled = !allFilled;
    
    // Reset validation state when credentials change
    if (isDbCredsValidated) {
        isDbCredsValidated = false;
        showDbCredValidationStatus('', '');
    }
}

/**
 * Hide database credentials section
 */
function hideDbCredentials() {
    dbCredentialsSection.style.display = 'none';
    dbCredentialsContent.innerHTML = '';
}

/**
 * IP address validation
 */
function validateIPAddress(ip) {
    const ipPattern = /^(\d{1,3}\.){3}\d{1,3}$/;
    if (!ipPattern.test(ip)) return false;
    
    const parts = ip.split('.');
    return parts.every(part => {
        const num = parseInt(part);
        return num >= 0 && num <= 255;
    });
}

// Add real-time IP validation
document.getElementById('ndbIp').addEventListener('blur', function() {
    const ip = this.value.trim();
    if (ip && !validateIPAddress(ip)) {
        this.classList.add('is-invalid');
    } else {
        this.classList.remove('is-invalid');
    }
});

// Reset validation when engine selection changes
document.querySelectorAll('.engine-checkbox').forEach(checkbox => {
    checkbox.addEventListener('change', function() {
        isValidated = false;
        validatedDatabases = null;
        selectedEnginesWithDatabases = {};
        hideDatabases();
        hideDbCredentials();
        validationStatus.innerHTML = '';
    });
});
