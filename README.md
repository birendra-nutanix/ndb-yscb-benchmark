# NDB YCSB Benchmark & Health Monitor

A comprehensive web-based platform for YCSB benchmark generation and NDB environment health monitoring.

## ⚡ Version 3.4.2 - Separated Dashboards!

**🎉 NEW in v3.4.2**: 
- ✅ **Separated Dashboards** - Dedicated routes for YCSB Generator (`/`) and NDB Health (`/ndb-health`)
- ✅ **Server-Side Routing** - Cleaner navigation with bookmarkable URLs
- ✅ **Improved Performance** - Only active dashboard loads and executes

**Version 3.4.0**: 
- ✅ **NDB Health Dashboard** - Monitor database health, alerts, and operations
- ✅ **Multi-Dashboard UI** - Sidebar navigation for YCSB Generator and NDB Health
- ✅ **Alerts Monitoring** - Track NDB alerts with time filters (1-30 days)
- ✅ **Operations Tracking** - Visualize successful/failed operations with charts
- ✅ **Diagnostic Bundle Collection** - Collect and upload NDB diagnostic bundles
- ✅ **Auto Cleanup** - Local files deleted after successful remote transfer

**Version 3.2.3**: Enhanced workload control with proportion validation

**Version 3.2**: Automatic remote transfer and deployment via SSH

**Version 3.0**: Shell script generation (no Python required on target VM)

## Features

### YCSB Benchmark Generator
- **Web UI**: Modern, responsive interface built with FastAPI and Bootstrap
- **NDB Integration**: Validates connection and discovers databases via NDB REST API
- **Multi-Engine Support**: PostgreSQL, MongoDB, MS SQL Server, Oracle, and MySQL
- **Type-Based Selection**: Choose specific deployment types (SI, HA, RAC, ReplicaSet, Sharded, AAG)
- **Credential Validation**: Test database connections before generating scripts
- **Remote Transfer**: Automatically transfer and extract scripts to remote hosts via SSH/SCP
- **SSH Connection Test**: Validate remote host connectivity before generation
- **Parallel Execution**: Master script launches all benchmarks using nohup and background processes
- **Per-Database Logging**: Each database gets its own log directory in /tmp
- **Flexible Execution**: Support for operationCount, maxExecutionTime, or both
- **ZIP Package**: Download complete package with scripts, launcher, and README
- **No Python Required**: Shell scripts run on any Unix/Linux with just YCSB installed

### NDB Health Dashboard ⭐ NEW
- **Database Overview**: Real-time count of databases by engine type
- **Alerts Monitoring**: View and track NDB alerts with time filters (1, 3, 7, 14, 30 days)
- **Operations Tracking**: Monitor successful and failed operations with visual charts
- **Diagnostic Bundle Collection**: Collect NDB diagnostic bundles and upload to file server
- **Time-based Filtering**: Consistent time filters across all monitoring features
- **Visual Analytics**: Chart.js integration for operation type distribution
- **Refresh Capability**: Manual refresh for real-time updates

## Architecture

```
┌─────────────┐
│  Web Browser│
└──────┬──────┘
       │ HTTP
       ▼
┌─────────────┐      ┌──────────────┐
│ FastAPI App │─────▶│  NDB REST API│
└──────┬──────┘      └──────────────┘
       │
       ▼
┌─────────────┐
│Shell Script │
│  Generator  │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ ZIP Package │
│  - master   │
│  - db1.sh   │
│  - db2.sh   │
│  - README   │
└──────┬──────┘
       │ Download
       ▼
┌─────────────┐      ┌──────────────┐
│  Target VM  │─────▶│     YCSB     │
│  (Extract)  │      │  Benchmarks  │
└─────────────┘      └──────┬───────┘
                            │
                            ▼
                     ┌──────────────┐
                     │/tmp/ycsb_*/  │
                     │  Log Files   │
                     └──────────────┘
```

## Prerequisites

### For Running the Web Application

- Python 3.8 or higher
- pip (Python package manager)

### For Running Generated Scripts (Target VM)

- **YCSB** installed at `/ycsb/bin/ycsb`
  - Download from: https://github.com/brianfrankcooper/YCSB/releases
  - Extract to `/ycsb` directory
- **JDBC drivers** for target databases (must be in YCSB classpath)
  - PostgreSQL: `postgresql-*.jar`
  - MySQL: `mysql-connector-java-*.jar`
  - Oracle: `ojdbc*.jar`
  - MS SQL Server: `mssql-jdbc-*.jar`
- **Bash shell** (standard on all Unix/Linux systems)
- **Network access** to all database hosts
- **Write permissions** to `/tmp` directory
- **NO Python required!** (Shell scripts only)

## Installation

### 1. Clone or Download

```bash
cd /path/to/ndb_ycsb_benchmark
```

### 2. Create Virtual Environment (Recommended)

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

## Usage

### Starting the Web Application

```bash
python app.py
```

Or using uvicorn directly:

```bash
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

The application will be available at:
- **YCSB Generator**: http://localhost:8000/
- **NDB Health Dashboard**: http://localhost:8000/ndb-health

### Using the Web Interface

#### Dashboard Navigation
The application provides two main dashboards accessible via the sidebar:
1. **YCSB Generator** (`/`) - Generate YCSB benchmark scripts
2. **NDB Health** (`/ndb-health`) - Monitor NDB environment health

---

#### YCSB Generator Dashboard

#### 1. NDB Connection (Section 1)
- Enter NDB IP address (e.g., `10.102.107.179`)
- Port: `8443` (default)
- Provide username and password
- SSL Verification: Check/uncheck based on your setup

#### 2. Database Engine Selection (Section 2)
- Check the database engines you want to benchmark
- Select specific types for each engine:
  - **PostgreSQL**: Single Instance (SI), High Availability (HA)
  - **MongoDB**: Single Instance (SI), ReplicaSet (RS), Sharded Cluster
  - **MySQL**: Single Instance (SI), High Availability (HA)
  - **Oracle**: Single Instance (SI), Single Instance HA (SIHA), Real Application Clusters (RAC)
  - **MS SQL Server**: Single Instance (SI), Always On Availability Groups (AAG)
- Click **"Validate NDB Connection"**
- Available databases will be displayed by engine and type

#### 3. Database Credentials (Section 3)
- Enter username and password for each selected engine
- Credentials are embedded in generated scripts (secure)
- Click **"Validate Database Credentials"**
- Validation results will show connection status for each database

#### 4. YCSB Parameters (Section 4)
- **Workload Type**: Select from A-F ⭐ NEW: Now properly honored in generated scripts!
  - A: Update heavy (50/50 read/update)
  - B: Read mostly (95/5 read/update)
  - C: Read only (100% read)
  - D: Read latest (95/5 read/insert)
  - E: Short ranges (95/5 scan/insert)
  - F: Read-modify-write
- **Record Count**: Initial data size (e.g., 1000000, max: 2,147,483,647)
- **Operation Count**: Operations per run iteration (e.g., 1000000, max: 2,147,483,647)
  - Set to 0 for unlimited (time-based execution)
- **Thread Count**: Concurrent threads per database (e.g., 10)
- **Target Throughput**: Operations per second (e.g., 1000)
- **Timeout (sec)**: Maximum execution time for each YCSB phase (default: 3600 seconds / 1 hour)
  - Set to 0 for unlimited (count-based execution)
  - LOAD phase exits when either `recordcount` OR `timeout` is reached (whichever comes first)
  - RUN phase exits when either `operationcount` OR `timeout` is reached (whichever comes first)
- **Advanced Options**: Override workload proportions (optional) ⭐ NEW: Now validated!
  - **Important**: If you specify any proportions, they must sum to exactly 1.0
  - Example: read=0.5, update=0.3, insert=0.2, scan=0.0 → Sum = 1.0 ✓
  - Leave all empty to use workload defaults
  - Custom proportions override workload file defaults

#### 5. Remote Transfer (Section 5) - Optional ⭐ NEW
- **Enable automatic transfer**: Check to enable remote deployment
- **Remote Host IP**: IP address of target cluster (e.g., `10.10.10.100`)
- **SSH Port**: SSH port number (default: `22`)
- **SSH Username**: Username for SSH authentication (e.g., `root`)
- **SSH Password**: Password for SSH authentication
- **Target Folder**: Destination folder on remote host (default: `/root/io_script`)
  - Folder will be deleted if exists and recreated
- **Test Connection**: Click to validate SSH credentials before generation
- When enabled, scripts are automatically:
  1. Generated and packaged as ZIP
  2. Transferred to remote host via SSH/SCP
  3. Extracted to target folder
  4. Ready to run immediately

See [FEATURE_REMOTE_TRANSFER.md](FEATURE_REMOTE_TRANSFER.md) for detailed documentation.

#### 6. Generate Scripts Package
- Click **"Generate YCSB Script"**
- If remote transfer is enabled: Scripts are deployed automatically
- If remote transfer is disabled: Download the ZIP package (e.g., `ycsb_benchmark_20260211_143022.zip`)

### Running Generated Scripts

#### 1. Extract the ZIP Package

```bash
# Copy to target VM
scp ycsb_benchmark_20260211_143022.zip user@target-vm:/opt/ycsb/

# SSH to target VM
ssh user@target-vm
cd /opt/ycsb/

# Extract
unzip ycsb_benchmark_20260211_143022.zip

# List contents
ls -la
# Output:
# run_all_benchmarks.sh
# ycsb_PgSiNos1dhcp02.sh
# ycsb_PgHaNos1dhcp02.sh
# ycsb_MongoSiNos1dhcp02.sh
# README.md
```

#### 2. Run All Benchmarks in Parallel

```bash
chmod +x run_all_benchmarks.sh
./run_all_benchmarks.sh
```

**Output:**
```
=========================================
YCSB Benchmark Master Launcher
=========================================
Total databases to benchmark: 3

Launching benchmark scripts...

[1/3] Started ycsb_PgSiNos1dhcp02.sh (PID: 12345)
[2/3] Started ycsb_PgHaNos1dhcp02.sh (PID: 12346)
[3/3] Started ycsb_MongoSiNos1dhcp02.sh (PID: 12347)

=========================================
All 3 benchmark scripts launched!
=========================================

Process IDs: 12345 12346 12347

To monitor logs, check the /tmp/ycsb_* directories
Each database has its own log directory with:
  - benchmark.log     : Main log with timestamps
  - load_phase.log    : Initial data load output
  - run_phase_N.log   : Run iteration outputs

To stop all benchmarks, press CTRL+C or run:
  kill 12345 12346 12347

Benchmarks are running in background...
This script will keep running to manage the processes.
Press CTRL+C to stop all benchmarks.
```

#### 3. Run Individual Database Benchmark

```bash
chmod +x ycsb_PgSiNos1dhcp02.sh
./ycsb_PgSiNos1dhcp02.sh
```

**Output:**
```
Created log directory: /tmp/ycsb_PgSiNos1dhcp02_20260211_143500
[2026-02-11 14:35:00] Starting YCSB benchmark for PgSiNos1dhcp02
[2026-02-11 14:35:00] Database: 10.120.0.17:5432/pgsinos1dhcp02
[2026-02-11 14:35:00] Workload: workloada
[2026-02-11 14:35:00] Threads: 10
[2026-02-11 14:35:00] Record Count: 1000000
[2026-02-11 14:35:00] Operation Count: 1000000
[2026-02-11 14:35:00] =========================================
[2026-02-11 14:35:00] LOAD PHASE - Loading initial data
[2026-02-11 14:35:00] =========================================
[2026-02-11 14:42:30] LOAD PHASE completed successfully
[2026-02-11 14:42:30] =========================================
[2026-02-11 14:42:30] RUN PHASE - Starting continuous benchmark
[2026-02-11 14:42:30] =========================================
[2026-02-11 14:42:30] Starting RUN iteration 1
[2026-02-11 14:52:15] RUN iteration 1 completed successfully
[2026-02-11 14:52:20] Starting RUN iteration 2
Records: 10,000
Operations: 10,000
Threads: 10

Continue? (yes/no): yes

Starting benchmarks...

================================================================================
Starting LOAD phase for 5 databases
================================================================================

[postgres_db1] Starting load phase...
[postgres_db2] Starting load phase...
[postgres_db3] Starting load phase...
[mysql_db1] Starting load phase...
[mysql_db2] Starting load phase...

[postgres_db1] load phase completed in 45.23s
  Throughput: 4523.12 ops/sec
  Avg Latency: 2.21 ms

...

HTML report generated: ycsb_results_20260211_103000.html

================================================================================
BENCHMARK COMPLETE
================================================================================
Total time: 234.56 seconds
Successful: 10/10
Report: ycsb_results_20260211_103000.html
================================================================================
```

## Configuration

### NDB API Endpoints

The application uses the following NDB REST API endpoints:

- Authentication: `POST /era/v0.9/auth/login`
- List Databases: `GET /era/v0.9/databases`

### Supported Database Engines

| Engine | NDB Type | YCSB Binding | Default Port |
|--------|----------|--------------|--------------|
| PostgreSQL | `postgres_database` | jdbc | 5432 |
| MongoDB | `mongodb_database` | mongodb | 27017 |
| MS SQL Server | `sqlserver_database` | jdbc | 1433 |
| Oracle | `oracle_database` | jdbc | 1521 |
| MySQL | `mysql_database` | jdbc | 3306 |

## Project Structure

```
ndb_ycsb_benchmark/
├── app.py                          # FastAPI main application
├── requirements.txt                # Python dependencies
├── README.md                       # This file
├── static/
│   ├── css/
│   │   └── styles.css             # Custom CSS
│   └── js/
│       └── app.js                 # Frontend JavaScript
├── templates/
│   └── index.html                 # Main UI template
├── generators/
│   ├── __init__.py
│   ├── script_generator.py        # Script template generator
│   └── ycsb_config.py             # YCSB command builder
├── validators/
│   ├── __init__.py
│   └── ndb_validator.py           # NDB connection validator
└── generated_scripts/              # Output directory
```

## API Documentation

Once the application is running, visit:

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### Key Endpoints

#### `POST /api/validate-ndb`

Validate NDB connection and fetch databases.

**Request:**
```json
{
  "ndb_connection": {
    "ip": "10.10.10.50",
    "username": "admin",
    "password": "password",
    "port": 8443,
    "verify_ssl": false
  },
  "selected_engines": ["postgresql", "mysql"]
}
```

**Response:**
```json
{
  "success": true,
  "message": "Found 5 databases across 2 engine types",
  "databases": {
    "postgresql": [...],
    "mysql": [...]
  }
}
```

#### `POST /api/generate-script`

Generate YCSB benchmark script.

**Request:**
```json
{
  "ndb_connection": {...},
  "selected_engines": ["postgresql"],
  "ycsb_params": {
    "phase": "both",
    "workload_type": "b",
    "record_count": 10000,
    "operation_count": 10000,
    "thread_count": 10,
    "target_throughput": 0,
    "retry_limit": 10,
    "retry_interval": 3,
    "timeout": 3600
  }
}
```

**Response:**
```json
{
  "success": true,
  "message": "Script generated successfully",
  "script_id": "uuid",
  "script_name": "ycsb_benchmark_20260211_103000.py"
}
```

## Database-Specific Optimizations

### MongoDB Performance Settings ⭐ NEW

Generated MongoDB scripts now include optimal YCSB parameters:

- **`mongodb.writeConcern=acknowledged`**: Ensures write operations are acknowledged by the server
- **`mongodb.readPreference=primary`**: Routes all reads to the primary node for consistency
- **`mongodb.maxConnectionPoolSize=100`**: Optimizes connection pooling for high-throughput workloads

These settings are automatically included in all MongoDB YCSB commands (both LOAD and RUN phases).

### PostgreSQL, MySQL, Oracle, MS SQL Server

Scripts automatically:
- Create the required `usertable` before LOAD phase
- Drop existing table if present
- Use appropriate JDBC drivers and connection strings
- Handle HA/cluster configurations by connecting to primary node

## Troubleshooting

### NDB Connection Issues

**Problem**: "Cannot connect to NDB"

**Solutions**:
- Verify NDB IP address and port
- Check network connectivity: `ping <ndb_ip>`
- Ensure NDB API is accessible: `curl -k https://<ndb_ip>:8443/era/v0.9/auth/login`
- Disable SSL verification if using self-signed certificates

### YCSB Execution Issues

**Problem**: "ycsb: command not found"

**Solutions**:
- Ensure YCSB is installed and in PATH
- Test: `ycsb --help`
- Add YCSB bin directory to PATH:
  ```bash
  export PATH=$PATH:/path/to/ycsb/bin
  ```

**Problem**: JDBC driver not found

**Solutions**:
- Download appropriate JDBC driver
- Place in YCSB lib directory
- Verify: `ls /path/to/ycsb/lib/*.jar`

### Generated Script Issues

**Problem**: Authentication fails

**Solutions**:
- Verify NDB credentials
- Check database credentials
- Ensure databases are in READY state

**Problem**: Timeout errors

**Solutions**:
- Increase timeout parameter
- Reduce thread count
- Reduce record/operation count for testing

## Security Considerations

- NDB credentials are **never stored** in generated scripts
- Database credentials are prompted at runtime using `getpass` (masked input)
- SSL verification can be disabled for self-signed certificates
- Generated scripts should be treated as sensitive (contain connection details)
- Recommended: Use dedicated service accounts with minimal privileges

## Performance Tips

1. **Thread Count**: Start with 10 threads per database, adjust based on results
2. **Record Count**: Use smaller values for initial testing (1000-10000)
3. **Target Throughput**: Set to 0 for maximum throughput testing
4. **Parallel Execution**: The script automatically uses multiprocessing for parallel execution
5. **Network**: Run from a host with good network connectivity to databases

## Known Limitations

- Generated scripts assume YCSB is pre-installed
- JDBC drivers must be manually installed for JDBC-based engines
- No real-time progress monitoring (check output files)
- SSL certificate validation may fail with self-signed certs (use verify_ssl=false)

## Contributing

Contributions are welcome! Please ensure:

1. Code follows PEP 8 style guidelines
2. All endpoints have proper error handling
3. UI remains responsive and accessible
4. Documentation is updated

## License

This project is provided as-is for use with Nutanix Database Service.

## Support

For issues or questions:

1. Check the Troubleshooting section
2. Review NDB API documentation
3. Verify YCSB installation and configuration
4. Check application logs for detailed error messages

## Version History

### v1.0.0 (2026-02-11)
- Initial release
- Support for 5 database engines
- Web-based script generator
- Parallel YCSB execution
- HTML report generation with charts
- NDB REST API integration

## Acknowledgments

- Built with FastAPI, Bootstrap, and Chart.js
- YCSB by Yahoo! Research
- Nutanix Database Service (NDB)


# 1. Extract the ZIP file
unzip ycsb_benchmark_20260214_153045.zip
cd ycsb_benchmark_20260214_153045/

# 2. Run the master script (ONE command starts everything)
./run_all_benchmarks.sh

# 3. Monitor (optional)
tail -f /tmp/ycsb_*/run_*.log

# 4. Stop when done (optional)
pkill -f /ycsb


operationcount=2147483647 # can be more than this number, otherwise java bufferexecption comes becaise java has int32