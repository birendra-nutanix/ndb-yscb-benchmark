"""
Shell Script Generator for YCSB Benchmarks (Refactored v2)

This module generates shell scripts for running YCSB benchmarks against
multiple database instances in parallel. Uses inheritance to eliminate code duplication.
"""

import os
from typing import Dict, List, Any
from datetime import datetime
from generators.base_script_generator import BaseYCSBScriptGenerator


class PostgreSQLScriptGenerator(BaseYCSBScriptGenerator):
    """PostgreSQL-specific script generator"""
    
    def generate_script(
        self,
        db_info: Dict[str, Any],
        db_credentials: Dict[str, str],
        ycsb_params: Dict[str, Any]
    ) -> str:
        """Generate YCSB shell script for PostgreSQL"""
        db_name = db_info['name']
        db_ip = db_info['primary_ip']
        db_port = db_info['port']
        database_name = db_info['database_name']  # Uses validated working name
        username = db_credentials['username']
        password = db_credentials['password']
        
        # Extract common parameters
        params = self._extract_ycsb_params(ycsb_params)
        log_folder = self._generate_log_folder(db_name)
        proportion_params = self._build_proportion_params(ycsb_params)
        insertstart_param = self._build_insertstart_param(ycsb_params)
        operation_count_param, max_execution_time_param, run_target_param, load_target_param = self._build_conditional_params(
            params['operation_count'], params['max_execution_time'], ycsb_params
        )
        
        # JDBC URL with keepalive to prevent idle connection drops
        jdbc_url = f"jdbc:postgresql://{db_ip}:{db_port}/{database_name}?tcpKeepAlive=true"
        
        # YCSB run command
        ycsb_run_cmd = f"""cd $YCSB_BIN
    ./bin/ycsb run jdbc -s -P workloads/$WORKLOAD -p db.driver=org.postgresql.Driver -p db.url="$JDBC_URL" -p db.user="$DB_USER" -p db.passwd="$DB_PASSWORD" -p recordcount=$RECORD_COUNT {insertstart_param} $OPERATION_COUNT_PARAM -p threadcount=$THREADS $RUN_TARGET_PARAM $MAX_EXECUTION_TIME_PARAM{proportion_params} -p core_workload_insertion_retry_limit=10 -p core_workload_insertion_retry_interval=3 > "$LOG_DIR/run_phase_$RUN_COUNTER.log" 2>&1"""
        
        script = self._generate_script_header("PostgreSQL", db_name)
        script += f"""
# Configuration
DB_HOST="{db_ip}"
DB_PORT="{db_port}"
DB_NAME="{database_name}"
DB_USER="{username}"
DB_PASSWORD="{password}"
YCSB_BIN="{self.ycsb_path}"
LOG_DIR="{log_folder}"
WORKLOAD="{params['workload']}"
RECORD_COUNT={params['record_count']}
OPERATION_COUNT={params['operation_count']}
THREADS={params['threads']}
LOAD_TARGET={params['load_target']}
RUN_TARGET={params['run_target']}
MAX_EXECUTION_TIME={params['max_execution_time']}

# Build conditional YCSB parameters
OPERATION_COUNT_PARAM="{operation_count_param}"
MAX_EXECUTION_TIME_PARAM="{max_execution_time_param}"
RUN_TARGET_PARAM="{run_target_param}"
LOAD_TARGET_PARAM="{load_target_param}"

# Create log directory
mkdir -p "$LOG_DIR"
echo "Created log directory: $LOG_DIR"

# Connection string
JDBC_URL="{jdbc_url}"
"""
        script += self._generate_log_function()
        script += f"""
log "Starting YCSB benchmark for {db_name}"
log "Database: $DB_HOST:$DB_PORT/$DB_NAME"
log "Workload: $WORKLOAD"
log "Threads: $THREADS"
log "Record Count: $RECORD_COUNT"
log "Operation Count: $OPERATION_COUNT"

# CREATE TABLE (Required by YCSB)
log "========================================="
log "Creating usertable (required by YCSB)"
log "========================================="

PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<EOF
CREATE TABLE IF NOT EXISTS usertable (
    YCSB_KEY VARCHAR(255) PRIMARY KEY,
    FIELD0 TEXT, FIELD1 TEXT, FIELD2 TEXT, FIELD3 TEXT, FIELD4 TEXT,
    FIELD5 TEXT, FIELD6 TEXT, FIELD7 TEXT, FIELD8 TEXT, FIELD9 TEXT
);
EOF

if [ $? -eq 0 ]; then
    log "Table created successfully"
else
    log "ERROR: Failed to create table"
    exit 1
fi

# LOAD PHASE
log "========================================="
log "LOAD PHASE - Loading initial data"
log "========================================="

cd $YCSB_BIN
./bin/ycsb load jdbc -s -P workloads/$WORKLOAD -p db.driver=org.postgresql.Driver -p db.url="$JDBC_URL" -p db.user="$DB_USER" -p db.passwd="$DB_PASSWORD" -p recordcount=$RECORD_COUNT -p threadcount=$THREADS $LOAD_TARGET_PARAM $MAX_EXECUTION_TIME_PARAM -p core_workload_insertion_retry_limit=10 -p core_workload_insertion_retry_interval=3 > "$LOG_DIR/load_phase.log" 2>&1

if [ $? -eq 0 ]; then
    log "LOAD PHASE completed successfully"
else
    log "ERROR: LOAD PHASE failed. Check $LOG_DIR/load_phase.log"
    exit 1
fi
"""
        script += self._generate_run_phase_loop(ycsb_run_cmd)
        return script


class MongoDBScriptGenerator(BaseYCSBScriptGenerator):
    """MongoDB-specific script generator"""
    
    def generate_script(
        self,
        db_info: Dict[str, Any],
        db_credentials: Dict[str, str],
        ycsb_params: Dict[str, Any]
    ) -> str:
        """Generate YCSB shell script for MongoDB"""
        db_name = db_info['name']
        db_ip = db_info['primary_ip']
        db_port = db_info['port']
        database_name = db_info['database_name']  # Uses validated working name
        username = db_credentials['username']
        password = db_credentials['password']
        
        params = self._extract_ycsb_params(ycsb_params)
        log_folder = self._generate_log_folder(db_name)
        proportion_params = self._build_proportion_params(ycsb_params)
        insertstart_param = self._build_insertstart_param(ycsb_params)
        operation_count_param, max_execution_time_param, run_target_param, load_target_param = self._build_conditional_params(
            params['operation_count'], params['max_execution_time'], ycsb_params
        )
        
        # MongoDB connection URL with maxIdleTimeMS to prevent idle connection drops
        mongo_url = f"mongodb://{username}:{password}@{db_ip}:{db_port}/{database_name}?authSource=admin&maxIdleTimeMS=60000"
        
        ycsb_run_cmd = f"""cd $YCSB_BIN
    ./bin/ycsb run mongodb -s -P workloads/$WORKLOAD -p mongodb.url="$MONGO_URL" -p mongodb.database="$DB_NAME" -p mongodb.writeConcern=acknowledged -p mongodb.readPreference=primary -p mongodb.maxConnectionPoolSize=100 -p recordcount=$RECORD_COUNT {insertstart_param} $OPERATION_COUNT_PARAM -p threadcount=$THREADS $RUN_TARGET_PARAM $MAX_EXECUTION_TIME_PARAM{proportion_params} -p core_workload_insertion_retry_limit=10 -p core_workload_insertion_retry_interval=3 > "$LOG_DIR/run_phase_$RUN_COUNTER.log" 2>&1"""
        
        script = self._generate_script_header("MongoDB", db_name)
        script += f"""
# Configuration
DB_HOST="{db_ip}"
DB_PORT="{db_port}"
DB_NAME="{database_name}"
DB_USER="{username}"
DB_PASSWORD="{password}"
YCSB_BIN="{self.ycsb_path}"
LOG_DIR="{log_folder}"
WORKLOAD="{params['workload']}"
RECORD_COUNT={params['record_count']}
OPERATION_COUNT={params['operation_count']}
THREADS={params['threads']}
LOAD_TARGET={params['load_target']}
RUN_TARGET={params['run_target']}
MAX_EXECUTION_TIME={params['max_execution_time']}

# Build conditional YCSB parameters
OPERATION_COUNT_PARAM="{operation_count_param}"
MAX_EXECUTION_TIME_PARAM="{max_execution_time_param}"
RUN_TARGET_PARAM="{run_target_param}"
LOAD_TARGET_PARAM="{load_target_param}"

# Create log directory
mkdir -p "$LOG_DIR"
echo "Created log directory: $LOG_DIR"

# MongoDB connection URL
MONGO_URL="{mongo_url}"
"""
        script += self._generate_log_function()
        script += f"""
log "Starting YCSB benchmark for {db_name}"
log "Database: $DB_HOST:$DB_PORT/$DB_NAME"
log "Workload: $WORKLOAD"
log "Threads: $THREADS"
log "Record Count: $RECORD_COUNT"
log "Operation Count: $OPERATION_COUNT"

# DROP COLLECTION (if exists)
log "========================================="
log "Dropping 'usertable' collection if exists"
log "========================================="

mongo "$MONGO_URL" --eval "db.usertable.drop()" > "$LOG_DIR/drop_collection.log" 2>&1

if [ $? -eq 0 ]; then
    log "Collection dropped successfully (or didn't exist)"
else
    log "WARNING: Failed to drop collection (may not exist yet)"
fi

# LOAD PHASE
log "========================================="
log "LOAD PHASE - Loading initial data"
log "========================================="
log "Note: MongoDB will auto-create 'usertable' collection during load phase"

cd $YCSB_BIN
./bin/ycsb load mongodb -s -P workloads/$WORKLOAD -p mongodb.url="$MONGO_URL" -p mongodb.database="$DB_NAME" -p mongodb.writeConcern=acknowledged -p mongodb.readPreference=primary -p mongodb.maxConnectionPoolSize=100 -p recordcount=$RECORD_COUNT -p threadcount=$THREADS $LOAD_TARGET_PARAM $MAX_EXECUTION_TIME_PARAM -p core_workload_insertion_retry_limit=10 -p core_workload_insertion_retry_interval=3 > "$LOG_DIR/load_phase.log" 2>&1

if [ $? -eq 0 ]; then
    log "LOAD PHASE completed successfully"
else
    log "ERROR: LOAD PHASE failed. Check $LOG_DIR/load_phase.log"
    exit 1
fi
"""
        script += self._generate_run_phase_loop(ycsb_run_cmd)
        return script


class MySQLScriptGenerator(BaseYCSBScriptGenerator):
    """MySQL-specific script generator"""
    
    def generate_script(
        self,
        db_info: Dict[str, Any],
        db_credentials: Dict[str, str],
        ycsb_params: Dict[str, Any]
    ) -> str:
        """Generate YCSB shell script for MySQL"""
        db_name = db_info['name']
        db_ip = db_info['primary_ip']
        db_port = db_info['port']
        database_name = db_info['database_name']  # MySQL: lowercase
        username = db_credentials['username']
        password = db_credentials['password']
        
        params = self._extract_ycsb_params(ycsb_params)
        log_folder = self._generate_log_folder(db_name)
        proportion_params = self._build_proportion_params(ycsb_params)
        insertstart_param = self._build_insertstart_param(ycsb_params)
        operation_count_param, max_execution_time_param, run_target_param, load_target_param = self._build_conditional_params(
            params['operation_count'], params['max_execution_time'], ycsb_params
        )
        
        # Handle MySQL HA (InnoDB Cluster) without router
        if db_info.get('is_cluster') and db_info.get('cluster_type') == 'ha' and len(db_info.get('ip_addresses', [])) > 1:
            # For InnoDB Cluster without a router, use standard jdbc:mysql:// with multiple hosts
            # and failover parameters, NOT jdbc:mysql:replication://
            ip_list = ",".join([f"{ip}:{db_port}" for ip in db_info['ip_addresses']])
            jdbc_url = f"jdbc:mysql://{ip_list}/{database_name}?useSSL=false&autoReconnect=true&failOverReadOnly=false&tcpKeepAlive=true"
        else:
            jdbc_url = f"jdbc:mysql://{db_ip}:{db_port}/{database_name}?useSSL=false&autoReconnect=true&tcpKeepAlive=true"
        ycsb_run_cmd = f"""cd $YCSB_BIN
    ./bin/ycsb run jdbc -s -P workloads/$WORKLOAD -p db.driver=com.mysql.jdbc.Driver -p db.url="$JDBC_URL" -p db.user="$DB_USER" -p db.passwd="$DB_PASSWORD" -p recordcount=$RECORD_COUNT {insertstart_param} $OPERATION_COUNT_PARAM -p threadcount=$THREADS $RUN_TARGET_PARAM $MAX_EXECUTION_TIME_PARAM{proportion_params} -p core_workload_insertion_retry_limit=10 -p core_workload_insertion_retry_interval=3 > "$LOG_DIR/run_phase_$RUN_COUNTER.log" 2>&1"""
        
        script = self._generate_script_header("MySQL", db_name)
        script += f"""
# Configuration
DB_HOST="{db_ip}"
DB_PORT="{db_port}"
DB_NAME="{database_name}"
DB_USER="{username}"
DB_PASSWORD="{password}"
YCSB_BIN="{self.ycsb_path}"
LOG_DIR="{log_folder}"
WORKLOAD="{params['workload']}"
RECORD_COUNT={params['record_count']}
OPERATION_COUNT={params['operation_count']}
THREADS={params['threads']}
LOAD_TARGET={params['load_target']}
RUN_TARGET={params['run_target']}
MAX_EXECUTION_TIME={params['max_execution_time']}

# Build conditional YCSB parameters
OPERATION_COUNT_PARAM="{operation_count_param}"
MAX_EXECUTION_TIME_PARAM="{max_execution_time_param}"
RUN_TARGET_PARAM="{run_target_param}"
LOAD_TARGET_PARAM="{load_target_param}"

# Create log directory
mkdir -p "$LOG_DIR"
echo "Created log directory: $LOG_DIR"

# Connection string
JDBC_URL="{jdbc_url}"
"""
        script += self._generate_log_function()
        script += f"""
log "Starting YCSB benchmark for {db_name}"
log "Database: $DB_HOST:$DB_PORT/$DB_NAME"
log "Workload: $WORKLOAD"
log "Threads: $THREADS"
log "Record Count: $RECORD_COUNT"
log "Operation Count: $OPERATION_COUNT"

# CREATE TABLE (Required by YCSB)
log "========================================="
log "Creating usertable (required by YCSB)"
log "========================================="

mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<EOF
CREATE TABLE IF NOT EXISTS usertable (
    YCSB_KEY VARCHAR(255) PRIMARY KEY,
    FIELD0 TEXT, FIELD1 TEXT, FIELD2 TEXT, FIELD3 TEXT, FIELD4 TEXT,
    FIELD5 TEXT, FIELD6 TEXT, FIELD7 TEXT, FIELD8 TEXT, FIELD9 TEXT
);
EOF

if [ $? -eq 0 ]; then
    log "Table created successfully"
else
    log "ERROR: Failed to create table"
    exit 1
fi

# LOAD PHASE
log "========================================="
log "LOAD PHASE - Loading initial data"
log "========================================="

cd $YCSB_BIN
./bin/ycsb load jdbc -s -P workloads/$WORKLOAD -p db.driver=com.mysql.jdbc.Driver -p db.url="$JDBC_URL" -p db.user="$DB_USER" -p db.passwd="$DB_PASSWORD" -p recordcount=$RECORD_COUNT -p threadcount=$THREADS $LOAD_TARGET_PARAM $MAX_EXECUTION_TIME_PARAM -p core_workload_insertion_retry_limit=10 -p core_workload_insertion_retry_interval=3 > "$LOG_DIR/load_phase.log" 2>&1

if [ $? -eq 0 ]; then
    log "LOAD PHASE completed successfully"
else
    log "ERROR: LOAD PHASE failed. Check $LOG_DIR/load_phase.log"
    exit 1
fi
"""
        script += self._generate_run_phase_loop(ycsb_run_cmd)
        return script


class OracleScriptGenerator(BaseYCSBScriptGenerator):
    """Oracle-specific script generator"""
    
    def generate_script(
        self,
        db_info: Dict[str, Any],
        db_credentials: Dict[str, str],
        ycsb_params: Dict[str, Any]
    ) -> str:
        """Generate YCSB shell script for Oracle"""
        db_name = db_info['name']
        db_ip = db_info['primary_ip']
        db_port = db_info['port']
        database_name = db_info['database_name']  # Oracle: as-is (typically uppercase)
        username = db_credentials['username']
        password = db_credentials['password']
        
        params = self._extract_ycsb_params(ycsb_params)
        log_folder = self._generate_log_folder(db_name)
        proportion_params = self._build_proportion_params(ycsb_params)
        insertstart_param = self._build_insertstart_param(ycsb_params)
        operation_count_param, max_execution_time_param, run_target_param, load_target_param = self._build_conditional_params(
            params['operation_count'], params['max_execution_time'], ycsb_params
        )
        
        jdbc_url = f"jdbc:oracle:thin:@{db_ip}:{db_port}:{database_name}"
        
        ycsb_run_cmd = f"""cd $YCSB_BIN
    ./bin/ycsb run jdbc -s -P workloads/$WORKLOAD -p db.driver=oracle.jdbc.driver.OracleDriver -p db.url="$JDBC_URL" -p db.user="$DB_USER" -p db.passwd="$DB_PASSWORD" -p recordcount=$RECORD_COUNT {insertstart_param} $OPERATION_COUNT_PARAM -p threadcount=$THREADS $RUN_TARGET_PARAM $MAX_EXECUTION_TIME_PARAM{proportion_params} -p core_workload_insertion_retry_limit=10 -p core_workload_insertion_retry_interval=3 > "$LOG_DIR/run_phase_$RUN_COUNTER.log" 2>&1"""
        
        script = self._generate_script_header("Oracle", db_name)
        script += f"""
# Configuration
DB_HOST="{db_ip}"
DB_PORT="{db_port}"
DB_NAME="{database_name}"
DB_USER="{username}"
DB_PASSWORD="{password}"
YCSB_BIN="{self.ycsb_path}"
LOG_DIR="{log_folder}"
WORKLOAD="{params['workload']}"
RECORD_COUNT={params['record_count']}
OPERATION_COUNT={params['operation_count']}
THREADS={params['threads']}
LOAD_TARGET={params['load_target']}
RUN_TARGET={params['run_target']}
MAX_EXECUTION_TIME={params['max_execution_time']}

# Build conditional YCSB parameters
OPERATION_COUNT_PARAM="{operation_count_param}"
MAX_EXECUTION_TIME_PARAM="{max_execution_time_param}"
RUN_TARGET_PARAM="{run_target_param}"
LOAD_TARGET_PARAM="{load_target_param}"

# Create log directory
mkdir -p "$LOG_DIR"
echo "Created log directory: $LOG_DIR"

# Connection string
JDBC_URL="{jdbc_url}"
"""
        script += self._generate_log_function()
        script += f"""
log "Starting YCSB benchmark for {db_name}"
log "Database: $DB_HOST:$DB_PORT/$DB_NAME"
log "Workload: $WORKLOAD"
log "Threads: $THREADS"
log "Record Count: $RECORD_COUNT"
log "Operation Count: $OPERATION_COUNT"

# CREATE TABLE (Required by YCSB)
log "========================================="
log "Creating usertable (required by YCSB)"
log "========================================="

sqlplus -S "$DB_USER/$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_NAME" <<EOF
BEGIN
   EXECUTE IMMEDIATE 'CREATE TABLE usertable (
       YCSB_KEY VARCHAR2(255) PRIMARY KEY,
       FIELD0 CLOB, FIELD1 CLOB, FIELD2 CLOB, FIELD3 CLOB, FIELD4 CLOB,
       FIELD5 CLOB, FIELD6 CLOB, FIELD7 CLOB, FIELD8 CLOB, FIELD9 CLOB
   )';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -955 THEN
         RAISE;
      END IF;
END;
/
EXIT;
EOF

if [ $? -eq 0 ]; then
    log "Table created successfully"
else
    log "ERROR: Failed to create table"
    exit 1
fi

# LOAD PHASE
log "========================================="
log "LOAD PHASE - Loading initial data"
log "========================================="

cd $YCSB_BIN
./bin/ycsb load jdbc -s -P workloads/$WORKLOAD -p db.driver=oracle.jdbc.driver.OracleDriver -p db.url="$JDBC_URL" -p db.user="$DB_USER" -p db.passwd="$DB_PASSWORD" -p recordcount=$RECORD_COUNT -p threadcount=$THREADS $LOAD_TARGET_PARAM $MAX_EXECUTION_TIME_PARAM -p core_workload_insertion_retry_limit=10 -p core_workload_insertion_retry_interval=3 > "$LOG_DIR/load_phase.log" 2>&1

if [ $? -eq 0 ]; then
    log "LOAD PHASE completed successfully"
else
    log "ERROR: LOAD PHASE failed. Check $LOG_DIR/load_phase.log"
    exit 1
fi
"""
        script += self._generate_run_phase_loop(ycsb_run_cmd)
        return script


class MSSQLScriptGenerator(BaseYCSBScriptGenerator):
    """MS SQL Server-specific script generator"""
    
    def generate_script(
        self,
        db_info: Dict[str, Any],
        db_credentials: Dict[str, str],
        ycsb_params: Dict[str, Any]
    ) -> str:
        """Generate YCSB shell script for MS SQL Server"""
        db_name = db_info['name']
        db_ip = db_info['primary_ip']
        db_port = db_info['port']
        database_name = db_info['database_name']  # MS SQL: as-is
        username = db_credentials['username']
        password = db_credentials['password']
        
        params = self._extract_ycsb_params(ycsb_params)
        log_folder = self._generate_log_folder(db_name)
        proportion_params = self._build_proportion_params(ycsb_params)
        insertstart_param = self._build_insertstart_param(ycsb_params)
        operation_count_param, max_execution_time_param, run_target_param, load_target_param = self._build_conditional_params(
            params['operation_count'], params['max_execution_time'], ycsb_params
        )
        
        # JDBC URL with SSL and performance parameters (based on working script)
        jdbc_url = f"jdbc:sqlserver://{db_ip}:{db_port};databaseName={database_name};encrypt=false;trustServerCertificate=true;sendStringParametersAsUnicode=false;disableStatementPooling=false;statementPoolingCacheSize=10;responseBuffering=full;"
        
        # MS SQL Server uses specific YCSB path
        mssql_ycsb_path = "/ycsb-0.16.0"
        mssql_jdbc_driver = "mssql-jdbc-12.6.1.jre11.jar"
        
        ycsb_run_cmd = f"""cd {mssql_ycsb_path}
    ./bin/ycsb run jdbc -s -P workloads/$WORKLOAD -cp {mssql_jdbc_driver} -p db.driver=com.microsoft.sqlserver.jdbc.SQLServerDriver -p db.url="$JDBC_URL" -p db.user="$DB_USER" -p db.passwd="$DB_PASSWORD" -p db.batchsize=1000 -p jdbc.batchupdateapi=true -p recordcount=$RECORD_COUNT {insertstart_param} $OPERATION_COUNT_PARAM -p threadcount=$THREADS $RUN_TARGET_PARAM $MAX_EXECUTION_TIME_PARAM{proportion_params} -p core_workload_insertion_retry_limit=10 -p core_workload_insertion_retry_interval=3 > "$LOG_DIR/run_phase_$RUN_COUNTER.log" 2>&1"""
        
        script = self._generate_script_header("MS SQL Server", db_name)
        script += f"""
# Configuration
DB_HOST="{db_ip}"
DB_PORT="{db_port}"
DB_NAME="{database_name}"
DB_USER="{username}"
DB_PASSWORD="{password}"
YCSB_BIN="{mssql_ycsb_path}"
LOG_DIR="{log_folder}"
WORKLOAD="{params['workload']}"
RECORD_COUNT={params['record_count']}
OPERATION_COUNT={params['operation_count']}
THREADS={params['threads']}
LOAD_TARGET={params['load_target']}
RUN_TARGET={params['run_target']}
MAX_EXECUTION_TIME={params['max_execution_time']}

# Build conditional YCSB parameters
OPERATION_COUNT_PARAM="{operation_count_param}"
MAX_EXECUTION_TIME_PARAM="{max_execution_time_param}"
RUN_TARGET_PARAM="{run_target_param}"
LOAD_TARGET_PARAM="{load_target_param}"

# Create log directory
mkdir -p "$LOG_DIR"
echo "Created log directory: $LOG_DIR"

# Connection string
JDBC_URL="{jdbc_url}"
"""
        script += self._generate_log_function()
        script += f"""
log "Starting YCSB benchmark for {db_name}"
log "Database: $DB_HOST:$DB_PORT/$DB_NAME"
log "Workload: $WORKLOAD"
log "Threads: $THREADS"
log "Record Count: $RECORD_COUNT"
log "Operation Count: $OPERATION_COUNT"

# CREATE TABLE (Required by YCSB)
log "========================================="
log "Creating usertable (required by YCSB)"
log "========================================="

sqlcmd -C -S "$DB_HOST,$DB_PORT" -U "$DB_USER" -P "$DB_PASSWORD" -d "$DB_NAME" -Q "
IF OBJECT_ID('usertable', 'U') IS NULL
BEGIN
    CREATE TABLE usertable (
        YCSB_KEY VARCHAR(255) NOT NULL,
        FIELD0 VARCHAR(100) NOT NULL, FIELD1 VARCHAR(100) NOT NULL, FIELD2 VARCHAR(100) NOT NULL,
        FIELD3 VARCHAR(100) NOT NULL, FIELD4 VARCHAR(100) NOT NULL, FIELD5 VARCHAR(100) NOT NULL,
        FIELD6 VARCHAR(100) NOT NULL, FIELD7 VARCHAR(100) NOT NULL, FIELD8 VARCHAR(100) NOT NULL,
        FIELD9 VARCHAR(100) NOT NULL,
        CONSTRAINT pk_usertable PRIMARY KEY (YCSB_KEY)
    );
END
" > "$LOG_DIR/create_table.log" 2>&1

if [ $? -eq 0 ]; then
    log "Table created successfully"
else
    log "ERROR: Failed to create table"
    exit 1
fi

# LOAD PHASE
log "========================================="
log "LOAD PHASE - Loading initial data"
log "========================================="

cd /ycsb-0.16.0
./bin/ycsb load jdbc -s -P workloads/$WORKLOAD -cp mssql-jdbc-12.6.1.jre11.jar -p db.driver=com.microsoft.sqlserver.jdbc.SQLServerDriver -p db.url="$JDBC_URL" -p db.user="$DB_USER" -p db.passwd="$DB_PASSWORD" -p db.batchsize=1000 -p jdbc.batchupdateapi=true -p recordcount=$RECORD_COUNT -p threadcount=$THREADS $LOAD_TARGET_PARAM $MAX_EXECUTION_TIME_PARAM -p core_workload_insertion_retry_limit=10 -p core_workload_insertion_retry_interval=3 > "$LOG_DIR/load_phase.log" 2>&1

if [ $? -eq 0 ]; then
    log "LOAD PHASE completed successfully"
else
    log "ERROR: LOAD PHASE failed. Check $LOG_DIR/load_phase.log"
    exit 1
fi
"""
        script += self._generate_run_phase_loop(ycsb_run_cmd)
        return script


class ShellScriptGenerator:
    """Main shell script generator that delegates to database-specific generators"""
    
    def __init__(self):
        self.postgres_generator = PostgreSQLScriptGenerator()
        self.mongodb_generator = MongoDBScriptGenerator()
        self.mysql_generator = MySQLScriptGenerator()
        self.oracle_generator = OracleScriptGenerator()
        self.mssql_generator = MSSQLScriptGenerator()
    
    def generate_postgres_script(self, db_info: Dict[str, Any], db_credentials: Dict[str, str], 
                                 ycsb_params: Dict[str, Any]) -> str:
        return self.postgres_generator.generate_script(db_info, db_credentials, ycsb_params)
    
    def generate_mongodb_script(self, db_info: Dict[str, Any], db_credentials: Dict[str, str],
                                ycsb_params: Dict[str, Any]) -> str:
        return self.mongodb_generator.generate_script(db_info, db_credentials, ycsb_params)
    
    def generate_mysql_script(self, db_info: Dict[str, Any], db_credentials: Dict[str, str],
                             ycsb_params: Dict[str, Any]) -> str:
        return self.mysql_generator.generate_script(db_info, db_credentials, ycsb_params)
    
    def generate_oracle_script(self, db_info: Dict[str, Any], db_credentials: Dict[str, str],
                              ycsb_params: Dict[str, Any]) -> str:
        return self.oracle_generator.generate_script(db_info, db_credentials, ycsb_params)
    
    def generate_mssql_script(self, db_info: Dict[str, Any], db_credentials: Dict[str, str],
                             ycsb_params: Dict[str, Any]) -> str:
        return self.mssql_generator.generate_script(db_info, db_credentials, ycsb_params)
    
    def generate_master_script(self, script_files: List[str]) -> str:
        """Generate master script to run all benchmarks in parallel"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        script = f"""#!/bin/bash

#############################################
# Master YCSB Benchmark Launcher
# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
# Total databases: {len(script_files)}
#############################################

set -e

echo "========================================="
echo "Starting YCSB Benchmarks"
echo "========================================="
echo "Total databases: {len(script_files)}"
echo "Execution mode: Parallel"
echo ""

# Make all scripts executable
chmod +x *.sh

# Array to store PIDs
declare -a PIDS

# Launch all benchmarks in parallel
"""
        
        for i, script_file in enumerate(script_files, 1):
            script += f"""
echo "[{i}/{len(script_files)}] Starting benchmark: {script_file}"
nohup ./{script_file} > {script_file}.out 2>&1 &
PIDS[${{#PIDS[@]}}]=$!
"""
        
        script += """
echo ""
echo "All benchmarks launched in background"
echo "PIDs: ${PIDS[@]}"
echo ""
echo "To monitor progress:"
"""
        
        for script_file in script_files:
            script += f"""echo "  tail -f {script_file}.out"
"""
        
        script += """
echo ""
echo "To check running processes:"
echo "  ps aux | grep ycsb"
echo ""
echo "To stop all benchmarks:"
echo "  pkill -f ycsb"
echo ""
echo "========================================="
echo "Master script completed"
echo "========================================="
"""
        return script
    
    def generate_readme(self, script_files: List[str], ycsb_params: Dict[str, Any]) -> str:
        """Generate README with instructions"""
        return f"""# YCSB Benchmark Scripts

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Contents

- `run_all_benchmarks.sh` - Master launcher script
{chr(10).join([f'- `{script}` - Individual database benchmark' for script in script_files])}

## Quick Start

### 1. Extract the ZIP file
```bash
unzip ycsb_benchmark_*.zip
cd ycsb_benchmark_*
```

### 2. Run all benchmarks in parallel
```bash
chmod +x run_all_benchmarks.sh
./run_all_benchmarks.sh
```

### 3. Monitor progress
```bash
# Watch all outputs
tail -f *.out

# Watch specific database
tail -f <database_name>.sh.out
```

## Configuration

- **Workload**: {ycsb_params.get('workload_type', 'a')}
- **Record Count**: {ycsb_params.get('record_count', 'N/A'):,}
- **Operation Count**: {ycsb_params.get('operation_count', 'N/A'):,} (0 = unlimited)
- **Timeout**: {ycsb_params.get('timeout', 'N/A'):,} seconds (0 = unlimited)
- **Threads**: {ycsb_params.get('threads', 'N/A')}
- **Load Target Throughput**: {ycsb_params.get('load_target_throughput', 'N/A')} ops/sec
- **Run Target Throughput**: {ycsb_params.get('run_target_throughput', 'N/A')} ops/sec
- **Max RUN Iterations**: 10

## Log Locations

Each benchmark creates its own log directory in `/tmp/ycsb_<database>_<timestamp>/`:
- `benchmark.log` - Main log with timestamps
- `load_phase.log` - LOAD phase output
- `run_phase_1.log` through `run_phase_10.log` - RUN phase outputs

## Stopping Benchmarks

```bash
# Stop all YCSB processes
pkill -f ycsb

# Or kill specific PIDs (shown by master script)
kill <PID>
```

## Troubleshooting

### Check if benchmarks are running
```bash
ps aux | grep ycsb
```

### View logs
```bash
ls -la /tmp/ycsb_*
tail -f /tmp/ycsb_*/benchmark.log
```

### Common Issues

1. **Permission denied**: Run `chmod +x *.sh`
2. **YCSB not found**: Ensure YCSB is installed at `/ycsb/bin/ycsb`
3. **Connection failed**: Verify database credentials and network connectivity
4. **Table creation failed**: Check database permissions

## Support

For issues or questions, refer to the main documentation.
"""
