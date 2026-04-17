"""
Main Script Generator - Orchestrates shell script generation and packaging
"""

import os
import zipfile
import tempfile
import logging
from pathlib import Path
from typing import Dict, Any, Tuple, List
from datetime import datetime

from .shell_script_generator import ShellScriptGenerator
from validators.ndb_validator import NDBValidator, NDBConnection

logger = logging.getLogger(__name__)


class ScriptGenerator:
    """
    Main script generator that orchestrates the creation of YCSB benchmark scripts
    and packages them into a ZIP file
    """
    
    @staticmethod
    def generate_script(
        ndb_config: Dict[str, Any],
        engine_selections: Dict[str, List[str]],
        db_credentials: Dict[str, Dict[str, str]],
        ycsb_params: Dict[str, Any]
    ) -> Tuple[str, str, bool]:
        """
        Generate YCSB benchmark scripts package
        
        Args:
            ndb_config: NDB connection configuration
            engine_selections: Selected engines and their types
            db_credentials: Database credentials per engine
            ycsb_params: YCSB parameters
            
        Returns:
            Tuple of (zip_path, zip_name, is_zip)
            - zip_path: Full path to generated ZIP file
            - zip_name: Name of the ZIP file
            - is_zip: Always True (for compatibility)
        """
        try:
            logger.info("Starting script generation")
            # engine_selections is a list of dicts: [{"engine": "postgresql", "types": ["si", "ha"]}, ...]
            selected_engines = [sel['engine'] for sel in engine_selections]
            logger.info(f"Selected engines: {selected_engines}")
            
            # Initialize NDB connection
            ndb_connection = NDBConnection(
                ip=ndb_config['ip'],
                username=ndb_config['username'],
                password=ndb_config['password'],
                port=ndb_config.get('port', 8443),
                verify_ssl=ndb_config.get('verify_ssl', False)
            )
            
            # Fetch databases from NDB
            validator = NDBValidator(ndb_connection)
            success, message, databases_by_type = validator.fetch_databases_by_type(engine_selections)
            
            if not success:
                raise Exception(f"Failed to fetch databases from NDB: {message}")
            
            # Filter databases by selected IDs if provided
            for sel in engine_selections:
                engine = sel['engine']
                selected_dbs = sel.get('selected_databases')
                
                if selected_dbs is not None and engine in databases_by_type:
                    selected_set = set(selected_dbs)
                    for db_type in list(databases_by_type[engine].keys()):
                        # Filter the database list
                        filtered_dbs = [db for db in databases_by_type[engine][db_type] if db.id in selected_set]
                        if filtered_dbs:
                            databases_by_type[engine][db_type] = filtered_dbs
                        else:
                            # Remove empty types
                            del databases_by_type[engine][db_type]
                    
                    # Remove empty engines
                    if not databases_by_type[engine]:
                        del databases_by_type[engine]
            
            logger.info(f"Fetched databases: {len(databases_by_type)} engine types")
            
            # Initialize shell script generator
            shell_generator = ShellScriptGenerator()
            
            # Import DBCredentialValidator to find working database names
            from validators.db_credential_validator import DBCredentialValidator
            
            # Generate individual database scripts
            script_files = []
            script_contents = {}
            
            # databases_by_type structure: {engine: {type: [DatabaseInfo]}}
            # We need to flatten this to iterate over all databases
            for engine, types_dict in databases_by_type.items():
                # types_dict is a dictionary like {"si": [db1, db2], "ha": [db3]}
                for db_type, databases in types_dict.items():
                    logger.info(f"Processing {engine}/{db_type}: {len(databases)} databases")
                    
                    for db in databases:
                        # Convert DatabaseInfo to dict if needed
                        db_dict = db.dict() if hasattr(db, 'dict') else db.model_dump()
                        
                        db_name = db_dict.get('name', 'unknown')
                        
                        logger.info(f"Generating script for {engine}/{db_type}: {db_name}")
                        
                        # Get credentials for this engine
                        creds = db_credentials.get(engine, {})
                        
                        # Test connection to find the working database name (case-sensitive fallback)
                        host = db_dict.get('primary_ip', db_dict.get('ip_addresses', ['unknown'])[0])
                        port = db_dict.get('port', 5432)
                        database_name = db_dict.get('database_name', 'postgres')
                        
                        success, message, working_db_name = DBCredentialValidator.validate_credentials(
                            engine=engine,
                            host=host,
                            port=port,
                            database=database_name,
                            username=creds.get('username', ''),
                            password=creds.get('password', ''),
                            test_connectivity_only=False
                        )
                        
                        if success and working_db_name:
                            db_dict['database_name'] = working_db_name
                            logger.info(f"Using validated database name: {working_db_name}")
                        else:
                            logger.warning(f"Validation failed for {db_name} during script generation, using original name. Error: {message}")
                        
                        # Generate script based on engine type
                        try:
                            if engine == 'postgresql':
                                script_content = shell_generator.generate_postgres_script(
                                    db_dict, creds, ycsb_params
                                )
                            elif engine == 'mongodb':
                                script_content = shell_generator.generate_mongodb_script(
                                    db_dict, creds, ycsb_params
                                )
                            elif engine == 'mysql':
                                script_content = shell_generator.generate_mysql_script(
                                    db_dict, creds, ycsb_params
                                )
                            elif engine == 'oracle':
                                script_content = shell_generator.generate_oracle_script(
                                    db_dict, creds, ycsb_params
                                )
                            elif engine == 'mssql':
                                script_content = shell_generator.generate_mssql_script(
                                    db_dict, creds, ycsb_params
                                )
                            else:
                                logger.warning(f"Unsupported engine: {engine}")
                                continue
                            
                            # Create script filename
                            safe_name = db_name.replace(' ', '_').replace('/', '_')
                            script_filename = f"ycsb_{safe_name}.sh"
                            script_files.append(script_filename)
                            script_contents[script_filename] = script_content
                            
                            logger.info(f"Generated script: {script_filename}")
                            
                        except Exception as e:
                            logger.error(f"Failed to generate script for {db_name}: {str(e)}")
                            import traceback
                            logger.error(traceback.format_exc())
                            continue
            
            if not script_files:
                raise Exception("No scripts were generated. Check database selection and credentials.")
            
            # Generate master launcher script
            logger.info("Generating master launcher script")
            master_script = shell_generator.generate_master_script(script_files)
            
            # Generate README
            logger.info("Generating README")
            readme_content = ScriptGenerator._generate_readme(
                script_files, engine_selections, ycsb_params
            )
            
            # Create ZIP package
            logger.info("Creating ZIP package")
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            zip_name = f"ycsb_benchmark_{timestamp}.zip"
            
            # Create temporary directory for ZIP creation
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Write all scripts to temp directory
                for filename, content in script_contents.items():
                    script_path = temp_path / filename
                    script_path.write_text(content)
                    # Make executable
                    os.chmod(script_path, 0o755)
                
                # Write master script
                master_path = temp_path / "run_all_benchmarks.sh"
                master_path.write_text(master_script)
                os.chmod(master_path, 0o755)
                
                # Write README
                readme_path = temp_path / "README.md"
                readme_path.write_text(readme_content)
                
                # Create ZIP file in temp location
                zip_temp_path = Path(tempfile.gettempdir()) / zip_name
                with zipfile.ZipFile(zip_temp_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for file_path in temp_path.iterdir():
                        zipf.write(file_path, file_path.name)
                
                logger.info(f"ZIP package created: {zip_name}")
                logger.info(f"Total scripts: {len(script_files)}")
                
                return str(zip_temp_path), zip_name, True
                
        except Exception as e:
            logger.error(f"Script generation failed: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    @staticmethod
    def _generate_readme(
        script_files: List[str],
        engine_selections: Dict[str, List[str]],
        ycsb_params: Dict[str, Any]
    ) -> str:
        """Generate README content for the script package"""
        
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Build engine summary
        # engine_selections is a list of dicts: [{"engine": "postgresql", "types": ["si", "ha"]}, ...]
        engine_summary = []
        for selection in engine_selections:
            engine = selection['engine']
            types = selection['types']
            engine_summary.append(f"  - {engine.upper()}: {', '.join(types)}")
        
        readme = f"""# YCSB Benchmark Scripts Package

**Generated:** {timestamp}  
**Total Scripts:** {len(script_files)}

## Overview

This package contains YCSB benchmark scripts for testing multiple databases in parallel.

## Selected Engines

{chr(10).join(engine_summary)}

## YCSB Configuration

- **Workload Type:** {ycsb_params.get('workload_type', 'a').upper()}
- **Record Count:** {ycsb_params.get('record_count', 'N/A'):,}
- **Operation Count:** {ycsb_params.get('operation_count', 'N/A'):,} (0 = unlimited)
- **Thread Count:** {ycsb_params.get('thread_count', 'N/A')}
- **Load Target Throughput:** {ycsb_params.get('load_target_throughput', 0)} ops/sec (0 = unlimited)
- **Run Target Throughput:** {ycsb_params.get('run_target_throughput', 0)} ops/sec (0 = unlimited)
- **Max Execution Time:** {ycsb_params.get('timeout', 3600)} seconds (0 = unlimited)

## Prerequisites

1. **YCSB Installation:**
   - YCSB must be installed at `/ycsb` on the target machine
   - Or modify the `YCSB_BIN` variable in each script

2. **Database Connectivity:**
   - Ensure network connectivity to all target databases
   - Verify database credentials are correct

3. **Required Tools:**
   - Bash shell
   - Database client tools (psql, mysql, sqlplus, sqlcmd, mongo)

## Quick Start

### 1. Extract the Package

```bash
unzip ycsb_benchmark_*.zip
cd ycsb_benchmark_*
```

### 2. Run All Benchmarks in Parallel

```bash
chmod +x run_all_benchmarks.sh
./run_all_benchmarks.sh
```

This will launch all database benchmarks in parallel using nohup.

### 3. Run Individual Database Benchmark

```bash
chmod +x ycsb_DatabaseName.sh
./ycsb_DatabaseName.sh
```

## Monitoring

### Check Running Processes

```bash
ps aux | grep ycsb
```

### View Logs

Each database creates its own log directory in `/tmp`:

```bash
# List all log directories
ls -la /tmp/ycsb_*

# View main log
tail -f /tmp/ycsb_DatabaseName_*/benchmark.log

# View load phase log
tail -f /tmp/ycsb_DatabaseName_*/load_phase.log

# View run phase logs
tail -f /tmp/ycsb_DatabaseName_*/run_phase_*.log
```

## Script Behavior

### LOAD Phase
- Creates/drops the `usertable` (PostgreSQL, MySQL, Oracle, MSSQL)
- Drops collection if exists (MongoDB)
- Loads initial dataset
- Exits when either `recordcount` OR `timeout` is reached (whichever comes first)

### RUN Phase
- Runs benchmark iterations (max 10 iterations)
- Each iteration exits when either `operationcount` OR `timeout` is reached
- Logs results for each iteration
- Continues on failure to complete all iterations

## Stopping Benchmarks

### Stop All Benchmarks

```bash
pkill -f "ycsb_.*\.sh"
```

### Stop Specific Database

```bash
pkill -f "ycsb_DatabaseName\.sh"
```

## Troubleshooting

### Issue: "ycsb: command not found"

**Solution:** Verify YCSB installation path and update `YCSB_BIN` variable in scripts.

### Issue: Database connection failed

**Solution:** 
- Verify database is running and accessible
- Check credentials in the script
- Test connection manually using database client tools

### Issue: Permission denied

**Solution:**
```bash
chmod +x *.sh
```

## Generated Scripts

"""
        
        for script_file in sorted(script_files):
            readme += f"- `{script_file}`\n"
        
        readme += f"""
## Support

For issues or questions, refer to the main NDB YCSB Benchmark Generator documentation.

---

**Package Generated:** {timestamp}
"""
        
        return readme


__all__ = ['ScriptGenerator']
