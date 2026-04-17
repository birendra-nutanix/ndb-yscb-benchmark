"""
Base Script Generator for YCSB Benchmarks

This module provides a base class with common functionality for all database-specific
script generators, eliminating code duplication.
"""

from typing import Dict, Any
from datetime import datetime
from abc import ABC, abstractmethod


class BaseYCSBScriptGenerator(ABC):
    """Base class for YCSB script generators with common functionality"""
    
    def __init__(self):
        self.ycsb_path = "/ycsb"
        self.max_retries = 10  # Maximum number of RUN phase retries
    
    def _build_conditional_params(self, operation_count: int, max_execution_time: int, ycsb_params: Dict[str, Any]) -> tuple:
        """
        Build conditional YCSB parameters based on values
        
        Args:
            operation_count: Number of operations (0 for unlimited)
            max_execution_time: Timeout in seconds (0 for unlimited)
            
        Returns:
            Tuple of (operation_count_param, max_execution_time_param)
        """
        operation_count_param = f"-p operationcount=$OPERATION_COUNT" if operation_count > 0 else ""
        max_execution_time_param = f"-p maxexecutiontime=$MAX_EXECUTION_TIME" if max_execution_time > 0 else ""
        run_target_param = f"-p target=$RUN_TARGET" if ycsb_params.get('run_target_throughput', 0) > 0 else ""
        load_target_param = f"-p target=$LOAD_TARGET" if ycsb_params.get('load_target_throughput', 0) > 0 else ""
        
        return operation_count_param, max_execution_time_param, run_target_param, load_target_param
    
    def _build_insertstart_param(self, ycsb_params: Dict[str, Any]) -> str:
        """
        Build insertstart parameter if insert proportion is specified
        
        Args:
            ycsb_params: YCSB parameters dictionary
            
        Returns:
            String with insertstart parameter or empty string
        """
        insert_proportion = ycsb_params.get('insert_proportion')
        
        # Only add insertstart if insert_proportion is specified and > 0
        if insert_proportion is not None and insert_proportion > 0:
            return "-p insertstart=$RECORD_COUNT"
        
        return ""
    
    def _build_proportion_params(self, ycsb_params: Dict[str, Any]) -> str:
        """
        Build workload proportion parameters string
        
        Args:
            ycsb_params: YCSB parameters dictionary
            
        Returns:
            String with proportion parameters
        """
        proportion_params = ""
        
        read_proportion = ycsb_params.get('read_proportion')
        update_proportion = ycsb_params.get('update_proportion')
        insert_proportion = ycsb_params.get('insert_proportion')
        scan_proportion = ycsb_params.get('scan_proportion')
        
        if read_proportion is not None:
            proportion_params += f" -p readproportion={read_proportion}"
        if update_proportion is not None:
            proportion_params += f" -p updateproportion={update_proportion}"
        if insert_proportion is not None:
            proportion_params += f" -p insertproportion={insert_proportion}"
        if scan_proportion is not None:
            proportion_params += f" -p scanproportion={scan_proportion}"
        
        return proportion_params
    
    def _extract_ycsb_params(self, ycsb_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract and return common YCSB parameters
        
        Args:
            ycsb_params: YCSB parameters dictionary
            
        Returns:
            Dictionary with extracted parameters
        """
        # Get workload type from user selection (a, b, c, d, e, f)
        workload_type = ycsb_params.get('workload_type', 'a')
        workload = f"workload{workload_type}"
        
        return {
            'workload': workload,
            'record_count': ycsb_params.get('record_count', 1000000),
            'operation_count': ycsb_params.get('operation_count', 1000000),
            'threads': ycsb_params.get('threads', 10),
            'load_target': ycsb_params.get('load_target_throughput', 0),
            'run_target': ycsb_params.get('run_target_throughput', 0),
            'max_execution_time': ycsb_params.get('timeout', 3600)
        }
    
    def _generate_log_folder(self, db_name: str) -> str:
        """
        Generate unique log folder path
        
        Args:
            db_name: Database name
            
        Returns:
            Log folder path
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"/tmp/ycsb_{db_name}_{timestamp}"
    
    def _generate_script_header(self, db_type: str, db_name: str) -> str:
        """
        Generate common script header
        
        Args:
            db_type: Database type (PostgreSQL, MongoDB, etc.)
            db_name: Database name
            
        Returns:
            Script header string
        """
        return f"""#!/bin/bash

#############################################
# YCSB Benchmark Script for {db_type}
# Database: {db_name}
# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
#############################################

set -e  # Exit on error
"""
    
    def _generate_log_function(self) -> str:
        """Generate bash logging function"""
        return """
# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_DIR/benchmark.log"
}
"""
    
    def _generate_run_phase_loop(self, ycsb_command: str) -> str:
        """
        Generate RUN phase loop with retry limit
        
        Args:
            ycsb_command: The YCSB run command to execute
            
        Returns:
            Bash script for RUN phase with retry logic
        """
        return f"""
# RUN PHASE - Limited retries
log "========================================="
log "RUN PHASE - Starting benchmark (max {self.max_retries} iterations)"
log "========================================="

RUN_COUNTER=1
MAX_RETRIES={self.max_retries}

while [ $RUN_COUNTER -le $MAX_RETRIES ]; do
    log "Starting RUN iteration $RUN_COUNTER of $MAX_RETRIES"
    
    {ycsb_command}
    
    if [ $? -eq 0 ]; then
        log "RUN iteration $RUN_COUNTER completed successfully"
    else
        log "ERROR: RUN iteration $RUN_COUNTER failed. Check $LOG_DIR/run_phase_$RUN_COUNTER.log"
        log "Continuing to next iteration..."
    fi
    
    RUN_COUNTER=$((RUN_COUNTER + 1))
    
    # Small delay between runs
    if [ $RUN_COUNTER -le $MAX_RETRIES ]; then
        sleep 5
    fi
done

log "========================================="
log "RUN PHASE completed: $((RUN_COUNTER - 1)) iterations executed"
log "========================================="
log "Benchmark finished. Check logs in $LOG_DIR"
"""
    
    @abstractmethod
    def generate_script(
        self,
        db_info: Dict[str, Any],
        db_credentials: Dict[str, str],
        ycsb_params: Dict[str, Any]
    ) -> str:
        """
        Generate database-specific YCSB script
        
        Must be implemented by subclasses
        
        Args:
            db_info: Database information
            db_credentials: Database credentials
            ycsb_params: YCSB parameters
            
        Returns:
            Complete shell script as string
        """
        pass
