"""
YCSB Command Configuration Builder
Generates YCSB commands for different database engines
"""

from typing import Dict, List, Optional
from pydantic import BaseModel


class YCSBParameters(BaseModel):
    """YCSB benchmark parameters"""
    phase: str  # 'load', 'run', or 'both'
    workload_type: str  # 'a', 'b', 'c', 'd', 'e', 'f'
    record_count: int
    operation_count: int
    thread_count: int
    load_target_throughput: int  # 0 for unlimited
    run_target_throughput: int  # 0 for unlimited
    retry_limit: int = 10
    retry_interval: int = 3
    read_proportion: Optional[float] = None
    update_proportion: Optional[float] = None
    insert_proportion: Optional[float] = None
    scan_proportion: Optional[float] = None
    duration_days: Optional[int] = None


class DatabaseCredentials(BaseModel):
    """Database connection credentials"""
    username: str
    password: str
    host: str
    port: int
    database_name: str


class YCSBConfigBuilder:
    """Builds YCSB commands for different database engines"""
    
    # JDBC drivers for each database engine
    JDBC_DRIVERS = {
        'postgresql': 'org.postgresql.Driver',
        'mysql': 'com.mysql.cj.jdbc.Driver',
        'oracle': 'oracle.jdbc.driver.OracleDriver',
        'mssql': 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
    }
    
    # Default workload characteristics
    WORKLOAD_DEFAULTS = {
        'a': {'read': 0.5, 'update': 0.5, 'insert': 0.0, 'scan': 0.0},  # Update heavy
        'b': {'read': 0.95, 'update': 0.05, 'insert': 0.0, 'scan': 0.0},  # Read mostly
        'c': {'read': 1.0, 'update': 0.0, 'insert': 0.0, 'scan': 0.0},  # Read only
        'd': {'read': 0.95, 'update': 0.0, 'insert': 0.05, 'scan': 0.0},  # Read latest
        'e': {'read': 0.0, 'update': 0.0, 'insert': 0.05, 'scan': 0.95},  # Short ranges
        'f': {'read': 0.5, 'update': 0.0, 'insert': 0.0, 'scan': 0.0, 'readmodifywrite': 0.5}  # Read-modify-write
    }
    
    @staticmethod
    def build_postgresql_command(
        params: YCSBParameters,
        credentials: DatabaseCredentials,
        output_file: str
    ) -> str:
        """Build YCSB command for PostgreSQL"""
        
        jdbc_url = f"jdbc:postgresql://{credentials.host}:{credentials.port}/{credentials.database_name}"
        
        cmd_parts = [
            "ycsb",
            params.phase,
            "jdbc",
            f"-P workloads/workload{params.workload_type}",
            f"-p db.driver={YCSBConfigBuilder.JDBC_DRIVERS['postgresql']}",
            f"-p db.url={jdbc_url}",
            f"-p db.user={credentials.username}",
            f"-p db.passwd={credentials.password}",
            f"-p recordcount={params.record_count}",
            f"-p operationcount={params.operation_count}",
            f"-p threadcount={params.thread_count}",
        ]
        
        if params.phase == 'load' and params.load_target_throughput > 0:
            cmd_parts.append(f"-p target={params.load_target_throughput}")
        elif params.phase != 'load' and params.run_target_throughput > 0:
            cmd_parts.append(f"-p target={params.run_target_throughput}")
        
        cmd_parts.extend([
            f"-p core_workload_insertion_retry_limit={params.retry_limit}",
            f"-p core_workload_insertion_retry_interval={params.retry_interval}",
        ])
        
        # Add custom proportions if specified
        if params.read_proportion is not None:
            cmd_parts.append(f"-p readproportion={params.read_proportion}")
        if params.update_proportion is not None:
            cmd_parts.append(f"-p updateproportion={params.update_proportion}")
        if params.insert_proportion is not None:
            cmd_parts.append(f"-p insertproportion={params.insert_proportion}")
        if params.scan_proportion is not None:
            cmd_parts.append(f"-p scanproportion={params.scan_proportion}")
        
        # PostgreSQL specific optimizations
        cmd_parts.extend([
            "-p db.batchsize=1000",
            "-p jdbc.fetchsize=10",
            "-p jdbc.autocommit=true",
            "-p jdbc.batchupdateapi=true",
        ])
        
        cmd_parts.append(f"-s > {output_file}")
        
        return " \\\n  ".join(cmd_parts)
    
    @staticmethod
    def build_mysql_command(
        params: YCSBParameters,
        credentials: DatabaseCredentials,
        output_file: str
    ) -> str:
        """Build YCSB command for MySQL"""
        
        jdbc_url = f"jdbc:mysql://{credentials.host}:{credentials.port}/{credentials.database_name}?useSSL=false&allowPublicKeyRetrieval=true"
        
        cmd_parts = [
            "ycsb",
            params.phase,
            "jdbc",
            f"-P workloads/workload{params.workload_type}",
            f"-p db.driver={YCSBConfigBuilder.JDBC_DRIVERS['mysql']}",
            f"-p db.url={jdbc_url}",
            f"-p db.user={credentials.username}",
            f"-p db.passwd={credentials.password}",
            f"-p recordcount={params.record_count}",
            f"-p operationcount={params.operation_count}",
            f"-p threadcount={params.thread_count}",
        ]
        
        if params.phase == 'load' and params.load_target_throughput > 0:
            cmd_parts.append(f"-p target={params.load_target_throughput}")
        elif params.phase != 'load' and params.run_target_throughput > 0:
            cmd_parts.append(f"-p target={params.run_target_throughput}")
        
        cmd_parts.extend([
            f"-p core_workload_insertion_retry_limit={params.retry_limit}",
            f"-p core_workload_insertion_retry_interval={params.retry_interval}",
        ])
        
        # Add custom proportions if specified
        if params.read_proportion is not None:
            cmd_parts.append(f"-p readproportion={params.read_proportion}")
        if params.update_proportion is not None:
            cmd_parts.append(f"-p updateproportion={params.update_proportion}")
        if params.insert_proportion is not None:
            cmd_parts.append(f"-p insertproportion={params.insert_proportion}")
        if params.scan_proportion is not None:
            cmd_parts.append(f"-p scanproportion={params.scan_proportion}")
        
        # MySQL specific optimizations
        cmd_parts.extend([
            "-p db.batchsize=1000",
            "-p jdbc.fetchsize=10",
            "-p jdbc.autocommit=true",
        ])
        
        cmd_parts.append(f"-s > {output_file}")
        
        return " \\\n  ".join(cmd_parts)
    
    @staticmethod
    def build_oracle_command(
        params: YCSBParameters,
        credentials: DatabaseCredentials,
        output_file: str
    ) -> str:
        """Build YCSB command for Oracle"""
        
        jdbc_url = f"jdbc:oracle:thin:@{credentials.host}:{credentials.port}:{credentials.database_name}"
        
        cmd_parts = [
            "ycsb",
            params.phase,
            "jdbc",
            f"-P workloads/workload{params.workload_type}",
            f"-p db.driver={YCSBConfigBuilder.JDBC_DRIVERS['oracle']}",
            f"-p db.url={jdbc_url}",
            f"-p db.user={credentials.username}",
            f"-p db.passwd={credentials.password}",
            f"-p recordcount={params.record_count}",
            f"-p operationcount={params.operation_count}",
            f"-p threadcount={params.thread_count}",
        ]
        
        if params.phase == 'load' and params.load_target_throughput > 0:
            cmd_parts.append(f"-p target={params.load_target_throughput}")
        elif params.phase != 'load' and params.run_target_throughput > 0:
            cmd_parts.append(f"-p target={params.run_target_throughput}")
        
        cmd_parts.extend([
            f"-p core_workload_insertion_retry_limit={params.retry_limit}",
            f"-p core_workload_insertion_retry_interval={params.retry_interval}",
        ])
        
        # Add custom proportions if specified
        if params.read_proportion is not None:
            cmd_parts.append(f"-p readproportion={params.read_proportion}")
        if params.update_proportion is not None:
            cmd_parts.append(f"-p updateproportion={params.update_proportion}")
        if params.insert_proportion is not None:
            cmd_parts.append(f"-p insertproportion={params.insert_proportion}")
        if params.scan_proportion is not None:
            cmd_parts.append(f"-p scanproportion={params.scan_proportion}")
        
        # Oracle specific optimizations
        cmd_parts.extend([
            "-p db.batchsize=1000",
            "-p jdbc.fetchsize=10",
            "-p jdbc.autocommit=true",
        ])
        
        cmd_parts.append(f"-s > {output_file}")
        
        return " \\\n  ".join(cmd_parts)
    
    @staticmethod
    def build_mssql_command(
        params: YCSBParameters,
        credentials: DatabaseCredentials,
        output_file: str
    ) -> str:
        """Build YCSB command for Microsoft SQL Server"""
        
        jdbc_url = f"jdbc:sqlserver://{credentials.host}:{credentials.port};databaseName={credentials.database_name};encrypt=false"
        
        cmd_parts = [
            "ycsb",
            params.phase,
            "jdbc",
            f"-P workloads/workload{params.workload_type}",
            f"-p db.driver={YCSBConfigBuilder.JDBC_DRIVERS['mssql']}",
            f"-p db.url={jdbc_url}",
            f"-p db.user={credentials.username}",
            f"-p db.passwd={credentials.password}",
            f"-p recordcount={params.record_count}",
            f"-p operationcount={params.operation_count}",
            f"-p threadcount={params.thread_count}",
        ]
        
        if params.phase == 'load' and params.load_target_throughput > 0:
            cmd_parts.append(f"-p target={params.load_target_throughput}")
        elif params.phase != 'load' and params.run_target_throughput > 0:
            cmd_parts.append(f"-p target={params.run_target_throughput}")
        
        cmd_parts.extend([
            f"-p core_workload_insertion_retry_limit={params.retry_limit}",
            f"-p core_workload_insertion_retry_interval={params.retry_interval}",
        ])
        
        # Add custom proportions if specified
        if params.read_proportion is not None:
            cmd_parts.append(f"-p readproportion={params.read_proportion}")
        if params.update_proportion is not None:
            cmd_parts.append(f"-p updateproportion={params.update_proportion}")
        if params.insert_proportion is not None:
            cmd_parts.append(f"-p insertproportion={params.insert_proportion}")
        if params.scan_proportion is not None:
            cmd_parts.append(f"-p scanproportion={params.scan_proportion}")
        
        # MSSQL specific optimizations
        cmd_parts.extend([
            "-p db.batchsize=1000",
            "-p jdbc.fetchsize=10",
            "-p jdbc.autocommit=true",
        ])
        
        cmd_parts.append(f"-s > {output_file}")
        
        return " \\\n  ".join(cmd_parts)
    
    @staticmethod
    def build_mongodb_command(
        params: YCSBParameters,
        credentials: DatabaseCredentials,
        output_file: str
    ) -> str:
        """Build YCSB command for MongoDB"""
        
        # MongoDB uses native binding, not JDBC
        mongodb_url = f"mongodb://{credentials.username}:{credentials.password}@{credentials.host}:{credentials.port}/{credentials.database_name}"
        
        cmd_parts = [
            "ycsb",
            params.phase,
            "mongodb",
            f"-P workloads/workload{params.workload_type}",
            f"-p mongodb.url={mongodb_url}",
            f"-p mongodb.database={credentials.database_name}",
            f"-p recordcount={params.record_count}",
            f"-p operationcount={params.operation_count}",
            f"-p threadcount={params.thread_count}",
        ]
        
        if params.phase == 'load' and params.load_target_throughput > 0:
            cmd_parts.append(f"-p target={params.load_target_throughput}")
        elif params.phase != 'load' and params.run_target_throughput > 0:
            cmd_parts.append(f"-p target={params.run_target_throughput}")
        
        cmd_parts.extend([
            f"-p core_workload_insertion_retry_limit={params.retry_limit}",
            f"-p core_workload_insertion_retry_interval={params.retry_interval}",
        ])
        
        # Add custom proportions if specified
        if params.read_proportion is not None:
            cmd_parts.append(f"-p readproportion={params.read_proportion}")
        if params.update_proportion is not None:
            cmd_parts.append(f"-p updateproportion={params.update_proportion}")
        if params.insert_proportion is not None:
            cmd_parts.append(f"-p insertproportion={params.insert_proportion}")
        if params.scan_proportion is not None:
            cmd_parts.append(f"-p scanproportion={params.scan_proportion}")
        
        # MongoDB specific settings
        cmd_parts.extend([
            "-p mongodb.writeConcern=acknowledged",
            "-p mongodb.readPreference=primary",
            "-p mongodb.maxConnectionPoolSize=100",
        ])
        
        cmd_parts.append(f"-s > {output_file}")
        
        return " \\\n  ".join(cmd_parts)
    
    @staticmethod
    def get_command_builder(engine: str):
        """Get the appropriate command builder for the engine"""
        builders = {
            'postgresql': YCSBConfigBuilder.build_postgresql_command,
            'mysql': YCSBConfigBuilder.build_mysql_command,
            'oracle': YCSBConfigBuilder.build_oracle_command,
            'mssql': YCSBConfigBuilder.build_mssql_command,
            'mongodb': YCSBConfigBuilder.build_mongodb_command,
        }
        return builders.get(engine.lower())
