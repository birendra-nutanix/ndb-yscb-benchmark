#!/usr/bin/env python3
"""
Test script to verify the NDB YCSB Benchmark Generator functionality
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from generators.script_generator import ScriptGenerator
from generators.ycsb_config import YCSBConfigBuilder, YCSBParameters, DatabaseCredentials


def test_script_generation():
    """Test script generation with sample configuration"""
    print("=" * 80)
    print("Testing NDB YCSB Benchmark Generator")
    print("=" * 80)
    print()
    
    # Sample NDB configuration
    ndb_config = {
        "ip": "10.10.10.50",
        "username": "admin",
        "port": 8443,
        "verify_ssl": False
    }
    
    # Selected engines
    selected_engines = ["postgresql", "mysql"]
    
    # YCSB parameters
    ycsb_params = {
        "phase": "both",
        "workload_type": "b",
        "record_count": 10000,
        "operation_count": 10000,
        "thread_count": 10,
        "load_target_throughput": 0,
        "run_target_throughput": 0,
        "retry_limit": 10,
        "retry_interval": 3,
        "timeout": 3600
    }
    
    print("Configuration:")
    print(f"  NDB IP: {ndb_config['ip']}")
    print(f"  Engines: {', '.join(selected_engines)}")
    print(f"  Workload: {ycsb_params['workload_type']}")
    print(f"  Records: {ycsb_params['record_count']:,}")
    print()
    
    # Generate script
    print("Generating script...")
    script_content, script_name = ScriptGenerator.generate_script(
        ndb_config=ndb_config,
        selected_engines=selected_engines,
        ycsb_params=ycsb_params,
        script_name="test_ycsb_benchmark.py"
    )
    
    print(f"✓ Script generated: {script_name}")
    print(f"  Size: {len(script_content):,} bytes")
    print()
    
    # Verify script content
    print("Verifying script content...")
    required_elements = [
        "import multiprocessing",
        "import subprocess",
        "import requests",
        "class NDBClient",
        "class YCSBCommandBuilder",
        "def execute_ycsb_for_database",
        "def generate_html_report",
        "def main()",
        "Chart.js",
    ]
    
    missing_elements = []
    for element in required_elements:
        if element not in script_content:
            missing_elements.append(element)
    
    if missing_elements:
        print("✗ Missing elements:")
        for element in missing_elements:
            print(f"  - {element}")
        return False
    else:
        print("✓ All required elements present")
    
    # Save test script
    output_path = Path(__file__).parent / "generated_scripts" / script_name
    with open(output_path, 'w') as f:
        f.write(script_content)
    
    print(f"✓ Test script saved: {output_path}")
    print()
    
    # Test YCSB command builders
    print("Testing YCSB command builders...")
    
    params = YCSBParameters(
        phase="load",
        workload_type="b",
        record_count=10000,
        operation_count=10000,
        thread_count=10,
        load_target_throughput=0,
        run_target_throughput=0
    )
    
    credentials = DatabaseCredentials(
        username="testuser",
        password="testpass",
        host="10.10.10.100",
        port=5432,
        database_name="testdb"
    )
    
    # Test PostgreSQL command
    pg_cmd = YCSBConfigBuilder.build_postgresql_command(
        params, credentials, "output.txt"
    )
    assert "ycsb load jdbc" in pg_cmd
    assert "org.postgresql.Driver" in pg_cmd
    print("✓ PostgreSQL command builder works")
    
    # Test MySQL command
    mysql_cmd = YCSBConfigBuilder.build_mysql_command(
        params, credentials, "output.txt"
    )
    assert "ycsb load jdbc" in mysql_cmd
    assert "com.mysql.cj.jdbc.Driver" in mysql_cmd
    print("✓ MySQL command builder works")
    
    # Test MongoDB command
    mongo_cmd = YCSBConfigBuilder.build_mongodb_command(
        params, credentials, "output.txt"
    )
    assert "ycsb load mongodb" in mongo_cmd
    assert "mongodb.url" in mongo_cmd
    print("✓ MongoDB command builder works")
    
    # Test Oracle command
    oracle_cmd = YCSBConfigBuilder.build_oracle_command(
        params, credentials, "output.txt"
    )
    assert "ycsb load jdbc" in oracle_cmd
    assert "oracle.jdbc.driver.OracleDriver" in oracle_cmd
    print("✓ Oracle command builder works")
    
    # Test MSSQL command
    mssql_cmd = YCSBConfigBuilder.build_mssql_command(
        params, credentials, "output.txt"
    )
    assert "ycsb load jdbc" in mssql_cmd
    assert "com.microsoft.sqlserver.jdbc.SQLServerDriver" in mssql_cmd
    print("✓ MSSQL command builder works")
    
    print()
    print("=" * 80)
    print("All tests passed! ✓")
    print("=" * 80)
    print()
    print("Next steps:")
    print("  1. Start the web application: python app.py")
    print("  2. Access UI: http://localhost:8000")
    print("  3. Or test the generated script:")
    print(f"     python {output_path}")
    print()
    
    return True


if __name__ == "__main__":
    try:
        success = test_script_generation()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
