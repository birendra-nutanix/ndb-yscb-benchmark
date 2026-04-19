"""
FastAPI Application for NDB YCSB Benchmark Generator
Main web application with REST API endpoints
"""

from fastapi import FastAPI, Request, HTTPException, Response, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
import os
import time
import shutil
import tempfile
import requests
import uvicorn
from pathlib import Path
from datetime import datetime, timedelta, timezone
import uuid
import traceback
import logging

import config
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from validators.ndb_validator import NDBValidator, NDBConnection, DatabaseInfo
from validators.db_credential_validator import DBCredentialValidator
from generators.script_generator import ScriptGenerator
from utils.remote_transfer import test_connection, RemoteTransfer, RemoteTransferConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="NDB YCSB Benchmark Generator",
    description="Generate YCSB benchmark scripts for Nutanix Database Service",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Setup directories
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
TEMPLATES_DIR = BASE_DIR / "templates"
GENERATED_SCRIPTS_DIR = BASE_DIR / "generated_scripts"

# Ensure directories exist
STATIC_DIR.mkdir(exist_ok=True)
TEMPLATES_DIR.mkdir(exist_ok=True)
GENERATED_SCRIPTS_DIR.mkdir(exist_ok=True)

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Setup templates
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# ============================================================================
# Pydantic Models
# ============================================================================

class NDBConnectionRequest(BaseModel):
    """Request model for NDB connection validation"""
    ip: str = Field(..., description="NDB IP address")
    username: str = Field(..., description="NDB username")
    password: str = Field(..., description="NDB password")
    port: int = Field(8443, description="NDB port")
    verify_ssl: bool = Field(False, description="Verify SSL certificate")


class EngineTypeSelection(BaseModel):
    """Engine with selected types and optional specific databases"""
    engine: str = Field(..., description="Database engine name")
    types: List[str] = Field(..., description="Selected deployment types")
    selected_databases: Optional[List[str]] = Field(None, description="List of specific database IDs to include")


class ValidateNDBWithTypesRequest(BaseModel):
    """Request model for NDB validation with engine types"""
    ndb_connection: NDBConnectionRequest
    engine_selections: List[EngineTypeSelection] = Field(..., description="Engine and type selections")


class YCSBParametersRequest(BaseModel):
    """Request model for YCSB parameters"""
    phase: str = Field(..., description="YCSB phase: load, run, or both")
    workload_type: str = Field(..., description="Workload type: a, b, c, d, e, or f")
    record_count: int = Field(..., description="Number of records to load", le=2147483647, gt=0)
    operation_count: int = Field(..., description="Number of operations to perform (0 for unlimited)", le=2147483647, ge=0)
    thread_count: int = Field(..., description="Number of threads", le=10000, gt=0)
    load_target_throughput: int = Field(0, description="Target throughput for load phase (0 for unlimited)", ge=0)
    run_target_throughput: int = Field(0, description="Target throughput for run phase (0 for unlimited)", ge=0)
    retry_limit: int = Field(10, description="Insertion retry limit", ge=0, le=100)
    retry_interval: int = Field(3, description="Insertion retry interval in seconds", ge=1, le=60)
    read_proportion: Optional[float] = Field(None, description="Read proportion override", ge=0, le=1)
    update_proportion: Optional[float] = Field(None, description="Update proportion override", ge=0, le=1)
    insert_proportion: Optional[float] = Field(None, description="Insert proportion override", ge=0, le=1)
    scan_proportion: Optional[float] = Field(None, description="Scan proportion override", ge=0, le=1)
    duration_days: Optional[int] = Field(None, description="Duration in days", ge=1, le=365)
    timeout: int = Field(3600, description="Timeout per database in seconds (0 for unlimited)", ge=0, le=604800)
    
    @validator('timeout')
    def validate_at_least_one_limit(cls, v, values):
        """Ensure at least one of operation_count or timeout is greater than 0"""
        if 'operation_count' in values:
            operation_count = values['operation_count']
            if operation_count == 0 and v == 0:
                raise ValueError('Either operation_count or timeout must be greater than 0. Both cannot be 0.')
        return v
    
    @validator('scan_proportion')
    def validate_proportions_sum(cls, v, values):
        """Ensure proportions sum to 1.0 if any are specified"""
        # Only validate if at least one proportion is specified
        proportions = []
        if values.get('read_proportion') is not None:
            proportions.append(values['read_proportion'])
        if values.get('update_proportion') is not None:
            proportions.append(values['update_proportion'])
        if values.get('insert_proportion') is not None:
            proportions.append(values['insert_proportion'])
        if v is not None:  # scan_proportion
            proportions.append(v)
        
        # If any proportions are specified, check the sum
        if len(proportions) > 0:
            total = sum(proportions)
            # Allow small floating point tolerance
            if abs(total - 1.0) > 0.001:
                raise ValueError(f'Workload proportions must sum to 1.0 (current sum: {total:.3f}). Adjust read/update/insert/scan proportions.')
        
        return v


class DatabaseCredentials(BaseModel):
    """Database credentials per engine"""
    username: str
    password: str


class RemoteTransferConfigRequest(BaseModel):
    """Request model for remote transfer configuration"""
    enabled: bool = Field(False, description="Enable remote transfer")
    host: str = Field("", description="Remote host IP address")
    username: str = Field("", description="SSH username")
    password: str = Field("", description="SSH password")
    target_folder: str = Field("/root/io_script", description="Target folder on remote host")
    port: int = Field(22, description="SSH port")


class GenerateScriptRequest(BaseModel):
    """Request model for script generation"""
    ndb_connection: NDBConnectionRequest
    engine_selections: List[EngineTypeSelection]  # Changed to include types
    db_credentials: Dict[str, DatabaseCredentials]
    ycsb_params: YCSBParametersRequest
    remote_transfer: Optional[RemoteTransferConfigRequest] = Field(None, description="Remote transfer configuration")
    output_directory: Optional[str] = Field(None, description="Output directory for results")
    result_prefix: Optional[str] = Field(None, description="Prefix for result files")


class ValidationResponse(BaseModel):
    """Response model for validation"""
    success: bool
    message: str
    databases: Optional[Dict[str, Any]] = None  # Can be flat or nested structure


class ScriptGenerationResponse(BaseModel):
    """Response model for script generation"""
    success: bool
    message: str
    script_id: Optional[str] = None
    script_name: Optional[str] = None
    is_zip: bool = False  # Indicates if download is a ZIP file
    remote_transfer_success: Optional[bool] = None  # Remote transfer status
    remote_transfer_message: Optional[str] = None  # Remote transfer details


class ValidateDBCredentialsRequest(BaseModel):
    """Request model for database credential validation"""
    databases: Dict[str, Any]  # Nested structure from NDB validation
    db_credentials: Dict[str, DatabaseCredentials]
    test_connectivity_only: bool = False


class DBValidationResult(BaseModel):
    """Result for a single database validation"""
    db_name: str
    host: str
    port: int
    success: bool
    message: str


class ValidateDBCredentialsResponse(BaseModel):
    """Response model for database credential validation"""
    success: bool
    message: str
    results: Optional[Dict[str, List[DBValidationResult]]] = None


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Serve the home page with dashboard selection"""
    return templates.TemplateResponse("base.html", {"request": request})


@app.get("/ycsb", response_class=HTMLResponse)
async def ycsb_dashboard(request: Request):
    """Serve the YCSB Generator page"""
    return templates.TemplateResponse("ycsb.html", {"request": request})


@app.get("/ndb-health", response_class=HTMLResponse)
async def ndb_health(request: Request):
    """Serve the NDB Health Dashboard page"""
    return templates.TemplateResponse("ndb-health.html", {"request": request})


@app.post("/api/validate-ndb-with-types", response_model=ValidationResponse)
async def validate_ndb_with_types(request: ValidateNDBWithTypesRequest):
    """
    Validate NDB connection and fetch databases filtered by engine types
    """
    try:
        # Create NDB connection
        ndb_conn = NDBConnection(
            ip=request.ndb_connection.ip,
            username=request.ndb_connection.username,
            password=request.ndb_connection.password,
            port=request.ndb_connection.port,
            verify_ssl=request.ndb_connection.verify_ssl
        )
        
        # Create validator
        validator = NDBValidator(ndb_conn)
        
        # Validate connection
        success, message = validator.validate_connection()
        
        if not success:
            return ValidationResponse(
                success=False,
                message=message,
                databases=None
            )
        
        # Convert engine selections to dict format
        engine_selections = [
            {
                "engine": sel.engine, 
                "types": sel.types,
                "selected_databases": sel.selected_databases
            }
            for sel in request.engine_selections
        ]
        
        # Fetch databases filtered by type
        success, message, databases = validator.fetch_databases_by_type(engine_selections)
        
        if not success:
            return ValidationResponse(
                success=False,
                message=message,
                databases=None
            )
        
        # Convert DatabaseInfo objects to dictionaries (nested structure)
        databases_dict = {}
        for engine, type_groups in databases.items():
            databases_dict[engine] = {}
            for db_type, db_list in type_groups.items():
                databases_dict[engine][db_type] = [db.dict() for db in db_list]
        
        # Count total databases
        total = sum(
            len(db_list)
            for type_groups in databases_dict.values()
            for db_list in type_groups.values()
        )
        
        return ValidationResponse(
            success=True,
            message=f"Found {total} database(s) matching selected types",
            databases=databases_dict
        )
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error("NDB validation failed")
        logger.error("=" * 80)
        logger.error(f"Exception: {str(e)}")
        logger.error(traceback.format_exc())
        logger.error("=" * 80)
        return ValidationResponse(
            success=False,
            message=f"Validation error: {str(e)}",
            databases=None
        )


@app.post("/api/validate-db-credentials", response_model=ValidateDBCredentialsResponse)
async def validate_db_credentials(request: ValidateDBCredentialsRequest):
    """
    Validate database credentials by attempting connections
    """
    try:
        # Flatten nested database structure
        flat_databases = {}
        for engine, type_groups in request.databases.items():
            if isinstance(type_groups, dict):
                # Nested structure (engine -> type -> list)
                db_list = []
                for db_type, dbs in type_groups.items():
                    db_list.extend(dbs)
                flat_databases[engine] = db_list
            else:
                # Already flat (engine -> list)
                flat_databases[engine] = type_groups
        
        # Convert credentials to dict format
        creds_dict = {}
        for engine, creds in request.db_credentials.items():
            creds_dict[engine] = {
                "username": creds.username,
                "password": creds.password
            }
        # Validate credentials
        all_success, summary, results = DBCredentialValidator.validate_multiple_databases(
            databases=flat_databases,
            credentials=creds_dict,
            test_connectivity_only=request.test_connectivity_only
        )
        
        # Convert results to response format
        response_results = {}
        for engine, engine_results in results.items():
            response_results[engine] = [
                DBValidationResult(
                    db_name=r['db_name'],
                    host=r['host'],
                    port=r['port'],
                    success=r['success'],
                    message=r['message']
                )
                for r in engine_results
            ]
        
        return ValidateDBCredentialsResponse(
            success=all_success,
            message=summary,
            results=response_results
        )
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error("Database credential validation failed")
        logger.error("=" * 80)
        logger.error(f"Exception: {str(e)}")
        logger.error(traceback.format_exc())
        logger.error("=" * 80)
        return ValidateDBCredentialsResponse(
            success=False,
            message=f"Credential validation error: {str(e)}",
            results=None
        )


@app.post("/api/test-ssh-connection")
async def test_ssh_connection(request: RemoteTransferConfigRequest):
    """
    Test SSH connection to remote host
    """
    try:
        if not request.host or not request.username or not request.password:
            return {
                "success": False,
                "message": "Host, username, and password are required"
            }
        
        logger.info(f"Testing SSH connection to {request.host}")
        success, message = test_connection(
            host=request.host,
            username=request.username,
            password=request.password,
            port=request.port
        )
        
        return {
            "success": success,
            "message": message
        }
        
    except Exception as e:
        logger.error(f"SSH connection test failed: {str(e)}\n{traceback.format_exc()}")
        return {
            "success": False,
            "message": f"Connection test error: {str(e)}"
        }


@app.post("/api/generate-script", response_model=ScriptGenerationResponse)
async def generate_script(request: GenerateScriptRequest):
    """
    Generate YCSB benchmark script
    """
    try:
        logger.info("=" * 80)
        logger.info("SCRIPT GENERATION REQUEST")
        logger.info("=" * 80)
        
        # Prepare NDB configuration
        # Note: NDB credentials are needed to query NDB for current database metadata
        # (IPs, ports, database names). They are NOT embedded in the generated scripts.
        # Only database credentials are embedded in the scripts.
        ndb_config = {
            "ip": request.ndb_connection.ip,
            "username": request.ndb_connection.username,
            "port": request.ndb_connection.port,
            "verify_ssl": request.ndb_connection.verify_ssl
        }
        
        # Prepare database credentials
        db_creds = {}
        for engine, creds in request.db_credentials.items():
            db_creds[engine] = {
                "username": creds.username,
                "password": creds.password
            }
        
        # Prepare YCSB parameters
        ycsb_params = {
            "phase": request.ycsb_params.phase,
            "workload_type": request.ycsb_params.workload_type,
            "record_count": request.ycsb_params.record_count,
            "operation_count": request.ycsb_params.operation_count,
            "threads": request.ycsb_params.thread_count,  # Map to 'threads' for shell scripts
            "load_target_throughput": request.ycsb_params.load_target_throughput,
            "run_target_throughput": request.ycsb_params.run_target_throughput,
            "retry_limit": request.ycsb_params.retry_limit,
            "retry_interval": request.ycsb_params.retry_interval,
            "timeout": request.ycsb_params.timeout,
            "read_proportion": request.ycsb_params.read_proportion,
            "update_proportion": request.ycsb_params.update_proportion,
            "insert_proportion": request.ycsb_params.insert_proportion,
            "scan_proportion": request.ycsb_params.scan_proportion
        }
        
        # Add optional proportions if specified
        if request.ycsb_params.read_proportion is not None:
            ycsb_params["read_proportion"] = request.ycsb_params.read_proportion
        if request.ycsb_params.update_proportion is not None:
            ycsb_params["update_proportion"] = request.ycsb_params.update_proportion
        if request.ycsb_params.insert_proportion is not None:
            ycsb_params["insert_proportion"] = request.ycsb_params.insert_proportion
        if request.ycsb_params.scan_proportion is not None:
            ycsb_params["scan_proportion"] = request.ycsb_params.scan_proportion
        if request.ycsb_params.duration_days is not None:
            ycsb_params["duration_days"] = request.ycsb_params.duration_days
        
        # Add password to ndb_config for script generation
        ndb_config["password"] = request.ndb_connection.password
        
        # Convert engine_selections to list of dicts
        engine_selections = []
        for selection in request.engine_selections:
            engine_selections.append({
                "engine": selection.engine,
                "types": selection.types,
                "selected_databases": selection.selected_databases
            })
        
        # Generate shell scripts package (ZIP)
        zip_path, zip_name, is_zip = ScriptGenerator.generate_script(
            ndb_config=ndb_config,
            engine_selections=engine_selections,
            db_credentials=db_creds,
            ycsb_params=ycsb_params
        )
        
        # Generate unique script ID
        script_id = str(uuid.uuid4())
        
        # Move ZIP to generated scripts directory
        final_path = GENERATED_SCRIPTS_DIR / f"{script_id}_{zip_name}"
        shutil.move(zip_path, final_path)
        
        # Handle remote transfer if enabled
        remote_transfer_success = None
        remote_transfer_message = None
        
        if request.remote_transfer and request.remote_transfer.enabled:
            try:
                logger.info("Remote transfer enabled, initiating transfer...")
                
                # Create transfer config
                transfer_config = RemoteTransferConfig(
                    host=request.remote_transfer.host,
                    username=request.remote_transfer.username,
                    password=request.remote_transfer.password,
                    target_folder=request.remote_transfer.target_folder,
                    port=request.remote_transfer.port
                )
                
                # Create transfer instance and execute
                transfer = RemoteTransfer(transfer_config)
                remote_transfer_success, remote_transfer_message = transfer.transfer_and_extract(str(final_path))
                
                if remote_transfer_success:
                    logger.info(f"Remote transfer successful: {remote_transfer_message}")
                    
                    # Delete local ZIP file after successful remote transfer
                    try:
                        final_path.unlink()
                        logger.info(f"Deleted local file after successful transfer: {final_path.name}")
                    except Exception as e:
                        logger.warning(f"Failed to delete local file {final_path.name}: {e}")
                else:
                    logger.error(f"Remote transfer failed: {remote_transfer_message}")
                    
            except Exception as e:
                remote_transfer_success = False
                remote_transfer_message = f"Remote transfer error: {str(e)}"
                logger.error(f"{remote_transfer_message}\n{traceback.format_exc()}")
        
        # Build response message
        if remote_transfer_success:
            message = f"Scripts generated and deployed to {request.remote_transfer.host}:{request.remote_transfer.target_folder}"
        else:
            message = "Shell scripts package generated successfully"
        
        return ScriptGenerationResponse(
            success=True,
            message=message,
            script_id=script_id,
            script_name=zip_name,
            is_zip=is_zip,
            remote_transfer_success=remote_transfer_success,
            remote_transfer_message=remote_transfer_message
        )
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error("Script generation failed")
        logger.error("=" * 80)
        logger.error(f"Exception: {str(e)}")
        logger.error(traceback.format_exc())
        logger.error("=" * 80)
        return ScriptGenerationResponse(
            success=False,
            message=f"Script generation error: {str(e)}",
            script_id=None,
            script_name=None
        )


@app.get("/api/download-script/{script_id}/{script_name}")
async def download_script(script_id: str, script_name: str):
    """
    Download generated script or ZIP package
    """
    try:
        script_path = GENERATED_SCRIPTS_DIR / f"{script_id}_{script_name}"
        
        if not script_path.exists():
            raise HTTPException(status_code=404, detail="Script not found")
        
        # Determine media type based on file extension
        media_type = "application/zip" if script_name.endswith('.zip') else "text/x-python"
        
        return FileResponse(
            path=script_path,
            filename=script_name,
            media_type=media_type
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("=" * 80)
        logger.error("Script download failed")
        logger.error("=" * 80)
        logger.error(f"Exception: {str(e)}")
        logger.error(traceback.format_exc())
        logger.error("=" * 80)
        raise HTTPException(status_code=500, detail=f"Download error: {str(e)}")


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }


# ============================================================================
# NDB Health Dashboard APIs
# ============================================================================

class NDBHealthConnectionRequest(BaseModel):
    """Request model for NDB Health connection"""
    ip: str
    port: int = 8443
    username: str
    password: str


@app.post("/api/ndb-health/overview")
async def get_ndb_health_overview(request: NDBHealthConnectionRequest):
    """
    Get overview of all databases in NDB with deployment type breakdown
    
    Args:
        request: NDB connection credentials
        
    Returns:
        Dictionary with summary and per-engine breakdown including deployment types
    """
    try:
        # Create NDB connection
        ndb_conn = NDBConnection(
            ip=request.ip,
            port=request.port,
            username=request.username,
            password=request.password
        )
        
        # Create validator and fetch databases
        validator = NDBValidator(ndb_conn)
        success, message, databases = validator.fetch_all_databases()
        if not success:
            raise HTTPException(status_code=400, detail=message)
        
        # Initialize response structure
        response = {
            "summary": {
                "total": 0,
                "ready": 0,
                "not_ready": 0
            },
            "by_engine": {
                "postgres": {
                    "total": 0,
                    "ready": 0,
                    "by_type": {
                        "si": 0,
                        "patroni": 0,
                        "clone": 0
                    }
                },
                "mysql": {
                    "total": 0,
                    "ready": 0,
                    "by_type": {
                        "si": 0,
                        "innodb_cluster": 0,
                        "clone": 0
                    }
                },
                "mariadb": {
                    "total": 0,
                    "ready": 0,
                    "by_type": {
                        "si": 0,
                        "innodb_cluster": 0,
                        "clone": 0
                    }
                },
                "oracle": {
                    "total": 0,
                    "ready": 0,
                    "by_type": {
                        "si": 0,
                        "rac": 0,
                        "clone": 0
                    }
                },
                "mssql": {
                    "total": 0,
                    "ready": 0,
                    "by_type": {
                        "si": 0,
                        "aag": 0,
                        "clone": 0
                    }
                },
                "mongodb": {
                    "total": 0,
                    "ready": 0,
                    "by_type": {
                        "si": 0,
                        "replicaset": 0,
                        "shard_replica_set": 0,
                        "clone": 0
                    }
                }
            }
        }
        
        # Process each database
        for db in databases:
            response["summary"]["total"] += 1
            
            # Check if database is ready
            status = db.get("status", "").upper()
            is_ready = (status == "READY")
            if is_ready:
                response["summary"]["ready"] += 1
            else:
                response["summary"]["not_ready"] += 1
            
            # Determine engine type
            db_type = db.get("type", "").lower()

            logicalClusterType = db.get("logicalClusterType", None)
            
            engine = None
            deployment_type = "si"
            
            if "postgres_database" in db_type:
                engine = "postgres"
                deployment_type = "PATRONI".lower() if logicalClusterType else "si"
            elif "mysql_database" in db_type:
                engine = "mysql"
                deployment_type = "INNODB_CLUSTER".lower() if logicalClusterType else "si"
            elif "mariadb_database" in db_type:
                engine = "mariadb"
                deployment_type = "INNODB_CLUSTER".lower() if logicalClusterType else "si"
            elif "oracle_database" in db_type:
                engine = "oracle"
                deployment_type = "rac".lower() if logicalClusterType else "si"
            elif "sqlserver_database" in db_type:
                engine = "mssql"
                deployment_type = "aag" if logicalClusterType else "si"
            elif "mongodb_database" in db_type:
                engine = "mongodb"
                if logicalClusterType:
                    if "REPLICASET" in logicalClusterType.upper():
                        deployment_type = "REPLICASET".lower()
                    else:
                        deployment_type = "SHARD_REPLICA_SET".lower()
                else:
                    deployment_type = "si"
            
            # If it's a clone, override deployment type
            if db.get("is_clone"):
                deployment_type = "clone"
            
            # Update counts
            if engine and engine in response["by_engine"]:
                response["by_engine"][engine]["total"] += 1
                if is_ready:
                    response["by_engine"][engine]["ready"] += 1
                
                if deployment_type in response["by_engine"][engine]["by_type"]:
                    response["by_engine"][engine]["by_type"][deployment_type] += 1

        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching NDB overview: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/ndb-health/alerts")
async def get_ndb_alerts(request: NDBHealthConnectionRequest, days: int = 1):
    """
    Get NDB alerts from last N days
    
    Args:
        request: NDB connection credentials
        days: Number of days to look back (1, 3, 7, 14, or 30)
        
    Returns:
        List of alerts from NDB
    """
    try:
        # Create NDB connection
        ndb_url = f"https://{request.ip}:{request.port}"
        
        # Authenticate using the same method as NDBValidator
        auth_url = f"{ndb_url}/era/v0.9/auth/token"
        auth_response = requests.get(
            auth_url,
            auth=(request.username, request.password),
            params={"expire": 240},
            verify=False,
            timeout=30
        )
        
        if auth_response.status_code != 200:
            raise HTTPException(status_code=401, detail=f"NDB authentication failed: {auth_response.text}")
        
        token_data = auth_response.json()
        token = token_data.get('token') or auth_response.headers.get('Authorization')
        
        if not token:
            raise HTTPException(status_code=401, detail="Authentication succeeded but no token received")
            
        headers = {
            "Authorization": f"Bearer {token}" if not str(token).startswith("Bearer") else token,
            "Content-Type": "application/json"
        }
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Fetch alerts
        alerts_response = requests.get(
            f"{ndb_url}/era/v0.9/alerts",
            headers=headers,
            verify=False,
            timeout=30
        )
        
        if alerts_response.status_code != 200:
            logger.warning(f"Failed to fetch alerts: {alerts_response.status_code}")
            return []
        
        alerts_data = alerts_response.json()
        alerts = alerts_data.get("entities", [])
        
        # Filter by date if needed
        filtered_alerts = []
        for alert in alerts:
            alert_date_str = alert.get("dateCreated")
            if alert_date_str:
                try:
                    alert_date = datetime.fromisoformat(alert_date_str.replace('Z', '+00:00'))
                    # Make start_date timezone-aware for comparison
                    if alert_date.tzinfo is not None and start_date.tzinfo is None:
                        start_date = start_date.replace(tzinfo=timezone.utc)
                    
                    if alert_date >= start_date:
                        filtered_alerts.append(alert)
                except Exception as e:
                    logger.warning(f"Failed to parse date {alert_date_str}: {e}")
                    filtered_alerts.append(alert)  # Include if date parsing fails
            else:
                filtered_alerts.append(alert)
        
        return filtered_alerts
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching alerts: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/ndb-health/operations")
async def get_ndb_operations(request: NDBHealthConnectionRequest, days: int = 1):
    """
    Get NDB operations from last N days
    
    Args:
        request: NDB connection credentials
        days: Number of days to look back (1, 3, 7, 14, or 30)
        
    Returns:
        Dictionary with successful and failed operations counts by type
    """
    try:
        # Create NDB connection
        ndb_url = f"https://{request.ip}:{request.port}"
        
        # Authenticate using the same method as NDBValidator
        auth_url = f"{ndb_url}/era/v0.9/auth/token"
        auth_response = requests.get(
            auth_url,
            auth=(request.username, request.password),
            params={"expire": 240},
            verify=False,
            timeout=30
        )
        
        if auth_response.status_code != 200:
            raise HTTPException(status_code=401, detail=f"NDB authentication failed: {auth_response.text}")
        
        token_data = auth_response.json()
        token = token_data.get('token') or auth_response.headers.get('Authorization')
        
        if not token:
            raise HTTPException(status_code=401, detail="Authentication succeeded but no token received")
            
        headers = {
            "Authorization": f"Bearer {token}" if not str(token).startswith("Bearer") else token,
            "Content-Type": "application/json"
        }
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # --- Fetch databases to map entity names to DB types ---
        ndb_conn = NDBConnection(
            ip=request.ip,
            port=request.port,
            username=request.username,
            password=request.password
        )
        validator = NDBValidator(ndb_conn)
        success, _, databases = validator.fetch_all_databases()
        
        db_map = {}
        if success:
            for db in databases:
                db_name = db.get("name")
                if not db_name:
                    continue
                    
                db_type_raw = db.get("type", "").lower()
                logicalClusterType = db.get("logicalClusterType", None)
                
                engine = "unknown"
                deployment_type = "si"
                
                if "postgres_database" in db_type_raw:
                    engine = "postgres"
                    deployment_type = "patroni" if logicalClusterType else "si"
                elif "mysql_database" in db_type_raw:
                    engine = "mysql"
                    deployment_type = "innodb_cluster" if logicalClusterType else "si"
                elif "mariadb_database" in db_type_raw:
                    engine = "mariadb"
                    deployment_type = "innodb_cluster" if logicalClusterType else "si"
                elif "oracle_database" in db_type_raw:
                    engine = "oracle"
                    deployment_type = "rac" if logicalClusterType else "si"
                elif "sqlserver_database" in db_type_raw:
                    engine = "mssql"
                    deployment_type = "aag" if logicalClusterType else "si"
                elif "mongodb_database" in db_type_raw:
                    engine = "mongodb"
                    if logicalClusterType:
                        deployment_type = "replicaset" if "REPLICASET" in logicalClusterType.upper() else "shard_replica_set"
                    else:
                        deployment_type = "si"
                
                # If it's a clone, override deployment type
                if db.get("is_clone"):
                    deployment_type = "clone"
                
                db_map[db_name] = f"{engine} {deployment_type}"

        # --- END Fetch databases ---
        
        # Fetch operations with pagination
        all_operations = []
        limit = 100
        skip = 0
        
        while True:
            # Construct the URL with pagination parameters
            operations_url = (
                f"{ndb_url}/era/v0.9/operations/short-info"
                f"?hide-subops=true&user-triggered=true&system-triggered=false"
                f"&time-zone=Asia/Calcutta&count-summary=true"
                f"&limit={limit}&skip={skip}&days={days}&descending=false"
            )
            
            operations_response = requests.get(
                operations_url,
                headers=headers,
                verify=False,
                timeout=30
            )
            
            if operations_response.status_code != 200:
                logger.warning(f"Failed to fetch operations: {operations_response.status_code}")
                # If we already have some operations from previous pages, we can continue with them
                # Otherwise, return empty stats
                if not all_operations:
                    return {
                        "successful": {},
                        "failed": {},
                        "total_successful": 0,
                        "total_failed": 0
                    }
                break
            
            operations_data = operations_response.json()
            
            # The new API returns the list in the "operations" key
            current_page_ops = operations_data.get("operations", [])
            all_operations.extend(current_page_ops)
            
            # Check if we need to fetch more
            summary = operations_data.get("summary", {})
            total_count = summary.get("filteredEntityCount", 0)
            
            # If we've fetched all available operations or the current page is empty, break the loop
            if not current_page_ops or len(current_page_ops) < limit:
                break
                
            # Increment skip for the next page
            skip += limit
        
        # Count operations by engine, type, and status
        operations_by_engine = {}
        
        # Helper to get engine and deployment type
        def get_op_engine_and_type(op):
            entity_name = op.get("entityName")
            if entity_name and entity_name in db_map:
                return db_map[entity_name].split(" ", 1)
            
            return None, None
            
        total_successful = 0
        total_failed = 0
        
        for op in all_operations:
            op_date_str = op.get("dateCreated") or op.get("startTime")  # The new API uses "startTime"
            if op_date_str:
                try:
                    # Handle date parsing
                    op_date = datetime.fromisoformat(op_date_str.replace('Z', '+00:00'))
                    
                    # Make start_date timezone-aware for comparison
                    if op_date.tzinfo is not None and start_date.tzinfo is None:
                        start_date = start_date.replace(tzinfo=timezone.utc)
                    
                    if op_date < start_date:
                        continue
                except Exception as e:
                    logger.warning(f"Failed to parse date {op_date_str}: {e}")
                    pass  # Include if date parsing fails
            
            # Get engine and deployment type
            engine, deployment_type = get_op_engine_and_type(op)
            
            # Skip operations that don't have a mapped entity name
            if not engine:
                logger.info(f"Skipping operation for missing/unmapped database: {op.get('entityName')} (Operation Type: {op.get('type')})")
                continue
                
            # The new API has "type" instead of "operationType"
            op_type = op.get("type", "unknown") 
            
            # The new API returns status as an integer string (e.g., "5" for SUCCESS)
            # Status map (common NDB statuses): "5" = Completed/Success, "4" = Failed, "1" = Running
            status = str(op.get("status", ""))
            is_success = status == "5" or status.upper() in ["COMPLETED", "SUCCESS"]
            is_failed = status == "4" or status.upper() in ["FAILED", "ERROR", "TIMEOUT"]
            
            if is_success or is_failed:
                if engine not in operations_by_engine:
                    operations_by_engine[engine] = {}
                if deployment_type not in operations_by_engine[engine]:
                    operations_by_engine[engine][deployment_type] = {"successful": {}, "failed": {}}
                    
                if is_success:
                    operations_by_engine[engine][deployment_type]["successful"][op_type] = \
                        operations_by_engine[engine][deployment_type]["successful"].get(op_type, 0) + 1
                    total_successful += 1
                elif is_failed:
                    operations_by_engine[engine][deployment_type]["failed"][op_type] = \
                        operations_by_engine[engine][deployment_type]["failed"].get(op_type, 0) + 1
                    total_failed += 1
        
        return {
            "operations_by_engine": operations_by_engine,
            "total_successful": total_successful,
            "total_failed": total_failed
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching operations: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# InfluxDB Sync
# ============================================================================

sync_tasks = {}

class SyncRequest(BaseModel):
    ndb_connection: dict
    days: int = 30

def sync_operations_to_influxdb(task_id: str, ndb_conn: dict, days: int):
    """Background task to fetch operations and store in InfluxDB"""
    try:
        sync_tasks[task_id] = {"status": "running", "message": "Authenticating with NDB...", "progress": 5}
        ndb_url = f"https://{ndb_conn['ip']}:{ndb_conn['port']}"
        
        # Authenticate
        auth = (ndb_conn['username'], ndb_conn['password'])
        auth_response = requests.get(
            f"{ndb_url}/era/v0.9/auth/token",
            auth=auth,
            params={"expire": 240},
            verify=False,
            timeout=30
        )
        if auth_response.status_code != 200:
            sync_tasks[task_id] = {"status": "failed", "message": f"NDB authentication failed: {auth_response.status_code}"}
            return
            
        token = auth_response.json().get("token")
        if not token:
            token = auth_response.headers.get("Authorization")
            
        if not token:
            sync_tasks[task_id] = {"status": "failed", "message": "NDB authentication succeeded but no token received"}
            return
            
        if not token.startswith("Bearer "):
            token = f"Bearer {token}"
            
        headers = {
            "Authorization": token,
            "Accept": "application/json"
        }
        
        # Get NDB Version and Commit ID
        sync_tasks[task_id] = {"status": "running", "message": "Fetching NDB info...", "progress": 10}
        ndb_version = "unknown"
        commit_id = "unknown"
        try:
            about_resp = requests.get(f"{ndb_url}/era/v0.9/services/about", headers=headers, verify=False, timeout=10)
            if about_resp.status_code == 200:
                about_data = about_resp.json()
                # ndb_version = about_data.get("version", "unknown")
                ndb_version = about_data.get("version", "unknown")
                commit_id = about_data.get("buildId", "unknown")
            elif about_resp.status_code in [401, 403]:
                sync_tasks[task_id] = {"status": "failed", "message": "NDB authentication failed on about endpoint"}
                return
        except Exception as e:
            logger.warning(f"Failed to fetch NDB about info: {e}")

        # Fetch databases to map entityName to engine/type and store in InfluxDB
        sync_tasks[task_id] = {"status": "running", "message": "Fetching databases...", "progress": 15}
        
        from validators.ndb_validator import NDBValidator, NDBConnection
        ndb_conn_obj = NDBConnection(
            ip=ndb_conn['ip'],
            username=ndb_conn['username'],
            password=ndb_conn['password'],
            port=ndb_conn.get('port', 8443),
            verify_ssl=ndb_conn.get('verify_ssl', False)
        )
        validator = NDBValidator(ndb_conn_obj)
        success, _, databases = validator.fetch_all_databases()
        
        db_map = {}
        lines = []
        
        def escape_tag(val):
            if val is None: return "unknown"
            return str(val).replace(',', '\\,').replace('=', '\\=').replace(' ', '\\ ')
            
        current_time_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
        
        if success:
            for db in databases:
                db_name = db.get("name")
                if not db_name:
                    continue
                    
                db_type_raw = db.get("type", "").lower()
                logicalClusterType = db.get("logicalClusterType", None)
                
                engine = "unknown"
                deployment_type = "si"
                
                if "postgres_database" in db_type_raw:
                    engine = "postgres"
                    deployment_type = "patroni" if logicalClusterType else "si"
                elif "mysql_database" in db_type_raw:
                    engine = "mysql"
                    deployment_type = "innodb_cluster" if logicalClusterType else "si"
                elif "mariadb_database" in db_type_raw:
                    engine = "mariadb"
                    deployment_type = "innodb_cluster" if logicalClusterType else "si"
                elif "oracle_database" in db_type_raw:
                    engine = "oracle"
                    deployment_type = "rac" if logicalClusterType else "si"
                elif "sqlserver_database" in db_type_raw:
                    engine = "mssql"
                    deployment_type = "aag" if logicalClusterType else "si"
                elif "mongodb_database" in db_type_raw:
                    engine = "mongodb"
                    if logicalClusterType:
                        deployment_type = "replicaset" if "REPLICASET" in logicalClusterType.upper() else "shard_replica_set"
                    else:
                        deployment_type = "si"
                
                # If it's a clone, override deployment type
                if db.get("is_clone"):
                    deployment_type = "clone"
                
                db_map[db_name] = f"{engine} {deployment_type}"
                
                # Create InfluxDB line protocol for database inventory
                db_tags = [
                    f"ndb_ip={escape_tag(ndb_conn['ip'])}",
                    f"ndb_version={escape_tag(ndb_version)}",
                    f"commit_id={escape_tag(commit_id)}",
                    f"database={escape_tag(db_name)}",
                    f"engine={escape_tag(engine)}",
                    f"database_type={escape_tag(deployment_type)}"
                ]
                db_fields = ["count=1i"]
                db_line = f"ndb_databases,{','.join(db_tags)} {','.join(db_fields)} {current_time_ns}"
                lines.append(db_line)

        # Fetch operations
        sync_tasks[task_id] = {"status": "running", "message": "Fetching operations...", "progress": 30}
        all_operations = []
        limit = 100
        skip = 0
        
        while True:
            operations_url = (
                f"{ndb_url}/era/v0.9/operations/short-info"
                f"?hide-subops=true&user-triggered=true&system-triggered=false"
                f"&time-zone=Asia/Calcutta&count-summary=true"
                f"&limit={limit}&skip={skip}&days={days}&descending=false"
            )
            operations_response = requests.get(operations_url, headers=headers, verify=False, timeout=30)
            if operations_response.status_code != 200:
                logger.error(f"Failed to fetch operations: {operations_response.text}")
                break
            operations_data = operations_response.json()
            current_page_ops = operations_data.get("operations", [])
            all_operations.extend(current_page_ops)
            
            total_count = operations_data.get("summary", {}).get("filteredEntityCount", 0)
            
            # If we've fetched all available operations or the current page is empty, break
            if not current_page_ops or len(current_page_ops) < limit:
                break
                
            skip += limit

        if not all_operations:
            sync_tasks[task_id] = {"status": "completed", "message": "No operations found in NDB for the given period.", "progress": 100}
            return

        # Connect to InfluxDB using direct HTTP API
        sync_tasks[task_id] = {"status": "running", "message": "Connecting to InfluxDB...", "progress": 60}
        
        influx_query_url = f"{config.INFLUXDB_URL}/api/v2/query?org={config.INFLUXDB_ORG}"
        influx_write_url = f"{config.INFLUXDB_URL}/api/v2/write?org={config.INFLUXDB_ORG}&bucket={config.INFLUXDB_BUCKET}&precision=ns"
        
        influx_headers = {
            "Authorization": f"Token {config.INFLUXDB_TOKEN}",
            "Content-Type": "application/vnd.flux",
            "Accept": "application/csv"
        }
        
        # Query existing operations
        sync_tasks[task_id] = {"status": "running", "message": "Checking existing records...", "progress": 70}
        query = f'''
            from(bucket: "{config.INFLUXDB_BUCKET}")
            |> range(start: -{days}d)
            |> filter(fn: (r) => r["_measurement"] == "ndb_operations")
            |> keep(columns: ["operation_id"])
            |> distinct(column: "operation_id")
        '''
        existing_ops = set()
        try:
            query_resp = requests.post(influx_query_url, headers=influx_headers, data=query, timeout=30)
            if query_resp.status_code == 200:
                # Parse simple CSV response
                for line in query_resp.text.split('\n'):
                    parts = line.split(',')
                    if len(parts) > 3 and parts[3] and parts[3] != 'operation_id' and parts[3] != '_result':
                        existing_ops.add(parts[3].strip())
            else:
                logger.warning(f"Failed to query existing operations from InfluxDB: {query_resp.status_code} - {query_resp.text}")
        except Exception as e:
            logger.warning(f"Failed to query existing operations from InfluxDB: {e}")
            
        # Process and write
        sync_tasks[task_id] = {"status": "running", "message": "Writing to InfluxDB...", "progress": 85}
        
        for op in all_operations:
            op_id = op.get("id")

            if not op_id or op_id in existing_ops:
                continue
                
            entity_name = op.get("entityName", "unknown")
            engine_deploy = db_map.get(entity_name, "unknown unknown").split(" ", 1)
            engine = engine_deploy[0] if len(engine_deploy) > 0 else "unknown"
            deploy_type = engine_deploy[1] if len(engine_deploy) > 1 else "unknown"
            op_type = op.get("type", "unknown")
            
            status_code = str(op.get("status", ""))
            is_success = status_code == "5" or status_code.upper() in ["COMPLETED", "SUCCESS"]
            is_failed = status_code == "4" or status_code.upper() in ["FAILED", "ERROR", "TIMEOUT"]
            status_str = "success" if is_success else ("failed" if is_failed else "running")
            
            # Skip operations that are not in a terminal state
            if status_str == "running":
                continue
            
            # Timestamps
            end_time_str = op.get("endTime") or op.get("startTime") or op.get("dateCreated")
            if not end_time_str:
                continue
                
            try:
                dt = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
                # Convert to nanoseconds for InfluxDB
                timestamp_ns = int(dt.timestamp() * 1e9)
            except Exception:
                continue
                
            duration_ms = 0
            if op.get("startTime") and op.get("endTime"):
                try:
                    start_dt = datetime.fromisoformat(op.get("startTime").replace('Z', '+00:00'))
                    end_dt = datetime.fromisoformat(op.get("endTime").replace('Z', '+00:00'))
                    duration_ms = int((end_dt - start_dt).total_seconds() * 1000)
                except Exception:
                    pass
            
            pct_complete = op.get("percentageComplete", 100)
            if pct_complete is None: pct_complete = 100
            
            # Build line protocol string
            tags = [
                f"ndb_ip={escape_tag(ndb_conn['ip'])}",
                f"ndb_version={escape_tag(ndb_version)}",
                f"commit_id={escape_tag(commit_id)}",
                f"operation_id={escape_tag(op_id)}",
                f"database={escape_tag(entity_name)}",
                f"database_type={escape_tag(deploy_type)}",
                f"engine={escape_tag(engine)}",
                f"operation_type={escape_tag(op_type)}",
                f"status={escape_tag(status_str)}"
            ]
            
            fields = [
                f"duration_ms={duration_ms}i",
                f"percentage_complete=\"{int(pct_complete)}\"",
                f"count=1i"
            ]
            
            line = f"ndb_operations,{','.join(tags)} {','.join(fields)} {timestamp_ns}"
            lines.append(line)
            
        if lines:
            write_headers = {
                "Authorization": f"Token {config.INFLUXDB_TOKEN}",
                "Content-Type": "text/plain; charset=utf-8"
            }
            # Write in batches of 5000 to be safe
            batch_size = 5000
            for i in range(0, len(lines), batch_size):
                batch = '\n'.join(lines[i:i+batch_size])
                write_resp = requests.post(influx_write_url, headers=write_headers, data=batch.encode('utf-8'), timeout=30)
                if write_resp.status_code not in [200, 204]:
                    logger.error(f"InfluxDB write failed: {write_resp.status_code} - {write_resp.text}")
                    sync_tasks[task_id] = {"status": "failed", "message": f"InfluxDB write failed: {write_resp.text}", "progress": 100}
                    return
            
        sync_tasks[task_id] = {
            "status": "completed", 
            "message": f"Successfully synced {len(lines)} new operations to InfluxDB.", 
            "progress": 100,
            "synced_count": len(lines)
        }
        
    except Exception as e:
        logger.error(f"InfluxDB Sync failed: {e}")
        logger.error(traceback.format_exc())
        sync_tasks[task_id] = {"status": "failed", "message": f"Error: {str(e)}", "progress": 100}


@app.post("/api/ndb-health/sync-influxdb")
async def trigger_influxdb_sync(request: SyncRequest, background_tasks: BackgroundTasks):
    task_id = str(uuid.uuid4())
    sync_tasks[task_id] = {"status": "running", "message": "Starting sync...", "progress": 0}
    background_tasks.add_task(sync_operations_to_influxdb, task_id, request.ndb_connection, request.days)
    return {"task_id": task_id}


@app.get("/api/ndb-health/sync-status/{task_id}")
async def get_sync_status(task_id: str):
    if task_id not in sync_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    return sync_tasks[task_id]


# ============================================================================
# Application Startup
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Application startup tasks"""
    print("=" * 80)
    print("NDB YCSB Benchmark Generator")
    print("=" * 80)
    print(f"Static files: {STATIC_DIR}")
    print(f"Templates: {TEMPLATES_DIR}")
    print(f"Generated scripts: {GENERATED_SCRIPTS_DIR}")
    print("=" * 80)
    print("Server starting...")
    print("Access the application at: http://localhost:8000")
    print("=" * 80)


if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
