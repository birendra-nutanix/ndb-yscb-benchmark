"""
NDB REST API Validator
Handles authentication and database discovery from Nutanix Database Service
"""

import requests
import urllib3
from typing import Dict, List, Optional, Tuple, Any
from pydantic import BaseModel

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class NDBConnection(BaseModel):
    """NDB connection parameters"""
    ip: str
    username: str
    password: str
    port: int = 8443
    verify_ssl: bool = False


class DatabaseInfo(BaseModel):
    """Database information from NDB"""
    id: str
    name: str
    engine: str
    engine_version: str
    status: str
    ip_addresses: List[str]
    primary_ip: str  # Primary/active IP for connections
    port: int
    database_name: Optional[str] = None
    is_cluster: bool = False  # HA, RAC, ReplicaSet, Sharded
    cluster_type: Optional[str] = None  # 'ha', 'rac', 'replicaset', 'sharded'


class NDBValidator:
    """Validates NDB connection and fetches database information"""
    
    # Engine type mapping
    ENGINE_TYPES = {
        'postgresql': 'postgres_database',
        'mongodb': 'mongodb_database',
        'mssql': 'sqlserver_database',
        'oracle': 'oracle_database',
        'mysql': 'mysql_database',
        'mariadb': 'mariadb_database'
    }
    
    def __init__(self, connection: NDBConnection):
        self.connection = connection
        self.base_url = f"https://{connection.ip}:{connection.port}/era/v0.9"
        self.auth_token = None
        self.session = requests.Session()
        self.session.verify = connection.verify_ssl
    
    def authenticate(self) -> Tuple[bool, str]:
        """
        Authenticate with NDB and get auth token
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            
            url = f"{self.base_url}/auth/token"
            
            auth=(self.connection.username, self.connection.password)
            params = {"expire": 240}
            response = self.session.get(url, auth=auth, params=params, timeout=10, verify=False)
            
            if response.status_code == 200:
                # NDB returns token in response body or headers
                data = response.json()
                self.auth_token = data.get('token') or response.headers.get('Authorization')
                
                if self.auth_token:
                    # Set authorization header for future requests
                    self.session.headers.update({
                        'Authorization': f'Bearer {self.auth_token}',
                        'Content-Type': 'application/json'
                    })
                    return True, "Authentication successful"
                else:
                    return False, "Authentication succeeded but no token received"
            elif response.status_code == 401:
                return False, "Invalid credentials"
            else:
                return False, f"Authentication failed: {response.status_code} - {response.text}"
                
        except requests.exceptions.ConnectionError:
            return False, f"Cannot connect to NDB at {self.connection.ip}:{self.connection.port}"
        except requests.exceptions.Timeout:
            return False, "Connection timeout - NDB not responding"
        except Exception as e:
            return False, f"Authentication error: {str(e)}"
    
    def _parse_database_info(self, db_data: dict, engine: str) -> Optional[DatabaseInfo]:
        """Parse database information from NDB API response"""
        try:
            # Get database nodes
            
            # Determine cluster type and primary IP
            is_cluster = False
            cluster_type = None
            primary_ip = None
            ip_addresses = []
            # Check for cluster configurations
            db_type = db_data.get('type', '').lower()
            # PostgreSQL HA
            if engine == 'postgresql' and db_type == self.ENGINE_TYPES.get(engine) and db_data.get('clustered', False):
                is_cluster = True
                cluster_type = 'ha'
                
                # Get logical cluster information
                dbserver_logical_cluster = db_data.get('dbserverlogicalCluster', {})
                logical_dbservers = dbserver_logical_cluster.get('logicalDbservers', [])
                
                # Find primary replica from logical cluster
                for logical_server in logical_dbservers:
                    if logical_server.get('primary', False):  # Check for primary=true
                        # Get the dbserver details
                        dbserver = logical_server.get('dbserver', {})
                        server_ips = dbserver.get('ipAddresses', [])
                        
                        if server_ips:
                            ip_addresses.extend(server_ips)
                            primary_ip = server_ips[0]
                            break
                    
            # Oracle RAC
            elif 'oracle' in db_type and db_type == self.ENGINE_TYPES.get(engine) and db_data.get('clustered', False):
                is_cluster = True
                cluster_type = 'rac'
                # Oracle RAC: Active-Active cluster (no primary/secondary)
                # All nodes can accept reads and writes
                dbserver_logical_cluster = db_data.get('dbserverlogicalCluster', {})
                logical_dbservers = dbserver_logical_cluster.get('logicalDbservers', [])
                
                # Collect all node IPs, filtering for data network (10.x.x.x)
                for logical_server in logical_dbservers:
                    dbserver = logical_server.get('dbserver', {})
                    server_ips = dbserver.get('ipAddresses', [])
                    
                    # Filter for data network IPs (starting with "10.")
                    data_network_ips = [ip for ip in server_ips if ip.startswith('10.')]
                    
                    if data_network_ips:
                        ip_addresses.extend(data_network_ips)
                        if not primary_ip:
                            # Use first data network IP (all RAC nodes are equal)
                            primary_ip = data_network_ips[0]
            
            # MongoDB ReplicaSet
            elif engine == 'mongodb' and db_type == self.ENGINE_TYPES.get(engine) and db_data.get('clustered', False):
                is_cluster = True
                is_shard_cluster = True if "SHARDED_CLUSTER".lower() in str(db_data.get("databaseClusterType", "")).lower() else False

                if not is_shard_cluster and db_data.get("databaseClusterType", None) is None:
                    cluster_type = 'replicaset'
                    # Find primary node
                    # Get logical cluster information
                    dbserver_logical_cluster = db_data.get('dbserverlogicalCluster', {})
                    logical_dbservers = dbserver_logical_cluster.get('logicalDbservers', [])
                    
                    # Find primary replica from logical cluster
                    for logical_server in logical_dbservers:
                        if logical_server.get('primary', False):  # Check for primary=true
                            # Get the dbserver details
                            dbserver = logical_server.get('dbserver', {})
                            server_ips = dbserver.get('ipAddresses', [])
                            
                            if server_ips:
                                ip_addresses.extend(server_ips)
                                primary_ip = server_ips[0]
                                break
                else:
                    # TODO
                    pass
            
            
            # MS SQL AAG (Always On Availability Group)
            elif 'sqlserver' in db_type and db_type == self.ENGINE_TYPES.get(engine) and db_data.get('clustered', False):
                is_cluster = True
                cluster_type = 'aag'
                # Get logical cluster information
                dbserver_logical_cluster = db_data.get('dbserverlogicalCluster', {})
                logical_dbservers = dbserver_logical_cluster.get('logicalDbservers', [])
                
                # Find primary replica from logical cluster
                for logical_server in logical_dbservers:
                    if logical_server.get('primary', False):  # Check for primary=true
                        # Get the dbserver details
                        dbserver = logical_server.get('dbserver', {})
                        server_ips = dbserver.get('ipAddresses', [])
                        
                        if server_ips:
                            ip_addresses.extend(server_ips)
                            primary_ip = server_ips[0]
                            break
            
            # MySQL or MariaDB HA
            elif ('mysql' in db_type or 'mariadb' in db_type) and db_type == self.ENGINE_TYPES.get(engine) and db_data.get('clustered', False):
                is_cluster = True
                cluster_type = 'ha'
                # Get logical cluster information
                dbserver_logical_cluster = db_data.get('dbserverlogicalCluster', {})
                logical_dbservers = dbserver_logical_cluster.get('logicalDbservers', [])
                
                # Find primary replica from logical cluster
                for logical_server in logical_dbservers:
                    if logical_server.get('primary', False):  # Check for primary=true
                        # Get the dbserver details
                        dbserver = logical_server.get('dbserver', {})
                        server_ips = dbserver.get('ipAddresses', [])
                        
                        if server_ips:
                            ip_addresses.extend(server_ips)
                            primary_ip = server_ips[0]
                            break
            
            # Single instance - use first available IP
            else:
                db_nodes = db_data.get('databaseNodes', [])
                for node in db_nodes:
                    if node.get("dbserver", {}).get('ipAddresses', []):
                        ip_addresses.extend(node.get("dbserver", {}).get('ipAddresses', []))
                if ip_addresses:
                    primary_ip = ip_addresses[0]
            
            # Fallback: if no primary IP found, use first IP
            if not primary_ip and ip_addresses:
                primary_ip = ip_addresses[0]
            
            # Get port based on engine type
            port_map = {
                'postgresql': 5432,
                'mongodb': 27017,
                'mssql': 1433,
                'oracle': 1521,
                'mysql': 3306,
                'mariadb': 3306
            }
            
            # Try to get port from properties or use default
            properties = db_data.get('properties', [])
            port = port_map.get(engine.lower(), 0)
            for prop in properties:
                if 'port' in prop.get('name', '').lower():
                    try:
                        port = int(prop.get('value', port))
                        break
                    except:
                        pass
            
            # Get database name
            
            db_name = db_data.get('name') or db_data.get('databaseName')
            
            return DatabaseInfo(
                id=db_data.get('id', ''),
                name=db_data.get('name', ''),
                engine=engine,
                engine_version=db_data.get('dbserverVersion', 'unknown'),
                status=db_data.get('status', 'unknown'),
                ip_addresses=ip_addresses,
                primary_ip=primary_ip or 'unknown',
                port=port,
                database_name=db_name,
                is_cluster=is_cluster,
                cluster_type=cluster_type
            )
        except Exception as e:
            print(f"Error parsing database info: {e}")
            return None
    
    def validate_connection(self) -> Tuple[bool, str]:
        """
        Validate NDB connection by attempting authentication
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        return self.authenticate()
    
    def fetch_all_databases(self) -> Tuple[bool, str, List[DatabaseInfo]]:
        """
        Fetch all databases from NDB without filtering using UI summary endpoint
        
        Returns:
            Tuple of (success, message, databases)
            databases: List of database dictionaries from NDB
        """
        if not self.auth_token:
            success, msg = self.authenticate()
            if not success:
                return False, msg, []
        
        all_databases = []
        
        try:
            url = f"{self.base_url}/databases/ui/summary"
            
            response = self.session.get(url, timeout=30)
            if response.status_code == 200:
                all_databases = response.json()
                
                # Fetch clones
                clone_url = f"{self.base_url}/clones"
                clone_response = self.session.get(clone_url, timeout=30)
                if clone_response.status_code == 200:
                    clones = clone_response.json()
                    # Mark clones so they can be identified
                    for clone in clones:
                        clone['is_clone'] = True
                    all_databases.extend(clones)
                
                total = len(all_databases)
                if total == 0:
                    return True, "No databases or clones found in NDB", all_databases
                
                return True, f"Found {total} database(s) and clone(s)", all_databases
            else:
                return False, f"Failed to fetch databases: {response.status_code}", []
        
        except Exception as e:
            return False, f"Error fetching databases: {str(e)}", []
    
    def fetch_databases_by_type(
        self,
        engine_selections: List[Dict[str, Any]]
    ) -> Tuple[bool, str, Dict[str, Dict[str, List[DatabaseInfo]]]]:
        """
        Fetch databases filtered by engine and deployment type
        
        Args:
            engine_selections: List of {"engine": str, "types": List[str]}
            
        Returns:
            Tuple of (success, message, databases)
            databases structure: {engine: {type: [DatabaseInfo]}}
        """
        if not self.auth_token:
            success, msg = self.authenticate()
            if not success:
                return False, msg, {}
        
        all_databases = {}
        
        try:
            for selection in engine_selections:
                
                engine = selection['engine']
                requested_types = selection['types']
                
                engine_key = self.ENGINE_TYPES.get(engine)
                if not engine_key:
                    continue
                
                url = f"{self.base_url}/databases"
                params = {
                    'detailed': 'true',
                    'load-dbserver-cluster': 'true',
                    'load-metrics': 'false'
                }
                
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    data = [ item for item in data if item.get('type', '').lower() == engine_key.lower()]
                    type_groups = {}
                    # Filter databases by engine type

                    for db in data:
                        db_type_str = db.get('type', '')
                        if engine_key.lower() not in db_type_str.lower():
                            continue
                        
                        # Determine database deployment type
                        db_type = self._determine_database_type(db, engine)
                        
                        # Check if this type was requested
                        if db_type not in requested_types:
                            continue
                        
                        # Parse database info
                        db_info = self._parse_database_info(db, engine)
                        if db_info:
                            if db_type not in type_groups:
                                type_groups[db_type] = []
                            type_groups[db_type].append(db_info)
                    
                    if type_groups:
                        all_databases[engine] = type_groups
                else:
                    return False, f"Failed to fetch databases: {response.status_code}", {}
            
            # Count total databases
            total = sum(
                len(db_list)
                for type_groups in all_databases.values()
                for db_list in type_groups.values()
            )
            
            if total == 0:
                return True, "No databases found matching selected types", all_databases
            
            return True, f"Found {total} database(s)", all_databases
            
        except Exception as e:
            return False, f"Error fetching databases: {str(e)}", {}
    
    def _determine_database_type(self, db_data: dict, engine: str) -> str:
        """
        Determine database deployment type from NDB data
        
        Args:
            db_data: Database data from NDB API
            engine: Engine name (postgresql, mongodb, etc.)
            
        Returns:
            Type string: 'si', 'ha', 'rac', 'siha', 'replicaset', 'sharded', 'aag'
        """
        db_type = db_data.get('type', '').lower()
        is_clustered = db_data.get('clustered', False)
        if engine == 'postgresql' and db_type ==  self.ENGINE_TYPES.get(engine):
            if is_clustered:
                return 'ha'
            return 'si'
        
        elif engine == 'mongodb' and db_type ==  self.ENGINE_TYPES.get(engine):
            if is_clustered:
                if "SHARDED_CLUSTER".lower() in str(db_data.get("databaseClusterType", "")).lower():
                    return 'sharded'
                elif db_data.get("databaseClusterType", None) is None:
                    return 'replicaset'
                else:
                    # it means shard replica set
                    return ""
            return "si"
        
        elif engine == 'oracle' and db_type ==  self.ENGINE_TYPES.get(engine):
            if is_clustered:
                return 'rac'
            return 'si'
        
        elif engine == 'mssql' and db_type ==  self.ENGINE_TYPES.get(engine):
            if is_clustered:
                return 'aag'
            return 'si'
        
        elif engine == 'mysql' and db_type ==  self.ENGINE_TYPES.get(engine):
            if is_clustered:
                return 'ha'
            return 'si'
        
        elif engine == 'mariadb' and db_type ==  self.ENGINE_TYPES.get(engine):
            if is_clustered:
                return 'ha'
            return 'si'
        
        # Default to single instance
        return 'si'
