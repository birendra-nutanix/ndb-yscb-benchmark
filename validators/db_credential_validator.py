"""
Database Credential Validator
Validates database credentials by attempting actual connections
"""

from typing import Dict, Tuple, Optional
import socket
import time


class DBCredentialValidator:
    """Validates database credentials for different engines"""
    
    @staticmethod
    def validate_postgresql(host: str, port: int, database: str, username: str, password: str) -> Tuple[bool, str, str]:
        """
        Validate PostgreSQL credentials
        
        Args:
            host: Database host/IP
            port: Database port
            database: Database name
            username: Database username
            password: Database password
            
        Returns:
            Tuple of (success, message, working_database_name)
        """
        try:
            import psycopg2
            
            try:
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=username,
                    password=password,
                    connect_timeout=10
                )
                conn.close()
                return True, f"PostgreSQL connection successful to {host}:{port}/{database}", database
            except Exception as e:
                # Try with lowercase database name as fallback
                database_lower = database.lower()
                if database != database_lower:
                    try:
                        conn = psycopg2.connect(
                            host=host,
                            port=port,
                            database=database_lower,
                            user=username,
                            password=password,
                            connect_timeout=10
                        )
                        conn.close()
                        return True, f"PostgreSQL connection successful to {host}:{port}/{database_lower} (lowercase fallback)", database_lower
                    except Exception:
                        pass # Raise original error if lowercase fails
                
                return False, f"PostgreSQL connection failed: {str(e)}", database
            
        except ImportError:
            return False, "psycopg2 library not installed. Install with: pip install psycopg2-binary", database
        except Exception as e:
            return False, f"PostgreSQL connection failed: {str(e)}", database
    
    @staticmethod
    def validate_mongodb(host: str, port: int, database: str, username: str, password: str) -> Tuple[bool, str, str]:
        """
        Validate MongoDB credentials
        
        Args:
            host: Database host/IP
            port: Database port
            database: Database name (or 'admin' for auth)
            username: Database username
            password: Database password
            
        Returns:
            Tuple of (success, message, working_database_name)
        """
        try:
            from pymongo import MongoClient
            from pymongo.errors import ConnectionFailure, OperationFailure
            
            try:
                # Try to connect with authentication
                client = MongoClient(
                    host=host,
                    port=port,
                    username=username,
                    password=password,
                    authSource='admin',
                    directConnection=True,
                    serverSelectionTimeoutMS=10000
                )
                
                # Test the connection
                client[database].command('ping')
                client.close()
                
                return True, f"MongoDB connection successful to {host}:{port}", database
            except Exception as e:
                database_lower = database.lower()
                if database != database_lower:
                    try:
                        client = MongoClient(
                            host=host,
                            port=port,
                            username=username,
                            password=password,
                            authSource='admin',
                            directConnection=True,
                            serverSelectionTimeoutMS=10000
                        )
                        
                        # Test the connection
                        client[database_lower].command('ping')
                        client.close()
                        
                        return True, f"MongoDB connection successful to {host}:{port} (lowercase fallback)", database_lower
                    except Exception:
                        pass # Raise original error if lowercase fails
                        
                raise e
            
        except ImportError:
            return False, "pymongo library not installed. Install with: pip install pymongo", database
        except (ConnectionFailure, OperationFailure) as e:
            return False, f"MongoDB connection failed: {str(e)}", database
        except Exception as e:
            return False, f"MongoDB connection error: {str(e)}", database
    
    @staticmethod
    def validate_mysql(host: str, port: int, database: str, username: str, password: str) -> Tuple[bool, str, str]:
        """
        Validate MySQL credentials
        
        Args:
            host: Database host/IP
            port: Database port
            database: Database name
            username: Database username
            password: Database password
            
        Returns:
            Tuple of (success, message, working_database_name)
        """
        try:
            import mysql.connector
            
            try:
                conn = mysql.connector.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=username,
                    password=password,
                    connection_timeout=10
                )
                conn.close()
                return True, f"MySQL connection successful to {host}:{port}/{database}", database
            except Exception as e:
                # Try lowercase fallback
                database_lower = database.lower()
                if database != database_lower:
                    try:
                        conn = mysql.connector.connect(
                            host=host,
                            port=port,
                            database=database_lower,
                            user=username,
                            password=password,
                            connection_timeout=10
                        )
                        conn.close()
                        return True, f"MySQL connection successful to {host}:{port}/{database_lower} (lowercase fallback)", database_lower
                    except Exception:
                        pass
                
                return False, f"MySQL connection failed: {str(e)}", database
            
        except ImportError:
            return False, "mysql-connector-python library not installed. Install with: pip install mysql-connector-python", database
        except Exception as e:
            return False, f"MySQL connection failed: {str(e)}", database
    
    @staticmethod
    def validate_oracle(host: str, port: int, service_name: str, username: str, password: str) -> Tuple[bool, str, str]:
        """
        Validate Oracle credentials
        
        Args:
            host: Database host/IP
            port: Database port
            service_name: Oracle service name
            username: Database username
            password: Database password
            
        Returns:
            Tuple of (success, message, working_database_name)
        """
        try:
            import oracledb
            
            try:
                # Create DSN (connection string)
                dsn = f"{host}:{port}/{service_name}"
                
                # Connect using oracledb (thin mode - no Oracle Client required)
                conn = oracledb.connect(user=username, password=password, dsn=dsn)
                conn.close()
                return True, f"Oracle connection successful to {host}:{port}/{service_name}", service_name
            except Exception as e:
                # Try lowercase fallback
                service_name_lower = service_name.lower()
                if service_name != service_name_lower:
                    try:
                        dsn_lower = f"{host}:{port}/{service_name_lower}"
                        conn = oracledb.connect(user=username, password=password, dsn=dsn_lower)
                        conn.close()
                        return True, f"Oracle connection successful to {host}:{port}/{service_name_lower} (lowercase fallback)", service_name_lower
                    except Exception:
                        pass
                
                return False, f"Oracle connection failed: {str(e)}", service_name
            
        except ImportError:
            return False, "oracledb library not installed. Install with: pip install oracledb", service_name
        except Exception as e:
            return False, f"Oracle connection failed: {str(e)}", service_name
    
    @staticmethod
    def validate_mssql(host: str, port: int, database: str, username: str, password: str) -> Tuple[bool, str, str]:
        """
        Validate MS SQL Server credentials
        
        Args:
            host: Database host/IP
            port: Database port
            database: Database name
            username: Database username
            password: Database password
            
        Returns:
            Tuple of (success, message, working_database_name)
        """
        try:
            import pymssql
            
            try:
                conn = pymssql.connect(
                    server=host,
                    port=port,
                    database=database,
                    user=username,
                    password=password,
                    timeout=10
                )
                conn.close()
                return True, f"MS SQL Server connection successful to {host}:{port}/{database}", database
            except Exception as e:
                # Try lowercase fallback
                database_lower = database.lower()
                if database != database_lower:
                    try:
                        conn = pymssql.connect(
                            server=host,
                            port=port,
                            database=database_lower,
                            user=username,
                            password=password,
                            timeout=10
                        )
                        conn.close()
                        return True, f"MS SQL Server connection successful to {host}:{port}/{database_lower} (lowercase fallback)", database_lower
                    except Exception:
                        pass
                
                return False, f"MS SQL Server connection failed: {str(e)}", database
            
        except ImportError:
            return False, "pymssql library not installed. Install with: pip install pymssql", database
        except Exception as e:
            return False, f"MS SQL Server connection failed: {str(e)}", database
    
    @staticmethod
    def validate_tcp_connectivity(host: str, port: int, timeout: int = 5) -> Tuple[bool, str]:
        """
        Test basic TCP connectivity to database host
        
        Args:
            host: Database host/IP
            port: Database port
            timeout: Connection timeout in seconds
            
        Returns:
            Tuple of (success, message)
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                return True, f"TCP connection successful to {host}:{port}"
            else:
                return False, f"TCP connection failed to {host}:{port} (port may be closed or filtered)"
                
        except socket.gaierror:
            return False, f"Hostname resolution failed for {host}"
        except Exception as e:
            return False, f"TCP connectivity test failed: {str(e)}"
    
    @classmethod
    def validate_credentials(
        cls,
        engine: str,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        test_connectivity_only: bool = False
    ) -> Tuple[bool, str, str]:
        """
        Validate database credentials for any supported engine
        
        Args:
            engine: Database engine (postgresql, mongodb, mysql, oracle, mssql)
            host: Database host/IP
            port: Database port
            database: Database name
            username: Database username
            password: Database password
            test_connectivity_only: If True, only test TCP connectivity
            
        Returns:
            Tuple of (success, message, working_database_name)
        """
        # First test TCP connectivity
        tcp_success, tcp_message = cls.validate_tcp_connectivity(host, port)
        if not tcp_success:
            return False, tcp_message, database
        
        # If only testing connectivity, return now
        if test_connectivity_only:
            return True, tcp_message, database
        
        # Validate credentials based on engine
        engine_lower = engine.lower()
        
        if engine_lower == 'postgresql':
            return cls.validate_postgresql(host, port, database, username, password)
        elif engine_lower == 'mongodb':
            return cls.validate_mongodb(host, port, database, username, password)
        elif engine_lower == 'mysql':
            return cls.validate_mysql(host, port, database, username, password)
        elif engine_lower == 'oracle':
            return cls.validate_oracle(host, port, database, username, password)
        elif engine_lower == 'mssql':
            return cls.validate_mssql(host, port, database, username, password)
        else:
            return False, f"Unsupported database engine: {engine}", database
    
    @classmethod
    def validate_multiple_databases(
        cls,
        databases: Dict[str, list],
        credentials: Dict[str, Dict[str, str]],
        test_connectivity_only: bool = False
    ) -> Tuple[bool, str, Dict[str, list]]:
        """
        Validate credentials for multiple databases
        
        Args:
            databases: Dictionary of engine -> list of database info dicts
            credentials: Dictionary of engine -> {username, password}
            test_connectivity_only: If True, only test TCP connectivity
            
        Returns:
            Tuple of (all_success, summary_message, results_dict)
            results_dict contains {engine: [{db_name, success, message}, ...]}
        """
        results = {}
        total_dbs = 0
        successful = 0
        failed = 0
        
        for engine, db_list in databases.items():
            if engine not in credentials:
                continue
            
            engine_results = []
            creds = credentials[engine]
            
            for db_info in db_list:
                total_dbs += 1
                db_name = db_info.get('name', 'unknown')
                host = db_info.get('primary_ip', db_info.get('ip_addresses', ['unknown'])[0])
                port = db_info.get('port', 5432)  # Default port
                database = db_info.get('database_name', 'postgres')
                
                success, message, working_database_name = cls.validate_credentials(
                    engine=engine,
                    host=host,
                    port=port,
                    database=database,
                    username=creds.get('username', ''),
                    password=creds.get('password', ''),
                    test_connectivity_only=test_connectivity_only
                )
                
                engine_results.append({
                    'db_name': db_name,
                    'host': host,
                    'port': port,
                    'success': success,
                    'message': message,
                    'working_database_name': working_database_name
                })
                
                if success:
                    successful += 1
                else:
                    failed += 1
            
            results[engine] = engine_results
        
        all_success = (failed == 0 and total_dbs > 0)
        summary = f"Validated {total_dbs} database(s): {successful} successful, {failed} failed"
        
        return all_success, summary, results
