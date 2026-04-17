"""
Remote Transfer Utility
Handles SSH connection and file transfer to remote clusters
"""

import paramiko
import os
import logging
import traceback
from pathlib import Path
from typing import Tuple, Optional

logger = logging.getLogger(__name__)


class RemoteTransferConfig:
    """Configuration for remote transfer"""
    def __init__(self, host: str, username: str, password: str, 
                 target_folder: str = "/root/io_script", port: int = 22):
        self.host = host
        self.username = username
        self.password = password
        self.target_folder = target_folder
        self.port = port


class RemoteTransfer:
    """Handles SSH/SCP operations for remote file transfer"""
    
    def __init__(self, config: RemoteTransferConfig):
        self.config = config
        self.ssh_client = None
        self.sftp_client = None
    
    def connect(self) -> Tuple[bool, str]:
        """
        Establish SSH connection to remote host
        
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            logger.info(f"Connecting to {self.config.host}:{self.config.port} as {self.config.username}")
            
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            self.ssh_client.connect(
                hostname=self.config.host,
                port=self.config.port,
                username=self.config.username,
                password=self.config.password,
                timeout=30,
                banner_timeout=30,
                auth_timeout=30
            )
            
            self.sftp_client = self.ssh_client.open_sftp()
            
            logger.info(f"Successfully connected to {self.config.host}")
            return True, f"Connected to {self.config.host}"
            
        except paramiko.AuthenticationException:
            error_msg = f"Authentication failed for {self.config.username}@{self.config.host}"
            logger.error(error_msg)
            return False, error_msg
        except paramiko.SSHException as e:
            error_msg = f"SSH error: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return False, error_msg
        except Exception as e:
            error_msg = f"Connection error: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return False, error_msg
    
    def execute_command(self, command: str) -> Tuple[bool, str, str]:
        """
        Execute a command on remote host
        
        Args:
            command: Command to execute
            
        Returns:
            Tuple[bool, str, str]: (success, stdout, stderr)
        """
        try:
            logger.info(f"Executing command: {command}")
            stdin, stdout, stderr = self.ssh_client.exec_command(command, timeout=60)
            
            stdout_str = stdout.read().decode('utf-8')
            stderr_str = stderr.read().decode('utf-8')
            exit_code = stdout.channel.recv_exit_status()
            
            if exit_code == 0:
                logger.info(f"Command executed successfully")
                return True, stdout_str, stderr_str
            else:
                logger.warning(f"Command failed with exit code {exit_code}: {stderr_str}")
                return False, stdout_str, stderr_str
                
        except Exception as e:
            error_msg = f"Command execution error: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return False, "", error_msg
    
    def prepare_target_folder(self) -> Tuple[bool, str]:
        """
        Delete target folder if exists and create fresh one
        
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            target = self.config.target_folder
            
            # Delete folder if exists
            logger.info(f"Deleting folder if exists: {target}")
            success, stdout, stderr = self.execute_command(f"rm -rf {target}")
            if not success and "No such file or directory" not in stderr:
                return False, f"Failed to delete folder: {stderr}"
            
            # Create fresh folder
            logger.info(f"Creating folder: {target}")
            success, stdout, stderr = self.execute_command(f"mkdir -p {target}")
            if not success:
                return False, f"Failed to create folder: {stderr}"
            
            logger.info(f"Target folder prepared: {target}")
            return True, f"Folder prepared: {target}"
            
        except Exception as e:
            error_msg = f"Folder preparation error: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return False, error_msg
    
    def transfer_file(self, local_path: str) -> Tuple[bool, str]:
        """
        Transfer file to remote host using SCP
        
        Args:
            local_path: Path to local file
            
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            if not os.path.exists(local_path):
                return False, f"Local file not found: {local_path}"
            
            filename = os.path.basename(local_path)
            remote_path = f"{self.config.target_folder}/{filename}"
            
            logger.info(f"Transferring {local_path} to {remote_path}")
            
            # Get file size for logging
            file_size = os.path.getsize(local_path)
            logger.info(f"File size: {file_size / 1024:.2f} KB")
            
            # Transfer file
            self.sftp_client.put(local_path, remote_path)
            
            logger.info(f"File transferred successfully to {remote_path}")
            return True, remote_path
            
        except Exception as e:
            error_msg = f"File transfer error: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return False, error_msg
    
    def unzip_file(self, remote_zip_path: str) -> Tuple[bool, str]:
        """
        Unzip file on remote host
        
        Args:
            remote_zip_path: Path to ZIP file on remote host
            
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            logger.info(f"Unzipping {remote_zip_path}")
            
            # Unzip in the target folder
            command = f"cd {self.config.target_folder} && unzip -o {os.path.basename(remote_zip_path)}"
            success, stdout, stderr = self.execute_command(command)
            
            if not success:
                return False, f"Unzip failed: {stderr}"
            
            # Delete the ZIP file after extraction
            logger.info(f"Cleaning up ZIP file")
            rm_command = f"rm -f {remote_zip_path}"
            self.execute_command(rm_command)
            
            logger.info(f"Files extracted to {self.config.target_folder}")
            return True, f"Files extracted to {self.config.target_folder}"
            
        except Exception as e:
            error_msg = f"Unzip error: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return False, error_msg
    
    def disconnect(self):
        """Close SSH and SFTP connections"""
        try:
            if self.sftp_client:
                self.sftp_client.close()
                logger.info("SFTP connection closed")
            if self.ssh_client:
                self.ssh_client.close()
                logger.info("SSH connection closed")
        except Exception as e:
            logger.warning(f"Error during disconnect: {str(e)}")
    
    def transfer_and_extract(self, local_zip_path: str) -> Tuple[bool, str]:
        """
        Complete workflow: connect, prepare folder, transfer, unzip, disconnect
        
        Args:
            local_zip_path: Path to local ZIP file
            
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            # Step 1: Connect
            success, message = self.connect()
            if not success:
                return False, f"Connection failed: {message}"
            
            # Step 2: Prepare target folder
            success, message = self.prepare_target_folder()
            if not success:
                self.disconnect()
                return False, f"Folder preparation failed: {message}"
            
            # Step 3: Transfer file
            success, remote_path = self.transfer_file(local_zip_path)
            if not success:
                self.disconnect()
                return False, f"File transfer failed: {remote_path}"
            
            # Step 4: Unzip
            success, message = self.unzip_file(remote_path)
            if not success:
                self.disconnect()
                return False, f"Unzip failed: {message}"
            
            # Step 5: Disconnect
            self.disconnect()
            
            final_message = f"✓ Script deployed to {self.config.host}:{self.config.target_folder}"
            logger.info(final_message)
            return True, final_message
            
        except Exception as e:
            error_msg = f"Transfer workflow error: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            self.disconnect()
            return False, error_msg


def test_connection(host: str, username: str, password: str, port: int = 22) -> Tuple[bool, str]:
    """
    Test SSH connection to remote host
    
    Args:
        host: Remote host IP/hostname
        username: SSH username
        password: SSH password
        port: SSH port (default: 22)
        
    Returns:
        Tuple[bool, str]: (success, message)
    """
    config = RemoteTransferConfig(host, username, password, "/tmp", port)
    transfer = RemoteTransfer(config)
    
    success, message = transfer.connect()
    transfer.disconnect()
    
    return success, message
