import os
import sys
import time
import shutil
import traceback
import requests
import subprocess
from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
import bittensor as bt
from sqlalchemy import text
import ssdeep 
import sqlite3
from azure.storage.blob.aio import BlobServiceClient
from azure.core.exceptions import AzureError
from dotenv import load_dotenv
import stat  # Make sure to import stat module
import asyncio
import async_timeout
import aiofiles
import hashlib
import azure.core
import configparser
from bettensor.validator.utils.database.database_factory import DatabaseFactory

class StateSync:
    def __init__(self, 
                 state_dir: str = "./state",
                 db_manager = None,
                 validator = None):
        """
        Initialize StateSync with Azure blob storage for both uploads and downloads
        
        Args:
            state_dir: Local directory for state files
            db_manager: Database manager instance
            validator: Validator instance for task management
        """
        # Load environment variables
        load_dotenv()
        
        # Store database manager and validator
        self.db_manager = db_manager
        self.validator = validator
        
        #hardcoded read-only token for easy distribution - will issue tokens via api in the future
        readonly_token = "sp=r&st=2024-11-05T18:31:28Z&se=2039-11-06T02:31:28Z&spr=https&sv=2022-11-02&sr=c&sig=NJPxzJsi3zgVjHtJK5BNNYXUqxG5Hi0WfM4Fg3sgBB4%3D"

        # Get Azure configuration from environment
        sas_url = os.getenv('AZURE_STORAGE_SAS_URL','https://devbettensorstore.blob.core.windows.net')
        container_name = os.getenv('AZURE_STORAGE_CONTAINER','data')
        credential = os.getenv('VALIDATOR_API_TOKEN', readonly_token)

        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

       

        # Azure setup using the async BlobServiceClient
        self.azure_enabled = bool(sas_url)
        if self.azure_enabled:
            self.blob_service = BlobServiceClient(account_url=sas_url, credential=credential)
            self.container = self.blob_service.get_container_client(container_name)
            bt.logging.info(f"Azure blob storage configured with container: {container_name}")
        else:
            bt.logging.error("Azure blob storage not configured - state sync will be disabled")

        self.state_files = [
            "validator.db",
            "state.pt", 
            "entropy_system_state.json",
            "state_hashes.txt",
            "state_metadata.json"
        ]

        self.hash_file = self.state_dir / "state_hashes.txt"
        self.metadata_file = self.state_dir / "state_metadata.json"


    def _compute_fuzzy_hash(self, filepath: Path) -> str:
        """Compute fuzzy hash of a file using ssdeep, with special handling for SQLite DB"""
        try:
            if filepath.name == "validator.db":
                # For SQLite DB, hash the table structure and row counts instead
                conn = sqlite3.connect(filepath)
                cursor = conn.cursor()
                
                # Get table schema and row counts
                hash_data = []
                
                # Get list of tables
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                
                for table in tables:
                    table_name = table[0]
                    # Get table schema
                    cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                    schema = cursor.fetchone()[0]
                    
                    # Get row count
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    
                    hash_data.append(f"{schema}:{count}")
                
                conn.close()
                
                # Create a stable string representation to hash
                db_state = "\n".join(sorted(hash_data))
                return ssdeep.hash(db_state.encode())
            else:
                # Regular file hashing
                with open(filepath, 'rb') as f:
                    return ssdeep.hash(f.read())
        except Exception as e:
            bt.logging.error(f"Error computing hash for {filepath}: {e}")
            return ""

    def _compare_fuzzy_hashes(self, hash1: str, hash2: str) -> int:
        """Compare two fuzzy hashes and return similarity percentage (0-100)"""
        try:
            if not hash1 or not hash2:
                return 0
            return ssdeep.compare(hash1, hash2)
        except Exception as e:
            bt.logging.error(f"Error comparing hashes: {e}")
            return 0

    def _update_hash_file(self):
        """Update the hash file with current file hashes"""
        try:
            hashes = {}
            for file in self.state_files:
                if file != "state_hashes.txt":  # Don't hash the hash file
                    filepath = self.state_dir / file
                    if filepath.exists():
                        hashes[file] = self._compute_fuzzy_hash(filepath)

            with open(self.hash_file, 'w') as f:
                json.dump(hashes, f, indent=2)
            return True
        except Exception as e:
            bt.logging.error(f"Error updating hash file: {e}")
            return False

    def check_state_similarity(self) -> bool:
        """Check if local files are sufficiently similar to stored hashes"""
        try:
            if not self.hash_file.exists():
                bt.logging.warning("No hash file found. Cannot compare states.")
                return False

            with open(self.hash_file, 'r') as f:
                stored_hashes = json.load(f)

            for file, stored_hash in stored_hashes.items():
                filepath = self.state_dir / file
                if filepath.exists():
                    current_hash = self._compute_fuzzy_hash(filepath)
                    similarity = self._compare_fuzzy_hashes(stored_hash, current_hash)
                    
                    if similarity < 80:  # Less than 80% similar
                        bt.logging.warning(f"File {file} has diverged significantly (similarity: {similarity}%)")
                        return False
                else:
                    bt.logging.warning(f"File {file} missing")
                    return False

            return True
        except Exception as e:
            bt.logging.error(f"Error checking state similarity: {e}")
            return False

    def _get_file_metadata(self, filepath: Path) -> dict:
        """Get metadata about a file including size, hash, and last modified time"""
        try:
            stats = filepath.stat()
            with open(filepath, 'rb') as f:
                # Read in chunks for large files
                hasher = hashlib.sha256()
                for chunk in iter(lambda: f.read(65536), b''):
                    hasher.update(chunk)
                    
            return {
                "size": stats.st_size,
                "modified": datetime.fromtimestamp(stats.st_mtime, timezone.utc).isoformat(),
                "hash": hasher.hexdigest(),
                "fuzzy_hash": self._compute_fuzzy_hash(filepath)
            }
        except Exception as e:
            bt.logging.error(f"Error getting metadata for {filepath}: {e}")
            return {}

    def _get_db_metadata(self, db_path: Path) -> dict:
        """Get detailed metadata about the database state"""
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            metadata = {
                "size": db_path.stat().st_size,
                "modified": datetime.fromtimestamp(db_path.stat().st_mtime, timezone.utc).isoformat(),
                "row_counts": {},
                "latest_dates": {},
                "hash": None,
                "fuzzy_hash": None
            }
            
            # Get row counts for key tables
            for table in ['predictions', 'game_data', 'miner_stats', 'scores']:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    metadata["row_counts"][table] = cursor.fetchone()[0]
                except sqlite3.Error:
                    metadata["row_counts"][table] = 0
            
            # Get latest dates for time-sensitive tables
            date_queries = {
                "predictions": "SELECT MAX(prediction_date) FROM predictions",
                "game_data": "SELECT MAX(event_start_date) FROM game_data",
                "score_state": "SELECT MAX(last_update_date) FROM score_state"
            }
            
            for table, query in date_queries.items():
                try:
                    cursor.execute(query)
                    result = cursor.fetchone()[0]
                    metadata["latest_dates"][table] = result if result else None
                except sqlite3.Error:
                    metadata["latest_dates"][table] = None
            
            # Compute hashes
            metadata["fuzzy_hash"] = self._compute_fuzzy_hash(db_path)
            
            # Compute SHA256 of table structure
            cursor.execute("SELECT sql FROM sqlite_master WHERE type='table'")
            schema = '\n'.join(sorted(row[0] for row in cursor if row[0]))
            metadata["hash"] = hashlib.sha256(schema.encode()).hexdigest()
            
            conn.close()
            return metadata
            
        except Exception as e:
            bt.logging.error(f"Error getting database metadata: {e}")
            return {}

    def _compare_db_states(self, local_meta: dict, remote_meta: dict) -> bool:
        """
        Compare database states to determine if pull is needed
        Returns: True if state should be pulled, False otherwise
        """
        try:
            # If either metadata is empty, can't make a good comparison
            if not local_meta or not remote_meta:
                return False
                
            # Compare latest dates
            for table, remote_date in remote_meta["latest_dates"].items():
                local_date = local_meta["latest_dates"].get(table)
                
                if not local_date or not remote_date:
                    continue
                    
                remote_dt = datetime.fromisoformat(remote_date)
                local_dt = datetime.fromisoformat(local_date)
                
                # If remote has significantly newer data (>1 hour), pull
                if remote_dt > local_dt + timedelta(hours=1):
                    bt.logging.info(f"Remote has newer {table} data: {remote_dt} vs {local_dt}")
                    return True
                    
            # Compare row counts
            for table, remote_count in remote_meta["row_counts"].items():
                local_count = local_meta["row_counts"].get(table, 0)
                
                # If difference is more than 10%, pull
                if abs(remote_count - local_count) / max(remote_count, local_count) > 0.1:
                    bt.logging.info(f"Significant row count difference in {table}: {local_count} vs {remote_count}")
                    return True
            
            return False
            
        except Exception as e:
            bt.logging.error(f"Error comparing database states: {e}")
            return False

    async def should_pull_state(self) -> bool:
        """
        Determine if the state should be pulled based on remote metadata.
        """
        try:
            # Check node config
            if os.environ.get("VALIDATOR_PULL_STATE", "True").lower() == "true":
                bt.logging.info("Node config requires state pull")
            else:
                bt.logging.info("Node config does not require state pull")
                return False

            # Check if enough blocks have passed since last pull
            if hasattr(self.validator, 'last_state_pull'):
                current_block = self.validator.subtensor.block
                blocks_since_pull = current_block - self.validator.last_state_pull
                min_blocks_between_pulls = 100  # About 20 minutes at 12s per block
                
                if blocks_since_pull < min_blocks_between_pulls:
                    bt.logging.debug(f"Not enough blocks since last pull ({blocks_since_pull}/{min_blocks_between_pulls})")
                    return False

            # Get remote metadata from Azure blob storage
            metadata_client = self.container.get_blob_client("state_metadata.json")
            temp_metadata = self.metadata_file.with_suffix('.tmp')
            
            try:
                # Download metadata file asynchronously
                async with aiofiles.open(temp_metadata, 'wb') as f:
                    stream = await metadata_client.download_blob()
                    async for chunk in stream.chunks():
                        await f.write(chunk)
                
                # Load remote metadata
                async with aiofiles.open(temp_metadata, 'r') as f:
                    remote_metadata_content = await f.read()
                    remote_metadata = json.loads(remote_metadata_content)
                    
                return await self._should_pull_state(remote_metadata)
                
            finally:
                # Clean up temp metadata file
                temp_metadata.unlink(missing_ok=True)
                
        except AzureError as e:
            bt.logging.error(f"Azure storage error checking state: {e}")
            return False
        except Exception as e:
            bt.logging.error(f"Error checking state status: {e}")
            return False

    async def pull_state(self):
        """Pull state from Azure and restore it."""
        if not self.azure_enabled:
            bt.logging.error("Azure blob storage not configured")
            return False

        # Get database type
        db_type = await self._get_database_type()
        
        # Determine backup file name
        if db_type == "postgres":
            db_backup = self.state_dir / "postgres_backup.dump"
            db_backup_blob = "postgres_backup.dump"
        else:
            db_backup = self.state_dir / "validator.db.backup"
            db_backup_blob = "validator.db"

        # Create state directory if it doesn't exist
        self.state_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Download state files
            state_files = [
                {"local_path": self.state_dir / "state.pt", "blob_name": "state.pt"},
                {"local_path": db_backup, "blob_name": db_backup_blob},
                {"local_path": self.state_dir / "metagraph.bin", "blob_name": "metagraph.bin"}
            ]

            for state_file in state_files:
                local_path = state_file["local_path"]
                blob_name = state_file["blob_name"]
                
                try:
                    await self._download_state_file(local_path, blob_name)
                except Exception as e:
                    bt.logging.warning(f"Failed to download {blob_name}: {e}")
                    # Continue with other files

            # Restore database from backup
            if db_backup.exists():
                if db_type == "postgres":
                    success = await self._restore_postgres_database(db_backup)
                else:
                    success = await self._restore_sqlite_database(db_backup)
                    
                if not success:
                    bt.logging.error("Failed to restore database from backup")
                    return False
            else:
                bt.logging.error("Database backup not found")
                return False

            # Validate that we downloaded and restored successfully
            success = await self._validate_state_files()
            if not success:
                bt.logging.error("State validation failed")
                return False
            
            bt.logging.info("State pulled and restored successfully")
            return True
            
        except Exception as e:
            bt.logging.error(f"Error during state pull: {e}")
            bt.logging.error(traceback.format_exc())
            return False

    async def _get_database_type(self):
        """
        Determine the database type from configuration.
        
        Returns:
            str: "postgres" or "sqlite"
        """
        # First check environment variable
        if os.environ.get("BETTENSOR_DB_TYPE"):
            return os.environ.get("BETTENSOR_DB_TYPE").lower()
            
        # Then check setup.cfg
        config_parser = configparser.ConfigParser()
        try:
            config_parser.read('setup.cfg')
            
            # Check for database section
            if 'database' in config_parser:
                db_section = config_parser['database']
                return db_section.get('default_type', 'sqlite').lower()
            else:
                # Check metadata section for backward compatibility
                return config_parser.get('metadata', 'database_type', fallback='sqlite').lower()
                
        except configparser.Error as e:
            bt.logging.error(f"Error reading database configuration from setup.cfg: {e}")
            return "sqlite"

    async def _upload_database_backup(self, backup_path, db_type):
        """
        Upload database backup to Azure.
        
        Args:
            backup_path: Path to backup file
            db_type: Database type ("postgres" or "sqlite")
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not backup_path.exists():
            bt.logging.error(f"Backup file not found: {backup_path}")
            return False
            
        try:
            # Determine blob name based on database type
            if db_type == "postgres":
                blob_name = "postgres_backup.dump"
            else:
                blob_name = "validator.db"
                
            # Upload to Azure
            with open(backup_path, "rb") as data:
                await self.blob_client.upload_blob(name=blob_name, data=data, overwrite=True)
                
            bt.logging.info(f"Uploaded {db_type} backup to Azure")
            return True
            
        except Exception as e:
            bt.logging.error(f"Error uploading {db_type} backup to Azure: {e}")
            return False

    async def _restore_postgres_database(self, backup_path):
        """
        Restore PostgreSQL database from backup.
        
        Args:
            backup_path: Path to PostgreSQL backup file (pg_dump format)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Get database connection info
            db_config = {}
            for env_var, config_key in [
                ("BETTENSOR_DB_HOST", "host"),
                ("BETTENSOR_DB_PORT", "port"),
                ("BETTENSOR_DB_USER", "user"),
                ("BETTENSOR_DB_PASSWORD", "password"),
                ("BETTENSOR_DB_NAME", "dbname")
            ]:
                if os.environ.get(env_var):
                    if config_key == "port":
                        db_config[config_key] = int(os.environ.get(env_var))
                    else:
                        db_config[config_key] = os.environ.get(env_var)
            
            # If not set in environment, use defaults
            if "host" not in db_config:
                db_config["host"] = "localhost"
            if "port" not in db_config:
                db_config["port"] = 5432
            if "user" not in db_config:
                db_config["user"] = "postgres"
            if "password" not in db_config:
                db_config["password"] = ""
            if "dbname" not in db_config:
                db_config["dbname"] = "bettensor_validator"
                
            # Build pg_restore command
            cmd = [
                "pg_restore",
                "-h", db_config["host"],
                "-p", str(db_config["port"]),
                "-U", db_config["user"],
                "-d", db_config["dbname"],
                "-c",  # Clean (drop) database objects before recreating
                "-v",  # Verbose output
                str(backup_path)
            ]
            
            # Set password environment variable for pg_restore
            env = os.environ.copy()
            if db_config["password"]:
                env["PGPASSWORD"] = db_config["password"]
                
            # Execute pg_restore
            process = await asyncio.create_subprocess_exec(
                *cmd,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                bt.logging.error(f"pg_restore failed: {stderr.decode()}")
                return False
                
            bt.logging.info(f"Restored PostgreSQL database from backup")
            return True
            
        except Exception as e:
            bt.logging.error(f"Error restoring PostgreSQL database: {e}")
            bt.logging.error(traceback.format_exc())
            return False

    async def _restore_sqlite_database(self, backup_path):
        """
        Restore SQLite database from backup.
        
        Args:
            backup_path: Path to SQLite database backup
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Determine target path
            target_path = self.state_dir / "validator.db"
            
            # Remove existing database if it exists
            if target_path.exists():
                target_path.unlink()
                
            # Copy backup to target path
            import shutil
            shutil.copy2(backup_path, target_path)
            
            bt.logging.info(f"Restored SQLite database from backup")
            return True
            
        except Exception as e:
            bt.logging.error(f"Error restoring SQLite database: {e}")
            return False

    async def _download_state_file(self, local_path, blob_name):
        """
        Download a state file from Azure.
        
        Args:
            local_path: Local path to save file
            blob_name: Blob name in Azure
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Check if file exists in Azure
            blob_properties = await self.blob_client.get_blob_properties(blob=blob_name)
            if not blob_properties:
                bt.logging.warning(f"Blob {blob_name} not found in Azure")
                return False
                
            # Download blob
            with open(local_path, "wb") as download_file:
                download_stream = await self.blob_client.download_blob(blob_name)
                data = await download_stream.readall()
                download_file.write(data)
                
            bt.logging.info(f"Downloaded {blob_name} to {local_path}")
            return True
            
        except Exception as e:
            bt.logging.error(f"Error downloading {blob_name}: {e}")
            return False

    async def _validate_state_files(self):
        """
        Validate that all required state files exist.
        
        Returns:
            bool: True if all files exist, False otherwise
        """
        required_files = [
            self.state_dir / "state.pt"
        ]
        
        # Check if database exists (either SQLite or PostgreSQL)
        db_type = await self._get_database_type()
        if db_type == "postgres":
            # For PostgreSQL, we verify by trying to connect to the database
            try:
                db_config = {'type': 'postgres'}
                db_manager = await DatabaseFactory.create_database_manager(db_config)
                connection_ok = await db_manager.ensure_connection()
                await db_manager.cleanup()
                
                if not connection_ok:
                    bt.logging.error("Cannot connect to PostgreSQL database")
                    return False
            except Exception as e:
                bt.logging.error(f"Error validating PostgreSQL database: {e}")
                return False
        else:
            # For SQLite, check if the file exists
            required_files.append(self.state_dir / "validator.db")
        
        # Check if all required files exist
        for file_path in required_files:
            if not file_path.exists():
                bt.logging.error(f"Required state file not found: {file_path}")
                return False
                
        return True

    async def _should_pull_state(self, remote_metadata: dict) -> bool:
        """
        Determine if state should be pulled based on remote metadata
        """
        try:
            # Check node config
            if os.environ.get("VALIDATOR_PULL_STATE", "True").lower() == "true":
                bt.logging.info("Node config requires state pull")
            else:
                bt.logging.info("Node config does not require state pull")
                return False

            # Check if enough blocks have passed since last pull
            if hasattr(self.validator, 'last_state_pull'):
                current_block = self.validator.subtensor.block
                blocks_since_pull = current_block - self.validator.last_state_pull
                min_blocks_between_pulls = 100  # About 20 minutes at 12s per block
                
                if blocks_since_pull < min_blocks_between_pulls:
                    bt.logging.debug(f"Not enough blocks since last pull ({blocks_since_pull}/{min_blocks_between_pulls})")
                    return False

            # Validate remote metadata structure
            required_fields = ["last_update", "files"]
            
            # Handle legacy format conversion first
            if not all(field in remote_metadata for field in required_fields):
                bt.logging.info("Using legacy metadata format")
                legacy_mapping = {
                    "last_update": "last_update",
                    "files": "files_uploaded"
                }
                
                # Map legacy fields to new fields
                mapped_metadata = {}
                for new_field, legacy_field in legacy_mapping.items():
                    if legacy_field in remote_metadata:
                        if new_field == "files":
                            # Convert legacy files list to new format
                            mapped_metadata[new_field] = {}
                            for file in remote_metadata[legacy_field]:
                                if (self.state_dir / file).exists():
                                    mapped_metadata[new_field][file] = self._get_file_metadata(self.state_dir / file)
                        else:
                            mapped_metadata[new_field] = remote_metadata[legacy_field]
                
                if all(field in mapped_metadata for field in required_fields):
                    remote_metadata = mapped_metadata
                else:
                    bt.logging.warning("Local metadata missing required fields")
                    return True

            # Load local metadata
            if not self.metadata_file.exists():
                bt.logging.info("No local metadata file - should pull state")
                return True
                
            try:
                async with aiofiles.open(self.metadata_file) as f:
                    content = await f.read()
                    local_metadata = json.loads(content)
            except (json.JSONDecodeError, FileNotFoundError):
                bt.logging.warning("Invalid or missing local metadata file")
                return True
                
            # Validate local metadata structure
            if not all(field in local_metadata for field in required_fields):
                bt.logging.warning("Local metadata missing required fields")
                return True
            
            try:
                remote_update = datetime.fromisoformat(remote_metadata["last_update"])
                local_update = datetime.fromisoformat(local_metadata["last_update"])
            except (ValueError, TypeError):
                bt.logging.warning("Invalid timestamp format in metadata")
                return True
            
            # If remote is older than local, don't pull
            if remote_update < local_update:
                bt.logging.debug("Remote state is older than local state")
                return False
                
            # If remote is more than 20 minutes newer than local, pull
            if (remote_update - local_update) > timedelta(minutes=20):
                bt.logging.info("Remote state is significantly newer")
                return True
            
            # Compare files
            remote_files = remote_metadata.get("files", {})
            local_files = local_metadata.get("files", {})
            
            # Handle legacy format
            if isinstance(remote_files, list):
                temp_files = {}
                for file in remote_files:
                    if (self.state_dir / file).exists():
                        temp_files[file] = self._get_file_metadata(self.state_dir / file)
                remote_files = temp_files
            
            for file in self.state_files:
                if file not in remote_files or file not in local_files:
                    bt.logging.debug(f"File {file} missing from metadata")
                    continue
                
                remote_data = remote_files[file]
                local_data = local_files[file]
                
                # Skip if missing hash data
                if not isinstance(remote_data, dict) or not isinstance(local_data, dict):
                    continue
                    
                remote_hash = remote_data.get("hash") or remote_data.get("fuzzy_hash")
                local_hash = local_data.get("hash") or local_data.get("fuzzy_hash")
                
                if not remote_hash or not local_hash:
                    continue
                
                # Compare using fuzzy hashing
                similarity = self._compare_fuzzy_hashes(remote_hash, local_hash)
                
                if similarity < 80:
                    bt.logging.info(f"File {file} has low similarity: {similarity}%")
                    return True
            
            return False
            
        except Exception as e:
            bt.logging.error(f"Error checking state status: {e}")
            bt.logging.error(traceback.format_exc())
            return False

    async def _update_metadata_file(self):
        """Update metadata file with current state information"""
        try:
            metadata = {
                "last_update": datetime.now(timezone.utc).isoformat(),
                "files": {}
            }
            
            for file in self.state_files:
                if file != "state_metadata.json":
                    filepath = self.state_dir / file
                    if filepath.exists():
                        if file == "validator.db":
                            metadata["files"][file] = self._get_db_metadata(filepath)
                        else:
                            metadata["files"][file] = self._get_file_metadata(filepath)

            # Ensure directory exists
            self.metadata_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Write metadata atomically using a temporary file
            temp_metadata = self.metadata_file.with_suffix('.tmp')
            try:
                async with aiofiles.open(temp_metadata, 'w') as f:
                    await f.write(json.dumps(metadata, indent=2))
                temp_metadata.replace(self.metadata_file)
                bt.logging.debug("Metadata file updated successfully")
            finally:
                # Clean up temp file if it still exists
                if temp_metadata.exists():
                    temp_metadata.unlink()
                    
            return True
        except Exception as e:
            bt.logging.error(f"Error updating metadata: {e}")
            bt.logging.error(traceback.format_exc())
            return False

    async def push_state(self):
        """Push state with proper database handling and other state files."""
        if not self.azure_enabled:
            bt.logging.error("Azure blob storage not configured")
            return False

        success = False
        db_manager = self.db_manager
        validator = self.validator
        
        # Determine database type
        db_type = await self._get_database_type()
        
        if db_type == "postgres":
            temp_db = self.state_dir / "postgres_backup.dump"
        else:
            temp_db = self.state_dir / "validator.db.backup"

        try:
            if not validator:
                bt.logging.error("No validator instance available")
                return False

            bt.logging.info(f"Preparing {db_type} database for state push...")

            # Create a verified database backup
            backup_result = await db_manager.create_verified_backup(temp_db)
            bt.logging.debug(f"Backup result: {backup_result}")
            if not backup_result:
                bt.logging.error("Database backup creation or verification failed")
                return False

            # Upload database backup
            upload_success = await self._upload_database_backup(temp_db, db_type)
            if not upload_success:
                bt.logging.error("Failed to upload database backup")
                return False

            # Upload other state files
            for state_file in [
                self.state_dir / "state.pt", 
                self.state_dir / "metagraph.bin"
            ]:
                if state_file.exists():
                    await self._upload_state_file(state_file)

            return True
        except Exception as e:
            bt.logging.error(f"Error during state push: {e}")
            bt.logging.error(traceback.format_exc())
            return False
        finally:
            # Clean up temporary files
            if temp_db.exists():
                try:
                    temp_db.unlink()
                except Exception as e:
                    bt.logging.warning(f"Failed to remove temporary backup: {e}")

    async def _safe_checkpoint(self, checkpoint_type="FULL", max_retries=3):
        """Execute WAL checkpoint with retries and proper error handling"""
        db_manager = self.db_manager
        
        for attempt in range(max_retries):
            try:
                await db_manager.execute_state_sync_query(f"PRAGMA wal_checkpoint({checkpoint_type});")
                return True
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    bt.logging.warning(f"Checkpoint retry {attempt + 1}/{max_retries}")
                    await asyncio.sleep(5 * (attempt + 1))  # Exponential backoff
                    continue
                raise
            except Exception as e:
                bt.logging.error(f"Checkpoint error: {e}")
                raise

    async def _update_metadata_file(self):
        """Update metadata file after successful upload."""
        metadata_path = self.state_dir / "state_metadata.json"
        metadata = {
            "last_update": datetime.utcnow().isoformat(),
            "files": {file: self._get_file_metadata(self.state_dir / file) 
                     for file in self.state_files 
                     if (self.state_dir / file).exists()}
        }
        try:
            async with aiofiles.open(metadata_path, 'w') as f:
                await f.write(json.dumps(metadata))
            bt.logging.debug("Metadata file updated.")
        except Exception as e:
            bt.logging.error(f"Failed to update metadata file: {e}")

    async def _update_hash_file(self):
        """Update hash file after successful upload."""
        hash_path = self.state_dir / "state_hashes.txt"
        try:
            hashes = {}
            for filename in self.state_files:
                filepath = self.state_dir / filename
                if not filepath.exists():
                    continue
                async with aiofiles.open(filepath, 'rb') as f:
                    data = await f.read()
                    hashes[filename] = hashlib.sha256(data).hexdigest()
            async with aiofiles.open(hash_path, 'w') as f:
                await f.write(json.dumps(hashes))
            bt.logging.debug("Hash file updated.")
        except Exception as e:
            bt.logging.error(f"Failed to update hash file: {e}")
