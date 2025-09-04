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
        # Record validator initialize time
        self.initialize_time = datetime.utcnow().isoformat()
                     
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
        """Pull latest state from Azure blob storage if needed."""
        if not self.azure_enabled:
            return False

        max_retries = 3
        retry_count = 0
        db_file = self.state_dir / "validator.db"
        
        while retry_count < max_retries:
            try:
                db_manager = self.db_manager
                bt.logging.info(f"Starting state pull process (attempt {retry_count + 1}/{max_retries})...")
                
                # Create temporary directory for downloads
                temp_dir = self.state_dir / "temp"
                temp_dir.mkdir(exist_ok=True)
                backup_file = None
                
                try:
                    # Download files to temp directory first
                    for filename in self.state_files:
                        blob_client = self.container.get_blob_client(filename)
                        temp_file = temp_dir / filename
                        
                        bt.logging.debug(f"Downloading {filename} to temporary location")
                        try:
                            # Download without conditional headers
                            download_stream = await blob_client.download_blob(
                                timeout=300,  # 5 minute timeout
                                max_concurrency=3,  # Limit concurrent connections
                                validate_content=True,  # Validate downloaded content
                                if_match=None,  # Don't use conditional headers
                                if_none_match=None,
                                if_modified_since=None,
                                if_unmodified_since=None
                            )
                            
                            async with aiofiles.open(temp_file, 'wb') as f:
                                async for chunk in download_stream.chunks():
                                    await f.write(chunk)
                            
                            os.chmod(temp_file, 0o666)
                            bt.logging.debug(f"Successfully downloaded {filename}")
                            
                        except azure.core.exceptions.ResourceModifiedError as e:
                            bt.logging.warning(f"Resource modified during download of {filename}, retrying without conditions")
                            # Retry without any conditions
                            download_stream = await blob_client.download_blob(
                                timeout=300,
                                max_concurrency=3,
                                validate_content=True
                            )
                            async with aiofiles.open(temp_file, 'wb') as f:
                                async for chunk in download_stream.chunks():
                                    await f.write(chunk)
                            os.chmod(temp_file, 0o666)
                            bt.logging.debug(f"Successfully downloaded {filename} on retry")
                            
                        except Exception as e:
                            bt.logging.error(f"Error downloading {filename}: {str(e)}")
                            raise
                    
                    # Special handling for database file
                    temp_db = temp_dir / "validator.db"
                    if temp_db.exists():
                        bt.logging.info("Verifying downloaded database integrity...")
                        try:
                            # Verify database integrity
                            async def verify_database(db_path):
                                try:
                                    conn = sqlite3.connect(db_path)
                                    cursor = conn.cursor()
                                    
                                    # Run integrity check
                                    cursor.execute("PRAGMA integrity_check")
                                    integrity_result = cursor.fetchone()[0]
                                    
                                    # Run quick check
                                    cursor.execute("PRAGMA quick_check")
                                    quick_check = cursor.fetchone()[0]
                                    
                                    cursor.close()
                                    conn.close()
                                    
                                    return integrity_result == "ok" and quick_check == "ok"
                                except Exception as e:
                                    bt.logging.error(f"Database verification failed: {e}")
                                    return False

                            # Verify the downloaded database
                            if not await verify_database(temp_db):
                                raise sqlite3.DatabaseError("Database integrity check failed")
                            
                            # Now handle the database swap
                            try:
                                # Wait for any pending operations to complete
                                await db_manager.wait_for_locks_to_clear(timeout=60)
                                
                                # Clean up any existing WAL/SHM files
                                wal_file = db_file.with_suffix('.db-wal')
                                shm_file = db_file.with_suffix('.db-shm')
                                if wal_file.exists():
                                    wal_file.unlink()
                                if shm_file.exists():
                                    shm_file.unlink()
                                
                                # Create backup of existing database
                                if db_file.exists():
                                    backup_file = db_file.with_suffix('.bak')
                                    shutil.copy2(db_file, backup_file)
                                    os.chmod(backup_file, 0o666)
                                    
                                    # Verify backup integrity
                                    if not await verify_database(backup_file):
                                        bt.logging.error("Backup creation failed integrity check")
                                        raise sqlite3.DatabaseError("Backup creation failed")
                                
                                # Move new database into place
                                shutil.move(temp_db, db_file)
                                os.chmod(db_file, 0o666)
                                
                                # Verify final database
                                if not await verify_database(db_file):
                                    raise sqlite3.DatabaseError("Final database failed integrity check")
                                
                                # Move other state files
                                for filename in self.state_files:
                                    if filename != "validator.db":
                                        src = temp_dir / filename
                                        dst = self.state_dir / filename
                                        if src.exists():
                                            shutil.move(src, dst)
                                            os.chmod(dst, 0o666)
                                
                                # Initialize new database connection
                                try:
                                    # Verify connection works
                                    async with db_manager.engine.connect() as conn:
                                        bt.logging.debug("Verifying connection")
                                        await conn.execute(text("SELECT 1"))
                                        
                                except Exception as e:
                                    bt.logging.error(f"Database initialization failed: {e}")
                                    if backup_file and backup_file.exists():
                                        shutil.move(backup_file, db_file)
                                    raise
                                
                                # Clean up backup file after successful swap
                                if backup_file and backup_file.exists():
                                    backup_file.unlink()
                                
                                # Update metadata file after successful pull
                                await self._update_metadata_file()
                                await self._update_hash_file()
                                
                                # Update last pull block
                                if hasattr(self.validator, 'subtensor'):
                                    self.validator.last_state_pull = self.validator.subtensor.block
                                
                                bt.logging.info("State pull method done")
                                return True
                                
                            except sqlite3.DatabaseError as e:
                                bt.logging.error(f"Database error during swap: {e}")
                                retry_count += 1
                                if retry_count < max_retries:
                                    bt.logging.info(f"Retrying after database error (attempt {retry_count + 1})...")
                                    # Clean up and try again
                                    if backup_file and backup_file.exists():
                                        shutil.move(backup_file, db_file)
                                    continue
                                raise
                                
                        except sqlite3.Error as e:
                            bt.logging.error(f"Database integrity check failed: {e}")
                            retry_count += 1
                            if retry_count < max_retries:
                                bt.logging.info(f"Retrying after integrity check failure (attempt {retry_count + 1})...")
                                continue
                            raise
                        
                finally:
                    # Cleanup temp directory
                    if temp_dir.exists():
                        shutil.rmtree(temp_dir)
                    
                    # Cleanup backup file if it still exists
                    if backup_file and backup_file.exists():
                        try:
                            backup_file.unlink()
                        except Exception as e:
                            bt.logging.warning(f"Failed to remove backup file: {e}")

            except Exception as e:
                bt.logging.error(f"Error during state pull: {str(e)}")
                bt.logging.error(traceback.format_exc())
                retry_count += 1
                
                if retry_count < max_retries:
                    # Clean up any WAL/SHM files before retrying
                    try:
                        wal_file = db_file.with_suffix('.db-wal')
                        shm_file = db_file.with_suffix('.db-shm')
                        if wal_file.exists():
                            wal_file.unlink()
                        if shm_file.exists():
                            shm_file.unlink()
                        bt.logging.info("Cleaned up WAL/SHM files before retry")
                    except Exception as cleanup_error:
                        bt.logging.warning(f"Error cleaning up WAL/SHM files: {cleanup_error}")
                    
                    # Wait before retrying
                    await asyncio.sleep(2 * retry_count)  # Exponential backoff
                    continue
                
                return False

        bt.logging.error(f"Failed to pull state after {max_retries} attempts")
        return False

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

    async def push_state(self):
        """Push state with proper SQLAlchemy async handling and other state files."""
        if not self.azure_enabled:
            bt.logging.error("Azure blob storage not configured")
            return False

        success = False
        db_manager = self.db_manager
        validator = self.validator
        temp_db = self.state_dir / "validator.db.backup"

        try:
            if not validator:
                bt.logging.error("No validator instance available")
                return False

            bt.logging.info("Preparing database for state push...")

            # Create a verified database backup using the backup API
            backup_result = await db_manager.create_verified_backup(temp_db)
            bt.logging.debug(f"Backup result: {backup_result}")
            if not backup_result:
                bt.logging.error("Database backup creation or verification failed")
                return False

            # Upload all state files to Azure
            for filename in self.state_files:
                filepath = self.state_dir / filename
                if not filepath.exists():
                    bt.logging.warning(f"State file {filename} does not exist, skipping.")
                    continue

                # Use temp database backup for validator.db
                upload_path = temp_db if filename == "validator.db" else filepath

                blob_client = self.container.get_blob_client(filename)
                try:
                    async with aiofiles.open(upload_path, 'rb') as f:
                        data = await f.read()

                    # Ensure data is bytes
                    if isinstance(data, dict):
                        bt.logging.error(f"Data for {filename} is a dict. Serializing to bytes.")
                        data = json.dumps(data).encode('utf-8')

                    await blob_client.upload_blob(data, overwrite=True)
                    bt.logging.debug(f"Uploaded {filename} to Azure")
                except Exception as e:
                    bt.logging.error(f"Failed to upload {filename} to Azure: {e}")
                    bt.logging.error(traceback.format_exc())
                    return False

            success = True

            # Update metadata and hash files after successful upload
            if success:
                await self._update_metadata_file()
                await self._update_hash_file()

            return True

        except Exception as e:
            bt.logging.error(f"Error pushing state: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False
        finally:
            # Cleanup temporary database file
            if temp_db.exists():
                try:
                    temp_db.unlink()
                    bt.logging.debug(f"Temporary backup file {temp_db} deleted.")
                except Exception as e:
                    bt.logging.error(f"Error cleaning up temporary database: {e}")

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
            "initialize_time": self.initialize_time,
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
