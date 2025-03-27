#!/usr/bin/env python3
import os
import sys
import asyncio
import time
import argparse
import bittensor as bt
import psutil
import json
import subprocess
import logging
from pathlib import Path

# Add parent directory to path so we can import our modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from bettensor.validator.migration.postgres_migration import PostgresMigration
from bettensor.validator.utils.database.database_factory import DatabaseFactory

# Configure logging
LOG_DIR = Path.home() / ".bettensor" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "postgres_migration.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("postgres_migration")

class AutoMigration:
    """
    Automated migration handler for the Bettensor validator.
    Manages the process of automatically upgrading from SQLite to PostgreSQL.
    """
    
    def __init__(self):
        """Initialize the auto migration handler."""
        self.migration_config_file = Path.home() / ".bettensor" / "migration_status.json"
        self.migration_lock_file = Path.home() / ".bettensor" / "migration.lock"
        self.migration_required = False
        self.migration_completed = False
        self.migration_attempted = False
        self.validator_pid = None
        self.migration_config = {}
    
    def _load_migration_status(self):
        """Load migration status from file if it exists."""
        if self.migration_config_file.exists():
            try:
                with open(self.migration_config_file, 'r') as f:
                    self.migration_config = json.load(f)
                    
                self.migration_required = self.migration_config.get('required', False)
                self.migration_completed = self.migration_config.get('completed', False)
                self.migration_attempted = self.migration_config.get('attempted', False)
                self.validator_pid = self.migration_config.get('validator_pid')
                
                logger.info(f"Loaded migration status: required={self.migration_required}, "
                           f"completed={self.migration_completed}, attempted={self.migration_attempted}")
            except Exception as e:
                logger.error(f"Error loading migration status: {e}")
    
    def _save_migration_status(self):
        """Save migration status to file."""
        try:
            self.migration_config.update({
                'required': self.migration_required,
                'completed': self.migration_completed,
                'attempted': self.migration_attempted,
                'validator_pid': self.validator_pid,
                'last_updated': time.time()
            })
            
            with open(self.migration_config_file, 'w') as f:
                json.dump(self.migration_config, f)
                
            logger.info("Saved migration status")
        except Exception as e:
            logger.error(f"Error saving migration status: {e}")
    
    def _check_migration_required(self):
        """
        Check if migration is required based on configuration.
        This can be determined by environment variables, flags, etc.
        """
        # Environment variable trigger
        if os.environ.get("BETTENSOR_MIGRATE_TO_POSTGRES", "").lower() == "true":
            logger.info("Migration required by environment variable")
            return True
            
        # Database configuration - if PostgreSQL is configured but not used
        config_file = Path.home() / ".bettensor" / "database.cfg"
        if config_file.exists():
            try:
                import configparser
                config = configparser.ConfigParser()
                config.read(config_file)
                
                if "Database" in config and config["Database"].get("type", "").lower() == "postgres":
                    # Already using PostgreSQL
                    logger.info("Already using PostgreSQL database")
                    return False
            except Exception as e:
                logger.error(f"Error reading database config: {e}")
        
        # If not explicitly required, check prerequisites to see if migration is possible
        try:
            # Check for PostgreSQL client
            result = subprocess.run(
                ['psql', '--version'], 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE
            )
            if result.returncode != 0:
                logger.info("PostgreSQL client not found, migration not required")
                return False
                
            # Check for PostgreSQL server
            result = subprocess.run(
                ['pg_isready'], 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE
            )
            if result.returncode not in [0, 1]:  # 0 = ready, 1 = accepting connections but not ready
                logger.info("PostgreSQL server not running, migration not required")
                return False
                
            logger.info("Prerequisites met, migration is possible")
            return True
            
        except FileNotFoundError:
            logger.info("PostgreSQL tools not found, migration not required")
            return False
    
    def _check_validator_running(self):
        """
        Check if validator is currently running.
        Returns the process ID if running, None otherwise.
        """
        if self.validator_pid:
            try:
                process = psutil.Process(self.validator_pid)
                if process.is_running() and "python" in process.name().lower():
                    # Double check it's our validator
                    cmdline = " ".join(process.cmdline()).lower()
                    if "bettensor" in cmdline and "validator" in cmdline:
                        logger.info(f"Validator is running with PID {self.validator_pid}")
                        return self.validator_pid
            except psutil.NoSuchProcess:
                logger.info(f"Validator with PID {self.validator_pid} is no longer running")
                
        # Try to find validator process
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = " ".join(proc.cmdline()).lower()
                if "python" in proc.name().lower() and "bettensor" in cmdline and "validator" in cmdline:
                    logger.info(f"Found validator process with PID {proc.pid}")
                    self.validator_pid = proc.pid
                    return proc.pid
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
                
        logger.info("Validator is not running")
        return None
    
    def _stop_validator(self):
        """
        Attempt to gracefully stop the validator process.
        Returns True if successful, False otherwise.
        """
        pid = self._check_validator_running()
        if not pid:
            logger.info("Validator is not running, no need to stop")
            return True
            
        logger.info(f"Attempting to stop validator (PID {pid})...")
        
        try:
            process = psutil.Process(pid)
            process.terminate()
            
            # Wait for process to terminate
            gone, alive = psutil.wait_procs([process], timeout=30)
            
            if process in alive:
                logger.warning("Validator did not terminate gracefully, killing process")
                process.kill()
            
            logger.info("Validator process stopped")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping validator: {e}")
            return False
    
    def _start_validator(self):
        """
        Start the validator process.
        Returns True if successful, False otherwise.
        """
        logger.info("Starting validator...")
        
        try:
            # Determine how the validator was run
            run_cmd = self.migration_config.get('run_command', 'python -m bettensor.validator.cli')
            
            # Start validator as a subprocess
            process = subprocess.Popen(
                run_cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True  # Detach from this process
            )
            
            time.sleep(5)  # Wait a bit to see if it crashes immediately
            
            if process.poll() is None:
                logger.info(f"Validator started with PID {process.pid}")
                self.validator_pid = process.pid
                return True
            else:
                stdout, stderr = process.communicate()
                logger.error(f"Validator failed to start: {stderr.decode()}")
                return False
                
        except Exception as e:
            logger.error(f"Error starting validator: {e}")
            return False
    
    async def _migrate_database(self):
        """
        Perform the actual database migration.
        Returns True if successful, False otherwise.
        """
        logger.info("Starting database migration...")
        
        try:
            # Determine SQLite database path
            sqlite_path = self.migration_config.get('sqlite_path', "./bettensor/validator/state/validator.db")
            
            # Get PostgreSQL config
            postgres_config = {
                "host": self.migration_config.get('postgres_host', "localhost"),
                "port": self.migration_config.get('postgres_port', 5432),
                "user": self.migration_config.get('postgres_user', "postgres"),
                "password": self.migration_config.get('postgres_password', ""),
                "dbname": self.migration_config.get('postgres_dbname', "bettensor_validator")
            }
            
            # Create migration instance
            migration = PostgresMigration(
                sqlite_path=sqlite_path,
                postgres_config=postgres_config
            )
            
            # Execute migration
            success = await migration.migrate()
            
            # Update status
            self.migration_attempted = True
            self.migration_completed = success
            self.migration_config['migration_status'] = migration.get_status()
            
            if success:
                logger.info("Database migration completed successfully")
            else:
                logger.error(f"Database migration failed: {migration.error}")
                
            return success
            
        except Exception as e:
            logger.error(f"Error during database migration: {e}")
            self.migration_attempted = True
            self.migration_completed = False
            return False
    
    def _create_lock_file(self):
        """Create lock file to prevent multiple migrations."""
        try:
            with open(self.migration_lock_file, 'w') as f:
                f.write(str(os.getpid()))
            return True
        except Exception as e:
            logger.error(f"Error creating lock file: {e}")
            return False
    
    def _release_lock_file(self):
        """Remove lock file."""
        try:
            if self.migration_lock_file.exists():
                self.migration_lock_file.unlink()
            return True
        except Exception as e:
            logger.error(f"Error removing lock file: {e}")
            return False
    
    def _check_lock_file(self):
        """
        Check if migration is already in progress.
        Returns True if locked by another process, False otherwise.
        """
        if self.migration_lock_file.exists():
            try:
                with open(self.migration_lock_file, 'r') as f:
                    pid = int(f.read().strip())
                
                # Check if process is running
                try:
                    process = psutil.Process(pid)
                    if process.is_running():
                        logger.info(f"Migration already in progress (PID {pid})")
                        return True
                except psutil.NoSuchProcess:
                    # Process not running, lock file is stale
                    logger.info(f"Removing stale lock file (PID {pid})")
                    self._release_lock_file()
            except Exception as e:
                logger.error(f"Error checking lock file: {e}")
                self._release_lock_file()
                
        return False
    
    async def run(self):
        """
        Run the automated migration process.
        Returns True if successful or not needed, False if failed.
        """
        logger.info("Starting automated migration process")
        
        # Load migration status
        self._load_migration_status()
        
        # Check if migration is already in progress
        if self._check_lock_file():
            logger.info("Migration already in progress, exiting")
            return False
            
        # Create lock file
        if not self._create_lock_file():
            logger.error("Failed to create lock file, exiting")
            return False
            
        try:
            # Check if migration is required
            self.migration_required = self._check_migration_required()
            if not self.migration_required:
                logger.info("Migration not required, exiting")
                self._release_lock_file()
                return True
                
            # Check if migration already completed
            if self.migration_completed:
                logger.info("Migration already completed, exiting")
                self._release_lock_file()
                return True
                
            # Check if validator is running
            self.validator_pid = self._check_validator_running()
            
            # Stop validator if running
            if self.validator_pid:
                if not self._stop_validator():
                    logger.error("Failed to stop validator, exiting")
                    self._release_lock_file()
                    return False
                    
            # Perform migration
            success = await self._migrate_database()
            
            # Start validator
            if not self._start_validator():
                logger.error("Failed to start validator after migration")
                
            # Save migration status
            self._save_migration_status()
            
            logger.info(f"Migration {'completed successfully' if success else 'failed'}")
            return success
            
        finally:
            # Always release lock file
            self._release_lock_file()


async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Automated PostgreSQL Migration")
    parser.add_argument("--force", action="store_true", help="Force migration even if not required")
    parser.add_argument("--sqlite-path", help="Path to SQLite database file")
    parser.add_argument("--postgres-host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--postgres-port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--postgres-user", default="postgres", help="PostgreSQL user")
    parser.add_argument("--postgres-password", default="", help="PostgreSQL password")
    parser.add_argument("--postgres-dbname", default="bettensor_validator", help="PostgreSQL database name")
    parser.add_argument("--validator-command", help="Command to restart validator")
    
    args = parser.parse_args()
    
    # Initialize auto migration
    auto_migration = AutoMigration()
    
    # Set configuration from arguments
    if args.force:
        os.environ["BETTENSOR_MIGRATE_TO_POSTGRES"] = "true"
        
    if args.sqlite_path:
        auto_migration.migration_config['sqlite_path'] = args.sqlite_path
        
    if args.postgres_host:
        auto_migration.migration_config['postgres_host'] = args.postgres_host
        
    if args.postgres_port:
        auto_migration.migration_config['postgres_port'] = args.postgres_port
        
    if args.postgres_user:
        auto_migration.migration_config['postgres_user'] = args.postgres_user
        
    if args.postgres_password:
        auto_migration.migration_config['postgres_password'] = args.postgres_password
        
    if args.postgres_dbname:
        auto_migration.migration_config['postgres_dbname'] = args.postgres_dbname
        
    if args.validator_command:
        auto_migration.migration_config['run_command'] = args.validator_command
    
    # Run migration
    success = await auto_migration.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main()) 