import os
import sys
import subprocess
import asyncio
import configparser
import logging
import traceback
from pathlib import Path
import bittensor as bt

# from bettensor.validator.migration.auto_migrate import AutoMigration # Removed old import
from bettensor.validator.migration.migration_manager import MigrationManager # Added new import

class AutoUpdate:
    """
    Auto update handler for the Bettensor validator.
    Manages post-update tasks like database migrations.
    """
    
    @staticmethod
    async def post_update():
        """
        Execute tasks after an auto-update.
        This function is called by the auto-updater after a successful update.
        """
        bt.logging.info("Running post-update tasks")
        
        # Check if database migration is needed
        if await AutoUpdate.should_migrate_to_postgres():
            bt.logging.info("PostgreSQL migration required after update")
            
            # Set environment variable to trigger migration
            os.environ["BETTENSOR_MIGRATE_TO_POSTGRES"] = "true"
            
            # Run migration
            try:
                # TODO: Need to get sqlite_path and pg_config properly here
                # Placeholder logic - replace with actual config loading
                sqlite_path = os.getenv("SQLITE_DB_PATH", "data/validator.db") 
                pg_config = {
                    "user": os.getenv("POSTGRES_USER", "postgres"),
                    "password": os.getenv("POSTGRES_PASSWORD"),
                    "host": os.getenv("POSTGRES_HOST", "localhost"),
                    "port": os.getenv("POSTGRES_PORT", "5432"),
                    "database": os.getenv("POSTGRES_DB", "bettensor_db"),
                }
                
                # migration = AutoMigration() # OLD
                migration = MigrationManager(sqlite_path=sqlite_path, pg_config=pg_config) # UPDATED Instantiation
                
                # success = await migration.run() # OLD
                success = await migration.migrate() # UPDATED Method Call
                
                if success:
                    bt.logging.info("PostgreSQL migration completed successfully after update")
                else:
                    bt.logging.error("PostgreSQL migration failed after update")
                    
            except Exception as e:
                bt.logging.error(f"Error during post-update migration: {e}")
                bt.logging.error(traceback.format_exc())
        else:
            bt.logging.info("PostgreSQL migration not required after update")
            
        # Add any other post-update tasks here
        await AutoUpdate.update_dependencies()
        
        bt.logging.info("Post-update tasks completed")
        
    @staticmethod
    async def should_migrate_to_postgres():
        """
        Check if migration to PostgreSQL is required based on setup.cfg.
        
        Returns:
            bool: True if migration is required, False otherwise
        """
        # Read configuration from setup.cfg
        config = configparser.ConfigParser()
        try:
            config.read('setup.cfg')
            
            # Check for migration flag
            if 'metadata' in config and config.getboolean('metadata', 'migrate_to_postgres', fallback=False):
                return True
                
            # Check database section
            if 'database' in config:
                db_section = config['database']
                
                # Check current and previous database versions
                current_version = db_section.get('current_version', '0.0.0')
                previous_version = db_section.get('previous_version', '0.0.0')
                
                # If current version is newer than previous, and default type is postgres, migration is needed
                if (current_version > previous_version) and (db_section.get('default_type', '').lower() == 'postgres'):
                    return True
                    
            return False
            
        except Exception as e:
            bt.logging.error(f"Error checking migration requirement: {e}")
            return False
            
    @staticmethod
    async def update_dependencies():
        """
        Update dependencies after update, if needed.
        """
        try:
            # Check if PostgreSQL dependencies are installed
            try:
                import psycopg2
                import asyncpg
                bt.logging.info("PostgreSQL dependencies already installed")
                return
            except ImportError:
                bt.logging.info("PostgreSQL dependencies not installed, installing now")
                
            # Install PostgreSQL dependencies
            cmd = [
                sys.executable, 
                "-m", 
                "pip", 
                "install", 
                "--user", 
                "psycopg2-binary", 
                "asyncpg"
            ]
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            stdout, stderr = process.communicate()
            
            if process.returncode != 0:
                bt.logging.error(f"Failed to install PostgreSQL dependencies: {stderr.decode()}")
            else:
                bt.logging.info("PostgreSQL dependencies installed successfully")
                
        except Exception as e:
            bt.logging.error(f"Error updating dependencies: {e}")
            bt.logging.error(traceback.format_exc())
            
    @staticmethod
    async def pre_update():
        """
        Execute tasks before an auto-update.
        This function is called by the auto-updater before updating.
        """
        bt.logging.info("Running pre-update tasks")
        
        # Add pre-update tasks here
        
        bt.logging.info("Pre-update tasks completed")


# Function to run as entry point for auto-updater
async def run_post_update():
    """Run post-update tasks from command line."""
    await AutoUpdate.post_update()
    
async def run_pre_update():
    """Run pre-update tasks from command line."""
    await AutoUpdate.pre_update()
    
if __name__ == "__main__":
    # Command line interface for auto-updater
    if len(sys.argv) > 1 and sys.argv[1] == "post-update":
        asyncio.run(run_post_update())
    elif len(sys.argv) > 1 and sys.argv[1] == "pre-update":
        asyncio.run(run_pre_update())
    else:
        print("Usage: python -m bettensor.validator.utils.auto_update [pre-update|post-update]")
        sys.exit(1) 