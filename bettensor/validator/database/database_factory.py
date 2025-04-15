import os
import configparser
import json
from pathlib import Path
import bittensor as bt

from bettensor.validator.database.database_manager import DatabaseManager as SQLiteDatabaseManager
from bettensor.validator.database.postgres_database_manager import PostgresDatabaseManager

class DatabaseFactory:
    """
    Factory class to create the appropriate database manager based on configuration.
    Handles creating either SQLite or PostgreSQL database managers.
    """
    
    @staticmethod
    async def create_database_manager(db_config=None):
        """
        Create and initialize the appropriate database manager based on configuration.
        
        Args:
            db_config: Database configuration dictionary or path to database file.
                If a string is provided, it's treated as the SQLite database path.
                If a dictionary is provided, it can contain:
                - 'type': 'sqlite' or 'postgres'
                - For PostgreSQL: 'host', 'port', 'user', 'password', 'dbname'
                - For SQLite: 'path'
                
        Returns:
            An initialized database manager (either SQLite or PostgreSQL)
        """
        # Initialize config with defaults
        config = {
            "type": "sqlite",
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "",
            "dbname": "bettensor_validator",
            "path": "./bettensor/validator/state/validator.db",
            "migration_in_progress": False,
            "migration_completed": False
        }
        
        # Check for config in repository first (preferred location)
        repo_config_files = [
            Path("./config/database.cfg"),  # Repository config
            Path("./database.cfg"),         # Root directory config
        ]
        
        file_config = {}
        config_file_used = None
        
        # Try repository config files first
        for config_file in repo_config_files:
            if config_file.exists():
                try:
                    # Try to parse as JSON
                    try:
                        with open(config_file, 'r') as f:
                            file_config = json.load(f)
                        bt.logging.info(f"Loaded JSON config from repository: {config_file}")
                        config_file_used = config_file
                        break
                    except json.JSONDecodeError:
                        # If JSON fails, try as INI format
                        config_parser = configparser.ConfigParser()
                        config_parser.read(config_file)
                        
                        if "Database" in config_parser:
                            db_section = config_parser["Database"]
                            file_config = {
                                "type": db_section.get("type", "sqlite"),
                                "host": db_section.get("host", "localhost"),
                                "port": db_section.getint("port", 5432),
                                "user": db_section.get("user", "postgres"),
                                "password": db_section.get("password", ""),
                                "dbname": db_section.get("dbname", "bettensor_validator"),
                                "path": db_section.get("path", "./bettensor/validator/state/validator.db")
                            }
                        bt.logging.info(f"Loaded INI config from repository: {config_file}")
                        config_file_used = config_file
                        break
                except Exception as e:
                    bt.logging.error(f"Error reading repository config file {config_file}: {e}")
        
        # Fall back to user's home directory config if repository config wasn't found
        if not config_file_used:
            user_config_file = Path.home() / ".bettensor" / "database.cfg"
            if user_config_file.exists():
                try:
                    # Try to parse as JSON
                    try:
                        with open(user_config_file, 'r') as f:
                            file_config = json.load(f)
                        bt.logging.info(f"Loaded JSON config from user directory: {user_config_file}")
                        config_file_used = user_config_file
                    except json.JSONDecodeError:
                        # If JSON fails, try as INI format
                        config_parser = configparser.ConfigParser()
                        config_parser.read(user_config_file)
                        
                        if "Database" in config_parser:
                            db_section = config_parser["Database"]
                            file_config = {
                                "type": db_section.get("type", "sqlite"),
                                "host": db_section.get("host", "localhost"),
                                "port": db_section.getint("port", 5432),
                                "user": db_section.get("user", "postgres"),
                                "password": db_section.get("password", ""),
                                "dbname": db_section.get("dbname", "bettensor_validator"),
                                "path": db_section.get("path", "./bettensor/validator/state/validator.db")
                            }
                        bt.logging.info(f"Loaded INI config from user directory: {user_config_file}")
                        config_file_used = user_config_file
                except Exception as e:
                    bt.logging.error(f"Error reading user config file {user_config_file}: {e}")
        
        # Create a default config file if none exists
        if not config_file_used:
            bt.logging.info("No configuration file found. Creating default configuration.")
            # Try to create in repository first
            repo_config_path = Path("./config")
            if repo_config_path.exists() or repo_config_path.parent.exists():
                # Create directory if it doesn't exist
                repo_config_path.mkdir(parents=True, exist_ok=True)
                config_file = repo_config_path / "database.cfg"
                DatabaseFactory.save_config(config, use_repo_config=True)
                bt.logging.info(f"Created default configuration in repository: {config_file}")
            else:
                # Fall back to user's home directory
                config_file = Path.home() / ".bettensor" / "database.cfg"
                config_dir = config_file.parent
                config_dir.mkdir(parents=True, exist_ok=True)
                with open(config_file, 'w') as f:
                    json.dump(config, f, indent=4)
                bt.logging.info(f"Created default configuration in user directory: {config_file}")
        
        # Update config with file values
        config.update(file_config)
        
        # Override with provided config if any
        if db_config:
            if isinstance(db_config, str):
                # Treat string as SQLite path
                config["type"] = "sqlite"
                config["path"] = db_config
            elif isinstance(db_config, dict):
                config.update(db_config)
        
        # Check for environment variables (highest priority)
        if os.environ.get("BETTENSOR_DB_TYPE"):
            config["type"] = os.environ.get("BETTENSOR_DB_TYPE")
        if os.environ.get("BETTENSOR_DB_HOST"):
            config["host"] = os.environ.get("BETTENSOR_DB_HOST")
        if os.environ.get("BETTENSOR_DB_PORT"):
            config["port"] = int(os.environ.get("BETTENSOR_DB_PORT"))
        if os.environ.get("BETTENSOR_DB_USER"):
            config["user"] = os.environ.get("BETTENSOR_DB_USER")
        if os.environ.get("BETTENSOR_DB_PASSWORD"):
            config["password"] = os.environ.get("BETTENSOR_DB_PASSWORD")
        if os.environ.get("BETTENSOR_DB_NAME"):
            config["dbname"] = os.environ.get("BETTENSOR_DB_NAME")
        if os.environ.get("BETTENSOR_DB_PATH"):
            config["path"] = os.environ.get("BETTENSOR_DB_PATH")
        
        # Debug info
        bt.logging.debug(f"Final database config: {config}")
        
        # Create appropriate database manager
        db_manager = None
        
        if config["type"].lower() == "postgres":
            bt.logging.info("Using PostgreSQL database manager")
            bt.logging.info(f"PostgreSQL password length: {len(config['password'])}, first char: {config['password'][0] if config['password'] else 'empty'}")
            
            # TEMPORARY FIX: Override with hardcoded password if empty
            if not config["password"]:
                bt.logging.warning("Empty PostgreSQL password detected, using hardcoded 'postgres' password as fallback")
                config["password"] = "postgres"
                
            db_manager = PostgresDatabaseManager(
                database_path=None,
                host=config["host"],
                port=config["port"],
                user=config["user"],
                password=config["password"],
                dbname=config["dbname"]
            )
        else:
            bt.logging.info("Using SQLite database manager")
            db_manager = SQLiteDatabaseManager(config["path"])
        
        # Initialize the database
        await db_manager.initialize()
        
        return db_manager
    
    @staticmethod
    def save_config(config, use_repo_config=True):
        """
        Save database configuration to file.
        
        Args:
            config: Dictionary containing database configuration
            use_repo_config: Whether to save to repository config (if possible)
        """
        # Try to save to repository config first if requested
        repo_config_path = Path("./config")
        repo_config_file = repo_config_path / "database.cfg"
        
        if use_repo_config and (repo_config_path.exists() or Path("./config").parent.exists()):
            # Create config directory if it doesn't exist
            repo_config_path.mkdir(parents=True, exist_ok=True)
            
            # Save as JSON
            with open(repo_config_file, 'w') as f:
                json.dump(config, f, indent=4)
            
            bt.logging.info(f"Database configuration saved to repository: {repo_config_file}")
            return
        
        # Fall back to user's home directory
        config_dir = Path.home() / ".bettensor"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_file = config_dir / "database.cfg"
        
        # Save as JSON for compatibility
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=4)
            
        bt.logging.info(f"Database configuration saved to user directory: {config_file}") 