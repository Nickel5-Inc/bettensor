import os
import configparser
from pathlib import Path
import bittensor as bt

from bettensor.validator.utils.database.database_manager import DatabaseManager as SQLiteDatabaseManager
from bettensor.validator.utils.database.postgres_database_manager import PostgresDatabaseManager

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
        # Default config directory
        config_dir = Path.home() / ".bettensor"
        config_file = config_dir / "database.cfg"
        
        # Parse config file if it exists
        file_config = {}
        if config_file.exists():
            try:
                config = configparser.ConfigParser()
                config.read(config_file)
                
                if "Database" in config:
                    db_section = config["Database"]
                    file_config = {
                        "type": db_section.get("type", "sqlite"),
                        "host": db_section.get("host", "localhost"),
                        "port": db_section.getint("port", 5432),
                        "user": db_section.get("user", "postgres"),
                        "password": db_section.get("password", ""),
                        "dbname": db_section.get("dbname", "bettensor_validator"),
                        "path": db_section.get("path", "./bettensor/validator/state/validator.db")
                    }
            except Exception as e:
                bt.logging.error(f"Error reading database config file: {e}")
        
        # Initialize config with defaults, then override with file config
        config = {
            "type": "sqlite",
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "",
            "dbname": "bettensor_validator",
            "path": "./bettensor/validator/state/validator.db"
        }
        config.update(file_config)
        
        # Override with provided config if any
        if db_config:
            if isinstance(db_config, str):
                # Treat string as SQLite path
                config["type"] = "sqlite"
                config["path"] = db_config
            elif isinstance(db_config, dict):
                config.update(db_config)
        
        # Check for environment variables
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
        
        # Create appropriate database manager
        db_manager = None
        
        if config["type"].lower() == "postgres":
            bt.logging.info("Using PostgreSQL database manager")
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
    def save_config(config):
        """
        Save database configuration to file.
        
        Args:
            config: Dictionary containing database configuration
        """
        config_dir = Path.home() / ".bettensor"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_file = config_dir / "database.cfg"
        
        parser = configparser.ConfigParser()
        parser["Database"] = {
            "type": config.get("type", "sqlite"),
            "host": config.get("host", "localhost"),
            "port": str(config.get("port", 5432)),
            "user": config.get("user", "postgres"),
            "password": config.get("password", ""),
            "dbname": config.get("dbname", "bettensor_validator"),
            "path": config.get("path", "./bettensor/validator/state/validator.db")
        }
        
        with open(config_file, 'w') as f:
            parser.write(f)
            
        bt.logging.info(f"Database configuration saved to {config_file}") 