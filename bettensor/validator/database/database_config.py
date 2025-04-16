# bettensor/validator/database/database_config.py
import os
import configparser
import json
from pathlib import Path
import bittensor as bt

DEFAULT_SQLITE_PATH = "./bettensor/validator/state/validator.db" # Default if no config found

def load_database_config(db_config_override=None):
    """
    Load database configuration from multiple sources with precedence.
    
    Precedence:
    1. Environment Variables (BETTENSOR_DB_*)
    2. `db_config_override` dictionary (if provided)
    3. Config file (JSON or INI) in ./config/database.cfg or ./database.cfg
    4. Config file (JSON or INI) in ~/.bettensor/database.cfg
    5. Default values
    
    Creates a default config file if none is found.
    
    Args:
        db_config_override: Optional dictionary to override loaded config.
        
    Returns:
        dict: The final database configuration.
    """
    # Initialize config with defaults
    config = {
        "type": "postgres", # Defaulting to postgres now
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "",
        "dbname": "bettensor_validator",
        "path": DEFAULT_SQLITE_PATH # Keep for migration reference
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
                    bt.logging.info(f"Loaded database config (JSON) from: {config_file}")
                    config_file_used = config_file
                    break
                except json.JSONDecodeError:
                    # If JSON fails, try as INI format
                    config_parser = configparser.ConfigParser()
                    config_parser.read(config_file)
                    
                    if "Database" in config_parser:
                        db_section = config_parser["Database"]
                        file_config = {
                            "type": db_section.get("type", config["type"]),
                            "host": db_section.get("host", config["host"]),
                            "port": db_section.getint("port", config["port"]),
                            "user": db_section.get("user", config["user"]),
                            "password": db_section.get("password", config["password"]),
                            "dbname": db_section.get("dbname", config["dbname"]),
                            "path": db_section.get("path", config["path"])
                        }
                    bt.logging.info(f"Loaded database config (INI) from: {config_file}")
                    config_file_used = config_file
                    break
            except Exception as e:
                bt.logging.error(f"Error reading database config file {config_file}: {e}")
    
    # Fall back to user's home directory config if repository config wasn't found
    if not config_file_used:
        user_config_file = Path.home() / ".bettensor" / "database.cfg"
        if user_config_file.exists():
            try:
                # Try to parse as JSON
                try:
                    with open(user_config_file, 'r') as f:
                        file_config = json.load(f)
                    bt.logging.info(f"Loaded database config (JSON) from user directory: {user_config_file}")
                    config_file_used = user_config_file
                except json.JSONDecodeError:
                    # If JSON fails, try as INI format
                    config_parser = configparser.ConfigParser()
                    config_parser.read(user_config_file)
                    
                    if "Database" in config_parser:
                        db_section = config_parser["Database"]
                        file_config = {
                            "type": db_section.get("type", config["type"]),
                            "host": db_section.get("host", config["host"]),
                            "port": db_section.getint("port", config["port"]),
                            "user": db_section.get("user", config["user"]),
                            "password": db_section.get("password", config["password"]),
                            "dbname": db_section.get("dbname", config["dbname"]),
                            "path": db_section.get("path", config["path"])
                        }
                    bt.logging.info(f"Loaded database config (INI) from user directory: {user_config_file}")
                    config_file_used = user_config_file
            except Exception as e:
                bt.logging.error(f"Error reading user database config file {user_config_file}: {e}")
    
    # Create a default config file if none exists
    if not config_file_used:
        bt.logging.warning("No database configuration file found. Creating default configuration.")
        # Save default config (only postgres relevant fields needed)
        default_save_config = {
            "type": config["type"],
            "host": config["host"],
            "port": config["port"],
            "user": config["user"],
            "password": config["password"],
            "dbname": config["dbname"]
        }
        save_database_config(default_save_config)
        # Use the defaults we already have
        file_config = {}

    # Update config with file values
    config.update(file_config)
    
    # Override with provided dict if any
    if db_config_override and isinstance(db_config_override, dict):
        config.update(db_config_override)
        bt.logging.debug(f"Database config updated with override dict: {db_config_override}")
    
    # Check for environment variables (highest priority)
    env_config = {}
    if os.environ.get("BETTENSOR_DB_TYPE"):
        env_config["type"] = os.environ.get("BETTENSOR_DB_TYPE")
    if os.environ.get("BETTENSOR_DB_HOST"):
        env_config["host"] = os.environ.get("BETTENSOR_DB_HOST")
    if os.environ.get("BETTENSOR_DB_PORT"):
        try:
            env_config["port"] = int(os.environ.get("BETTENSOR_DB_PORT"))
        except (ValueError, TypeError):
            bt.logging.warning("Invalid BETTENSOR_DB_PORT environment variable. Using default/config value.")
    if os.environ.get("BETTENSOR_DB_USER"):
        env_config["user"] = os.environ.get("BETTENSOR_DB_USER")
    if os.environ.get("BETTENSOR_DB_PASSWORD") is not None: # Allow empty string from env
        env_config["password"] = os.environ.get("BETTENSOR_DB_PASSWORD")
    if os.environ.get("BETTENSOR_DB_NAME"):
        env_config["dbname"] = os.environ.get("BETTENSOR_DB_NAME")
    if os.environ.get("BETTENSOR_DB_PATH"): # Only relevant for migration
        env_config["path"] = os.environ.get("BETTENSOR_DB_PATH")
        
    if env_config:
         config.update(env_config)
         bt.logging.info(f"Database config updated with environment variables: {list(env_config.keys())}")

    # Final validation - ensure type is postgres for validator operation
    # The migration manager might temporarily use sqlite path, but operation requires postgres.
    if config["type"].lower() != "postgres":
        bt.logging.warning(f"Database type configured as '{config['type']}' but validator requires 'postgres'. Proceeding with postgres assumption.")
        config["type"] = "postgres"

    # Debug info (mask password)
    log_config = config.copy()
    if log_config.get("password"):
        log_config["password"] = "****"
    bt.logging.debug(f"Final database config: {log_config}")
    
    return config

def save_database_config(config, use_repo_config=True):
    """
    Save database configuration to file (JSON format preferred).
    
    Args:
        config: Dictionary containing database configuration to save.
        use_repo_config: Whether to try saving to repository config first.
    """
    config_to_save = config.copy()
    # Remove non-essential keys if present
    config_to_save.pop("path", None)
    
    # Try to save to repository config first if requested
    repo_config_path = Path("./config")
    repo_config_file = repo_config_path / "database.cfg"
    
    saved_path = None
    
    if use_repo_config:
        try:
            # Ensure parent directory exists
            if repo_config_path.parent.exists():
                repo_config_path.mkdir(parents=True, exist_ok=True)
                with open(repo_config_file, 'w') as f:
                    json.dump(config_to_save, f, indent=4)
                saved_path = repo_config_file
                bt.logging.info(f"Database configuration saved to repository: {saved_path}")
        except Exception as e:
             bt.logging.warning(f"Could not save config to repository ({repo_config_file}): {e}. Trying user directory.")
             saved_path = None # Ensure we fallback

    # Fall back to user's home directory if repo save failed or wasn't requested/possible
    if not saved_path:
        try:
            config_dir = Path.home() / ".bettensor"
            config_dir.mkdir(parents=True, exist_ok=True)
            config_file = config_dir / "database.cfg"
            
            with open(config_file, 'w') as f:
                json.dump(config_to_save, f, indent=4)
            saved_path = config_file
            bt.logging.info(f"Database configuration saved to user directory: {saved_path}")
        except Exception as e:
            bt.logging.error(f"Failed to save database configuration to user directory ({config_file}): {e}")

    return saved_path 