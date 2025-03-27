#!/bin/bash
# The MIT License (MIT)
# Copyright © 2023 Bettensor
# See license at: https://github.com/bettensor/bettensor/blob/main/LICENSE

# PostgreSQL Migration Script for Bettensor Validator
# This script helps users migrate their validator database from SQLite to PostgreSQL

set -e

YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Default values
SQLITE_PATH="./bettensor/validator/state/validator.db"
POSTGRES_HOST="localhost"
POSTGRES_PORT="5432"
POSTGRES_USER="postgres"
POSTGRES_PASSWORD=""
POSTGRES_DBNAME="bettensor_validator"
BACKUP_PATH="$HOME/bettensor_sqlite_backup.db"
FORCE=false
RESTART_VALIDATOR=true

# Print banner
echo -e "${BOLD}${GREEN}"
echo "====================================================================================="
echo "  _____           _                            _____   ____   _      __  __ _       "
echo " |  __ \         | |                          / ____| / __ \ | |    |  \/  (_)      "
echo " | |__) |___  ___| |_ __ _ _ __ ___  ___    | (___  | |  | || |    | \  / |_  __ _ "
echo " |  ___// _ \/ __| __/ _\` | '__/ _ \/ __|    \___ \ | |  | || |    | |\/| | |/ _\` |"
echo " | |   | (_) \__ \ || (_| | | |  __/\__ \    ____) || |__| || |____| |  | | | (_| |"
echo " |_|    \___/|___/\__\__, |_|  \___||___/   |_____/  \___\_\______|_|  |_|_|\__, |"
echo "                       __/ |                                                  __/ |"
echo "                      |___/                                                  |___/ "
echo "====================================================================================="
echo -e "${NC}"

# Function to display usage
usage() {
    echo -e "${BOLD}Usage:${NC} $0 [options]"
    echo
    echo "This script migrates the Bettensor validator database from SQLite to PostgreSQL."
    echo
    echo -e "${BOLD}Options:${NC}"
    echo "  -h, --help                 Show this help message and exit"
    echo "  -s, --sqlite-path PATH     Path to SQLite database file"
    echo "                             Default: $SQLITE_PATH"
    echo "  --host HOST                PostgreSQL host"
    echo "                             Default: $POSTGRES_HOST"
    echo "  -p, --port PORT            PostgreSQL port"
    echo "                             Default: $POSTGRES_PORT"
    echo "  -u, --user USER            PostgreSQL user"
    echo "                             Default: $POSTGRES_USER"
    echo "  --password PASSWORD        PostgreSQL password"
    echo "  -d, --dbname DBNAME        PostgreSQL database name"
    echo "                             Default: $POSTGRES_DBNAME"
    echo "  -b, --backup PATH          Path to backup SQLite database"
    echo "                             Default: $BACKUP_PATH"
    echo "  -f, --force                Force migration without confirmation"
    echo "  --no-restart               Don't restart validator after migration"
    echo
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            ;;
        -s|--sqlite-path)
            SQLITE_PATH="$2"
            shift 2
            ;;
        --host)
            POSTGRES_HOST="$2"
            shift 2
            ;;
        -p|--port)
            POSTGRES_PORT="$2"
            shift 2
            ;;
        -u|--user)
            POSTGRES_USER="$2"
            shift 2
            ;;
        --password)
            POSTGRES_PASSWORD="$2"
            shift 2
            ;;
        -d|--dbname)
            POSTGRES_DBNAME="$2"
            shift 2
            ;;
        -b|--backup)
            BACKUP_PATH="$2"
            shift 2
            ;;
        -f|--force)
            FORCE=true
            shift
            ;;
        --no-restart)
            RESTART_VALIDATOR=false
            shift
            ;;
        *)
            echo -e "${RED}Error: Unknown option $1${NC}"
            usage
            ;;
    esac
done

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check if Python is installed
if ! command_exists python; then
    echo -e "${RED}Error: Python is not installed. Please install Python 3.8 or higher.${NC}"
    exit 1
fi

# Create backup directory if it doesn't exist
backup_dir=$(dirname "$BACKUP_PATH")
mkdir -p "$backup_dir"

# Check if SQLite database exists
if [ ! -f "$SQLITE_PATH" ]; then
    echo -e "${RED}Error: SQLite database not found at $SQLITE_PATH${NC}"
    echo "Please provide the correct path using the -s or --sqlite-path option."
    exit 1
fi

# Check if PostgreSQL is installed
if ! command_exists psql; then
    echo -e "${YELLOW}Warning: PostgreSQL client (psql) not found.${NC}"
    echo "It's recommended to install PostgreSQL client tools for better verification."
    echo "You can install them with:"
    echo "  - Ubuntu/Debian: sudo apt-get install postgresql-client"
    echo "  - macOS: brew install postgresql"
    echo "  - Windows: Download from https://www.postgresql.org/download/windows/"
    echo
    
    if [ "$FORCE" != "true" ]; then
        read -p "Continue without PostgreSQL client tools? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Migration cancelled."
            exit 1
        fi
    fi
fi

# Check if psycopg2 is installed
python -c "import psycopg2" >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo -e "${YELLOW}Warning: psycopg2 Python module not found.${NC}"
    echo "This is required for the migration. Attempting to install it..."
    
    pip install psycopg2-binary
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to install psycopg2. Please install it manually:${NC}"
        echo "  pip install psycopg2-binary"
        exit 1
    else
        echo -e "${GREEN}Successfully installed psycopg2.${NC}"
    fi
fi

# Check if asyncpg is installed
python -c "import asyncpg" >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo -e "${YELLOW}Warning: asyncpg Python module not found.${NC}"
    echo "This is required for the PostgreSQL database manager. Attempting to install it..."
    
    pip install asyncpg
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to install asyncpg. Please install it manually:${NC}"
        echo "  pip install asyncpg"
        exit 1
    else
        echo -e "${GREEN}Successfully installed asyncpg.${NC}"
    fi
fi

# If password is not provided, ask for it
if [ -z "$POSTGRES_PASSWORD" ]; then
    echo -e "${YELLOW}PostgreSQL password not provided.${NC}"
    read -sp "Enter PostgreSQL password for user $POSTGRES_USER: " POSTGRES_PASSWORD
    echo
fi

# Backup SQLite database
echo -e "${YELLOW}Backing up SQLite database to $BACKUP_PATH...${NC}"
cp "$SQLITE_PATH" "$BACKUP_PATH"
echo -e "${GREEN}Backup created at $BACKUP_PATH${NC}"

# Confirm migration
if [ "$FORCE" != "true" ]; then
    echo -e "${YELLOW}${BOLD}Warning:${NC} This will migrate your database from SQLite to PostgreSQL."
    echo "Make sure your PostgreSQL server is running and accessible with the provided credentials."
    echo
    echo -e "${BOLD}Migration Details:${NC}"
    echo "  SQLite Database: $SQLITE_PATH"
    echo "  PostgreSQL Host: $POSTGRES_HOST"
    echo "  PostgreSQL Port: $POSTGRES_PORT"
    echo "  PostgreSQL User: $POSTGRES_USER"
    echo "  PostgreSQL Database: $POSTGRES_DBNAME"
    echo
    echo "A backup of your SQLite database will be created at: $BACKUP_PATH"
    echo
    read -p "Do you want to continue with the migration? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Migration cancelled."
        exit 1
    fi
fi

# Test PostgreSQL connection
echo -e "${YELLOW}Testing PostgreSQL connection...${NC}"
if command_exists psql; then
    PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -c "\q" postgres
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to connect to PostgreSQL. Please check your credentials and ensure the server is running.${NC}"
        exit 1
    fi
else
    # Use Python to test connection if psql is not available
    python -c "
import psycopg2
try:
    conn = psycopg2.connect(
        host='$POSTGRES_HOST',
        port=$POSTGRES_PORT,
        user='$POSTGRES_USER',
        password='$POSTGRES_PASSWORD',
        database='postgres'
    )
    conn.close()
    print('Connection successful')
except Exception as e:
    print(f'Connection failed: {e}')
    exit(1)
"
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to connect to PostgreSQL. Please check your credentials and ensure the server is running.${NC}"
        exit 1
    fi
fi
echo -e "${GREEN}PostgreSQL connection successful!${NC}"

# Run migration
echo -e "${YELLOW}Starting migration...${NC}"
echo "This may take some time depending on the size of your database."
echo

# Get directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Run migration using Python module
python -m bettensor.validator.migration.auto_migrate \
    --force \
    --sqlite-path "$SQLITE_PATH" \
    --postgres-host "$POSTGRES_HOST" \
    --postgres-port "$POSTGRES_PORT" \
    --postgres-user "$POSTGRES_USER" \
    --postgres-password "$POSTGRES_PASSWORD" \
    --postgres-dbname "$POSTGRES_DBNAME"

if [ $? -ne 0 ]; then
    echo -e "${RED}Migration failed. Please check the error messages above.${NC}"
    echo "Your original SQLite database has not been modified and is still at $SQLITE_PATH"
    echo "A backup was also created at $BACKUP_PATH"
    exit 1
fi

echo -e "${GREEN}${BOLD}Migration completed successfully!${NC}"
echo

# Restart validator if requested
if [ "$RESTART_VALIDATOR" = true ]; then
    echo -e "${YELLOW}Restarting validator...${NC}"
    
    # Find validator process ID
    VALIDATOR_PID=$(pgrep -f "python.*bettensor.validator.cli" || true)
    
    if [ -n "$VALIDATOR_PID" ]; then
        echo "Stopping validator process (PID: $VALIDATOR_PID)..."
        kill "$VALIDATOR_PID"
        sleep 2
        
        # Check if process is still running
        if kill -0 "$VALIDATOR_PID" 2>/dev/null; then
            echo "Validator did not stop gracefully, forcing termination..."
            kill -9 "$VALIDATOR_PID"
            sleep 1
        fi
    else
        echo "No running validator process found, starting new instance..."
    fi
    
    # Start validator in background
    echo "Starting validator..."
    python -m bettensor.validator.cli > /dev/null 2>&1 &
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Validator restarted successfully with PID $!${NC}"
    else
        echo -e "${RED}Failed to restart validator. Please start it manually:${NC}"
        echo "  python -m bettensor.validator.cli"
    fi
fi

echo
echo -e "${GREEN}${BOLD}PostgreSQL Migration Complete!${NC}"
echo
echo "Your validator is now using PostgreSQL for its database."
echo "The original SQLite database is still available at: $SQLITE_PATH"
echo "A backup was also created at: $BACKUP_PATH"
echo
echo "If you encounter any issues, you can revert to SQLite using the following command:"
echo "  python -m bettensor.validator.utils.database_tools set-type sqlite"
echo
echo "Thank you for upgrading to PostgreSQL!" 