#!/usr/bin/env python3
import os
import sys
import asyncio
import sqlite3
import json
import argparse
import configparser
from datetime import datetime
from pathlib import Path
from tabulate import tabulate

# Add parent directory to path so we can import our modules
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
sys.path.append(parent_dir)

try:
    import bittensor as bt
    import psycopg2
    from bettensor.validator.database.database_config import load_database_config
except ImportError as e:
    print(f"Error importing required modules: {e}")
    print("Please make sure you have installed all required dependencies")
    sys.exit(1)

def format_size(size_bytes):
    """Format size in bytes to a human-readable format"""
    if size_bytes == 0:
        return "0 B"
    
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB']
    i = 0
    while size_bytes >= 1024 and i < len(suffixes) - 1:
        size_bytes /= 1024
        i += 1
    
    return f"{size_bytes:.2f} {suffixes[i]}"

def check_sqlite_db(db_path):
    """Check SQLite database status and tables"""
    results = {
        "exists": False,
        "size": 0,
        "tables": {},
        "row_counts": {},
        "error": None
    }
    
    db_file = Path(db_path)
    
    if not db_file.exists():
        results["error"] = f"SQLite database not found at {db_path}"
        return results
    
    results["exists"] = True
    results["size"] = db_file.stat().st_size
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get list of tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = [row[0] for row in cursor.fetchall()]
        results["tables"] = {table: {} for table in tables}
        
        # Get row counts for each table
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            results["row_counts"][table] = count
            
            # Get table structure
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            results["tables"][table] = {
                "columns": [col[1] for col in columns],
                "types": [col[2] for col in columns]
            }
        
        conn.close()
        
    except Exception as e:
        results["error"] = f"Error checking SQLite database: {e}"
    
    return results

def check_postgres_db(config):
    """Check PostgreSQL database status and tables"""
    results = {
        "exists": False,
        "tables": {},
        "row_counts": {},
        "size": 0,
        "error": None
    }
    
    try:
        # Connect to default database to check existence
        conn = psycopg2.connect(
            host=config.get("host", "localhost"),
            port=config.get("port", 5432),
            user=config.get("user", "postgres"),
            password=config.get("password", ""),
            dbname="postgres"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", 
                      (config.get("dbname", "bettensor_validator"),))
        exists = cursor.fetchone() is not None
        
        if not exists:
            results["error"] = f"PostgreSQL database '{config.get('dbname', 'bettensor_validator')}' does not exist"
            conn.close()
            return results
        
        results["exists"] = True
        conn.close()
        
        # Connect to the actual database
        conn = psycopg2.connect(
            host=config.get("host", "localhost"),
            port=config.get("port", 5432),
            user=config.get("user", "postgres"),
            password=config.get("password", ""),
            dbname=config.get("dbname", "bettensor_validator")
        )
        cursor = conn.cursor()
        
        # Get list of tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = [row[0] for row in cursor.fetchall()]
        results["tables"] = {table: {} for table in tables}
        
        # Get row counts for each table
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            results["row_counts"][table] = count
            
            # Get table structure
            cursor.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{table}'
                AND table_schema = 'public'
            """)
            columns = cursor.fetchall()
            results["tables"][table] = {
                "columns": [col[0] for col in columns],
                "types": [col[1] for col in columns]
            }
        
        # Get database size
        cursor.execute("""
            SELECT pg_database_size(%s)
        """, (config.get("dbname", "bettensor_validator"),))
        results["size"] = cursor.fetchone()[0]
        
        # Get indexes information
        cursor.execute("""
            SELECT tablename, indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
            ORDER BY tablename, indexname
        """)
        indexes = cursor.fetchall()
        results["indexes"] = {}
        
        for table, index in indexes:
            if table not in results["indexes"]:
                results["indexes"][table] = []
            results["indexes"][table].append(index)
        
        conn.close()
        
    except Exception as e:
        results["error"] = f"Error checking PostgreSQL database: {e}"
    
    return results

def print_database_status(sqlite_status, postgres_status, config):
    """Print the status of both databases in a readable format"""
    print("\n==== Bettensor Database Status ====\n")
    
    # Print current configuration
    print(f"Active Database Type: {config.get('type', 'sqlite')}")
    if config.get('type', 'sqlite') == 'sqlite':
        print(f"SQLite Path: {config.get('path', './bettensor/validator/state/validator.db')}")
    else:
        print(f"PostgreSQL Host: {config.get('host', 'localhost')}")
        print(f"PostgreSQL Port: {config.get('port', 5432)}")
        print(f"PostgreSQL Database: {config.get('dbname', 'bettensor_validator')}")
    print()
    
    # Print SQLite status
    print("=== SQLite Database ===")
    if sqlite_status["error"]:
        print(f"Error: {sqlite_status['error']}")
    elif sqlite_status["exists"]:
        print(f"Status: Exists")
        print(f"Size: {format_size(sqlite_status['size'])}")
        print(f"Tables: {len(sqlite_status['tables'])}")
        
        # Table with row counts
        table_data = []
        for table, count in sqlite_status["row_counts"].items():
            table_data.append([table, count])
        
        print("\nTable Row Counts:")
        print(tabulate(table_data, headers=["Table", "Rows"], tablefmt="simple"))
    else:
        print("Status: Not Found")
    print()
    
    # Print PostgreSQL status
    print("=== PostgreSQL Database ===")
    if postgres_status["error"]:
        print(f"Error: {postgres_status['error']}")
    elif postgres_status["exists"]:
        print(f"Status: Exists")
        print(f"Size: {format_size(postgres_status['size'])}")
        print(f"Tables: {len(postgres_status['tables'])}")
        
        # Table with row counts
        table_data = []
        for table, count in postgres_status["row_counts"].items():
            table_data.append([table, count])
        
        print("\nTable Row Counts:")
        print(tabulate(table_data, headers=["Table", "Rows"], tablefmt="simple"))
        
        # Print indexes
        if "indexes" in postgres_status:
            print("\nIndexes:")
            index_data = []
            for table, indexes in postgres_status["indexes"].items():
                for index in indexes:
                    index_data.append([table, index])
            
            print(tabulate(index_data, headers=["Table", "Index"], tablefmt="simple"))
    else:
        print("Status: Not Found")

def compare_databases(sqlite_status, postgres_status):
    """Compare SQLite and PostgreSQL databases for data consistency"""
    if not sqlite_status["exists"] or not postgres_status["exists"]:
        return {
            "comparable": False,
            "reason": "One or both databases don't exist"
        }
    
    results = {
        "comparable": True,
        "tables": {},
        "missing_tables": {
            "sqlite": [],
            "postgres": []
        },
        "row_count_matches": True
    }
    
    # Check for missing tables
    sqlite_tables = set(sqlite_status["tables"].keys())
    postgres_tables = set(postgres_status["tables"].keys())
    
    results["missing_tables"]["postgres"] = list(sqlite_tables - postgres_tables)
    results["missing_tables"]["sqlite"] = list(postgres_tables - sqlite_tables)
    
    # Compare row counts for common tables
    common_tables = sqlite_tables.intersection(postgres_tables)
    for table in common_tables:
        sqlite_count = sqlite_status["row_counts"][table]
        postgres_count = postgres_status["row_counts"][table]
        match = sqlite_count == postgres_count
        
        results["tables"][table] = {
            "sqlite_count": sqlite_count,
            "postgres_count": postgres_count,
            "row_count_match": match
        }
        
        if not match:
            results["row_count_matches"] = False
    
    return results

def print_comparison(comparison):
    """Print the comparison results in a readable format"""
    print("\n==== Database Comparison ====\n")
    
    if not comparison["comparable"]:
        print(f"Cannot compare databases: {comparison.get('reason', 'Unknown reason')}")
        return
    
    # Print missing tables
    sqlite_missing = comparison["missing_tables"]["sqlite"]
    postgres_missing = comparison["missing_tables"]["postgres"]
    
    if sqlite_missing:
        print(f"Tables in PostgreSQL but missing in SQLite: {', '.join(sqlite_missing)}")
    
    if postgres_missing:
        print(f"Tables in SQLite but missing in PostgreSQL: {', '.join(postgres_missing)}")
    
    if not sqlite_missing and not postgres_missing:
        print("All tables exist in both databases.")
    
    # Print row count comparison
    print("\nRow Count Comparison:")
    table_data = []
    for table, info in comparison["tables"].items():
        sqlite_count = info["sqlite_count"]
        postgres_count = info["postgres_count"]
        match = "✓" if info["row_count_match"] else "✗"
        
        table_data.append([table, sqlite_count, postgres_count, match])
    
    print(tabulate(table_data, headers=["Table", "SQLite Rows", "PostgreSQL Rows", "Match"], tablefmt="simple"))
    
    # Print overall match status
    if comparison["row_count_matches"]:
        print("\nAll row counts match between SQLite and PostgreSQL.")
    else:
        print("\nSome row counts don't match between SQLite and PostgreSQL.")

async def main():
    """Main entry point for the database status checker"""
    parser = argparse.ArgumentParser(description="Check Bettensor database status")
    parser.add_argument("--sqlite-path", help="Path to SQLite database file")
    parser.add_argument("--postgres-host", help="PostgreSQL host")
    parser.add_argument("--postgres-port", type=int, help="PostgreSQL port")
    parser.add_argument("--postgres-user", help="PostgreSQL user")
    parser.add_argument("--postgres-password", help="PostgreSQL password")
    parser.add_argument("--postgres-dbname", help="PostgreSQL database name")
    parser.add_argument("--json", action="store_true", help="Output in JSON format")
    parser.add_argument("--compare", action="store_true", help="Compare SQLite and PostgreSQL databases")
    
    args = parser.parse_args()
    
    # Get current configuration using the new function
    config_override = {}
    if args.sqlite_path:
        config_override["path"] = args.sqlite_path
    if args.postgres_host:
        config_override["host"] = args.postgres_host
    if args.postgres_port:
        config_override["port"] = args.postgres_port
    if args.postgres_user:
        config_override["user"] = args.postgres_user
    if args.postgres_password:
        config_override["password"] = args.postgres_password
    if args.postgres_dbname:
        config_override["dbname"] = args.postgres_dbname
        
    config = load_database_config(config_override) # Load config with overrides
    
    # Check SQLite status using the path from the final config
    sqlite_status = check_sqlite_db(config["path"])
    
    # Check PostgreSQL status
    postgres_status = check_postgres_db(config)
    
    if args.json:
        # Output as JSON
        output = {
            "config": config,
            "sqlite": sqlite_status,
            "postgres": postgres_status,
            "timestamp": datetime.now().isoformat()
        }
        
        if args.compare:
            output["comparison"] = compare_databases(sqlite_status, postgres_status)
        
        print(json.dumps(output, indent=2))
    else:
        # Print formatted status
        print_database_status(sqlite_status, postgres_status, config)
        
        if args.compare:
            comparison = compare_databases(sqlite_status, postgres_status)
            print_comparison(comparison)

if __name__ == "__main__":
    asyncio.run(main()) 