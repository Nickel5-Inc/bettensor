# PostgreSQL Migration for Bettensor Validator

This directory contains the tools and scripts necessary to migrate the Bettensor validator from SQLite to PostgreSQL.

## Prerequisites

1. **PostgreSQL Server**:
   - Install PostgreSQL server:
     ```bash
     # For Ubuntu/Debian
     sudo apt-get update
     sudo apt-get install postgresql postgresql-contrib
     
     # For macOS with Homebrew
     brew install postgresql
     
     # For Windows, download installer from https://www.postgresql.org/download/windows/
     ```

2. **PostgreSQL Client Libraries**:
   - Install Python PostgreSQL libraries:
     ```bash
     pip install psycopg2-binary asyncpg
     ```

3. **PostgreSQL User Setup**:
   - Set up a PostgreSQL user:
     ```bash
     # Connect as postgres user
     sudo -u postgres psql
     
     # Inside PostgreSQL prompt
     CREATE USER bettensor WITH PASSWORD 'your_secure_password';
     CREATE DATABASE bettensor_validator;
     GRANT ALL PRIVILEGES ON DATABASE bettensor_validator TO bettensor;
     
     # Exit PostgreSQL prompt
     \q
     ```

## Manual Migration

If you want to manually trigger the migration process:

```bash
# Run the migration script
python -m bettensor.validator.migration.auto_migrate --force
```

Additional options:
```
--sqlite-path PATH      Path to SQLite database file
--postgres-host HOST    PostgreSQL host (default: localhost)
--postgres-port PORT    PostgreSQL port (default: 5432)
--postgres-user USER    PostgreSQL user (default: postgres)
--postgres-password PWD PostgreSQL password
--postgres-dbname NAME  PostgreSQL database name (default: bettensor_validator)
--validator-command CMD Command to restart validator
```

## Testing the Migration

1. **Backup your data**:
   ```bash
   cp -r ~/.bettensor ~/.bettensor.backup
   cp ./bettensor/validator/state/validator.db ./validator.db.backup
   ```

2. **Run the migration**:
   ```bash
   python -m bettensor.validator.migration.auto_migrate --force
   ```

3. **Verify the migration**:
   ```bash
   # Connect to PostgreSQL
   psql -h localhost -U postgres -d bettensor_validator
   
   # Inside PostgreSQL prompt, list tables
   \dt
   
   # Sample query to verify data
   SELECT COUNT(*) FROM miner_stats;
   
   # Exit
   \q
   ```

4. **Run the validator with PostgreSQL**:
   ```bash
   # The validator should automatically use PostgreSQL after migration
   python -m bettensor.validator.cli
   ```

## Configuration

The database configuration is stored in `~/.bettensor/database.cfg` and can be manually adjusted if needed:

```ini
[Database]
type = postgres
host = localhost
port = 5432
user = postgres
password = your_password
dbname = bettensor_validator
path = ./bettensor/validator/state/validator.db
```

## Fresh Installation

For new installations, the validator will automatically use PostgreSQL if:

1. The `database.cfg` file specifies PostgreSQL, or
2. The `setup.cfg` specifies PostgreSQL as the default database type

## Rollback

If you need to rollback to SQLite:

```bash
# Edit database.cfg to switch back to SQLite
sed -i 's/type = postgres/type = sqlite/g' ~/.bettensor/database.cfg

# Or use this script to set the database type
python -c "from bettensor.validator.utils.database.database_factory import DatabaseFactory; DatabaseFactory.save_config({'type': 'sqlite', 'path': './bettensor/validator/state/validator.db'})"

# Restore SQLite database from backup
cp ./validator.db.backup ./bettensor/validator/state/validator.db
```

## Troubleshooting

### Common Issues

1. **PostgreSQL service not running**:
   ```bash
   # Start PostgreSQL service
   sudo service postgresql start  # Linux
   brew services start postgresql  # macOS
   ```

2. **Permission denied errors**:
   - Ensure your PostgreSQL user has the correct permissions
   - Check that the password in the configuration matches

3. **Migration fails with database errors**:
   - Check PostgreSQL logs: `/var/log/postgresql/postgresql-*.log`
   - Verify that no other applications are using the SQLite database

4. **Missing Python dependencies**:
   ```bash
   pip install psycopg2-binary asyncpg
   ```

For additional support, please file an issue on GitHub. 