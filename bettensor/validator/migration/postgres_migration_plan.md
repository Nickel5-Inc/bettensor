# PostgreSQL Migration Plan for Validator System

## Overview

This document outlines our plan to migrate from SQLite to PostgreSQL for the validator system. The migration will be fully automated so that validator nodes can seamlessly transition when they update to the latest version, whether through auto-updater or manual updates.

## Phase 1: Preparation and Development

### Database Abstraction Layer
- Create a new `PostgresDatabaseManager` class that implements the same interface as the current `DatabaseManager`
- Implement a database factory to instantiate the appropriate DB manager based on configuration
- Ensure both managers can co-exist to facilitate testing and smooth migration

```python
class DatabaseManagerFactory:
    @staticmethod
    def get_database_manager(db_type="sqlite", **kwargs):
        if db_type == "postgres":
            return PostgresDatabaseManager(**kwargs)
        else:
            return SQLiteDatabaseManager(**kwargs)
```

### Schema Migration Scripts
- Develop scripts to create all tables with PostgreSQL syntax
- Account for data type differences between SQLite and PostgreSQL
- Properly handle indexes, constraints, and foreign keys
- Create idempotent table creation scripts (CREATE IF NOT EXISTS)

### Query Adaptation
- Identify and modify all queries that use SQLite-specific syntax:
  - Replace `?` placeholders with `%s` for PostgreSQL parameter style
  - Adjust `AUTOINCREMENT` syntax to `SERIAL` or `IDENTITY`
  - Fix timestamp handling differences
  - Convert SQLite-specific functions to PostgreSQL equivalents
  - Ensure transaction handling is compatible
  - Adapt any specific SQLite features to PostgreSQL alternatives

### State Sync System Adaptation
- Implement PostgreSQL `pg_dump`/`pg_restore` based backup system
- Add version tracking to database backups
- Maintain backward compatibility with existing SQLite backups during transition
- Ensure S3 or other storage mechanisms can handle both backup types

### Migration Script Development
- Create data transfer script to move data from SQLite to PostgreSQL
- Implement progress tracking and error handling with detailed logging
- Add validation to ensure data integrity post-migration
- Include retry mechanisms for handling network or other temporary failures

## Phase 2: Testing Strategy

### Local Testing
- Create comprehensive test suite that validates both database backends
- Verify all operations function identically with both backends
- Test edge cases and failure scenarios
- Implement continuous integration tests to prevent regressions

### Migration Testing
- Run migrations on test datasets of increasing size
- Measure migration time and resource utilization
- Identify and fix potential bottlenecks
- Test interrupted migrations and recovery procedures

### Performance Testing
- Compare operation speeds between SQLite and PostgreSQL
- Benchmark common query patterns
- Identify optimization opportunities (indexes, query rewrites)
- Test with realistic load patterns

## Phase 3: Deployment Strategy

### Feature Flag System
- Add configuration option to select database backend
- Default to SQLite initially
- Include auto-detection of previous database type
- Implement graceful fallback if PostgreSQL migration fails

```python
# In config.py
DB_BACKEND = "sqlite"  # Can be "sqlite" or "postgres"
```

### Prerequisite Automation
- Create installation scripts for PostgreSQL if not present
- Automate PostgreSQL user and database creation
- Generate secure credentials and store them safely
- Test prerequisite installation on all supported platforms

### Phased Deployment
- Deploy code that supports both backends
- Enable database selection through configuration
- Add migration triggering mechanism
- Create monitoring endpoints to track migration status

### Coordinated Migration
- Design migration trigger mechanism with version control
- Implement migration as part of the auto-update process
- Add capability to pause and resume long migrations
- Create fallback mechanism to revert to SQLite if migration fails

```python
def migration_logic():
    if should_migrate_to_postgres():
        success = migrate_to_postgres()
        if success:
            update_config(db_backend="postgres")
        else:
            log_failure_and_revert()
```

### Monitoring and Support
- Add extensive logging during migration
- Implement metrics to track migration success rate
- Create dashboard for monitoring migration status across network
- Prepare support documentation and troubleshooting guides

## Implementation Plan

### Migration Script Core Logic

```python
async def migrate_to_postgres():
    """Main migration function that will be executed by each validator node"""
    try:
        # 1. Check prerequisites
        if not check_postgres_available():
            install_postgres()
        
        # 2. Create PostgreSQL database and user if needed
        create_postgres_database()
        
        # 3. Initialize schema in PostgreSQL
        initialize_postgres_schema()
        
        # 4. Check existing SQLite database
        sqlite_db = get_sqlite_connection()
        
        # 5. Extract data from SQLite
        tables_data = extract_data_from_sqlite(sqlite_db)
        
        # 6. Transform data if needed
        transformed_data = transform_data_for_postgres(tables_data)
        
        # 7. Load data into PostgreSQL
        postgres_db = get_postgres_connection()
        load_data_into_postgres(postgres_db, transformed_data)
        
        # 8. Verify data integrity
        if verify_data_integrity(sqlite_db, postgres_db):
            # 9. Switch connection to PostgreSQL
            update_connection_to_postgres()
            
            # 10. Update config to use PostgreSQL by default
            update_config(db_backend="postgres")
            
            # 11. Create initial PostgreSQL backup for state sync
            create_postgres_backup_for_state_sync()
            
            return True
        else:
            bt.logging.error("Data verification failed, rolling back")
            return False
            
    except Exception as e:
        bt.logging.error(f"Migration failed: {str(e)}")
        rollback_migration()
        return False
```

### Rollback Mechanism

```python
def rollback_migration():
    """Revert to SQLite if PostgreSQL migration fails"""
    bt.logging.info("Rolling back to SQLite database")
    update_config(db_backend="sqlite")
    # Any additional cleanup needed
```

## Automated Testing

The migration process will include automated tests to ensure:
1. Data integrity before and after migration
2. Performance meets or exceeds previous SQLite implementation
3. Error handling works correctly
4. Recovery procedures function as expected

## Migration Timeline

1. Development Phase: Implement all components with thorough testing
2. Beta Testing: Test with select validator nodes
3. General Availability: Release to all validator nodes via auto-updater
4. Monitoring Period: Closely monitor network performance
5. Completion: Remove legacy SQLite code once all nodes have migrated

## Conclusion

This migration plan ensures a smooth, automated transition from SQLite to PostgreSQL for all validator nodes. By carefully implementing each phase and including robust testing, error handling, and rollback procedures, we can minimize disruption while improving database performance and reliability. 