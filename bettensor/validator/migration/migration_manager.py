class MigrationManager:
    """Manages database migrations between SQLite and PostgreSQL."""
    
    def __init__(self, sqlite_path: str, pg_config: dict):
        """
        Initialize the migration manager.
        
        Args:
            sqlite_path: Path to SQLite database
            pg_config: PostgreSQL configuration dictionary
        """
        self.sqlite_path = sqlite_path
        self.pg_config = pg_config
        self.migration_status = {
            "started": False,
            "completed": False,
            "errors": [],
            "data_migrated": {},
            "last_error": None,
            "last_attempt": None
        }
        self._load_migration_status()
        
    def _load_migration_status(self):
        """Load migration status from file."""
        try:
            status_file = Path(self.sqlite_path).parent / "migration_status.json"
            if status_file.exists():
                with open(status_file, "r") as f:
                    self.migration_status.update(json.load(f))
        except Exception as e:
            bt.logging.error(f"Error loading migration status: {e}")
            
    def _save_migration_status(self):
        """Save migration status to file."""
        try:
            status_file = Path(self.sqlite_path).parent / "migration_status.json"
            with open(status_file, "w") as f:
                json.dump(self.migration_status, f)
        except Exception as e:
            bt.logging.error(f"Error saving migration status: {e}")
            
    async def check_migration_needed(self) -> bool:
        """
        Check if migration is needed.
        
        Returns:
            bool: True if migration is needed, False otherwise
        """
        try:
            # Check if migration was already completed
            if self.migration_status.get("completed", False):
                bt.logging.info("Migration already completed")
                return False
                
            # Check if SQLite database exists and has data
            if not os.path.exists(self.sqlite_path):
                bt.logging.info("No SQLite database found, skipping migration")
                self.migration_status["completed"] = True
                self._save_migration_status()
                return False
                
            # Check SQLite database size
            sqlite_size = os.path.getsize(self.sqlite_path)
            if sqlite_size < 1024:  # Less than 1KB
                bt.logging.info("SQLite database is empty, skipping migration")
                self.migration_status["completed"] = True
                self._save_migration_status()
                return False
                
            # Connect to PostgreSQL to check if tables exist and have data
            conn = psycopg2.connect(**self.pg_config)
            cursor = conn.cursor()
            
            try:
                # Check if key tables exist and have data
                tables_to_check = ["game_data", "predictions", "miner_stats"]
                tables_exist = True
                tables_have_data = True
                
                for table in tables_to_check:
                    cursor.execute(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = %s
                        )
                    """, (table,))
                    
                    if not cursor.fetchone()[0]:
                        tables_exist = False
                        break
                        
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    if cursor.fetchone()[0] == 0:
                        tables_have_data = False
                        
                if tables_exist and tables_have_data:
                    bt.logging.info("PostgreSQL database already has data, skipping migration")
                    self.migration_status["completed"] = True
                    self._save_migration_status()
                    return False
                    
                return True
                
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            bt.logging.error(f"Error checking migration status: {e}")
            self.migration_status["last_error"] = str(e)
            self._save_migration_status()
            return True
            
    async def migrate(self) -> bool:
        """
        Perform the migration from SQLite to PostgreSQL.
        
        Returns:
            bool: True if migration was successful, False otherwise
        """
        try:
            if not await self.check_migration_needed():
                return True
                
            bt.logging.info("Starting migration from SQLite to PostgreSQL")
            self.migration_status["started"] = True
            self.migration_status["last_attempt"] = datetime.now().isoformat()
            self._save_migration_status()
            
            # Connect to SQLite
            sqlite_conn = sqlite3.connect(self.sqlite_path)
            sqlite_conn.row_factory = sqlite3.Row
            
            try:
                # Get list of tables
                cursor = sqlite_conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                tables = [row[0] for row in cursor.fetchall()]
                
                # Connect to PostgreSQL
                pg_conn = psycopg2.connect(**self.pg_config)
                pg_conn.autocommit = False
                pg_cursor = pg_conn.cursor()
                
                try:
                    # Process each table
                    for table in tables:
                        if self.migration_status.get("data_migrated", {}).get(table, False):
                            bt.logging.info(f"Table {table} already migrated, skipping")
                            continue
                            
                        bt.logging.info(f"Migrating table: {table}")
                        
                        # Get table schema
                        cursor.execute(f"PRAGMA table_info({table})")
                        columns = [col[1] for col in cursor.fetchall()]
                        
                        # Create table in PostgreSQL
                        create_table_query = f"""
                        CREATE TABLE IF NOT EXISTS {table} (
                            {', '.join(f'{col} {self._get_postgres_type(table, col)}' for col in columns)}
                        )
                        """
                        pg_cursor.execute(create_table_query)
                        
                        # Get data from SQLite
                        cursor.execute(f"SELECT * FROM {table}")
                        rows = cursor.fetchall()
                        
                        if not rows:
                            bt.logging.info(f"Table {table} is empty, skipping")
                            self.migration_status.setdefault("data_migrated", {})[table] = True
                            self._save_migration_status()
                            continue
                            
                        # Insert data in batches
                        batch_size = 1000
                        for i in range(0, len(rows), batch_size):
                            batch = rows[i:i + batch_size]
                            
                            # Convert rows to list of tuples
                            values = []
                            for row in batch:
                                row_data = []
                                for col_idx, col in enumerate(columns):
                                    val = row[col_idx]
                                    if val == "":
                                        val = None
                                    elif table == "game_data" and col == "outcome":
                                        # Handle outcome column specially
                                        if val is not None:
                                            try:
                                                val = int(val)
                                            except (ValueError, TypeError):
                                                val = 3  # Default to "Unfinished"
                                    row_data.append(val)
                                values.append(tuple(row_data))
                                
                            # Build insert query
                            placeholders = ", ".join(["%s"] * len(columns))
                            insert_query = f"""
                            INSERT INTO {table} ({", ".join(columns)})
                            VALUES ({placeholders})
                            ON CONFLICT DO NOTHING
                            """
                            
                            try:
                                pg_cursor.executemany(insert_query, values)
                                pg_conn.commit()
                                bt.logging.info(f"Inserted {len(values)} rows into {table}")
                            except Exception as e:
                                pg_conn.rollback()
                                bt.logging.error(f"Error inserting into {table}: {e}")
                                raise
                                
                        # Mark table as migrated
                        self.migration_status.setdefault("data_migrated", {})[table] = True
                        self._save_migration_status()
                        
                    # Migration completed successfully
                    self.migration_status["completed"] = True
                    self.migration_status["errors"] = []
                    self._save_migration_status()
                    
                    bt.logging.info("Migration completed successfully")
                    return True
                    
                finally:
                    pg_cursor.close()
                    pg_conn.close()
                    
            finally:
                sqlite_conn.close()
                
        except Exception as e:
            bt.logging.error(f"Migration failed: {e}")
            self.migration_status["last_error"] = str(e)
            self.migration_status["errors"].append(str(e))
            self._save_migration_status()
            return False
            
    def _get_postgres_type(self, table: str, column: str) -> str:
        """Helper method to determine PostgreSQL column type"""
        # Special cases for known tables/columns
        if table == "game_data":
            if column == "outcome":
                return "INTEGER"  # Ensure outcome is always INTEGER
            if column in ["team_a_odds", "team_b_odds", "tie_odds"]:
                return "DOUBLE PRECISION"
        elif table == "predictions":
            if column in ["predicted_odds", "wager", "payout"]:
                return "DOUBLE PRECISION"
            if column in ["predicted_outcome", "outcome"]:
                return "INTEGER"
        elif table == "miner_stats":
            if column in ["miner_uid", "miner_current_tier"]:
                return "INTEGER"
            if "score" in column.lower() or "ratio" in column.lower():
                return "DOUBLE PRECISION"
                
        # Default mappings
        return "TEXT"  # Default to TEXT for unknown types 

    async def migrate_data(self) -> bool:
        """
        Migrate data from SQLite to PostgreSQL
        
        Returns:
            bool: True if data migration was successful, False otherwise
        """
        try:
            # Connect to SQLite
            sqlite_conn = sqlite3.connect(self.sqlite_path)
            sqlite_conn.row_factory = sqlite3.Row
            sqlite_cursor = sqlite_conn.cursor()
            
            # Connect to PostgreSQL
            pg_conn = psycopg2.connect(**self.pg_config)
            pg_cursor = pg_conn.cursor()
            
            # Get list of tables from SQLite
            sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = [row[0] for row in sqlite_cursor.fetchall()]
            
            all_succeeded = True
            
            # Process each table
            for table in tables:
                # Skip already migrated tables
                if self.migration_status.get("data_migrated", {}).get(table, False):
                    bt.logging.info(f"Table {table} already migrated, skipping")
                    continue
                
                try:
                    bt.logging.info(f"Migrating table: {table}")
                    
                    # Get table schema from SQLite
                    sqlite_cursor.execute(f"PRAGMA table_info({table})")
                    columns = [col[1] for col in sqlite_cursor.fetchall()]
                    
                    # Get all data from SQLite table
                    sqlite_cursor.execute(f"SELECT * FROM {table}")
                    rows = sqlite_cursor.fetchall()
                    
                    if not rows:
                        bt.logging.info(f"Table {table} is empty, skipping")
                        self.migration_status.setdefault("data_migrated", {})[table] = True
                        self._save_migration_status()
                        continue
                    
                    # Convert rows to dictionaries
                    data = []
                    for row in rows:
                        row_dict = {columns[i]: row[i] for i in range(len(columns))}
                        
                        # Special handling for game_data table outcome field
                        if table == 'game_data' and 'outcome' in row_dict:
                            outcome = row_dict['outcome']
                            if outcome is None or outcome == '' or outcome == 'Unfinished':
                                row_dict['outcome'] = 3  # Default unfinished value
                            elif isinstance(outcome, str):
                                try:
                                    row_dict['outcome'] = int(outcome)
                                except ValueError:
                                    # Map string outcomes to integers
                                    outcome_map = {
                                        'TeamA': 0,
                                        'TeamB': 1,
                                        'Draw': 2,
                                        'Unfinished': 3,
                                        'Pending': 3,
                                        'Cancelled': 4
                                    }
                                    row_dict['outcome'] = outcome_map.get(outcome, 3)
                        data.append(row_dict)
                    
                    # Process in batches of 1000
                    batch_size = 1000
                    for i in range(0, len(data), batch_size):
                        batch = data[i:i + batch_size]
                        
                        # Create the INSERT query
                        columns_str = ', '.join(columns)
                        placeholders = ', '.join(['%s'] * len(columns))
                        insert_query = f"""
                        INSERT INTO {table} ({columns_str})
                        VALUES ({placeholders})
                        ON CONFLICT DO NOTHING
                        """
                        
                        # Convert batch to list of tuples
                        batch_values = [
                            tuple(row[col] if row[col] != '' else None for col in columns)
                            for row in batch
                        ]
                        
                        try:
                            # Execute the batch insert
                            pg_cursor.executemany(insert_query, batch_values)
                            pg_conn.commit()
                            bt.logging.info(f"Inserted batch of {len(batch)} rows into {table}")
                        except Exception as e:
                            bt.logging.error(f"Error inserting batch into {table}: {e}")
                            pg_conn.rollback()
                            all_succeeded = False
                    
                    # Mark this table as migrated
                    self.migration_status.setdefault("data_migrated", {})[table] = True
                    self._save_migration_status()
                    
                except Exception as e:
                    bt.logging.error(f"Error migrating table {table}: {e}")
                    self.migration_status["errors"].append(f"Error migrating table {table}: {str(e)}")
                    self._save_migration_status()
                    all_succeeded = False
            
            # Close connections
            sqlite_cursor.close()
            sqlite_conn.close()
            pg_cursor.close()
            pg_conn.close()
            
            return all_succeeded
            
        except Exception as e:
            bt.logging.error(f"Failed to migrate data: {e}")
            self.migration_status["errors"].append(f"Data migration error: {str(e)}")
            self._save_migration_status()
            return False 