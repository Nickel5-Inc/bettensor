"""
database_init.py

This file contains the code for initializing the database for the validator.
"""

def get_vesting_tables_statements():
    """
    Get SQL statements to create vesting system tables.
    
    Returns:
        list: List of SQL CREATE TABLE statements for vesting system tables
    """
    statements = []
    
    # Create blockchain_state table
    statements.append("""
        CREATE TABLE IF NOT EXISTS blockchain_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    
    # Create stake_transactions table
    statements.append("""
        CREATE TABLE IF NOT EXISTS stake_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_number INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            transaction_type TEXT NOT NULL,
            flow_type TEXT NOT NULL,
            hotkey TEXT NOT NULL,
            coldkey TEXT,
            call_amount REAL,
            final_amount REAL,
            fee REAL,
            tx_hash TEXT,
            origin_netuid INTEGER,
            destination_netuid INTEGER,
            destination_coldkey TEXT,
            destination_hotkey TEXT,
            validated INTEGER DEFAULT 0
        )
    """)
    
    # Create indices for stake_transactions
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_transactions_hotkey ON stake_transactions(hotkey)")
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_transactions_coldkey ON stake_transactions(coldkey)")
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_transactions_block ON stake_transactions(block_number)")
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_transactions_timestamp ON stake_transactions(timestamp)")
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_transactions_type ON stake_transactions(transaction_type)")
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_transactions_flow ON stake_transactions(flow_type)")
    
    # Create stake_balance_changes table
    statements.append("""
        CREATE TABLE IF NOT EXISTS stake_balance_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_number INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            hotkey TEXT NOT NULL,
            coldkey TEXT,
            stake REAL NOT NULL,
            stake_change REAL,
            change_type TEXT,
            epoch INTEGER
        )
    """)
    
    # Create indices for stake_balance_changes
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_balance_changes_hotkey ON stake_balance_changes(hotkey)")
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_balance_changes_coldkey ON stake_balance_changes(coldkey)")
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_balance_changes_block ON stake_balance_changes(block_number)")
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_balance_changes_timestamp ON stake_balance_changes(timestamp)")
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_balance_changes_epoch ON stake_balance_changes(epoch)")
    
    # Create stake_metrics table
    statements.append("""
        CREATE TABLE IF NOT EXISTS stake_metrics (
            hotkey TEXT PRIMARY KEY,
            coldkey TEXT,
            total_stake REAL NOT NULL DEFAULT 0,
            manual_stake REAL NOT NULL DEFAULT 0,
            earned_stake REAL NOT NULL DEFAULT 0,
            first_stake_timestamp INTEGER,
            last_update INTEGER NOT NULL
        )
    """)
    
    # Create index for stake_metrics
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_metrics_coldkey ON stake_metrics(coldkey)")
    
    # Create coldkey_metrics table
    statements.append("""
        CREATE TABLE IF NOT EXISTS coldkey_metrics (
            coldkey TEXT PRIMARY KEY,
            total_stake REAL NOT NULL DEFAULT 0,
            manual_stake REAL NOT NULL DEFAULT 0,
            earned_stake REAL NOT NULL DEFAULT 0,
            hotkey_count INTEGER NOT NULL DEFAULT 0,
            last_update INTEGER NOT NULL
        )
    """)
    
    # Create stake_change_history table
    statements.append("""
        CREATE TABLE IF NOT EXISTS stake_change_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            hotkey TEXT NOT NULL,
            coldkey TEXT,
            change_type TEXT NOT NULL,
            flow_type TEXT NOT NULL,
            amount REAL NOT NULL,
            total_stake_after REAL NOT NULL,
            manual_stake_after REAL NOT NULL,
            earned_stake_after REAL NOT NULL
        )
    """)
    
    # Create indices for stake_change_history
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_change_history_hotkey ON stake_change_history(hotkey)")
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_change_history_coldkey ON stake_change_history(coldkey)")
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_change_history_timestamp ON stake_change_history(timestamp)")
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_change_history_type ON stake_change_history(change_type)")
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_change_history_flow ON stake_change_history(flow_type)")
    
    # Create vesting_module_state table
    statements.append("""
        CREATE TABLE IF NOT EXISTS vesting_module_state (
            module_name TEXT PRIMARY KEY,
            last_block INTEGER,
            last_timestamp INTEGER,
            last_epoch INTEGER,
            module_data TEXT
        )
    """)
    
    # Create stake_tranches table
    statements.append("""
        CREATE TABLE IF NOT EXISTS stake_tranches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hotkey TEXT NOT NULL,
            coldkey TEXT,
            initial_amount REAL NOT NULL,
            remaining_amount REAL NOT NULL,
            entry_timestamp INTEGER NOT NULL,
            is_emission INTEGER NOT NULL,
            last_update INTEGER NOT NULL
        )
    """)
    
    # Create index for stake_tranches
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_tranches_hotkey ON stake_tranches(hotkey)")
    
    # Create stake_tranche_exits table
    statements.append("""
        CREATE TABLE IF NOT EXISTS stake_tranche_exits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tranche_id INTEGER NOT NULL,
            exit_timestamp INTEGER NOT NULL,
            exit_amount REAL NOT NULL,
            exit_reason TEXT NOT NULL,
            FOREIGN KEY (tranche_id) REFERENCES stake_tranches(id) ON DELETE CASCADE
        )
    """)
    
    # Create index for stake_tranche_exits
    statements.append("CREATE INDEX IF NOT EXISTS idx_stake_tranche_exits_tranche_id ON stake_tranche_exits(tranche_id)")
    
    # Create aggregated_tranche_metrics table
    statements.append("""
        CREATE TABLE IF NOT EXISTS aggregated_tranche_metrics (
            hotkey TEXT PRIMARY KEY,
            coldkey TEXT,
            total_tranches INTEGER NOT NULL DEFAULT 0,
            active_tranches INTEGER NOT NULL DEFAULT 0,
            avg_tranche_age INTEGER NOT NULL DEFAULT 0,
            oldest_tranche_age INTEGER NOT NULL DEFAULT 0,
            total_tranche_amount REAL NOT NULL DEFAULT 0,
            manual_tranches INTEGER NOT NULL DEFAULT 0,
            emission_tranches INTEGER NOT NULL DEFAULT 0,
            emission_amount REAL NOT NULL DEFAULT 0,
            manual_amount REAL NOT NULL DEFAULT 0,
            last_update INTEGER NOT NULL
        )
    """)
    
    # Create index for aggregated_tranche_metrics
    statements.append("CREATE INDEX IF NOT EXISTS idx_aggregated_tranche_metrics_coldkey ON aggregated_tranche_metrics(coldkey)")
    
    return statements

def initialize_database():
    statements = []
    
    # 1. Disable foreign key constraints (optional but recommended)
    statements.append("PRAGMA foreign_keys = OFF;")
    
    # Add db_version table creation
    statements.append("""
        CREATE TABLE IF NOT EXISTS db_version (
            version INTEGER PRIMARY KEY,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Ensure there's at least one version entry
    statements.append("""
        INSERT OR IGNORE INTO db_version (version)
        VALUES (1)
    """)
    
    # 2. Create main miner_stats table first
    statements.append("""
        CREATE TABLE IF NOT EXISTS miner_stats (
            miner_uid INTEGER PRIMARY KEY,
            miner_hotkey TEXT UNIQUE,
            miner_coldkey TEXT,
            miner_rank INTEGER DEFAULT 0,
            miner_status TEXT DEFAULT 'active',
            miner_cash REAL DEFAULT 0.0,
            miner_current_incentive REAL DEFAULT 0.0,
            miner_current_tier INTEGER DEFAULT 1,
            miner_current_scoring_window INTEGER DEFAULT 0,
            miner_current_composite_score REAL DEFAULT 0.0,
            miner_current_entropy_score REAL DEFAULT 0.0,
            miner_current_sharpe_ratio REAL DEFAULT 0.0,
            miner_current_sortino_ratio REAL DEFAULT 0.0,
            miner_current_roi REAL DEFAULT 0.0,
            miner_current_clv_avg REAL DEFAULT 0.0,
            miner_last_prediction_date TEXT,
            miner_lifetime_earnings REAL DEFAULT 0.0,
            miner_lifetime_wager_amount REAL DEFAULT 0.0,
            miner_lifetime_roi REAL DEFAULT 0.0,
            miner_lifetime_predictions INTEGER DEFAULT 0,
            miner_lifetime_wins INTEGER DEFAULT 0,
            miner_lifetime_losses INTEGER DEFAULT 0,
            miner_win_loss_ratio REAL DEFAULT 0.0,
            most_recent_weight REAL DEFAULT 0.0
        )
    """)
    
    # Add most_recent_weight column to miner_stats if it doesn't exist
    statements.append("""
        SELECT CASE 
            WHEN NOT EXISTS(
                SELECT 1 FROM pragma_table_info('miner_stats') WHERE name='most_recent_weight'
            )
        THEN (
            SELECT sql FROM (
                SELECT 'ALTER TABLE miner_stats ADD COLUMN most_recent_weight REAL DEFAULT 0.0;' as sql
            )
        )
        ELSE (
            SELECT '' as sql
        )
        END;
    """)
    
    # Add miner_15_day_roi column to miner_stats if it doesn't exist
    statements.append("""
        SELECT CASE 
            WHEN NOT EXISTS(
                SELECT 1 FROM pragma_table_info('miner_stats') WHERE name='miner_15_day_roi'
            )
        THEN (
            SELECT sql FROM (
                SELECT 'ALTER TABLE miner_stats ADD COLUMN miner_15_day_roi REAL DEFAULT 0.0;' as sql
            )
        )
        ELSE (
            SELECT '' as sql
        )
        END;
    """)
    
    # Add miner_30_day_roi column to miner_stats if it doesn't exist
    statements.append("""
        SELECT CASE 
            WHEN NOT EXISTS(
                SELECT 1 FROM pragma_table_info('miner_stats') WHERE name='miner_30_day_roi'
            )
        THEN (
            SELECT sql FROM (
                SELECT 'ALTER TABLE miner_stats ADD COLUMN miner_30_day_roi REAL DEFAULT 0.0;' as sql
            )
        )
        ELSE (
            SELECT '' as sql
        )
        END;
    """)
    
    # Add miner_45_day_roi column to miner_stats if it doesn't exist
    statements.append("""
        SELECT CASE 
            WHEN NOT EXISTS(
                SELECT 1 FROM pragma_table_info('miner_stats') WHERE name='miner_45_day_roi'
            )
        THEN (
            SELECT sql FROM (
                SELECT 'ALTER TABLE miner_stats ADD COLUMN miner_45_day_roi REAL DEFAULT 0.0;' as sql
            )
        )
        ELSE (
            SELECT '' as sql
        )
        END;
    """)
    
    # 3. Handle backup table - first drop it if it exists then recreate it with all columns
    statements.append("""
        DROP TABLE IF EXISTS miner_stats_backup;
    """)
    
    statements.append("""
        CREATE TABLE IF NOT EXISTS miner_stats_backup (
            miner_uid INTEGER,
            miner_hotkey TEXT,
            miner_coldkey TEXT,
            miner_rank INTEGER,
            miner_status TEXT,
            miner_cash REAL,
            miner_current_incentive REAL,
            miner_current_tier INTEGER,
            miner_current_scoring_window INTEGER,
            miner_current_composite_score REAL,
            miner_current_entropy_score REAL,
            miner_current_sharpe_ratio REAL,
            miner_current_sortino_ratio REAL,
            miner_current_roi REAL,
            miner_current_clv_avg REAL,
            miner_last_prediction_date TEXT,
            miner_lifetime_earnings REAL,
            miner_lifetime_wager_amount REAL,
            miner_lifetime_roi REAL,
            miner_lifetime_predictions INTEGER,
            miner_lifetime_wins INTEGER,
            miner_lifetime_losses INTEGER,
            miner_win_loss_ratio REAL,
            most_recent_weight REAL DEFAULT 0.0,
            miner_15_day_roi REAL DEFAULT 0.0,
            miner_30_day_roi REAL DEFAULT 0.0,
            miner_45_day_roi REAL DEFAULT 0.0
        )
    """)
    
    # 4. Backup existing data with proper casting - handle only the base columns
    statements.append("""
        INSERT OR IGNORE INTO miner_stats_backup(
            miner_uid, miner_hotkey, miner_coldkey, miner_rank, miner_status,
            miner_cash, miner_current_incentive, miner_current_tier, miner_current_scoring_window,
            miner_current_composite_score, miner_current_entropy_score, miner_current_sharpe_ratio,
            miner_current_sortino_ratio, miner_current_roi, miner_current_clv_avg,
            miner_last_prediction_date, miner_lifetime_earnings, miner_lifetime_wager_amount,
            miner_lifetime_roi, miner_lifetime_predictions, miner_lifetime_wins,
            miner_lifetime_losses, miner_win_loss_ratio, most_recent_weight,
            miner_15_day_roi, miner_30_day_roi, miner_45_day_roi
        )
        SELECT 
            CAST(miner_uid AS INTEGER),
            miner_hotkey,
            miner_coldkey,
            CAST(COALESCE(miner_rank, 0) AS INTEGER),
            COALESCE(miner_status, 'active'),
            CAST(COALESCE(miner_cash, 0.0) AS REAL),
            CAST(COALESCE(miner_current_incentive, 0.0) AS REAL),
            CAST(COALESCE(miner_current_tier, 1) AS INTEGER),
            CAST(COALESCE(miner_current_scoring_window, 0) AS INTEGER),
            CAST(COALESCE(miner_current_composite_score, 0.0) AS REAL),
            CAST(COALESCE(miner_current_entropy_score, 0.0) AS REAL),
            CAST(COALESCE(miner_current_sharpe_ratio, 0.0) AS REAL),
            CAST(COALESCE(miner_current_sortino_ratio, 0.0) AS REAL),
            CAST(COALESCE(miner_current_roi, 0.0) AS REAL),
            CAST(COALESCE(miner_current_clv_avg, 0.0) AS REAL),
            miner_last_prediction_date,
            CAST(COALESCE(miner_lifetime_earnings, 0.0) AS REAL),
            CAST(COALESCE(miner_lifetime_wager_amount, 0.0) AS REAL),
            CAST(COALESCE(miner_lifetime_roi, 0.0) AS REAL),
            CAST(COALESCE(miner_lifetime_predictions, 0) AS INTEGER),
            CAST(COALESCE(miner_lifetime_wins, 0) AS INTEGER),
            CAST(COALESCE(miner_lifetime_losses, 0) AS INTEGER),
            CAST(COALESCE(miner_win_loss_ratio, 0.0) AS REAL),
            CAST(COALESCE(most_recent_weight, 0.0) AS REAL),
            0.0,  -- Default value for miner_15_day_roi 
            0.0,  -- Default value for miner_30_day_roi
            0.0   -- Default value for miner_45_day_roi
        FROM miner_stats WHERE EXISTS (SELECT 1 FROM miner_stats)
    """)

    # 5. Update the restore statement to explicitly list columns for compatibility
    statements.append("""
        INSERT OR REPLACE INTO miner_stats(
            miner_uid, miner_hotkey, miner_coldkey, miner_rank, miner_status,
            miner_cash, miner_current_incentive, miner_current_tier, miner_current_scoring_window,
            miner_current_composite_score, miner_current_entropy_score, miner_current_sharpe_ratio,
            miner_current_sortino_ratio, miner_current_roi, miner_current_clv_avg,
            miner_last_prediction_date, miner_lifetime_earnings, miner_lifetime_wager_amount,
            miner_lifetime_roi, miner_lifetime_predictions, miner_lifetime_wins,
            miner_lifetime_losses, miner_win_loss_ratio, most_recent_weight
        )
        SELECT 
            CAST(miner_uid AS INTEGER) as miner_uid,
            miner_hotkey,
            miner_coldkey,
            CAST(COALESCE(miner_rank, 0) AS INTEGER) as miner_rank,
            COALESCE(miner_status, 'active') as miner_status,
            CAST(COALESCE(miner_cash, 0.0) AS REAL) as miner_cash,
            CAST(COALESCE(miner_current_incentive, 0.0) AS REAL) as miner_current_incentive,
            CAST(COALESCE(miner_current_tier, 1) AS INTEGER) as miner_current_tier,
            CAST(COALESCE(miner_current_scoring_window, 0) AS INTEGER) as miner_current_scoring_window,
            CAST(COALESCE(miner_current_composite_score, 0.0) AS REAL) as miner_current_composite_score,
            CAST(COALESCE(miner_current_entropy_score, 0.0) AS REAL) as miner_current_entropy_score,
            CAST(COALESCE(miner_current_sharpe_ratio, 0.0) AS REAL) as miner_current_sharpe_ratio,
            CAST(COALESCE(miner_current_sortino_ratio, 0.0) AS REAL) as miner_current_sortino_ratio,
            CAST(COALESCE(miner_current_roi, 0.0) AS REAL) as miner_current_roi,
            CAST(COALESCE(miner_current_clv_avg, 0.0) AS REAL) as miner_current_clv_avg,
            miner_last_prediction_date,
            CAST(COALESCE(miner_lifetime_earnings, 0.0) AS REAL) as miner_lifetime_earnings,
            CAST(COALESCE(miner_lifetime_wager_amount, 0.0) AS REAL) as miner_lifetime_wager_amount,
            CAST(COALESCE(miner_lifetime_roi, 0.0) AS REAL) as miner_lifetime_roi,
            CAST(COALESCE(miner_lifetime_predictions, 0) AS INTEGER) as miner_lifetime_predictions,
            CAST(COALESCE(miner_lifetime_wins, 0) AS INTEGER) as miner_lifetime_wins,
            CAST(COALESCE(miner_lifetime_losses, 0) AS INTEGER) as miner_lifetime_losses,
            CAST(COALESCE(miner_win_loss_ratio, 0.0) AS REAL) as miner_win_loss_ratio,
            CAST(COALESCE(most_recent_weight, 0.0) AS REAL) as most_recent_weight
        FROM miner_stats_backup 
        WHERE EXISTS (SELECT 1 FROM miner_stats_backup)
    """)
    
    # 6. Copy ROI values from backup if they exist
    statements.append("""
        SELECT CASE 
            WHEN EXISTS(
                SELECT 1 FROM pragma_table_info('miner_stats_backup') WHERE name='miner_15_day_roi'
            ) AND EXISTS(
                SELECT 1 FROM pragma_table_info('miner_stats') WHERE name='miner_15_day_roi'
            )
        THEN (
            SELECT sql FROM (
                SELECT 'UPDATE miner_stats SET miner_15_day_roi = (SELECT CAST(COALESCE(miner_15_day_roi, 0.0) AS REAL) FROM miner_stats_backup WHERE miner_stats_backup.miner_uid = miner_stats.miner_uid);' as sql
            )
        )
        ELSE (
            SELECT '' as sql
        )
        END;
    """)
    
    statements.append("""
        SELECT CASE 
            WHEN EXISTS(
                SELECT 1 FROM pragma_table_info('miner_stats_backup') WHERE name='miner_30_day_roi'
            ) AND EXISTS(
                SELECT 1 FROM pragma_table_info('miner_stats') WHERE name='miner_30_day_roi'
            )
        THEN (
            SELECT sql FROM (
                SELECT 'UPDATE miner_stats SET miner_30_day_roi = (SELECT CAST(COALESCE(miner_30_day_roi, 0.0) AS REAL) FROM miner_stats_backup WHERE miner_stats_backup.miner_uid = miner_stats.miner_uid);' as sql
            )
        )
        ELSE (
            SELECT '' as sql
        )
        END;
    """)
    
    statements.append("""
        SELECT CASE 
            WHEN EXISTS(
                SELECT 1 FROM pragma_table_info('miner_stats_backup') WHERE name='miner_45_day_roi'
            ) AND EXISTS(
                SELECT 1 FROM pragma_table_info('miner_stats') WHERE name='miner_45_day_roi'
            )
        THEN (
            SELECT sql FROM (
                SELECT 'UPDATE miner_stats SET miner_45_day_roi = (SELECT CAST(COALESCE(miner_45_day_roi, 0.0) AS REAL) FROM miner_stats_backup WHERE miner_stats_backup.miner_uid = miner_stats.miner_uid);' as sql
            )
        )
        ELSE (
            SELECT '' as sql
        )
        END;
    """)
    
    # 6. Create dependent tables
    statements.extend([
        # Create predictions table
        """CREATE TABLE IF NOT EXISTS predictions (
            prediction_id TEXT PRIMARY KEY,
            game_id INTEGER,
            miner_uid INTEGER,
            prediction_date TEXT,
            predicted_outcome INTEGER,
            predicted_odds REAL,
            team_a TEXT,
            team_b TEXT,
            wager REAL,
            team_a_odds REAL,
            team_b_odds REAL,
            tie_odds REAL,
            model_name TEXT,
            confidence_score REAL,
            outcome INTEGER,
            payout REAL,
            sent_to_site INTEGER DEFAULT 0,
            validators_sent_to INTEGER DEFAULT 0,
            validators_confirmed INTEGER DEFAULT 0
        )""",
        
        # Create game_data table
        """CREATE TABLE IF NOT EXISTS game_data (
            game_id TEXT PRIMARY KEY UNIQUE,
            external_id INTEGER UNIQUE,
            team_a TEXT,
            team_b TEXT,
            team_a_odds REAL,
            team_b_odds REAL,
            tie_odds REAL,
            can_tie BOOLEAN,
            event_start_date TEXT,
            create_date TEXT,
            last_update_date TEXT,
            sport TEXT,
            league TEXT,
            outcome INTEGER DEFAULT 3,
            active INTEGER
        )""",
        
        # Create keys table
        """CREATE TABLE IF NOT EXISTS keys (
            hotkey TEXT PRIMARY KEY,
            coldkey TEXT
        )""",
        
        # Create scores table
        """CREATE TABLE IF NOT EXISTS scores (
            miner_uid INTEGER,
            day_id INTEGER,
            score_type TEXT,
            clv_score REAL,
            roi_score REAL,
            sortino_score REAL,
            entropy_score REAL,
            composite_score REAL,
            PRIMARY KEY (miner_uid, day_id, score_type),
            FOREIGN KEY (miner_uid) REFERENCES miner_stats(miner_uid)
        )""",
        
        # Create score_state table
        """CREATE TABLE IF NOT EXISTS score_state (
            state_id INTEGER PRIMARY KEY AUTOINCREMENT,
            current_day INTEGER,
            current_date TEXT,
            reference_date TEXT,
            invalid_uids TEXT,
            valid_uids TEXT,
            tiers TEXT,
            amount_wagered TEXT,
            last_update_date TEXT
        )""",
        
        # Store game pools and their entropy scores
        """CREATE TABLE IF NOT EXISTS entropy_game_pools (
            game_id INTEGER,
            outcome INTEGER,
            entropy_score REAL DEFAULT 0.0,
            PRIMARY KEY (game_id, outcome)
        )""",
        
        # Store individual predictions and their entropy contributions
        """CREATE TABLE IF NOT EXISTS entropy_predictions (
            prediction_id TEXT PRIMARY KEY,
            game_id INTEGER,
            outcome INTEGER,
            miner_uid INTEGER,
            odds REAL,
            wager REAL,
            prediction_date TEXT,
            entropy_contribution REAL,
            FOREIGN KEY (game_id, outcome) REFERENCES entropy_game_pools(game_id, outcome)
        )""",
        
        # Store daily miner entropy scores
        """CREATE TABLE IF NOT EXISTS entropy_miner_scores (
            miner_uid INTEGER,
            day INTEGER,
            contribution REAL,
            PRIMARY KEY (miner_uid, day)
        )""",
        
        # Store closed games tracking
        """CREATE TABLE IF NOT EXISTS entropy_closed_games (
            game_id INTEGER PRIMARY KEY,
            close_time TEXT
        )""",
        
        # Store system state
        """CREATE TABLE IF NOT EXISTS entropy_system_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            current_day INTEGER DEFAULT 0,
            num_miners INTEGER,
            max_days INTEGER,
            last_processed_date TEXT
        )"""
    ])
    
    # 7. Create triggers
    statements.extend([
        """CREATE TRIGGER IF NOT EXISTS delete_old_predictions
        AFTER INSERT ON predictions
        BEGIN
            DELETE FROM predictions
            WHERE prediction_date < date('now', '-50 days');
        END""",
        
        """CREATE TRIGGER IF NOT EXISTS delete_old_game_data
        AFTER INSERT ON game_data
        BEGIN
            DELETE FROM game_data
            WHERE event_start_date < date('now', '-50 days');
        END""",
        
        """CREATE TRIGGER IF NOT EXISTS delete_old_score_state
        AFTER INSERT ON score_state
        BEGIN
            DELETE FROM score_state
            WHERE last_update_date < date('now', '-7 days');
        END""",
        
        """CREATE TRIGGER IF NOT EXISTS cleanup_old_entropy_predictions
        AFTER INSERT ON entropy_predictions
        BEGIN
            DELETE FROM entropy_predictions
            WHERE prediction_date < date('now', '-45 days');
        END""",
        
        """CREATE TRIGGER IF NOT EXISTS cleanup_old_entropy_closed_games
        AFTER INSERT ON entropy_closed_games
        BEGIN
            DELETE FROM entropy_closed_games
            WHERE close_time < date('now', '-45 days');
        END"""
    ])

    # 8. Re-enable foreign key constraints
    statements.append("PRAGMA foreign_keys = ON;")
    
    # 9. Initialize version
    statements.append(
        """INSERT OR IGNORE INTO db_version (version) VALUES (1)"""
    )
    
    # Note: Vesting tables are now initialized separately in the vesting database
    # Do not include vesting_statements in the main database
    
    return statements

