"""
PostgreSQL database initialization script.
Creates all required tables for the validator.
"""

import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import bittensor as bt

logger = logging.getLogger(__name__)

def create_postgres_tables(host="localhost", port=5432, user="postgres", password="postgres", dbname="bettensor_validator"):
    """Create all required tables in PostgreSQL database."""
    
    # Connect to default database first
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname="postgres"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    try:
        # Create database if it doesn't exist
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
        exists = cursor.fetchone()
        
        if not exists:
            bt.logging.info(f"Creating database {dbname}")
            cursor.execute(f"CREATE DATABASE {dbname}")
            bt.logging.info(f"Database {dbname} created")
        else:
            bt.logging.info(f"Database {dbname} already exists")
            
        cursor.close()
        conn.close()
        
        # Connect to the target database
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Create tables
        tables = [
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
                external_id TEXT UNIQUE,
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
            
            # Create score_state table
            """CREATE TABLE IF NOT EXISTS score_state (
                state_id SERIAL PRIMARY KEY,
                current_day INTEGER,
                "current_date" TEXT,
                reference_date TEXT,
                invalid_uids TEXT,
                valid_uids TEXT,
                tiers TEXT,
                amount_wagered TEXT,
                last_update_date TEXT
            )""",
            
            # Create miner_stats table
            """CREATE TABLE IF NOT EXISTS miner_stats (
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
            
            # Create entropy tables
            """CREATE TABLE IF NOT EXISTS entropy_game_pools (
                game_id INTEGER,
                outcome INTEGER,
                entropy_score REAL DEFAULT 0.0,
                PRIMARY KEY (game_id, outcome)
            )""",
            
            """CREATE TABLE IF NOT EXISTS entropy_predictions (
                prediction_id TEXT PRIMARY KEY,
                game_id INTEGER,
                outcome INTEGER,
                miner_uid TEXT,
                odds REAL,
                wager REAL,
                prediction_date TEXT,
                entropy_contribution REAL,
                FOREIGN KEY (game_id, outcome) REFERENCES entropy_game_pools(game_id, outcome)
            )""",
            
            """CREATE TABLE IF NOT EXISTS entropy_miner_scores (
                miner_uid TEXT,
                day INTEGER,
                contribution REAL,
                PRIMARY KEY (miner_uid, day)
            )""",
            
            """CREATE TABLE IF NOT EXISTS entropy_closed_games (
                game_id INTEGER PRIMARY KEY,
                close_time TEXT
            )""",
            
            """CREATE TABLE IF NOT EXISTS entropy_system_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                current_day INTEGER DEFAULT 0,
                num_miners INTEGER,
                max_days INTEGER,
                last_processed_date TEXT
            )""",
            
            # Create keys table
            """CREATE TABLE IF NOT EXISTS keys (
                hotkey TEXT PRIMARY KEY,
                coldkey TEXT
            )""",
            
            # Create db_version table
            """CREATE TABLE IF NOT EXISTS db_version (
                version INTEGER PRIMARY KEY,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        ]
        
        # Execute table creation statements
        for table_sql in tables:
            try:
                cursor.execute(table_sql)
                bt.logging.info(f"Created table: {table_sql.split()[2]}")
            except Exception as e:
                bt.logging.error(f"Error creating table: {e}")
                raise
                
        # Create indices
        indices = [
            "CREATE INDEX IF NOT EXISTS idx_predictions_miner_uid ON predictions(miner_uid)",
            "CREATE INDEX IF NOT EXISTS idx_predictions_date ON predictions(prediction_date)",
            "CREATE INDEX IF NOT EXISTS idx_game_data_event_date ON game_data(event_start_date)",
            "CREATE INDEX IF NOT EXISTS idx_miner_stats_hotkey ON miner_stats(miner_hotkey)",
            "CREATE INDEX IF NOT EXISTS idx_scores_miner_day ON scores(miner_uid, day_id)",
            "CREATE INDEX IF NOT EXISTS idx_entropy_predictions_game ON entropy_predictions(game_id)",
            "CREATE INDEX IF NOT EXISTS idx_entropy_miner_scores_day ON entropy_miner_scores(day)"
        ]
        
        for index_sql in indices:
            try:
                cursor.execute(index_sql)
                bt.logging.info(f"Created index: {index_sql.split()[2]}")
            except Exception as e:
                bt.logging.error(f"Error creating index: {e}")
                raise
                
        # Initialize db_version if not exists
        cursor.execute("INSERT INTO db_version (version) VALUES (1) ON CONFLICT DO NOTHING")
        
        bt.logging.info("PostgreSQL database initialization completed successfully")
        return True
        
    except Exception as e:
        bt.logging.error(f"Error initializing PostgreSQL database: {e}")
        raise
    finally:
        cursor.close()
        conn.close() 