"""
Migration test to verify our consolidated migration approach works correctly
"""
import os
import sys
import asyncio
import unittest
import tempfile
import sqlite3
import psycopg2
import shutil
from pathlib import Path
import bittensor as bt

# Add the parent directory to the path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from .migration_manager import MigrationManager

class TestMigration(unittest.TestCase):
    """Test the MigrationManager class"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures"""
        # Create a temporary directory for the test
        cls.temp_dir = tempfile.mkdtemp()
        
        # Set up a test SQLite database
        cls.sqlite_path = os.path.join(cls.temp_dir, "test.db")
        
        # Create test tables in SQLite
        conn = sqlite3.connect(cls.sqlite_path)
        cursor = conn.cursor()
        
        # Create predictions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                prediction_id TEXT PRIMARY KEY,
                game_id TEXT,
                miner_uid TEXT,
                prediction_date TEXT,
                predicted_outcome TEXT,
                predicted_odds REAL,
                team_a TEXT,
                team_b TEXT,
                wager REAL,
                team_a_odds REAL,
                team_b_odds REAL,
                tie_odds REAL,
                model_name TEXT,
                confidence_score REAL,
                outcome TEXT,
                payout REAL
            )
        """)
        
        # Create game_data table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS game_data (
                game_id TEXT PRIMARY KEY,
                team_a TEXT,
                team_b TEXT,
                sport TEXT,
                league TEXT,
                create_date TEXT,
                last_update_date TEXT,
                event_start_date TEXT,
                active INTEGER,
                outcome TEXT,
                team_a_odds REAL,
                team_b_odds REAL,
                tie_odds REAL,
                can_tie BOOLEAN
            )
        """)
        
        # Insert test data
        # Add a few predictions
        predictions = [
            ("pred1", "game1", "miner1", "2023-01-01", "Team A Win", 1.5, "Team A", "Team B", 100.0, 1.5, 2.5, 3.5, "Model1", 0.8, "Win", 150.0),
            ("pred2", "game2", "miner1", "2023-01-02", "Team B Win", 2.5, "Team A", "Team B", 200.0, 1.6, 2.6, 3.6, "Model1", 0.7, "Loss", 0.0),
            # Add a duplicate prediction ID to test conflict handling
            ("pred1", "game3", "miner2", "2023-01-03", "Team A Win", 1.7, "Team C", "Team D", 300.0, 1.7, 2.7, 3.7, "Model2", 0.9, "Win", 510.0)
        ]
        
        for pred in predictions:
            try:
                cursor.execute("""
                    INSERT INTO predictions 
                    (prediction_id, game_id, miner_uid, prediction_date, predicted_outcome, predicted_odds, 
                     team_a, team_b, wager, team_a_odds, team_b_odds, tie_odds, model_name, confidence_score, outcome, payout)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, pred)
            except sqlite3.IntegrityError as e:
                print(f"Skipping duplicate: {e}")
        
        # Add a couple of games
        games = [
            ("game1", "Team A", "Team B", "football", "NFL", "2023-01-01", "2023-01-01", "2023-01-05", 1, "Pending", 1.5, 2.5, 3.5, 0),
            ("game2", "Team C", "Team D", "basketball", "NBA", "2023-01-02", "2023-01-02", "2023-01-06", 1, "Pending", 1.6, 2.6, 3.6, 0)
        ]
        
        for game in games:
            cursor.execute("""
                INSERT INTO game_data 
                (game_id, team_a, team_b, sport, league, create_date, last_update_date, event_start_date, 
                 active, outcome, team_a_odds, team_b_odds, tie_odds, can_tie)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, game)
        
        conn.commit()
        conn.close()
        
        # PostgreSQL connection config for testing
        cls.pg_config = {
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "postgres",
            "dbname": "test_migration"
        }
        
        # Create a test database in PostgreSQL
        try:
            # Connect to default postgres database
            conn = psycopg2.connect(
                host=cls.pg_config["host"],
                port=cls.pg_config["port"],
                user=cls.pg_config["user"],
                password=cls.pg_config["password"],
                dbname="postgres"
            )
            conn.autocommit = True
            cursor = conn.cursor()
            
            # Drop test database if it exists
            cursor.execute("DROP DATABASE IF EXISTS test_migration")
            
            # Create test database
            cursor.execute("CREATE DATABASE test_migration")
            
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error setting up PostgreSQL database: {e}")
            raise
    
    @classmethod
    def tearDownClass(cls):
        """Tear down test fixtures"""
        # Remove temp directory and SQLite database
        shutil.rmtree(cls.temp_dir)
        
        # Drop test PostgreSQL database
        try:
            conn = psycopg2.connect(
                host=cls.pg_config["host"],
                port=cls.pg_config["port"],
                user=cls.pg_config["user"],
                password=cls.pg_config["password"],
                dbname="postgres"
            )
            conn.autocommit = True
            cursor = conn.cursor()
            
            # Drop test database
            cursor.execute("DROP DATABASE IF EXISTS test_migration")
            
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error tearing down PostgreSQL database: {e}")
    
    def test_migration(self):
        """Test the full migration process"""
        # Create a new MigrationManager
        migration_manager = MigrationManager(
            sqlite_path=self.sqlite_path,
            pg_config=self.pg_config
        )
        
        # Run the migration
        loop = asyncio.get_event_loop()
        success = loop.run_until_complete(migration_manager.run_migration())
        
        self.assertTrue(success, "Migration should complete successfully")
        
        # Verify that data was migrated correctly
        try:
            # Connect to PostgreSQL
            conn = psycopg2.connect(**self.pg_config)
            cursor = conn.cursor()
            
            # Check predictions table
            cursor.execute("SELECT COUNT(*) FROM predictions")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 2, "There should be 2 predictions in PostgreSQL (skipping duplicate)")
            
            # Check game_data table
            cursor.execute("SELECT COUNT(*) FROM game_data")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 2, "There should be 2 games in PostgreSQL")
            
            # Check that the specific data was migrated correctly
            cursor.execute("SELECT prediction_id, game_id, miner_uid FROM predictions WHERE prediction_id = 'pred1'")
            row = cursor.fetchone()
            self.assertIsNotNone(row, "Prediction pred1 should exist")
            self.assertEqual(row[0], "pred1", "Prediction ID should be pred1")
            self.assertEqual(row[1], "game1", "Game ID should be game1")
            self.assertEqual(row[2], "miner1", "Miner UID should be miner1")
            
            cursor.execute("SELECT game_id, team_a, team_b FROM game_data WHERE game_id = 'game1'")
            row = cursor.fetchone()
            self.assertIsNotNone(row, "Game game1 should exist")
            self.assertEqual(row[0], "game1", "Game ID should be game1")
            self.assertEqual(row[1], "Team A", "Team A should be Team A")
            self.assertEqual(row[2], "Team B", "Team B should be Team B")
            
            cursor.close()
            conn.close()
        except Exception as e:
            self.fail(f"Failed to verify migration: {e}")

if __name__ == "__main__":
    unittest.main() 