"""
Database Manager for the Bettensor Miner.

This module handles database operations for the miner,
including storing and retrieving game data, predictions, and statistics.
"""

import asyncio
import json
import os
import sqlite3
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import bittensor as bt


class DatabaseManager:
    """
    Manages database operations for the miner.
    
    Responsibilities:
    1. Initialize and maintain the SQLite database
    2. Provide methods for storing and retrieving data
    3. Handle database migrations and updates
    4. Optimize database performance
    """
    
    def __init__(self, db_path: str = "./bettensor_miner.db"):
        """
        Initialize the database manager.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.conn = None
        self.initialized = False
        
        # Statistics
        self.stats = {
            "queries_executed": 0,
            "writes_executed": 0,
            "average_query_time": 0.0,
            "last_backup_time": None,
        }
        
        bt.logging.info(f"DatabaseManager initialized with database at {db_path}")
    
    async def initialize(self):
        """Initialize the database connection and create tables if they don't exist."""
        if self.initialized:
            bt.logging.warning("Database already initialized")
            return
        
        try:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
            
            # Connect to the database
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  # Return rows as dictionaries
            
            # Create tables
            await self._create_tables()
            
            self.initialized = True
            bt.logging.info("Database initialized successfully")
            
        except Exception as e:
            bt.logging.error(f"Error initializing database: {str(e)}")
            raise
    
    async def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.initialized = False
            bt.logging.info("Database connection closed")
    
    async def _create_tables(self):
        """Create database tables if they don't exist."""
        cursor = self.conn.cursor()
        
        # Games table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS games (
            game_id TEXT PRIMARY KEY,
            sport TEXT NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            start_time INTEGER NOT NULL,
            end_time INTEGER,
            status TEXT NOT NULL,
            home_score INTEGER,
            away_score INTEGER,
            metadata TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """)
        
        # Predictions table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            prediction_id TEXT PRIMARY KEY,
            game_id TEXT NOT NULL,
            sport TEXT NOT NULL,
            home_win_probability REAL NOT NULL,
            away_win_probability REAL NOT NULL,
            draw_probability REAL NOT NULL,
            confidence REAL NOT NULL,
            submitted_at INTEGER NOT NULL,
            validator_responses INTEGER NOT NULL DEFAULT 0,
            metadata TEXT,
            FOREIGN KEY (game_id) REFERENCES games (game_id)
        )
        """)
        
        # Validator responses table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS validator_responses (
            response_id TEXT PRIMARY KEY,
            prediction_id TEXT NOT NULL,
            validator_uid INTEGER NOT NULL,
            status TEXT NOT NULL,
            received_at INTEGER NOT NULL,
            metadata TEXT,
            FOREIGN KEY (prediction_id) REFERENCES predictions (prediction_id)
        )
        """)
        
        # Statistics table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS statistics (
            stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            stat_type TEXT NOT NULL,
            stat_value REAL NOT NULL,
            metadata TEXT
        )
        """)
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_games_sport ON games (sport)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_games_status ON games (status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_predictions_game_id ON predictions (game_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_validator_responses_prediction_id ON validator_responses (prediction_id)")
        
        self.conn.commit()
        bt.logging.info("Database tables created successfully")
    
    async def execute_query(self, query: str, params: Tuple = ()) -> List[Dict]:
        """
        Execute a database query and return the results.
        
        Args:
            query: SQL query string
            params: Query parameters
        
        Returns:
            List of dictionaries representing rows
        """
        if not self.initialized:
            await self.initialize()
        
        start_time = time.time()
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            
            result = [dict(row) for row in cursor.fetchall()]
            
            query_time = time.time() - start_time
            self.stats["queries_executed"] += 1
            self.stats["average_query_time"] = (
                0.9 * self.stats["average_query_time"] + 0.1 * query_time
                if self.stats["average_query_time"] > 0
                else query_time
            )
            
            return result
            
        except Exception as e:
            bt.logging.error(f"Error executing query {query}: {str(e)}")
            raise
    
    async def execute_write(self, query: str, params: Tuple = ()) -> int:
        """
        Execute a write operation and return the number of affected rows.
        
        Args:
            query: SQL query string
            params: Query parameters
        
        Returns:
            Number of affected rows
        """
        if not self.initialized:
            await self.initialize()
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            self.conn.commit()
            
            self.stats["writes_executed"] += 1
            return cursor.rowcount
            
        except Exception as e:
            self.conn.rollback()
            bt.logging.error(f"Error executing write operation {query}: {str(e)}")
            raise
    
    async def get_game(self, game_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a game by ID.
        
        Args:
            game_id: The game ID
        
        Returns:
            Game data or None if not found
        """
        results = await self.execute_query(
            "SELECT * FROM games WHERE game_id = ?",
            (game_id,)
        )
        
        return results[0] if results else None
    
    async def get_games_by_sport(self, sport: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get games by sport and optional status.
        
        Args:
            sport: The sport type
            status: Optional game status filter
        
        Returns:
            List of game data
        """
        if status:
            return await self.execute_query(
                "SELECT * FROM games WHERE sport = ? AND status = ? ORDER BY start_time DESC",
                (sport, status)
            )
        else:
            return await self.execute_query(
                "SELECT * FROM games WHERE sport = ? ORDER BY start_time DESC",
                (sport,)
            )
    
    async def save_game(self, game_data: Dict[str, Any]) -> bool:
        """
        Save a game to the database.
        
        Args:
            game_data: Game data dictionary
        
        Returns:
            True if successful, False otherwise
        """
        try:
            current_time = int(time.time())
            
            # Check if game exists
            existing_game = await self.get_game(game_data["game_id"])
            
            if existing_game:
                # Update existing game
                query = """
                UPDATE games SET
                    sport = ?,
                    home_team = ?,
                    away_team = ?,
                    start_time = ?,
                    end_time = ?,
                    status = ?,
                    home_score = ?,
                    away_score = ?,
                    metadata = ?,
                    updated_at = ?
                WHERE game_id = ?
                """
                
                params = (
                    game_data["sport"],
                    game_data["home_team"],
                    game_data["away_team"],
                    game_data["start_time"],
                    game_data.get("end_time"),
                    game_data["status"],
                    game_data.get("home_score"),
                    game_data.get("away_score"),
                    json.dumps(game_data.get("metadata", {})),
                    current_time,
                    game_data["game_id"]
                )
                
            else:
                # Insert new game
                query = """
                INSERT INTO games (
                    game_id, sport, home_team, away_team, start_time, end_time,
                    status, home_score, away_score, metadata, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                params = (
                    game_data["game_id"],
                    game_data["sport"],
                    game_data["home_team"],
                    game_data["away_team"],
                    game_data["start_time"],
                    game_data.get("end_time"),
                    game_data["status"],
                    game_data.get("home_score"),
                    game_data.get("away_score"),
                    json.dumps(game_data.get("metadata", {})),
                    current_time,
                    current_time
                )
            
            await self.execute_write(query, params)
            return True
            
        except Exception as e:
            bt.logging.error(f"Error saving game {game_data.get('game_id')}: {str(e)}")
            return False
    
    async def save_prediction(self, prediction_data: Dict[str, Any]) -> bool:
        """
        Save a prediction to the database.
        
        Args:
            prediction_data: Prediction data dictionary
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if prediction exists
            results = await self.execute_query(
                "SELECT prediction_id FROM predictions WHERE prediction_id = ?",
                (prediction_data["prediction_id"],)
            )
            
            current_time = int(time.time())
            
            if results:
                # Update existing prediction
                query = """
                UPDATE predictions SET
                    game_id = ?,
                    sport = ?,
                    home_win_probability = ?,
                    away_win_probability = ?,
                    draw_probability = ?,
                    confidence = ?,
                    metadata = ?
                WHERE prediction_id = ?
                """
                
                params = (
                    prediction_data["game_id"],
                    prediction_data["sport"],
                    prediction_data["home_win_probability"],
                    prediction_data["away_win_probability"],
                    prediction_data.get("draw_probability", 0.0),
                    prediction_data["confidence"],
                    json.dumps(prediction_data.get("metadata", {})),
                    prediction_data["prediction_id"]
                )
                
            else:
                # Insert new prediction
                query = """
                INSERT INTO predictions (
                    prediction_id, game_id, sport, home_win_probability,
                    away_win_probability, draw_probability, confidence,
                    submitted_at, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                params = (
                    prediction_data["prediction_id"],
                    prediction_data["game_id"],
                    prediction_data["sport"],
                    prediction_data["home_win_probability"],
                    prediction_data["away_win_probability"],
                    prediction_data.get("draw_probability", 0.0),
                    prediction_data["confidence"],
                    current_time,
                    json.dumps(prediction_data.get("metadata", {}))
                )
            
            await self.execute_write(query, params)
            return True
            
        except Exception as e:
            bt.logging.error(f"Error saving prediction {prediction_data.get('prediction_id')}: {str(e)}")
            return False
    
    async def backup_database(self, backup_path: Optional[str] = None) -> bool:
        """
        Create a backup of the database.
        
        Args:
            backup_path: Path to save the backup, defaults to db_path + timestamp
        
        Returns:
            True if successful, False otherwise
        """
        if not self.initialized:
            bt.logging.warning("Cannot backup database: not initialized")
            return False
        
        if not backup_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"{self.db_path}.backup_{timestamp}"
        
        try:
            # Close current connection temporarily
            self.conn.close()
            
            # Copy the database file
            source_db = sqlite3.connect(self.db_path)
            backup_db = sqlite3.connect(backup_path)
            source_db.backup(backup_db)
            
            # Close backup connections
            backup_db.close()
            source_db.close()
            
            # Reopen original connection
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            
            # Update stats
            self.stats["last_backup_time"] = datetime.now().isoformat()
            
            bt.logging.info(f"Database backed up to {backup_path}")
            return True
            
        except Exception as e:
            bt.logging.error(f"Error backing up database: {str(e)}")
            
            # Ensure connection is reopened
            if not self.conn:
                self.conn = sqlite3.connect(self.db_path)
                self.conn.row_factory = sqlite3.Row
                
            return False
    
    async def save_statistic(self, stat_type: str, stat_value: float, metadata: Optional[Dict] = None) -> bool:
        """
        Save a statistics entry.
        
        Args:
            stat_type: Type of statistic
            stat_value: Numeric value
            metadata: Optional metadata dictionary
        
        Returns:
            True if successful, False otherwise
        """
        try:
            current_time = int(time.time())
            
            query = """
            INSERT INTO statistics (
                timestamp, stat_type, stat_value, metadata
            ) VALUES (?, ?, ?, ?)
            """
            
            params = (
                current_time,
                stat_type,
                stat_value,
                json.dumps(metadata or {})
            )
            
            await self.execute_write(query, params)
            return True
            
        except Exception as e:
            bt.logging.error(f"Error saving statistic {stat_type}: {str(e)}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the database manager.
        
        Returns:
            A dictionary of statistics
        """
        stats = self.stats.copy()
        
        # Add additional stats if database is initialized
        if self.initialized and self.conn:
            try:
                cursor = self.conn.cursor()
                
                # Count records in each table
                cursor.execute("SELECT COUNT(*) as count FROM games")
                stats["games_count"] = cursor.fetchone()["count"]
                
                cursor.execute("SELECT COUNT(*) as count FROM predictions")
                stats["predictions_count"] = cursor.fetchone()["count"]
                
                cursor.execute("SELECT COUNT(*) as count FROM validator_responses")
                stats["responses_count"] = cursor.fetchone()["count"]
                
                # Database file size
                stats["database_size_bytes"] = os.path.getsize(self.db_path)
                
            except Exception as e:
                bt.logging.error(f"Error getting database stats: {str(e)}")
        
        return stats 