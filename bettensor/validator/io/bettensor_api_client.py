# File: bettensor/utils/bettensor_api.py

import requests
from datetime import datetime, timedelta, timezone
import bittensor as bt
import json
import os
from ..database.database_manager import DatabaseManager
from .base_api_client import BaseAPIClient
import aiohttp
from sqlalchemy import text
import traceback


class BettensorAPIClient(BaseAPIClient):
    def __init__(self, db_manager: DatabaseManager):
        super().__init__()

        self.base_url = "https://dev-bettensor-api.azurewebsites.net/"
        self.db_manager = db_manager
        self.games = []
        self.api_key = os.getenv("BETTENSOR_API_KEY", None)
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        self.session = aiohttp.ClientSession()  # Use aiohttp for async requests

    async def fetch_all_game_data(self, last_update_date):
        """
        fetch and update game data from bettensor API. overridden from BaseAPIClient

        """
        if last_update_date is None:
            # set to 1 week ago
            last_update_date = datetime.now(timezone.utc) - timedelta(days=15)

        # Fetch the games from the API
        bt.logging.info(f"Fetching games from API with last update date: {last_update_date}")
        games = await self.get_games(last_update_date)
        # print one game
        # if len(games) > 0:
        #     bt.logging.trace(f"Games: {games[0]}")

        return games

    async def get_games(self, last_update_date=None, items_per_page=100):
        """Get all games from the API with pagination."""
        all_games = []
        page_index = 0
        start_date = (
            datetime.now(timezone.utc) - timedelta(days=15)
        ).isoformat()  # 15 days ago

        while True:
            # Add await here
            games = await self.get_games_page(
                start_date, items_per_page, page_index, last_update_date
            )
            if not games:
                break
            all_games.extend(games)
            page_index += 1
        return all_games

    async def get_games_page(self, start_date, items_per_page, page_index, last_update_date):
        """Get a single page of games from the API."""
        params = {
            "PageIndex": page_index,
            "ItemsPerPage": items_per_page,
            "SortOrder": "StartDate",
            "LastUpdateDate": last_update_date.isoformat() if last_update_date else None,
            "StartDate": start_date,
            "LeagueFilter": "true",
        }

        try:
            # Add await here
            response = await self.session.get(
                f"{self.base_url}/Games/TeamGames/Search",
                params=params,
                headers=self.headers
            )
            # Add await here
            data = await response.json()
            return data
        except Exception as e:
            bt.logging.error(f"Error fetching games from API: {e}")
            return None

    def transform_game_data(self, game):
        """Transform API game data into internal format with validation."""
        try:
            bt.logging.debug(f"Transforming game data: {game}")
            
            # Validate required fields
            required_fields = ['team_a', 'team_b', 'external_id', 'date', 'sport', 'league']
            missing_fields = [field for field in required_fields if not game.get(field)]
            if missing_fields:
                bt.logging.error(f"Missing required fields: {missing_fields}")
                return None
            
            # Extract odds with validation
            team_a_odds = game.get('team_a_odds')
            team_b_odds = game.get('team_b_odds')
            tie_odds = game.get('tie_odds')
            
            if not all(isinstance(odds, (int, float)) for odds in [team_a_odds, team_b_odds] if odds is not None):
                bt.logging.error(f"Invalid odds values: team_a={team_a_odds}, team_b={team_b_odds}, tie={tie_odds}")
                return None
            
            transformed = {
                "home": game['team_a'],
                "away": game['team_b'],
                "game_id": game['external_id'],
                "date": game['date'],
                "odds": {
                    "average_home_odds": team_a_odds,
                    "average_away_odds": team_b_odds,
                    "average_tie_odds": tie_odds,
                },
                "sport": game['sport'].lower(),
                "league": game['league'],
            }
            
            bt.logging.debug(f"Successfully transformed game data: {transformed}")
            return transformed
            
        except Exception as e:
            bt.logging.error(f"Error transforming game data: {e}")
            bt.logging.error(f"Raw game data: {game}")
            bt.logging.error(f"Transform error details: {traceback.format_exc()}")
            return None

    async def get_data(self, endpoint, params):
        url = self.base_url + endpoint
        response = await self.session.get(url, params=params)
        return response.json()

    async def get_game_by_id(self, game_id: str):
        """Fetch a single game by its ID from the API."""
        try:
            bt.logging.debug(f"Attempting to fetch game {game_id} from API")
            response = await self.session.get(
                f"{self.base_url}/Games/TeamGames/{game_id}",
                headers=self.headers
            )
            
            if response.status == 404:
                bt.logging.warning(f"Game {game_id} not found in API (404)")
                return None
            
            if response.status != 200:
                bt.logging.error(f"Error fetching game {game_id}: HTTP {response.status}")
                response_text = await response.text()
                bt.logging.error(f"API response: {response_text}")
                return None
            
            game_data = await response.json()
            if not game_data:
                bt.logging.warning(f"Empty response for game {game_id}")
                return None
            
            bt.logging.debug(f"Raw game data from API: {game_data}")
            transformed_data = self.transform_game_data(game_data)
            bt.logging.debug(f"Transformed game data: {transformed_data}")
            return transformed_data
            
        except aiohttp.ClientError as e:
            bt.logging.error(f"Network error fetching game {game_id}: {e}")
            return None
        except json.JSONDecodeError as e:
            bt.logging.error(f"JSON decode error for game {game_id}: {e}")
            return None
        except Exception as e:
            bt.logging.error(f"Unexpected error fetching game {game_id}: {e}")
            bt.logging.error(f"Error details: {traceback.format_exc()}")
            return None

    async def fetch_and_store_missing_game(self, game_id: str, db_manager: DatabaseManager) -> bool:
        """
        Fetch a missing game from the API and store it in the database.
        Returns True if game was successfully fetched and stored.
        """
        try:
            bt.logging.debug(f"Fetching missing game {game_id}")
            game_data = await self.get_game_by_id(game_id)
            if not game_data:
                bt.logging.warning(f"Could not fetch game {game_id} from API")
                return False
            
            bt.logging.debug(f"Preparing to store game {game_id} in database")
            # Insert the game into the database
            query = """
            INSERT INTO game_data (
                game_id, home_team, away_team, event_start_date, 
                home_odds, away_odds, tie_odds, sport, league
            ) VALUES (
                :game_id, :home, :away, :date,
                :home_odds, :away_odds, :tie_odds, :sport, :league
            ) ON CONFLICT (game_id) DO UPDATE SET
                home_team = EXCLUDED.home_team,
                away_team = EXCLUDED.away_team,
                event_start_date = EXCLUDED.event_start_date,
                home_odds = EXCLUDED.home_odds,
                away_odds = EXCLUDED.away_odds,
                tie_odds = EXCLUDED.tie_odds,
                sport = EXCLUDED.sport,
                league = EXCLUDED.league
            """
            
            params = {
                "game_id": game_data["game_id"],
                "home": game_data["home"],
                "away": game_data["away"],
                "date": game_data["date"],
                "home_odds": game_data["odds"]["average_home_odds"],
                "away_odds": game_data["odds"]["average_away_odds"],
                "tie_odds": game_data["odds"]["average_tie_odds"],
                "sport": game_data["sport"],
                "league": game_data["league"]
            }
            
            bt.logging.debug(f"Database parameters: {params}")
            
            async with db_manager.engine.begin() as conn:
                await conn.execute(text(query), params)
                
            bt.logging.info(f"Successfully stored game {game_id} in database")
            return True
            
        except Exception as e:
            bt.logging.error(f"Error storing game {game_id} in database: {e}")
            bt.logging.error(f"Storage error details: {traceback.format_exc()}")
            return False
