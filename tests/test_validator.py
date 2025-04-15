"""
Test script for validator functions and files
"""

import unittest
from unittest.mock import Mock, patch
import datetime
import tempfile
import os
from datetime import timedelta
from argparse import ArgumentParser

import bittensor as bt
from bettensor.validator.bettensor_validator import BettensorValidator
from bettensor.validator.io.sports_data import SportsData
from bettensor.validator.io.external_api_client import ExternalAPIClient
from bettensor.validator.utils.database.database_manager import DatabaseManager
from bettensor.validator.utils.scoring.entropy_system import EntropySystem


class TestValidator(unittest.TestCase):
    @patch(
        "bettensor.validator.io.external_api_client.ExternalAPIClient",
        autospec=True,
    )
    def setUp(self, MockExternalAPIClient):
        # Create a temporary database
        self.temp_db = tempfile.NamedTemporaryFile(delete=False)
        self.db_path = self.temp_db.name

        # Initialize database manager
        self.db_manager = DatabaseManager(self.db_path)
        self.db_manager._initialize_database()

        # Mock EntropySystem
        self.mock_entropy_system = Mock(spec=EntropySystem)

        # Initialize a mock ExternalAPIClient instance
        self.mock_api_client = MockExternalAPIClient.return_value

        # Mock the process_game_result method
        self.mock_api_client.process_game_result = Mock()

        # Initialize SportsData with mocks
        self.sports_data = SportsData(
            self.db_manager, self.mock_entropy_system, self.mock_api_client
        )

        # Prepare ArgumentParser without adding conflicting arguments
        parser = ArgumentParser()
        parser.add_argument("--subtensor.network", type=str, default="test_network")
        parser.add_argument("--netuid", type=int, default=1)
        parser.add_argument("--wallet.name", type=str, default="test_wallet")
        # Add other required arguments as needed, except those that BettensorValidator adds

        # Initialize BettensorValidator with the parser
        self.validator = BettensorValidator(parser=parser)
        # Assign the db_manager to the validator to resolve AttributeError
        self.validator.db_manager = self.db_manager
        # Assign sports_data if required by validator
        self.validator.sports_data = self.sports_data
        # Assign the mocked api_client to the validator
        self.validator.api_client = self.mock_api_client

    def tearDown(self):
        # Close and remove the temporary database
        self.db_manager.conn.close()
        os.unlink(self.db_path)

    def test_update_game_data_flow(self):
        # Define the mock response for fetch_and_update_game_data
        mock_games = [
            {
                "externalId": "123",
                "teamA": "Team A",
                "teamB": "Team B",
                "sport": "Football",
                "league": "NFL",
                "date": "2023-06-01T18:00:00Z",
                "outcome": "Unfinished",
                "odds": {
                    "average_home_odds": 1.5,
                    "average_away_odds": 2.5,
                    "average_tie_odds": None,
                },
            }
        ]

        self.mock_api_client.fetch_all_game_data.return_value = mock_games

        # Call fetch_and_update_game_data which should insert into the database
        self.sports_data.fetch_and_update_game_data()

        # Verify that fetch_all_game_data was called once
        self.mock_api_client.fetch_all_game_data.assert_called_once()

        # Retrieve the inserted game from the database
        game = self.db_manager.fetch_one(
            "SELECT external_id, team_a, team_b FROM game_data WHERE external_id = ?",
            ("123",),
        )
        self.assertIsNotNone(game)
        self.assertEqual(game[0], "123")
        self.assertEqual(game[1], "Team A")
        self.assertEqual(game[2], "Team B")

    def test_update_recent_games_flow(self):
        # Insert a game into the database with event_start_date older than five hours
        six_hours_ago = datetime.datetime.now(datetime.timezone.utc) - timedelta(
            hours=6
        )
        self.db_manager.execute_query(
            """
            INSERT INTO game_data (team_a, team_b, sport, league, external_id, event_start_date, outcome)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Team A",
                "Team B",
                "Football",
                "NFL",
                "123",
                six_hours_ago.isoformat(),
                "Unfinished",
            ),
        )

        # Define mocked API response to simulate a finished game
        mock_game_data = {
            "results": [
                {
                    "time_status": "3",  # 3 means the game has finished
                    "ss": "20-27",  # Example score
                }
            ]
        }

        self.mock_api_client.get_nfl_result.return_value = mock_game_data

        # Spy on the process_game_result method
        original_process_game_result = self.mock_api_client.process_game_result
        self.mock_api_client.process_game_result = Mock(
            side_effect=original_process_game_result
        )

        # Call update_recent_games which should process the game
        self.validator.update_recent_games()

        # Assert determine_winner was called correctly
        expected_game_info = {
            "external_id": "123",
            "team_a": "Team A",
            "team_b": "Team B",
            "sport": "Football",
            "league": "NFL",
            "event_start_date": six_hours_ago.isoformat(),
        }
        self.mock_api_client.determine_winner.assert_called_once_with(
            expected_game_info
        )

        # Assert process_game_result was called with correct parameters
        self.mock_api_client.process_game_result.assert_called_once_with(
            "Football", mock_game_data["results"][0], "123", "Team A", "Team B"
        )

        # Verify that the outcome was updated in the database
        game = self.db_manager.fetch_one(
            "SELECT outcome FROM game_data WHERE external_id = ?", ("123",)
        )
        self.assertEqual(
            game[0], 1
        )  # 1 represents away team win based on score "20-27"

    def test_full_update_flow(self):
        # Insert a game into the database with event_start_date older than five hours
        six_hours_ago = datetime.datetime.now(datetime.timezone.utc) - timedelta(
            hours=6
        )
        self.db_manager.execute_query(
            """
            INSERT INTO game_data (team_a, team_b, sport, league, external_id, event_start_date, outcome)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Team A",
                "Team B",
                "Football",
                "NFL",
                "123",
                six_hours_ago.isoformat(),
                "Unfinished",
            ),
        )

        # Define mocked API response
        mock_games = [
            {
                "externalId": "123",
                "teamA": "Team A",
                "teamB": "Team B",
                "sport": "Football",
                "league": "NFL",
                "date": six_hours_ago.isoformat(),
                "outcome": "Unfinished",
                "odds": {
                    "average_home_odds": 1.5,
                    "average_away_odds": 2.5,
                    "average_tie_odds": None,
                },
            }
        ]
        self.mock_api_client.fetch_all_game_data.return_value = mock_games

        # Define a side effect for determine_winner to ensure process_game_result is called
        def mock_determine_winner(game_info):
            # Verify the structure of game_info
            self.assertIsInstance(game_info, dict)
            self.assertEqual(game_info["external_id"], "123")
            return 0  # 0 represents home team win

        with patch.object(
            self.mock_api_client, "determine_winner", side_effect=mock_determine_winner
        ) as mock_det_winner:
            # Mock process_game_result to track its calls
            self.mock_api_client.process_game_result = Mock(return_value=0)

            # Call update_recent_games which should process the game
            self.validator.update_recent_games()

            # Assert determine_winner was called correctly
            expected_game_info = {
                "external_id": "123",
                "team_a": "Team A",
                "team_b": "Team B",
                "sport": "Football",
                "league": "NFL",
                "event_start_date": six_hours_ago.isoformat(),
            }
            mock_det_winner.assert_called_once_with(expected_game_info)

            # Assert process_game_result was called with correct parameters
            self.mock_api_client.process_game_result.assert_called_once_with(
                "Football",
                unittest.mock.ANY,  # Replace with actual game_response if needed
                "123",
                "Team A",
                "Team B",
            )

            # Verify that the outcome was updated in the database
            game = self.db_manager.fetch_one(
                "SELECT outcome FROM game_data WHERE external_id = ?", ("123",)
            )
            self.assertEqual(game[0], 0)  # 0 represents home team win

    def test_api_error_handling(self):
        # Configure the mock to raise an exception when fetch_all_game_data is called
        self.mock_api_client.fetch_all_game_data.side_effect = Exception("API Failure")

        with self.assertRaises(Exception) as context:
            # Assuming that update_recent_games triggers fetch_all_game_data indirectly
            self.sports_data.fetch_and_update_game_data()

        # Verify that the exception message is as expected
        self.assertTrue("API Failure" in str(context.exception))


if __name__ == "__main__":
    unittest.main()
