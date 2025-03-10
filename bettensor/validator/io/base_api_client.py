import json
import bittensor as bt
from datetime import datetime, timedelta, timezone


class BaseAPIClient:
    """
    Base class for handling all sports data API requests.
    """

    def __init__(self, last_update_date=None):
        self.last_update_date = last_update_date
        if last_update_date is None:
            self.last_update_date = datetime.now(timezone.utc) - timedelta(days=15)
        pass

    def fetch_and_update_game_data(self):
        """
        fetch and update game data from external APIs.
        Override this method in the child classes.
        """
        pass

    def fetch_and_update_single_game_data(self, external_id):
        """
        fetch and update single game data from external APIs.
        Override this method in the child classes.
        """

        pass
