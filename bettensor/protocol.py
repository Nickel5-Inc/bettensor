# The MIT License (MIT)
# Copyright © 2023 Yuma Rao
# TODO(developer): Set your name
# Copyright © 2023 <your name>

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Tuple
import uuid
import bittensor as bt
from pydantic import BaseModel, Field
import sqlite3


class MinerStats(BaseModel):
    """
    Data class for miner stats
    """
    model_config = {
        "arbitrary_types_allowed": True,
        "validate_assignment": True,
        "extra": "allow"
    }

    miner_hotkey: str = Field(..., description="Miner hotkey")
    miner_coldkey: str = Field(..., description="Miner coldkey")
    miner_uid: str = Field(..., description="Miner UID")
    miner_rank: str = Field(..., description="Miner rank")
    miner_status: str = Field(..., description="Miner status")
    miner_cash: str = Field(..., description="Miner cash")
    miner_current_incentive: str = Field(..., description="Miner current incentive")
    miner_current_tier: str = Field(..., description="Miner current tier")
    miner_current_scoring_window: str = Field(..., description="Miner current scoring window")
    miner_current_composite_score: str = Field(..., description="Miner current composite score")
    miner_current_sharpe_ratio: str = Field(..., description="Miner current sharpe ratio")
    miner_current_sortino_ratio: str = Field(..., description="Miner current sortino ratio")
    miner_current_roi: str = Field(..., description="Miner current ROI")
    miner_current_clv_avg: str = Field(..., description="Miner current CLV average")
    miner_last_prediction_date: Optional[str] = Field(None, description="Miner last prediction date")
    miner_lifetime_earnings: str = Field(..., description="Miner lifetime earnings")
    miner_lifetime_wager_amount: str = Field(..., description="Miner lifetime wager amount")
    miner_lifetime_roi: str = Field(..., description="Miner lifetime ROI")
    miner_lifetime_predictions: str = Field(..., description="Miner lifetime predictions")
    miner_lifetime_wins: str = Field(..., description="Miner lifetime wins")
    miner_lifetime_losses: str = Field(..., description="Miner lifetime losses")
    miner_win_loss_ratio: str = Field(..., description="Miner win/loss ratio")
    most_recent_weight: Optional[str] = Field(None, description="Most recent weight")

    @classmethod
    def create(cls, row):
        """
        takes a row from the miner_stats table and returns a MinerStats object
        """
        return cls(
            miner_hotkey=str(row[0]),
            miner_coldkey=str(row[1]),
            miner_uid=str(row[2]),
            miner_rank=str(row[3]),
            miner_status=str(row[4]),
            miner_cash=str(row[5]),
            miner_current_incentive=str(row[6]),
            miner_current_tier=str(row[7]),
            miner_current_scoring_window=str(row[8]),
            miner_current_composite_score=str(row[9]),
            miner_current_sharpe_ratio=str(row[10]),
            miner_current_sortino_ratio=str(row[11]),
            miner_current_roi=str(row[12]),
            miner_current_clv_avg=str(row[13]),
            miner_last_prediction_date=str(row[14]) if row[14] is not None else None,
            miner_lifetime_earnings=str(row[15]),
            miner_lifetime_wager_amount=str(row[16]),
            miner_lifetime_roi=str(row[17]),
            miner_lifetime_predictions=str(row[18]),
            miner_lifetime_wins=str(row[19]),
            miner_lifetime_losses=str(row[20]),
            miner_win_loss_ratio=str(row[21]),
            most_recent_weight=str(row[22]) if len(row) > 22 and row[22] is not None else None
        )


class Metadata(BaseModel):
    """Synapse Metadata class, add more fields if needed"""

    model_config = {
        "arbitrary_types_allowed": True,
        "validate_assignment": True,
        "extra": "allow"
    }

    synapse_id: str = Field(..., description="UUID of the synapse")
    neuron_uid: str = Field(..., description="UUID of the serving neuron")
    timestamp: str = Field(..., description="Timestamp of the synapse")
    subnet_version: str = Field(..., description="Subnet version of the neuron sending the synapse")
    synapse_type: str = Field(..., description="Type of the synapse | 'prediction' or 'game_data'")

    @classmethod
    def create(cls, subnet_version, neuron_uid, synapse_type):
        """
        Creates a new metadata object
        Args:
            neuron_id: UUID
            subnet_id: str
        Returns:
            Metadata: A new metadata object to attach to a synapse
        """
        synapse_id = str(uuid.uuid4())
        timestamp = datetime.now(timezone.utc).isoformat()
        bt.logging.debug(
            f"Creating Metadata with synapse_id: {synapse_id}, neuron_uid: {neuron_uid}, timestamp: {timestamp}, subnet_version: {subnet_version}, synapse_type: {synapse_type}"
        )
        return Metadata(
            synapse_id=synapse_id,
            neuron_uid=str(neuron_uid),  # Ensure neuron_uid is a string
            timestamp=timestamp,
            subnet_version=str(subnet_version),  # Ensure subnet_version is a string
            synapse_type=synapse_type,
        )


class TeamGamePrediction(BaseModel):
    """
    Data class from json. Will need to be modified in the future for more complex prediction types.
    """
    
    model_config = {
        "arbitrary_types_allowed": True,
        "validate_assignment": True,
        "protected_namespaces": (),
        "extra": "allow"
    }

    prediction_id: str = Field(..., description="UUID of the prediction")
    game_id: str = Field(..., description="Game ID - Not Unique (External ID from API)")
    miner_uid: str = Field(
        ..., description="UUID or UID of the miner that made the prediction"
    )
    prediction_date: str = Field(..., description="Prediction date of the prediction")
    predicted_outcome: str = Field(..., description="Predicted outcome")
    predicted_odds: float = Field(
        ..., description="Predicted outcome odds at the time of prediction"
    )
    team_a: Optional[str] = Field(None, description="Team A, typically the home team")
    team_b: Optional[str] = Field(None, description="Team B, typically the away team")
    wager: float = Field(..., description="Wager of the prediction")
    team_a_odds: float = Field(..., description="Team A odds at the time of prediction")
    team_b_odds: float = Field(..., description="Team B odds at the time of prediction")
    tie_odds: Optional[float] = Field(
        None, description="Tie odds at the time of prediction"
    )
    model_name: Optional[str] = Field(
        None,
        description="Name of the model that made the prediction - null if submitted by a human",
    )
    confidence_score: Optional[float] = Field(
        None, description="Confidence score of the prediction (model based predictions only)"
    )
    outcome: str = Field(..., description="Outcome of prediction")
    payout: float = Field(
        ...,
        description="Payout of prediction - for local tracking, not used in scoring",
    )


class TeamGame(BaseModel):
    """
    Data class from json. May need to be modified in the future for more complex prediction types
    """
    
    model_config = {
        "arbitrary_types_allowed": True,
        "validate_assignment": True,
        "extra": "allow"
    }

    game_id: str = Field(
        ...,
        description="ID of the team game - Formerly 'externalId' (No need for unique ID here)",
    )
    team_a: str = Field(..., description="Team A (Typically the home team)")
    team_b: str = Field(..., description="Team B (Typically the away team)")
    sport: str = Field(..., description="Sport")
    league: str = Field(..., description="League name")
    create_date: str = Field(..., description="Create date")
    last_update_date: str = Field(..., description="Last update date")
    event_start_date: str = Field(..., description="Event start date")
    active: bool = Field(..., description="Active")
    outcome: str = Field(..., description="Outcome")
    team_a_odds: float = Field(..., description="Team A odds")
    team_b_odds: float = Field(..., description="Team B odds")
    tie_odds: float = Field(..., description="Tie odds")
    can_tie: bool = Field(..., description="Can tie")
    
    @classmethod
    def create_from_row(cls, row_dict):
        """
        Create a TeamGame from a database row with null handling
        """
        current_time = datetime.now(timezone.utc).isoformat()
        
        # Get event_start_date for default create_date calculation
        event_start_date = row_dict.get('event_start_date')
        if event_start_date is None:
            event_start_date = current_time
            
        # Calculate create_date as event_start_date minus 1 week if null
        create_date = row_dict.get('create_date')
        if create_date is None:
            try:
                # Parse the event start date and subtract 1 week
                event_dt = datetime.fromisoformat(event_start_date.replace('Z', '+00:00'))
                create_dt = event_dt - timedelta(days=7)
                create_date = create_dt.isoformat()
            except (ValueError, AttributeError):
                # Fallback to current time if parsing fails
                create_date = current_time
            
        last_update_date = row_dict.get('last_update_date')
        if last_update_date is None:
            last_update_date = current_time
            
        return cls(
            game_id=row_dict.get('game_id', str(uuid.uuid4())),
            team_a=row_dict.get('team_a', 'Team A'),
            team_b=row_dict.get('team_b', 'Team B'),
            sport=row_dict.get('sport', 'Unknown'),
            league=row_dict.get('league', 'Unknown'),
            create_date=create_date,
            last_update_date=last_update_date,
            event_start_date=event_start_date,
            active=bool(row_dict.get('active', True)),
            outcome=str(row_dict.get('outcome', '3')),
            team_a_odds=float(row_dict.get('team_a_odds', 0)),
            team_b_odds=float(row_dict.get('team_b_odds', 0)),
            tie_odds=float(row_dict.get('tie_odds', 0)),
            can_tie=bool(row_dict.get('can_tie', False))
        )


class GameData(bt.Synapse):
    """
    This class defines the synapse object for game data, consisting of a dictionary of TeamGame objects with a UUID as key.
    """
    model_config = {
        "arbitrary_types_allowed": True,
        "validate_assignment": True,
        "extra": "allow"
    }

    metadata: Optional[Metadata] = Field(default=None)
    gamedata_dict: Optional[Dict[str, TeamGame]] = Field(default=None)
    prediction_dict: Optional[Dict[str, TeamGamePrediction]] = Field(default=None)
    confirmation_dict: Optional[Dict[str, Dict[str, str]]] = Field(default=None)
    error: Optional[str] = Field(default=None)

    @classmethod
    def create(
        cls,
        db_path: str,
        wallet: bt.wallet,
        subnet_version: str,
        neuron_uid: int,  # Note: This is an int
        synapse_type: str,
        gamedata_dict: Dict[str, TeamGame] = None,
        prediction_dict: Dict[str, TeamGamePrediction] = None,
        confirmation_dict: Dict[str, Dict[str, str]] = None,
    ):
        metadata = Metadata.create(
            subnet_version=subnet_version,
            neuron_uid=str(neuron_uid),  # Convert to string here
            synapse_type=synapse_type,
        )
        if synapse_type == "prediction":
            gamedata_dict = None
            confirmation_dict = None
        elif synapse_type == "game_data":
            prediction_dict = None
            confirmation_dict = None
        elif synapse_type == "confirmation":
            gamedata_dict = None
            prediction_dict = None
        else: # error type
            gamedata_dict = None
            prediction_dict = None
            confirmation_dict = None

        return cls(
            metadata=metadata,
            gamedata_dict=gamedata_dict,
            prediction_dict=prediction_dict,
            confirmation_dict=confirmation_dict,
            synapse_type=synapse_type,
            name="GameData"
        )

    def deserialize(self):
        return self.gamedata_dict, self.prediction_dict, self.metadata


