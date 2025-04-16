import traceback
import requests
import json
from datetime import datetime
import bittensor as bt
from argparse import ArgumentParser
from dateutil import parser
import pytz


class WebsiteHandler:
    def __init__(self, validator):
        # Initialize the parser and validator
        self.validator = validator

    async def ensure_keys_table(self):
        """
        Creates keys table in db
        """
        query = """
        CREATE TABLE IF NOT EXISTS keys (
            hotkey TEXT PRIMARY KEY,
            coldkey TEXT
        )
        """
        await self.validator.db_manager.execute_query(query)

    async def get_or_update_coldkey(self, hotkey: str) -> str:
        """
        Retrieves coldkey from metagraph if it doesn't exist in keys table
        """

        query = "SELECT coldkey FROM keys WHERE hotkey = ?"
        result = await self.validator.db_manager.fetch_one(query, (hotkey,))

        if result:
            return result['coldkey']
        else:
            # If not found, fetch from Metagraph and insert into the database
            coldkey = None  # Initialize coldkey

            for neuron in self.validator.metagraph.neurons:
                if neuron.hotkey == hotkey:
                    coldkey = neuron.coldkey
                    insert_query = "INSERT INTO keys (hotkey, coldkey) VALUES (?, ?)"
                    await self.validator.db_manager.execute_query(insert_query, (hotkey, coldkey))
                    return coldkey  # Exit after finding and inserting the coldkey

            # If coldkey is not found after iterating through neurons
            coldkey = "dummy_coldkey"
            insert_query = "INSERT INTO keys (hotkey, coldkey) VALUES (?, ?)"
            await self.validator.db_manager.execute_query(insert_query, (hotkey, coldkey))
            return coldkey

    async def fetch_predictions_from_db(self):
        query = """
        SELECT * FROM predictions 
        WHERE sent_to_site = 0
        """
        return await self.validator.db_manager.fetch_all(query)

    async def mark_predictions_as_sent(self):
        query = """
        UPDATE predictions 
        SET sent_to_site = 1 
        WHERE sent_to_site = 0
        """
        await self.validator.db_manager.execute_query(query)

    async def fetch_and_send_predictions(self):
        """Fetch predictions from DB and send to website"""
        try:
            predictions = await self.fetch_predictions_from_db()
            
            if not predictions:
                bt.logging.info("No new predictions found in the database.")
                return None

            if not isinstance(predictions, list):
                bt.logging.error("Invalid predictions format. Expected list of dictionaries.")
                return None

            # Process predictions here...
            
            result = await self.send_predictions(predictions)
            
            
            return result

        except Exception as e:
            bt.logging.error(f"Error in fetch_and_send_predictions: {str(e)}")
            bt.logging.error(f"Error traceback: {traceback.format_exc()}")
            return None

    async def send_predictions(self, predictions_by_miner_uid):
        """
        Send organized predictions to the Bettensor API.
        """
        url = "https://www.bettensor.com/API/Predictions/"
        headers = {"Content-Type": "application/json"}
        network = self.validator.subtensor.network

        try:
            # Convert list to dictionary grouped by miner_uid
            if isinstance(predictions_by_miner_uid, list):
                grouped_predictions = {}
                for prediction in predictions_by_miner_uid:
                    miner_uid = prediction.get('miner_uid')
                    if miner_uid not in grouped_predictions:
                        grouped_predictions[miner_uid] = []
                    grouped_predictions[miner_uid].append(prediction)
                predictions_by_miner_uid = grouped_predictions

            for miner_uid, predictions in predictions_by_miner_uid.items():
                try:
                    # Fetch or update coldkey once per miner_uid
                    hotkey = self.validator.metagraph.hotkeys[miner_uid]
                    if hotkey:
                        coldkey = await self.get_or_update_coldkey(hotkey)
                    else:
                        bt.logging.warning(f"Invalid miner_uid: {miner_uid}. Setting coldkey to 'dummy_coldkey'.")
                        coldkey = "dummy_coldkey"
                    
                    # Await the miner stats query
                    miner_stats = await self.validator.db_manager.fetch_one(
                        "SELECT * FROM miner_stats WHERE miner_uid = ?", 
                        (miner_uid,)
                    )

                    transformed_data = []
                    for prediction in predictions:
                        try:
                            if not isinstance(prediction, dict):
                                bt.logging.error(f"Invalid prediction format for miner_uid {miner_uid}: Expected dict, got {type(prediction).__name__}. Skipping prediction.")
                                continue

                            metadata = {
                                "miner_uid": miner_uid,
                                "miner_hotkey": hotkey,
                                "miner_coldkey": coldkey,
                                "miner_stats": miner_stats,
                                "prediction_date": prediction.get("prediction_date"),
                                "predicted_outcome": prediction.get("predicted_outcome"),
                                "wager": prediction.get("wager"),
                                "predicted_odds": prediction.get("predicted_odds"),
                                "team_a_odds": prediction.get("team_a_odds"),
                                "team_b_odds": prediction.get("team_b_odds"),
                                "tie_odds": prediction.get("tie_odds"),
                                "is_model_prediction": prediction.get("is_model_prediction"),
                                "outcome": prediction.get("outcome"),
                                "payout": prediction.get("payout"),
                                "subtensor_network": network,
                            }

                            transformed_prediction = {
                                "externalGameId": str(prediction.get("game_id")),
                                "minerHotKey": hotkey,
                                "minerColdKey": coldkey,
                                "predictionDate": self._format_prediction_date(prediction.get("prediction_date")),
                                "predictedOutcome": prediction.get("predicted_outcome"),
                                "wager": prediction.get("wager"),
                                "modelName": prediction.get("model_name"),
                                "predictionOdds": prediction.get("predicted_odds"),
                                "metaData": json.dumps(
                                    metadata,
                                    default=lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
                                ),
                            }

                            transformed_data.append(transformed_prediction)

                        except Exception as e:
                            bt.logging.error(f"Error processing prediction_id {prediction.get('prediction_id', 'Unknown')} for miner_uid {miner_uid}: {e}")
                            bt.logging.error(f"Error traceback: {traceback.format_exc()}")
                            continue  # Skip this prediction and continue with the next

                    if not transformed_data:
                        bt.logging.info(f"No valid predictions to send for miner_uid: {miner_uid}")
                        continue

                    bt.logging.info(f"Sending {len(transformed_data)} predictions to API for miner_uid: {miner_uid}")
                    #bt.logging.debug(f"First prediction (for debugging): {json.dumps(transformed_data[0], indent=2)}")

                    response = requests.post(url, data=json.dumps(transformed_data), headers=headers)
                    if response.status_code in [200, 201]:
                       #bt.logging.info(f"Response status code for miner_uid {miner_uid}: {response.status_code}")
                        #bt.logging.debug(f"Response content: {response.text}")
                        
                        # Mark these specific predictions as sent using IN clause instead of ANY
                        prediction_ids = [p.get('prediction_id') for p in predictions]
                        placeholders = ','.join('?' * len(prediction_ids))
                        await self.validator.db_manager.execute_query(
                            f"UPDATE predictions SET sent_to_site = 1 WHERE prediction_id IN ({placeholders})",
                            prediction_ids
                        )
                    else:
                        bt.logging.error(f"Failed to send predictions for miner_uid {miner_uid}. Status code: {response.status_code}, Response: {response.text}")

                except Exception as e:
                    bt.logging.error(f"Error processing miner_uid {miner_uid}: {e}")
                    bt.logging.error(f"Error traceback: {traceback.format_exc()}")
                    continue

            return True

        except Exception as e:
            bt.logging.error(f"Error sending predictions: {e}")
            bt.logging.error(f"Error traceback: {traceback.format_exc()}")
            return False

    def _format_prediction_date(self, prediction_date):
        """
        Ensure prediction_date is in ISO 8601 format with UTC timezone.

        :param prediction_date: Original prediction date string
        :return: Formatted date string
        """
        try:
            # First, try to parse the date assuming it's already in ISO format
            date = parser.isoparse(prediction_date)
            
            # Ensure the date is timezone-aware (use UTC if no timezone)
            if date.tzinfo is None:
                date = date.replace(tzinfo=pytz.UTC)
            
            # Format to ISO 8601 with 'Z' indicating UTC
            return date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        except ValueError:
            # If parsing fails, attempt to parse with a more flexible parser
            try:
                date = parser.parse(prediction_date)
                if date.tzinfo is None:
                    date = date.replace(tzinfo=pytz.UTC)
                return date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            except ValueError:
                bt.logging.warning(f"Invalid date format for prediction_date '{prediction_date}'. Using current UTC time.")
                return datetime.now(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    async def fetch_and_send_predictions(self):
        """
        Fetch predictions from the database and send them to the API.

        :return: API response status code
        """
        try:
            network = self.validator.subtensor.network
            bt.logging.info(f"Network: {network}")
            if network == "finney":
                await self.ensure_keys_table()  # Ensure the keys table exists
                predictions = await self.fetch_predictions_from_db()
                if predictions:
                    bt.logging.debug("Sending predictions to the Bettensor website.")
                    return await self.send_predictions(predictions)
                else:
                    bt.logging.info("No new predictions found in the database.")
            else:
                bt.logging.info("Not on Finney network, skipping prediction sending.")
            return None
        except Exception as e:
            bt.logging.error(f"Error fetching and sending predictions: {e}")
            bt.logging.error(f"Error traceback: {traceback.format_exc()}")