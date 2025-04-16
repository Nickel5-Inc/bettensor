import functools
import math
import multiprocessing
import traceback
import numpy as np
import torch
import sqlite3
from datetime import datetime, timezone, timedelta
import asyncio
import bittensor as bt
from bettensor import __spec_version__


class WeightSetter:
    def __init__(
        self,
        metagraph: "bt.metagraph",
        wallet: "bt.wallet", #type: ignore
        subtensor: "bt.subtensor",
        neuron_config: "bt.config",
        db_path: str,
    ):
        self.metagraph = metagraph
        self.wallet = wallet
        self.subtensor = subtensor
        self.neuron_config = neuron_config

        self.db_path = db_path

    def connect_db(self):
        return sqlite3.connect(self.db_path)
    
    @staticmethod
    def timeout_with_multiprocess(seconds):
        # Thanks Omron (SN2) for the timeout decorator
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                def target_func(result_dict, *args, **kwargs):
                    try:
                        result_dict["result"] = func(*args, **kwargs)
                    except Exception as e:
                        result_dict["exception"] = e

                manager = multiprocessing.Manager()
                result_dict = manager.dict()
                process = multiprocessing.Process(
                    target=target_func, args=(result_dict, *args), kwargs=kwargs
                )
                process.start()
                process.join(seconds)

                if process.is_alive():
                    process.terminate()
                    process.join()
                    bt.logging.warning(
                        f"Function '{func.__name__}' timed out after {seconds} seconds"
                    )
                    return False

                if "exception" in result_dict:
                    raise result_dict["exception"]

                return result_dict.get("result", False)

            return wrapper

        return decorator

    def set_weights(self, weights: torch.Tensor):
        try:
            # Log start of weight setting process
            bt.logging.info("Starting weight setting process...")
            
            # --- Explicitly sync metagraph right before setting weights --- 
            bt.logging.info("Syncing metagraph within set_weights for latest state...")
            try:
                self.metagraph.sync(subtensor=self.subtensor)
                bt.logging.info("Metagraph synced successfully within set_weights.")
            except Exception as sync_err:
                bt.logging.error(f"Failed to sync metagraph within set_weights: {sync_err}. Proceeding with potentially stale metagraph.")
            # --- End explicit sync --- 
            
            # Get the number of UIDs in the *potentially newly synced* metagraph
            num_metagraph_uids = len(self.metagraph.uids)
            metagraph_uids_tensor = self.metagraph.uids # Keep as tensor for direct use later

            # Ensure weights vector is sized correctly relative to the *current* metagraph UIDs
            if len(weights) > num_metagraph_uids:
                bt.logging.info(f"Weights vector length ({len(weights)}) is longer than current metagraph UIDs length ({num_metagraph_uids}). Trimming weights.")
                weights_to_set = weights[:num_metagraph_uids]
            elif len(weights) < num_metagraph_uids:
                bt.logging.info(f"Weights vector length ({len(weights)}) is shorter than current metagraph UIDs length ({num_metagraph_uids}). Padding weights with zeros.")
                pad_len = num_metagraph_uids - len(weights)
                weights_to_set = torch.cat([weights, torch.zeros(pad_len, dtype=torch.float32)])
            else:
                bt.logging.info(f"Weights vector length ({len(weights)}) matches current metagraph UIDs length ({num_metagraph_uids}). Using as is.")
                weights_to_set = weights # Lengths match
            
            # Initialize subtensor connection (consider reusing validator's connection if possible)
            weights_subtensor = bt.subtensor(network=self.neuron_config.network)
            if weights_subtensor is None:
                bt.logging.error("Subtensor connection failed.")
                return False

            # Get min_allowed_weights from subtensor
            try:
                min_allowed_weights = weights_subtensor.min_allowed_weights(netuid=self.neuron_config.netuid)
                bt.logging.info(f"Subtensor reports min_allowed_weights for netuid {self.neuron_config.netuid}: {min_allowed_weights}")
            except Exception as e:
                 bt.logging.error(f"Could not retrieve min_allowed_weights for netuid {self.neuron_config.netuid}: {e}. Skipping weight set.")
                 return False # Cannot proceed without knowing the minimum

            # Check if the number of UIDs in our metagraph is sufficient AFTER syncing
            if num_metagraph_uids < min_allowed_weights:
                bt.logging.error(f"Number of UIDs in metagraph ({num_metagraph_uids}) is less than min_allowed_weights ({min_allowed_weights}) for netuid {self.neuron_config.netuid}. Cannot set weights.")
                return False # Do not attempt to set weights
            
            # ensure params are set and valid 
            # Check weights_to_set specifically
            if self.neuron_config.netuid is None or self.wallet is None or metagraph_uids_tensor is None or weights_to_set is None:
                bt.logging.error("Invalid parameters for subtensor.set_weights()")
                bt.logging.error(f"Neuron config netuid: {self.neuron_config.netuid}")
                bt.logging.error(f"Wallet: {self.wallet}")
                bt.logging.error(f"Metagraph uids: {metagraph_uids_tensor}")
                bt.logging.error(f"Weights to set: {weights_to_set}")
                bt.logging.error(f"Version key: {__spec_version__}")
                return False
            
            # Check length consistency one last time before calling
            if len(metagraph_uids_tensor) != len(weights_to_set):
                 bt.logging.error(f"Final length mismatch before set_weights call: UIDs={len(metagraph_uids_tensor)}, Weights={len(weights_to_set)}")
                 return False
                 
            bt.logging.info(f"Setting weights for netuid: {self.neuron_config.netuid}")
            bt.logging.info(f"Wallet: {self.wallet}")
            bt.logging.info(f"Uids being sent ({len(metagraph_uids_tensor)}): {metagraph_uids_tensor}")
            bt.logging.info(f"Weights being sent ({len(weights_to_set)}): {weights_to_set}")
            bt.logging.info(f"Version key: {__spec_version__}")
            bt.logging.info(f"Subtensor: {weights_subtensor}")

            # Log before critical operation
            bt.logging.info("Calling subtensor.set_weights()")
            
            result = weights_subtensor.set_weights(
                netuid=self.neuron_config.netuid, 
                wallet=self.wallet, 
                uids=metagraph_uids_tensor, # Use potentially newly synced metagraph UIDs
                weights=weights_to_set, # Use correctly sized weights based on synced metagraph
                version_key=__spec_version__, 
                wait_for_inclusion=True, 
            )
        
            bt.logging.debug(f"Set weights result: {result}")

            if isinstance(result, tuple) and len(result) >= 1:
                success = result[0]
                bt.logging.debug(f"Set weights message: {success}")
                if success:
                    bt.logging.info("Successfully set weights.")
                    return True
                
            else:
                bt.logging.warning(f"Unexpected result format in setting weights: {result}")
                
        except TimeoutError:
            bt.logging.error("Timeout occurred while setting weights in subtensor call")
            raise
        except Exception as e:
            bt.logging.error(f"Error setting weights: {str(e)}")
            bt.logging.error(f"Error traceback: {traceback.format_exc()}")
            raise

        bt.logging.error("Failed to set weights after all attempts.")
        return False


    