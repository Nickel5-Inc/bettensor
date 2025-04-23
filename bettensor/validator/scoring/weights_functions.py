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
            
            # --- MODIFIED: Use full UID range and full weights tensor ---
            # Assume the subnet has 256 potential UIDs. Get this dynamically if possible, otherwise hardcode.
            # Let's try to get max_n, fallback to 256
            try:
                max_allowed_uids = self.subtensor.subnetwork_n(netuid=self.neuron_config.netuid)
                if max_allowed_uids <= 0: # Handle potential invalid return
                    bt.logging.warning(f"subtensor.subnetwork_n returned {max_allowed_uids}, defaulting max UIDs to 256.")
                    max_allowed_uids = 256
            except Exception as e:
                bt.logging.warning(f"Could not fetch subnetwork_n from subtensor: {e}. Defaulting max UIDs to 256.")
                max_allowed_uids = 256
                
            # Ensure input weights tensor is the correct size (should be 256 from scoring)
            if len(weights) != max_allowed_uids:
                 bt.logging.warning(f"Input weights tensor length ({len(weights)}) does not match expected max UIDs ({max_allowed_uids}). Resizing.")
                 # Pad or trim weights to match max_allowed_uids
                 if len(weights) < max_allowed_uids:
                     pad_len = max_allowed_uids - len(weights)
                     weights_full = torch.cat([weights, torch.zeros(pad_len, dtype=torch.float32)])
                 else:
                     weights_full = weights[:max_allowed_uids]
                 # Renormalize if resized
                 if torch.sum(weights_full) > 0:
                     weights_full = weights_full / torch.sum(weights_full)
            else:
                 weights_full = weights # Use the full weights tensor as is

            # Create the full UID tensor (0 to max_allowed_uids - 1)
            uids_full = torch.arange(0, max_allowed_uids, dtype=torch.int64)
            bt.logging.info(f"Prepared full UID tensor (0-{max_allowed_uids - 1}) and full weights tensor (length {len(weights_full)}).")
            # --- END MODIFICATION ---

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

            # Check if the number of *potential* UIDs meets the minimum requirement
            # This check might be less relevant now, as we send the full vector,
            # but kept for safety. The subtensor itself will likely perform the check.
            if max_allowed_uids < min_allowed_weights:
                 bt.logging.warning(f"Max potential UIDs ({max_allowed_uids}) is less than min_allowed_weights ({min_allowed_weights}) for netuid {self.neuron_config.netuid}.")
                 # Continue anyway, let subtensor decide, but log warning.

            # ensure params are set and valid 
            # Check weights_full specifically
            if self.neuron_config.netuid is None or self.wallet is None or uids_full is None or weights_full is None:
                bt.logging.error("Invalid parameters for subtensor.set_weights()")
                bt.logging.error(f"Neuron config netuid: {self.neuron_config.netuid}")
                bt.logging.error(f"Wallet: {self.wallet}")
                bt.logging.error(f"Full UIDs: {uids_full}")
                bt.logging.error(f"Full Weights: {weights_full}")
                bt.logging.error(f"Version key: {__spec_version__}")
                return False
            
            # Check length consistency one last time before calling
            if len(uids_full) != len(weights_full):
                 bt.logging.error(f"Final length mismatch before set_weights call: UIDs={len(uids_full)}, Weights={len(weights_full)}")
                 return False
                 
            bt.logging.info(f"Setting weights for netuid: {self.neuron_config.netuid}")
            bt.logging.info(f"Wallet: {self.wallet}")
            bt.logging.info(f"Uids being sent ({len(uids_full)}): [0...{max_allowed_uids - 1}]") # Log range instead of full tensor
            bt.logging.info(f"Weights being sent ({len(weights_full)}): {weights_full}") # Log full tensor
            bt.logging.info(f"Version key: {__spec_version__}")
            bt.logging.info(f"Subtensor: {weights_subtensor}")

            # Log before critical operation
            bt.logging.info("Calling subtensor.set_weights() with full UID/Weight vectors")
            
            result = weights_subtensor.set_weights(
                netuid=self.neuron_config.netuid, 
                wallet=self.wallet, 
                uids=uids_full, # Use the full UID tensor
                weights=weights_full, # Use the full weights tensor
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


    