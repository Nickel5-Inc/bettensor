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
        wallet: "bt.wallet",
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
                    return None

                if "exception" in result_dict:
                    raise result_dict["exception"]

                return result_dict.get("result", None)

            return wrapper

        return decorator

    @timeout_with_multiprocess(60)
    def set_weights(self, weights: torch.Tensor):
        # Ensure weights and uids are the same length
        if len(weights) != len(self.metagraph.uids):
            bt.logging.error(f"Weights and UIDs are not the same length: {len(weights)} != {len(self.metagraph.uids)}")
            bt.logging.error(f"Weights: {len(weights)}")
            bt.logging.error(f"UIDs: {len(self.metagraph.uids)}")
            # Trim weights to the length of UIDs
            weights = weights[:len(self.metagraph.uids)]
        
        try:
            bt.logging.info("Attempting to set weights")
            result = self.subtensor.set_weights(
                netuid=self.neuron_config.netuid, 
                wallet=self.wallet, 
                uids=self.metagraph.uids, 
                weights=weights, 
                version_key=__spec_version__, 
                wait_for_inclusion=True, 
            )
        
            bt.logging.trace(f"Set weights result: {result}")

            if isinstance(result, tuple) and len(result) >= 1:
                success = result[0]
                bt.logging.trace(f"Set weights message: {success}")
                if success:
                    bt.logging.info("Successfully set weights.")
                    return True
                
            else:
                bt.logging.warning(
                    f"Unexpected result format in setting weights: {result}"
                )
        except TimeoutError:
            bt.logging.error("Timeout occurred while setting weights.")
        except Exception as e:
            bt.logging.error(f"Error setting weights: {str(e)}")
            bt.logging.error(f"Error traceback: {traceback.format_exc()}")

        bt.logging.error("Failed to set weights after all attempts.")
        return False


    