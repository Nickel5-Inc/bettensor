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
            
            # Ensure weights and uids are the same length
            if len(weights) != len(self.metagraph.uids):
                bt.logging.error(f"Weights and UIDs are not the same length: {len(weights)} != {len(self.metagraph.uids)}")
                bt.logging.error(f"Weights: {len(weights)}")
                bt.logging.error(f"UIDs: {len(self.metagraph.uids)}")
                # Trim weights to the length of UIDs
                weights = weights[:len(self.metagraph.uids)]
            

            weights_subtensor = bt.subtensor(network=self.neuron_config.network)
            
            #ensure subtensor is connected
            if weights_subtensor is None:
                bt.logging.error("Subtensor is not connected, attempting to reconnect...")
                weights_subtensor = bt.subtensor(network=self.neuron_config.network)

            # ensure params are set and valid 
            if self.neuron_config.netuid is None or self.wallet is None or self.metagraph.uids is None or weights is None:
                bt.logging.error("Invalid parameters for subtensor.set_weights()")
                bt.logging.error(f"Neuron config netuid: {self.neuron_config.netuid}")
                bt.logging.error(f"Wallet: {self.wallet}")
                bt.logging.error(f"Metagraph uids: {self.metagraph.uids}")
                bt.logging.error(f"Weights: {weights}")
                bt.logging.error(f"Version key: {__spec_version__}")
                return False
            
            bt.logging.info(f"Setting weights for netuid: {self.neuron_config.netuid}")
            bt.logging.info(f"Wallet: {self.wallet}")
            bt.logging.info(f"Uids: {self.metagraph.uids}")
            bt.logging.info(f"Weights: {weights}")
            bt.logging.info(f"Version key: {__spec_version__}")
            bt.logging.info(f"Subtensor: {weights_subtensor}")

            # Log before critical operation
            bt.logging.info("Calling subtensor.set_weights()")
            
            result = weights_subtensor.set_weights(
                netuid=self.neuron_config.netuid, 
                wallet=self.wallet, 
                uids=self.metagraph.uids, 
                weights=weights, 
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


    