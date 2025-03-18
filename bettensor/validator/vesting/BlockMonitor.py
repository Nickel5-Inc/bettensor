import time
import logging
import json
from substrateinterface import SubstrateInterface

class BlockMonitor:
    def __init__(self, substrate, logger, verbose=False):
        self.substrate = substrate
        self.logger = logger
        self.verbose = verbose  # If True, print full raw extrinsic and event data.
        self.should_exit = False  # Flag to stop monitoring
        # Dictionary to store pending stake extrinsic calls.
        # Keys: unique call IDs; Values: dict with call details.
        self.pending_calls = {}
        self.call_counter = 0  # Unique counter for pending calls

    def _get_block_hash(self, block_num):
        """Retrieve the block hash for a given block number."""
        try:
            block_hash = self.substrate.get_block_hash(block_num)
            return block_hash
        except Exception as e:
            self.logger.error(f"Error getting block hash for block {block_num}: {e}")
            return None

    def _get_extrinsics(self, block_hash):
        """Retrieve all extrinsics for a given block hash."""
        try:
            block = self.substrate.get_block(block_hash=block_hash)
            return block.get('extrinsics', [])
        except Exception as e:
            self.logger.error(f"Error getting extrinsics for block {block_hash}: {e}")
            return []

    def _process_extrinsics(self, block_num, extrinsics):
        """Process extrinsics in a block to record pending stake-related calls."""
        for ext in extrinsics:
            try:
                # Use ext.value if available; otherwise assume ext is a dict.
                ext_dict = ext.value if hasattr(ext, 'value') else ext

                if self.verbose:
                    self.logger.info("Raw extrinsic data: %s", json.dumps(ext_dict, indent=2, default=str))
                
                # Extract the coldkey from the extrinsic's "address" field.
                coldkey = ext_dict.get('address')
                
                call_info = ext_dict.get('call', {})
                call_module = call_info.get('call_module')
                call_function = call_info.get('call_function')
                # Only process stake-related calls from SubtensorModule.
                if call_module == 'SubtensorModule' and call_function in [
                    'add_stake', 'remove_stake', 'add_stake_limit', 'remove_stake_limit'
                ]:
                    params = call_info.get('call_args', [])
                    hotkey = None
                    call_amount = None  # The call amount as submitted in the extrinsic.
                    netuid_param = None
                    for param in params:
                        pname = param.get('name')
                        if pname == 'hotkey':
                            hotkey = param.get('value')
                        elif pname in ['amount', 'amount_staked', 'amount_unstaked']:
                            try:
                                call_amount = float(param.get('value', 0)) / 1e9
                            except Exception as conv_e:
                                self.logger.error(f"Error converting call amount from {param.get('value')}: {conv_e}")
                        elif pname == 'netuid':
                            try:
                                netuid_param = int(param.get('value', -1))
                            except Exception as conv_e:
                                self.logger.error(f"Error converting netuid: {conv_e}")
                    if call_amount is None:
                        self.logger.info(f"Call args for {call_function}: {json.dumps(params, indent=2, default=str)}")
                    
                    call_id = self.call_counter
                    self.call_counter += 1
                    self.pending_calls[call_id] = {
                        'block_num': block_num,
                        'call_function': call_function,
                        'coldkey': coldkey,      # The extrinsic signer (coldkey)
                        'hotkey': hotkey,
                        'netuid': netuid_param,
                        'call_amount': call_amount,  # The amount as submitted in the extrinsic.
                        'final_amount': None,        # To be updated based on event.
                        'validated': False,
                        'raw': call_info            # Full raw call info for debugging.
                    }
                    self.logger.info(
                        f"Recorded pending call {call_id} in block {block_num}: {call_function} - "
                        f"Coldkey: {coldkey}, Hotkey: {hotkey}, Call Amount: {call_amount if call_amount is not None else 'None'}, "
                        f"Netuid: {netuid_param}"
                    )
            except Exception as e:
                self.logger.error(f"Error processing extrinsic in block {block_num}: {e}")

    def _process_events(self, block_num, events_decoded):
        """Process events in a block and attempt to match them with pending extrinsic calls."""
        try:
            for idx, event_record in enumerate(events_decoded.value):
                try:
                    if self.verbose:
                        self.logger.info("Raw event record %s: %s", idx, str(event_record))
                    
                    # Extract event information - handle both dict and object-style access
                    if hasattr(event_record, 'get'):  # Dictionary-like object
                        event = event_record.get('event', {})
                        if hasattr(event, 'get'):
                            module_id = event.get('module_id')
                            event_id = event.get('event_id')
                            attributes = event.get('attributes')
                        else:
                            # Handle case where event is an object
                            module_id = event.module_id if hasattr(event, 'module_id') else None
                            event_id = event.event_id if hasattr(event, 'event_id') else None
                            attributes = event.attributes if hasattr(event, 'attributes') else None
                    else:  # Direct attribute access for scale_info objects
                        # Handle the case where event_record is an object with direct attribute access
                        if hasattr(event_record, 'event'):
                            event = event_record.event
                            if hasattr(event, 'module_id'):
                                module_id = event.module_id
                            elif hasattr(event, 'get') and event.get('module_id'):
                                module_id = event.get('module_id')
                            else:
                                module_id = None
                                
                            if hasattr(event, 'event_id'):
                                event_id = event.event_id
                            elif hasattr(event, 'get') and event.get('event_id'):
                                event_id = event.get('event_id')
                            else:
                                event_id = None
                                
                            if hasattr(event, 'attributes'):
                                attributes = event.attributes
                            elif hasattr(event, 'get') and event.get('attributes'):
                                attributes = event.get('attributes')
                            else:
                                attributes = None
                        else:
                            # Try to get attributes directly from event_record
                            module_id = event_record.module_id if hasattr(event_record, 'module_id') else None
                            event_id = event_record.event_id if hasattr(event_record, 'event_id') else None
                            attributes = event_record.attributes if hasattr(event_record, 'attributes') else None
                    
                    # Log what we extracted for debugging
                    self.logger.debug(f"Extracted from event: module_id={module_id}, event_id={event_id}, attributes type={type(attributes)}")
                    
                    # We care about stake events from SubtensorModule.
                    if module_id == 'SubtensorModule' and event_id in ['StakeAdded', 'StakeRemoved']:
                        event_hotkey = None
                        event_netuid = None
                        event_amount = None
                        
                        # Handle different attribute formats
                        if isinstance(attributes, dict):
                            # Try dictionary access
                            event_hotkey = attributes.get('hotkey')
                            event_amount = attributes.get('amount')
                            try:
                                if event_amount is not None:
                                    event_amount = float(event_amount) / 1e9
                            except:
                                self.logger.error(f"Error converting amount from dict: {event_amount}")
                            event_netuid = attributes.get('netuid')
                        elif isinstance(attributes, (list, tuple)) and len(attributes) >= 3:
                            # Assume format: [caller, hotkey, amount, new_total, netuid]
                            try:
                                event_hotkey = str(attributes[1]) if len(attributes) > 1 else None
                                try:
                                    event_amount = float(attributes[2]) / 1e9 if len(attributes) > 2 else None
                                except Exception as conv_e:
                                    self.logger.error(f"Error converting event amount: {conv_e}")
                                try:
                                    event_netuid = int(attributes[4]) if len(attributes) > 4 else None
                                except Exception as conv_e:
                                    self.logger.error(f"Error converting event netuid: {conv_e}")
                            except Exception as e:
                                self.logger.error(f"Error accessing attributes list/tuple: {e}")
                        else:
                            # Try to handle custom attribute objects
                            try:
                                # If attributes has a value attribute, use that
                                if hasattr(attributes, 'value'):
                                    attr_values = attributes.value
                                # Otherwise try to convert to a list if iterable
                                elif hasattr(attributes, '__iter__'):
                                    attr_values = list(attributes)
                                else:
                                    attr_values = []
                                    self.logger.warning(f"Cannot extract values from attributes: {type(attributes)}")
                                
                                # Process the values
                                if len(attr_values) > 1:
                                    event_hotkey = str(attr_values[1])
                                if len(attr_values) > 2:
                                    try:
                                        event_amount = float(attr_values[2]) / 1e9
                                    except:
                                        pass
                                if len(attr_values) > 4:
                                    try:
                                        event_netuid = int(attr_values[4])
                                    except:
                                        pass
                            except Exception as e:
                                self.logger.error(f"Error handling custom attributes object: {e}")
                                self.logger.info(f"Event {idx} attributes not in expected format: {attributes}")
                        
                        self.logger.info(
                            f"Event {idx} in block {block_num}: {event_id} - Hotkey: {event_hotkey}, "
                            f"Event Amount: {event_amount if event_amount is not None else 'None'}, Netuid: {event_netuid}"
                        )
                        
                        # Mapping: for add_stake & add_stake_limit, expect StakeAdded; for remove_stake & remove_stake_limit, expect StakeRemoved.
                        mapping = {
                            'add_stake': 'StakeAdded',
                            'add_stake_limit': 'StakeAdded',
                            'remove_stake': 'StakeRemoved',
                            'remove_stake_limit': 'StakeRemoved'
                        }
                        for call_id, call_data in list(self.pending_calls.items()):
                            expected_event_id = mapping.get(call_data['call_function'])
                            if expected_event_id != event_id:
                                continue
                            if call_data['hotkey'] != event_hotkey or call_data['netuid'] != event_netuid:
                                continue
                            # For stake orders, record final amount from the event; for unstake orders, use the call amount.
                            if call_data['call_function'] in ['add_stake', 'add_stake_limit']:
                                final_amount = event_amount
                            else:
                                final_amount = call_data['call_amount']
                            call_data['final_amount'] = final_amount
                            fee = None
                            if call_data['call_function'] in ['add_stake', 'add_stake_limit'] and call_data['call_amount'] is not None and event_amount is not None:
                                fee = call_data['call_amount'] - event_amount
                            self.logger.info(
                                f"Validated pending call {call_id} (from block {call_data['block_num']}) with event "
                                f"{event_id} in block {block_num}: Coldkey: {call_data['coldkey']}, Hotkey: {event_hotkey}, "
                                f"Final Amount: {final_amount:.9f}"
                                f"{', Fee: ' + str(fee) if fee is not None else ''}, Netuid: {event_netuid}"
                            )
                            del self.pending_calls[call_id]
                except Exception as e:
                    self.logger.error(f"Error processing event {idx} in block {block_num}: {e}")
                    import traceback
                    self.logger.error(traceback.format_exc())
        except Exception as e:
            self.logger.error(f"Error processing events for block {block_num}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())

    def _purge_old_calls(self, current_block):
        """Purge pending extrinsic calls older than a threshold.
           Standard stake calls are purged after 20 blocks.
           Limit orders (add_stake_limit, remove_stake_limit) are retained for 216000 blocks (~1 month).
        """
        to_delete = []
        for call_id, call_data in self.pending_calls.items():
            if call_data['call_function'] in ['add_stake_limit', 'remove_stake_limit']:
                threshold = 216000
            else:
                threshold = 20
            if current_block - call_data['block_num'] >= threshold:
                self.logger.info(f"Discarding pending call {call_id} from block {call_data['block_num']} (older than {threshold} blocks)")
                to_delete.append(call_id)
        for call_id in to_delete:
            del self.pending_calls[call_id]

    def block_header_handler(self, block_header, update_nr, subscription_id):
        """Callback for new block headers. Processes extrinsics and events and purges old pending calls."""
        self.logger.info("Block header received: %s", json.dumps(block_header, indent=2, default=str))
        try:
            block_num_val = block_header.get("header", {}).get("number")
            block_num = int(block_num_val, 16) if isinstance(block_num_val, str) else block_num_val
            self.logger.info("Extracted block number: %s", block_num)
            block_hash = self.substrate.get_block_hash(block_num)
            if not block_hash:
                self.logger.error("Could not compute block hash for block number %s", block_num)
                return
            self.logger.info("Computed block hash for block %s: %s", block_num, block_hash)
        except Exception as e:
            self.logger.error("Error computing block hash from header: %s", e)
            return

        extrinsics = self._get_extrinsics(block_hash)
        self._process_extrinsics(block_num, extrinsics)

        try:
            events_decoded = self.substrate.query("System", "Events", block_hash=block_hash)
            self.logger.info("Processing events for block %s", block_num)
            self._process_events(block_num, events_decoded)
        except Exception as e:
            self.logger.error(f"Error querying events for block {block_hash}: {e}")

        self._purge_old_calls(block_num)

    def start_monitoring(self):
        self.logger.info(f"Starting block monitoring at {self.substrate.url}")
        subscription = self.substrate.subscribe_block_headers(self.block_header_handler)
        try:
            while not self.should_exit:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt detected, stopping monitoring.")
            self.should_exit = True
        subscription.unsubscribe()
        self.logger.info("Block monitoring stopped")

# Example usage:
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("BlockMonitor")
    
    substrate = SubstrateInterface(
        url="wss://entrypoint-finney.opentensor.ai:443"
    )
    # Set verbose=True to print full raw extrinsic and event data.
    monitor = BlockMonitor(substrate, logger, verbose=False)
    monitor.start_monitoring()
