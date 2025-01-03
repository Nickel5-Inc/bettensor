# The MIT License (MIT)
# Copyright © 2023 Yuma Rao
# Copyright © 2023 Opentensor Foundation

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the “Software”), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.

# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import os
import torch
import argparse
import bittensor as bt
from loguru import logger


def check_config(cls, config: "bt.Config"):
    """Checks/validates the config namespace object."""
    bt.logging.check_config(config)

    # Create full path for logging
    full_path = os.path.expanduser(
        "{}/{}/{}/netuid{}/{}".format(
            config.logging.logging_dir,
            config.wallet.name,
            config.wallet.hotkey,
            config.netuid,
            config.neuron.name,
        )
    )
    config.neuron.full_path = os.path.expanduser(full_path)
    if not os.path.exists(config.neuron.full_path):
        os.makedirs(config.neuron.full_path, exist_ok=True)


def add_args(cls, parser: argparse.ArgumentParser):
    """Add neuron specific arguments to the parser."""
    # Core neuron arguments
    parser.add_argument('--neuron.name', type=str, help='Trials for this neuron go in neuron.root/neuron.name', default='core_validator')
    parser.add_argument('--neuron.blocks_per_epoch', type=int, help='Blocks until the miner sets weights on chain', default=100)
    parser.add_argument('--neuron.no_set_weights', action='store_true', help='If True, the miner does not set weights', default=False)
    parser.add_argument('--neuron.max_batch_size', type=int, help='The maximum batch size for forward requests', default=1)
    parser.add_argument('--neuron.max_sequence_len', type=int, help='The maximum sequence length for forward requests', default=256)
    
    # Device and network arguments
    parser.add_argument('--neuron.device', type=str, help='Device to run on.', default="cuda" if torch.cuda.is_available() else "cpu")
    parser.add_argument('--neuron.epoch_length', type=int, help='The default epoch length (how often we set weights, measured in 12 second blocks).', default=100)
    parser.add_argument('--mock', action='store_true', help='Mock neuron and all network components.', default=False)
    
    # Blacklist arguments
    parser.add_argument('--neuron.blacklist.force_validator_permit', action='store_true', help='If True, force validator permit check', default=False)
    parser.add_argument('--neuron.blacklist.allow_non_registered', action='store_true', help='If True, allow non-registered users to mine', default=False)
    parser.add_argument('--neuron.blacklist.minimum_stake_requirement', type=float, help='Minimum stake requirement to avoid blacklist', default=0.0)
    parser.add_argument('--neuron.blacklist.minimum_hotkeys_per_wallet', type=int, help='Minimum number of hotkeys per wallet to avoid blacklist', default=0)
    parser.add_argument('--neuron.blacklist.minimum_registrations_per_wallet', type=int, help='Minimum number of registrations per wallet to avoid blacklist', default=0)
    parser.add_argument('--neuron.blacklist.minimum_subnet_stake_requirement', type=float, help='Minimum subnet stake requirement to avoid blacklist', default=0.0)
    
    # Other neuron settings
    parser.add_argument('--neuron.default_priority', type=float, help='Default priority for requests', default=0.0)
    parser.add_argument('--neuron.events_retention_size', type=str, help='Events retention size', default="2 GB")
    parser.add_argument('--neuron.dont_save_events', action='store_true', help='If True, dont save events', default=False)
    parser.add_argument('--netuid', type=int, help='Subnet netuid', default=1)


def add_validator_args(cls, parser: argparse.ArgumentParser):
    """Add validator specific arguments to the parser."""
    # Core validator arguments
    parser.add_argument(
        "--alpha",
        type=float,
        default=0.9,
        help="The alpha value for the validator."
    )
    parser.add_argument(
        "--db",
        type=str,
        default="./bettensor/validator/state/validator.db",
        help="Path to the validator database"
    )
    parser.add_argument(
        "--max_targets",
        type=int,
        default=256,
        help="Sets the value for the number of targets to query - set to 256 to ensure all miners are queried, it is now batched"
    )
    parser.add_argument(
        "--load_state",
        type=str,
        default="True",
        help="WARNING: Setting this value to False clears the old state."
    )
    
    # Additional validator settings
    parser.add_argument(
        "--neuron.timeout",
        type=float,
        help="The timeout for each forward call in seconds.",
        default=10,
    )
    parser.add_argument(
        "--neuron.num_concurrent_forwards",
        type=int,
        help="The number of concurrent forwards running at any time.",
        default=1,
    )
    parser.add_argument(
        "--neuron.sample_size",
        type=int,
        help="The number of miners to query in a single step.",
        default=50,
    )
    parser.add_argument(
        "--neuron.disable_set_weights",
        action="store_true",
        help="Disables setting weights.",
        default=False,
    )
    parser.add_argument(
        "--neuron.moving_average_alpha",
        type=float,
        help="Moving average alpha parameter, how much to add of the new observation.",
        default=0.1,
    )
    parser.add_argument(
        "--neuron.axon_off",
        "--axon_off",
        action="store_true",
        help="Set this flag to not attempt to serve an Axon.",
        default=False,
    )
    parser.add_argument(
        "--neuron.vpermit_tao_limit",
        type=int,
        help="The maximum number of TAO allowed to query a validator with a vpermit.",
        default=4096,
    )


def config(cls):
    """Get config from the argument parser."""
    parser = argparse.ArgumentParser()
    bt.wallet.add_args(parser)
    bt.subtensor.add_args(parser)
    bt.logging.add_args(parser)
    bt.axon.add_args(parser)
    cls.add_args(parser)
    return bt.config(parser)
