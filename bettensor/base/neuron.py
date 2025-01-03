# The MIT License (MIT)
# Copyright © 2023 Yuma Rao

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

import copy
import typing
import os
import argparse
import bittensor as bt
from os import path, makedirs
from abc import ABC, abstractmethod

# Import config functions
from bettensor.utils.config import check_config, add_args, config


class BaseNeuron:
    """
    Base class for Bittensor miners. This class is abstract and should be inherited by a subclass.
    It contains the core logic for all neurons; validators and miners.
    """

    @classmethod
    def check_config(cls, config: "bt.Config"):
        """Checks/validates the config namespace object."""
        check_config(cls, config)

    @classmethod
    def add_args(cls, parser):
        """Add neuron specific arguments to the parser."""
        add_args(cls, parser)

    @classmethod
    def config(cls):
        """Get config from the argument parser."""
        return config(cls)

    def __init__(self, config=None):
        """Initialize the neuron."""
        base_config = copy.deepcopy(config or self.config())
        self.config = self.config()
        self.config.merge(base_config)
        self.check_config(self.config)

        # Set up logging with the provided configuration
        bt.logging.set_config(config=self.config.logging)

        # Log the configuration for reference
        bt.logging.info(self.config)

        self.step = 0
        self.last_updated_block = 0
        self.base_path = f"{path.expanduser('~')}/bettensor"
