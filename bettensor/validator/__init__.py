"""Bettensor Validator Module.

This module provides the validator functionality for the Bettensor network.
"""

from bettensor.validator.bettensor_validator import BettensorValidator
from bettensor.validator.core.event_driven_validator import EventDrivenValidator

__all__ = [
    'BettensorValidator',
    'EventDrivenValidator'
]
