#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Tests for the enhanced vesting adjustment method with explicit weight redistribution.
"""

# Ensure we can import the bettensor module
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import asyncio
import logging
import numpy as np
import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock, AsyncMock

import bittensor as bt

from tests.validator.test_vesting_system import (
    DatabaseManager, StakeTracker, StakeRequirements,
    MockMetagraph, MockSubtensorClient, setup_test_db
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VestingSystem:
    """
    Mock implementation of the VestingSystem for testing enhanced redistribution.
    """
    
    def __init__(self, db_manager, metagraph, subtensor_client, minimum_stake=0.3, retention_window_days=30, retention_target=0.9):
        """Initialize the mock vesting system."""
        self.db_manager = db_manager
        self.metagraph = metagraph
        self.subtensor_client = subtensor_client
        self.minimum_stake = minimum_stake
        self.retention_window_days = retention_window_days
        self.retention_target = retention_target
        
    async def initialize(self):
        """Initialize the vesting system."""
        # Mock implementation
        pass
        
    async def apply_vesting_adjustments(self, weights):
        """
        Standard application of vesting adjustments.
        
        Args:
            weights: Dictionary of miner weights
            
        Returns:
            Dictionary of adjusted weights
        """
        # Mock implementation - reduces all weights by 20%
        adjusted = weights.copy()
        for uid in adjusted:
            adjusted[uid] = adjusted[uid] * 0.8
            
        # Normalize to sum to 1.0
        total = sum(adjusted.values())
        if total > 0:
            for uid in adjusted:
                adjusted[uid] = adjusted[uid] / total
                
        return adjusted
        
    async def apply_enhanced_vesting_adjustments(self, weights, threshold_factor=0.5, absolute_threshold=0.01):
        """
        Enhanced version of apply_vesting_adjustments with explicit redistribution.
        
        Args:
            weights: Dictionary of miner weights
            threshold_factor: Relative threshold as a factor of original weight
            absolute_threshold: Minimum absolute weight to maintain
            
        Returns:
            Dictionary of adjusted weights with explicit redistribution
        """
        # First apply standard vesting adjustments
        adjusted_weights = await self.apply_vesting_adjustments(weights)
        
        # Make a copy of original weights for comparison
        original_weights = weights.copy()
        
        # Initialize weight to redistribute
        weight_to_redistribute = 0.0
        
        # Track miners below threshold
        miners_below_threshold = []
        
        # Identify miners below threshold
        for uid, weight in adjusted_weights.items():
            # Skip miners that were already zeroed out
            if weight <= 0:
                continue
                
            # Calculate relative threshold based on original weight
            original_weight = original_weights.get(uid, 0)
            relative_threshold = original_weight * threshold_factor
            
            # Use maximum of relative and absolute threshold
            effective_threshold = max(relative_threshold, absolute_threshold)
            
            # Check if miner falls below threshold
            if weight < effective_threshold:
                # Add to redistribution pool
                weight_to_redistribute += weight
                miners_below_threshold.append(uid)
                
                # Zero out the weight
                adjusted_weights[uid] = 0.0
        
        # Skip redistribution if no weight to redistribute
        if weight_to_redistribute <= 0.0:
            return adjusted_weights
            
        # Find miners eligible for redistribution (weights > 0)
        eligible_miners = [uid for uid, weight in adjusted_weights.items() if weight > 0]
        
        if not eligible_miners:
            # Safety check - don't zero out all miners
            # If all miners would fall below threshold, revert to standard adjustments
            return await self.apply_vesting_adjustments(weights)
            
        # Calculate total weight of eligible miners before redistribution
        eligible_weight_sum = sum(adjusted_weights[uid] for uid in eligible_miners)
        
        # Redistribute proportionally based on current adjusted weights
        for uid in eligible_miners:
            # Calculate proportion of this miner in the remaining pool
            proportion = adjusted_weights[uid] / eligible_weight_sum
            
            # Calculate additional weight to add
            additional_weight = weight_to_redistribute * proportion
            
            # Apply additional weight
            adjusted_weights[uid] += additional_weight
            
        # Verify weights still sum to 1.0 (accounting for floating-point error)
        weight_sum = sum(adjusted_weights.values())
        if abs(weight_sum - 1.0) > 1e-6 and weight_sum > 0:
            # Renormalize
            for uid in adjusted_weights:
                adjusted_weights[uid] = adjusted_weights[uid] / weight_sum
            
        return adjusted_weights


@pytest.fixture
def setup_test_db():
    """Set up a mock database for testing."""
    db_manager = AsyncMock()
    metagraph = MagicMock()
    subtensor_client = MagicMock()
    
    # Configure metagraph
    metagraph.hotkeys = ["hotkey1", "hotkey2", "hotkey3", "hotkey4", "hotkey5"]
    
    vesting_system = VestingSystem(db_manager, metagraph, subtensor_client)
    
    return vesting_system


class TestEnhancedRedistribution:
    """Test class for the enhanced redistribution approach."""
    
    @pytest.mark.asyncio
    async def test_threshold_based_redistribution(self, setup_test_db):
        """Test that miners below the relative threshold are zeroed out and their weight is redistributed."""
        vesting_system = setup_test_db
        
        # Initialize weights - 5 miners with equal weights
        weights = {0: 0.2, 1: 0.2, 2: 0.2, 3: 0.2, 4: 0.2}
        
        # Mock the standard vesting adjustments to reduce specific weights
        async def mock_adjustments(w):
            adjusted = w.copy()
            # Miner 0 - reduced by 60% (below threshold)
            adjusted[0] = w[0] * 0.4
            # Miner 1 - reduced by 30% (above threshold)
            adjusted[1] = w[1] * 0.7
            # Miner 2 - reduced by 20% (above threshold)
            adjusted[2] = w[2] * 0.8
            # Miner 3 - reduced by 10% (above threshold)
            adjusted[3] = w[3] * 0.9
            # Miner 4 - no reduction (above threshold)
            adjusted[4] = w[4] * 1.0
            
            # Normalize
            total = sum(adjusted.values())
            for uid in adjusted:
                adjusted[uid] = adjusted[uid] / total
                
            return adjusted
            
        # Replace the standard adjustment method
        vesting_system.apply_vesting_adjustments = mock_adjustments
        
        # Apply enhanced adjustments with threshold factor of 0.5 (50% of original)
        adjusted = await vesting_system.apply_enhanced_vesting_adjustments(
            weights, threshold_factor=0.5, absolute_threshold=0.01
        )
        
        # Verify miners below threshold are zeroed out
        assert adjusted[0] == 0.0, "Miner 0 should be zeroed out (below 50% threshold)"
        
        # Verify miners above threshold are not zeroed out
        assert adjusted[1] > 0.0, "Miner 1 should not be zeroed out"
        assert adjusted[2] > 0.0, "Miner 2 should not be zeroed out"
        assert adjusted[3] > 0.0, "Miner 3 should not be zeroed out"
        assert adjusted[4] > 0.0, "Miner 4 should not be zeroed out"
        
        # Verify the total weight is still 1.0
        assert abs(sum(adjusted.values()) - 1.0) < 1e-6, "Total weight should be 1.0"
        
    @pytest.mark.asyncio
    async def test_absolute_threshold_redistribution(self, setup_test_db):
        """Test that miners below the absolute threshold are zeroed out and their weight is redistributed."""
        vesting_system = setup_test_db
        
        # Initialize weights - unequal weights
        weights = {0: 0.05, 1: 0.1, 2: 0.15, 3: 0.3, 4: 0.4}
        
        # Mock the standard vesting adjustments to reduce specific weights
        async def mock_adjustments(w):
            adjusted = w.copy()
            # Reduce all by different amounts
            adjusted[0] = w[0] * 0.4  # 0.02 - below absolute threshold
            adjusted[1] = w[1] * 0.5  # 0.05 - below absolute threshold
            adjusted[2] = w[2] * 0.6  # 0.09 - above absolute threshold
            adjusted[3] = w[3] * 0.7  # 0.21 - above absolute threshold
            adjusted[4] = w[4] * 0.8  # 0.32 - above absolute threshold
            
            # Normalize
            total = sum(adjusted.values())
            for uid in adjusted:
                adjusted[uid] = adjusted[uid] / total
                
            return adjusted
            
        # Replace the standard adjustment method
        vesting_system.apply_vesting_adjustments = mock_adjustments
        
        # Apply enhanced adjustments with absolute threshold of 0.05
        adjusted = await vesting_system.apply_enhanced_vesting_adjustments(
            weights, threshold_factor=0.5, absolute_threshold=0.05
        )
        
        # Verify miners below absolute threshold are zeroed out
        assert adjusted[0] == 0.0, "Miner 0 should be zeroed out (below absolute threshold)"
        
        # Verify miners above absolute threshold are not zeroed out
        assert adjusted[2] > 0.0, "Miner 2 should not be zeroed out"
        assert adjusted[3] > 0.0, "Miner 3 should not be zeroed out"
        assert adjusted[4] > 0.0, "Miner 4 should not be zeroed out"
        
        # Verify the total weight is still 1.0
        assert abs(sum(adjusted.values()) - 1.0) < 1e-6, "Total weight should be 1.0"
        
    @pytest.mark.asyncio
    async def test_no_redistribution_needed(self, setup_test_db):
        """Test case where no miners fall below the threshold."""
        vesting_system = setup_test_db
        
        # Initialize weights
        weights = {0: 0.2, 1: 0.2, 2: 0.2, 3: 0.2, 4: 0.2}
        
        # Mock apply_vesting_adjustments to keep all miners above threshold
        async def no_reduction(weights):
            """Mock that doesn't reduce any weights significantly."""
            adjusted = weights.copy()
            # All miners stay above 50% of original
            for uid in adjusted:
                adjusted[uid] = adjusted[uid] * 0.7  # 70% of original
            return adjusted
            
        vesting_system.apply_vesting_adjustments = no_reduction
        
        # Apply enhanced adjustments
        adjusted = await vesting_system.apply_enhanced_vesting_adjustments(
            weights, threshold_factor=0.5, absolute_threshold=0.01
        )
        
        # Verify no miners are zeroed out
        for uid in adjusted:
            assert adjusted[uid] > 0.0, f"Miner {uid} should not be zeroed out"
        
        # Verify the total weight is still 1.0
        assert abs(sum(adjusted.values()) - 1.0) < 1e-6, "Total weight should be 1.0"
        
    @pytest.mark.asyncio
    async def test_all_validators_below_threshold(self, setup_test_db):
        """Test safety mechanism when all miners would fall below threshold."""
        vesting_system = setup_test_db
        
        # Initialize weights
        weights = {0: 0.2, 1: 0.2, 2: 0.2, 3: 0.2, 4: 0.2}
        
        # Mock that reduces all weights below threshold
        async def reduce_all(weights):
            """Mock that reduces all weights below threshold."""
            adjusted = weights.copy()
            # All miners reduced to 40% (below 50% threshold)
            for uid in adjusted:
                adjusted[uid] = adjusted[uid] * 0.4
            return adjusted
            
        vesting_system.apply_vesting_adjustments = reduce_all
        
        # Apply enhanced adjustments
        adjusted = await vesting_system.apply_enhanced_vesting_adjustments(
            weights, threshold_factor=0.5, absolute_threshold=0.01
        )
        
        # Verify no miners are zeroed out (safety mechanism)
        for uid in adjusted:
            assert adjusted[uid] > 0.0, f"Miner {uid} should not be zeroed out when all are below threshold"
        
        # Verify the total weight is still 1.0
        assert abs(sum(adjusted.values()) - 1.0) < 1e-6, "Total weight should be 1.0"


# Run tests if executed as script
if __name__ == "__main__":
    pytest.main(["-xvs", __file__]) 