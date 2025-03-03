#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script for the ROI-based tier demotion functionality.
"""

import logging
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


def meets_tier_requirements(miner, tier, roi_scores, has_sufficient_history=True):
    """
    Simplified version of _meets_tier_requirements to test ROI-based demotion.
    
    Args:
        miner (int): The miner's UID
        tier (int): The tier to check requirements for
        roi_scores (float): The miner's ROI scores over last 15 days
        has_sufficient_history (bool): Whether the miner has sufficient history
        
    Returns:
        bool: True if miner meets requirements, False otherwise
    """
    if tier <= 2:  # Tier 0, 1, and 2 have no ROI requirements
        return True
        
    # Basic requirements are met
    meets_wager = True
    has_history = True
    
    # For tiers 3 and above, check ROI requirement (must be non-negative)
    roi_requirement = True
    if tier >= 3:  # Tiers 3, 4, 5, 6
        # Only apply ROI requirement if miner has sufficient ROI history
        if has_sufficient_history:
            avg_roi = np.mean(roi_scores)
            roi_requirement = avg_roi >= 0
            
            logger.info(f"ROI requirement for tier {tier-1}:")
            logger.info(f"  15-day average ROI: {avg_roi:.4f}")
            logger.info(f"  Requirement met: {roi_requirement}")
            
            if not roi_requirement:
                logger.info(f"Miner {miner} failed ROI requirement for tier {tier-1} with avg ROI {avg_roi:.4f}")
    
    meets_requirement = meets_wager and has_history and roi_requirement
    return meets_requirement


def test_roi_demotion():
    """Test the ROI-based tier demotion logic."""
    logger.info("Testing ROI-based tier demotion logic")
    
    test_cases = [
        # (miner_id, tier, ROI scores, has_sufficient_history, expected_result)
        (1, 2, np.array([-0.1, -0.2, -0.3]), True, True),  # Tier 2, negative ROI, but no ROI requirements
        (2, 3, np.array([0.1, 0.2, 0.3]), True, True),     # Tier 3, positive ROI, should meet requirements
        (3, 3, np.array([-0.1, -0.2, -0.3]), True, False), # Tier 3, negative ROI, should not meet requirements
        (4, 4, np.array([0.1, 0.0, -0.4]), True, False),   # Tier 4, mixed ROI with negative avg, should not meet requirements
        (5, 5, np.array([0.2, 0.1, -0.05]), True, True),   # Tier 5, mixed ROI but positive avg, should meet requirements
        (6, 3, np.array([-0.1, -0.2, -0.3]), False, True), # Tier 3, negative ROI, but insufficient history, should meet requirements
    ]
    
    for miner_id, tier, roi_scores, has_history, expected in test_cases:
        result = meets_tier_requirements(miner_id, tier, roi_scores, has_history)
        
        # Log info about the test case
        logger.info(f"Miner {miner_id}, Tier {tier}, ROI avg: {np.mean(roi_scores):.4f}, Sufficient history: {has_history}")
        logger.info(f"  Expected: {expected}, Actual: {result}")
        
        # Check if the result matches the expectation
        if result == expected:
            logger.info("  ✅ Test passed")
        else:
            logger.error("  ❌ Test failed")
            return False
    
    logger.info("All ROI demotion tests passed!")
    return True


if __name__ == "__main__":
    test_roi_demotion() 