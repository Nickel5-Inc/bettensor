#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script for the ROI metrics in the Bettensor scoring system.
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone

import bittensor as bt
import numpy as np

from bettensor.validator.utils.database.database_manager import DatabaseManager
from bettensor.validator.utils.scoring.scoring import ScoringSystem

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


async def test_roi_metrics(db_path):
    """Test the ROI metrics calculation and storage."""
    logger.info(f"Testing ROI metrics using database at {db_path}")

    # Create a database manager
    db_manager = DatabaseManager(db_path)
    await db_manager.initialize()

    # Create a scoring system
    scoring_system = ScoringSystem(
        db_manager=db_manager,
        num_miners=256,
        max_days=45,
        reference_date=datetime.now(timezone.utc)
    )

    # Initialize the scoring system
    await scoring_system.initialize()

    # Check if the ROI columns exist in the miner_stats table
    check_columns_query = "PRAGMA table_info(miner_stats)"
    columns = await db_manager.fetch_all(check_columns_query)
    column_names = [col["name"] for col in columns]
    
    roi_columns = ["miner_15_day_roi", "miner_30_day_roi", "miner_45_day_roi", "miner_lifetime_roi"]
    missing_columns = [col for col in roi_columns if col not in column_names]
    
    if missing_columns:
        logger.error(f"Missing ROI columns in miner_stats table: {missing_columns}")
        return False
    else:
        logger.info("All ROI columns exist in miner_stats table")

    # Fill the ROI scores array with some sample data for testing
    current_day = scoring_system.current_day
    
    # Set some sample ROI scores for test miners
    test_miners = [42, 43, 44]  # Test with a few miners
    
    for miner in test_miners:
        for day_offset in range(45):
            day_idx = (current_day - day_offset) % scoring_system.max_days
            # Vary ROI values from -0.2 to 0.4 with some random noise
            roi_value = 0.1 * np.sin(day_offset / 7.0) + 0.1 + 0.05 * np.random.randn()
            scoring_system.roi_scores[miner, day_idx] = roi_value
    
    # Call the update_miner_roi_stats method
    await scoring_system.update_miner_roi_stats()
    
    # Verify that the ROI metrics were stored in the database
    for miner in test_miners:
        query = """
            SELECT 
                miner_15_day_roi, 
                miner_30_day_roi, 
                miner_45_day_roi, 
                miner_lifetime_roi,
                miner_current_roi
            FROM miner_stats 
            WHERE miner_uid = ?
        """
        result = await db_manager.fetch_one(query, (miner,))
        
        if result:
            logger.info(f"ROI metrics for miner {miner}:")
            logger.info(f"  15-day ROI: {result['miner_15_day_roi']:.4f}")
            logger.info(f"  30-day ROI: {result['miner_30_day_roi']:.4f}")
            logger.info(f"  45-day ROI: {result['miner_45_day_roi']:.4f}")
            logger.info(f"  Lifetime ROI: {result['miner_lifetime_roi']:.4f}")
            logger.info(f"  Current ROI: {result['miner_current_roi']:.4f}")
            
            # Check if the 15-day ROI is the same as current ROI (they should be the same)
            if abs(result['miner_15_day_roi'] - result['miner_current_roi']) > 1e-6:
                logger.warning(f"15-day ROI and current ROI differ: {result['miner_15_day_roi']} vs {result['miner_current_roi']}")
            
            # Calculate expected 15-day ROI from the scoring system's ROI array
            start_15_day = (current_day - 14) % scoring_system.max_days
            expected_roi_15_day = scoring_system._calculate_window_roi(miner, start_15_day, current_day)
            
            # Check if calculated ROI matches stored ROI
            if abs(expected_roi_15_day - result['miner_15_day_roi']) > 1e-6:
                logger.warning(f"Calculated 15-day ROI {expected_roi_15_day:.4f} differs from stored value {result['miner_15_day_roi']:.4f}")
            else:
                logger.info(f"15-day ROI calculation verified for miner {miner}")
        else:
            logger.error(f"No ROI metrics found for miner {miner}")
    
    # Check if the ROI value is used in tier requirements
    # Create a miner with negative ROI in tier 3
    test_miner = 45
    
    # Set up the miner for testing
    tier_query = "UPDATE miner_stats SET miner_current_tier = 3, miner_15_day_roi = -0.1 WHERE miner_uid = ?"
    await db_manager.execute_query(tier_query, (test_miner,))
    
    # Set fixed values in the composite scores array for this miner
    scoring_system.composite_scores[test_miner, current_day, 0] = 0.5  # Daily composite
    
    # Add this miner to valid_uids
    scoring_system.valid_uids.add(test_miner)
    
    # Set ROI scores to negative value
    scoring_system.roi_scores[test_miner, :] = -0.1
    
    # Set the tier directly
    scoring_system.tiers[test_miner, current_day] = 3
    
    # Check if the miner meets the tier requirements
    meets_requirements = scoring_system._meets_tier_requirements(test_miner, 3)
    logger.info(f"Miner {test_miner} with negative ROI meets tier 3 requirements: {meets_requirements}")
    
    # We expect this to be False since the miner has negative ROI
    if meets_requirements:
        logger.warning("ROI requirement not enforced: miner with negative ROI meets tier 3 requirements")
    else:
        logger.info("ROI requirement successfully enforced: miner with negative ROI does not meet tier 3 requirements")
    
    return True


async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Test the ROI metrics functionality")
    parser.add_argument(
        "--db-path", 
        type=str, 
        default=":memory:",
        help="Path to SQLite database file (default: in-memory)"
    )
    
    args = parser.parse_args()
    
    # Expand user path if necessary
    db_path = os.path.expanduser(args.db_path)
    
    try:
        success = await test_roi_metrics(db_path)
        if success:
            logger.info("ROI metrics test completed successfully")
        else:
            logger.error("ROI metrics test failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Error during testing: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main()) 