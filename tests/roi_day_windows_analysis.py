#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Impact analysis for ROI-based tier demotion across day windows only.
This script analyzes existing miner data to estimate how many miners would be
demoted from tiers 3+ if they have negative ROI in any of these timeframes:
- 15-day ROI
- 30-day ROI
- 45-day ROI
Lifetime ROI is excluded from this analysis.
"""

import argparse
import logging
import os
import sys
import sqlite3
import numpy as np
from datetime import datetime, timedelta, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


def analyze_day_windows_roi_impact(db_path, print_miners=False):
    """
    Analyze the impact of demoting miners if they have negative ROI in any day window.
    
    Args:
        db_path (str): Path to the SQLite database
        print_miners (bool): Whether to print individual miner information
        
    Returns:
        dict: Statistics about how many miners would be demoted
    """
    logger.info(f"Analyzing day windows ROI impact using database at {db_path}")
    
    conn = None
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        
        # Check if scores table exists
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='scores'")
        has_scores_table = cursor.fetchone() is not None
        
        if not has_scores_table:
            logger.error("Scores table not found in the database.")
            return None
        
        # Get the current date
        current_date = datetime.now(timezone.utc)
        
        # Get all active miners from miner_stats
        cursor.execute("""
            SELECT 
                miner_uid, 
                miner_current_tier,
                miner_current_roi
            FROM miner_stats
            WHERE miner_current_tier >= 3
            ORDER BY miner_current_tier DESC, miner_uid
        """)
        
        miners = cursor.fetchall()
        logger.info(f"Found {len(miners)} miners in tiers 3+")
        
        if not miners:
            logger.warning("No miners found in tiers 3 or above.")
            return None
        
        # Find the most recent ROI scores for each miner
        cursor.execute("""
            SELECT 
                s.miner_uid,
                s.day_id,
                s.roi_score
            FROM scores s
            WHERE s.score_type = 'daily'
            ORDER BY s.day_id DESC
        """)
        
        scores = cursor.fetchall()
        
        if not scores:
            logger.warning("No scores found.")
            return None
        
        # Convert to numpy array for easier processing
        scores_array = np.array(scores, dtype=np.float64)
        
        # Get unique days sorted in descending order (most recent first)
        unique_days = np.unique(scores_array[:, 1])
        days = np.sort(unique_days)[::-1]  # Sort descending
        
        # Calculate ROI for each window
        miner_roi_windows = {}
        unique_miners = np.unique(scores_array[:, 0]).astype(int)
        
        for miner_uid in unique_miners:
            # Get scores for this miner
            miner_mask = scores_array[:, 0] == miner_uid
            miner_scores = scores_array[miner_mask]
            
            # Sort by day_id descending
            day_sorted_indices = np.argsort(miner_scores[:, 1])[::-1]
            sorted_miner_scores = miner_scores[day_sorted_indices]
            
            # Calculate ROI for each window
            windows = {}
            
            # 15-day ROI
            if len(sorted_miner_scores) >= 5:  # Only if we have at least 5 days of data
                roi_15_days = sorted_miner_scores[:min(15, len(sorted_miner_scores)), 2]
                windows['15_day'] = {
                    'roi': np.mean(roi_15_days),
                    'days': len(roi_15_days)
                }
            
            # 30-day ROI
            if len(sorted_miner_scores) >= 5:
                roi_30_days = sorted_miner_scores[:min(30, len(sorted_miner_scores)), 2]
                windows['30_day'] = {
                    'roi': np.mean(roi_30_days),
                    'days': len(roi_30_days)
                }
            
            # 45-day ROI
            if len(sorted_miner_scores) >= 5:
                roi_45_days = sorted_miner_scores[:min(45, len(sorted_miner_scores)), 2]
                windows['45_day'] = {
                    'roi': np.mean(roi_45_days),
                    'days': len(roi_45_days)
                }
            
            miner_roi_windows[miner_uid] = windows
        
        # Track demotions by tier
        tier_counts = {3: 0, 4: 0, 5: 0, 6: 0}
        tier_demotions = {3: 0, 4: 0, 5: 0, 6: 0}
        
        # For each reason separately
        demotion_reasons = {
            '15_day': 0,
            '30_day': 0,
            '45_day': 0,
            'multiple': 0
        }
        
        # Analyze each miner
        miners_to_demote = []
        
        for miner_uid, tier, lifetime_roi in miners:
            tier = int(tier)
            miner_uid = int(miner_uid)
            tier_counts[tier] = tier_counts.get(tier, 0) + 1
            
            # Check if miner would be demoted
            if miner_uid in miner_roi_windows:
                windows = miner_roi_windows[miner_uid]
                
                # We'll demote if ANY window has negative ROI
                negative_windows = []
                
                for window, data in windows.items():
                    roi = data['roi']
                    if roi is not None and roi < 0:
                        negative_windows.append(window)
                
                if negative_windows:
                    reason = 'multiple' if len(negative_windows) > 1 else negative_windows[0]
                    demotion_reasons[reason] += 1
                    tier_demotions[tier] = tier_demotions.get(tier, 0) + 1
                    
                    roi_values = {w: data['roi'] for w, data in windows.items() if data['roi'] is not None}
                    miners_to_demote.append((miner_uid, tier, roi_values, negative_windows))
        
        # Print detailed information about miners that would be demoted
        if print_miners and miners_to_demote:
            logger.info("\nMiners to demote (having negative ROI in at least one day window):")
            for miner_uid, tier, roi_values, negative_windows in sorted(miners_to_demote, key=lambda x: x[0]):
                logger.info(f"Miner {miner_uid} (Tier {tier}):")
                
                for window, roi in roi_values.items():
                    negative = window in negative_windows
                    marker = "❌" if negative else "✅"
                    logger.info(f"  {marker} {window.replace('_', '-')} ROI: {roi:.4f}")
        
        # Calculate percentage of miners affected
        tier_percentages = {}
        for tier in tier_counts.keys():
            if tier_counts[tier] > 0:
                percentage = (tier_demotions[tier] / tier_counts[tier]) * 100
                tier_percentages[tier] = percentage
            else:
                tier_percentages[tier] = 0
        
        # Print summary statistics
        logger.info("\nImpact Analysis Summary:")
        logger.info("------------------------")
        logger.info("Tier | Total Miners | Demotions | Percentage")
        logger.info("-----------------------------------------")
        for tier in sorted(tier_counts.keys()):
            logger.info(f"  {tier}  |     {tier_counts[tier]:<9} |    {tier_demotions[tier]:<6} |   {tier_percentages[tier]:.1f}%")
        
        total_miners = sum(tier_counts.values())
        total_demotions = sum(tier_demotions.values())
        overall_percentage = (total_demotions / total_miners) * 100 if total_miners > 0 else 0
        
        logger.info("-----------------------------------------")
        logger.info(f"Total |     {total_miners:<9} |    {total_demotions:<6} |   {overall_percentage:.1f}%")
        
        # Print demotion reasons
        logger.info("\nDemotion Reasons:")
        logger.info("----------------")
        for reason, count in demotion_reasons.items():
            if count > 0:
                percentage = (count / total_demotions) * 100 if total_demotions > 0 else 0
                logger.info(f"{reason.replace('_', '-')}: {count} miners ({percentage:.1f}%)")
        
        return {
            'total_miners': total_miners,
            'total_demotions': total_demotions,
            'tier_counts': tier_counts,
            'tier_demotions': tier_demotions,
            'tier_percentages': tier_percentages,
            'overall_percentage': overall_percentage,
            'demotion_reasons': demotion_reasons,
            'miners_to_demote': miners_to_demote
        }
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        return None
    except Exception as e:
        logger.error(f"Error analyzing ROI impact: {e}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Analyze impact of day windows ROI-based tier demotion")
    parser.add_argument(
        "--db-path", 
        type=str, 
        default="~/.bettensor/database.db",
        help="Path to SQLite database file (default: ~/.bettensor/database.db)"
    )
    parser.add_argument(
        "--print-miners",
        action="store_true",
        help="Print information about individual miners that would be demoted"
    )
    
    args = parser.parse_args()
    
    # Expand user path if necessary
    db_path = os.path.expanduser(args.db_path)
    
    if not os.path.exists(db_path):
        logger.error(f"Database file not found: {db_path}")
        sys.exit(1)
    
    try:
        analyze_day_windows_roi_impact(db_path, args.print_miners)
    except Exception as e:
        logger.error(f"Error during analysis: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main() 