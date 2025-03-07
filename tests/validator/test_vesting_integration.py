import pytest
import numpy as np
from unittest.mock import MagicMock, patch

from validator.utils.vesting.integration import VestingSystemIntegration
from validator.utils.vesting.system import VestingSystem
from validator.utils.vesting.stake_tracker import StakeTracker


class MockScoringSystem:
    """Mock scoring system for testing integration."""
    
    def __init__(self):
        self.original_calculate_weights = self.calculate_weights
        
    def calculate_weights(self, uids, scores):
        """Calculate weights based on scores.
        
        Args:
            uids: List of miner UIDs
            scores: List of scores for each miner
            
        Returns:
            Dictionary mapping miner UIDs to weights
        """
        if not uids or len(uids) == 0:
            return {}
            
        norm_scores = np.array(scores) / np.sum(scores) if np.sum(scores) > 0 else np.ones_like(scores) / len(scores)
        return {uid: float(weight) for uid, weight in zip(uids, norm_scores)}


class TestVestingSystemIntegration:
    
    @pytest.fixture
    def setup(self):
        """Set up test environment with mocked dependencies."""
        self.scoring_system = MockScoringSystem()
        self.vesting_system = MagicMock(spec=VestingSystem)
        
        # Configure vesting_system.apply_vesting_adjustments mock
        def apply_vesting_adjustments_mock(weights, *args, **kwargs):
            adjusted = weights.copy()
            adjusted = {uid: weight * 0.8 for uid, weight in adjusted.items()}
            # Normalize
            total = sum(adjusted.values())
            return {uid: weight / total for uid, weight in adjusted.items()} if total > 0 else adjusted
            
        # Configure vesting_system.apply_enhanced_vesting_adjustments mock
        def apply_enhanced_vesting_adjustments_mock(weights, threshold_factor=0.5, absolute_threshold=0.01):
            # First apply regular adjustments
            adjusted = apply_vesting_adjustments_mock(weights)
            
            # Apply threshold-based redistribution
            weights_to_redistribute = 0
            original_adjusted = adjusted.copy()
            eligible_uids = []
            
            # Identify miners below threshold
            for uid, weight in adjusted.items():
                original_weight = weights.get(uid, 0)
                relative_threshold = original_weight * threshold_factor
                effective_threshold = max(relative_threshold, absolute_threshold)
                
                if weight < effective_threshold and weight > 0:
                    weights_to_redistribute += weight
                    adjusted[uid] = 0
                else:
                    eligible_uids.append(uid)
            
            # Redistribute to eligible miners
            if weights_to_redistribute > 0 and eligible_uids:
                eligible_total = sum(adjusted[uid] for uid in eligible_uids)
                for uid in eligible_uids:
                    if eligible_total > 0:
                        adjusted[uid] += weights_to_redistribute * (adjusted[uid] / eligible_total)
            
            # Normalize
            total = sum(adjusted.values())
            return {uid: weight / total for uid, weight in adjusted.items()} if total > 0 else adjusted
        
        self.vesting_system.apply_vesting_adjustments.side_effect = apply_vesting_adjustments_mock
        self.vesting_system.apply_enhanced_vesting_adjustments.side_effect = apply_enhanced_vesting_adjustments_mock
        
        return self.scoring_system, self.vesting_system
    
    def test_integration_installation(self, setup):
        """Test that integration properly installs and uninstalls."""
        scoring_system, vesting_system = setup
        
        # Store the original method
        original_method = scoring_system.calculate_weights
        
        # Install integration
        integration = VestingSystemIntegration(scoring_system, vesting_system)
        integration.install()
        
        # Verify method was patched
        assert scoring_system.calculate_weights != original_method
        
        # Uninstall integration
        integration.uninstall()
        
        # Verify original method was restored
        assert scoring_system.calculate_weights == original_method
    
    def test_standard_redistribution(self, setup):
        """Test the standard redistribution approach."""
        scoring_system, vesting_system = setup
        
        # Install integration with standard redistribution
        integration = VestingSystemIntegration(
            scoring_system, 
            vesting_system,
            use_enhanced_redistribution=False
        )
        integration.install()
        
        # Define test data - miners
        uids = [1, 2, 3]
        scores = [10, 20, 30]
        
        # Calculate weights
        weights = scoring_system.calculate_weights(uids, scores)
        
        # Check that vesting_system.apply_vesting_adjustments was called
        vesting_system.apply_vesting_adjustments.assert_called_once()
        
        # Check that vesting_system.apply_enhanced_vesting_adjustments was not called
        vesting_system.apply_enhanced_vesting_adjustments.assert_not_called()
        
        # Verify weights were adjusted and sum to 1
        assert abs(sum(weights.values()) - 1.0) < 1e-6
    
    def test_enhanced_redistribution(self, setup):
        """Test the enhanced redistribution approach."""
        scoring_system, vesting_system = setup
        
        # Install integration with enhanced redistribution
        integration = VestingSystemIntegration(
            scoring_system, 
            vesting_system,
            use_enhanced_redistribution=True,
            threshold_factor=0.6,
            absolute_threshold=0.02
        )
        integration.install()
        
        # Define test data - miners
        uids = [1, 2, 3]
        scores = [10, 20, 30]
        
        # Calculate weights
        weights = scoring_system.calculate_weights(uids, scores)
        
        # Check that vesting_system.apply_enhanced_vesting_adjustments was called with correct params
        vesting_system.apply_enhanced_vesting_adjustments.assert_called_once()
        args, kwargs = vesting_system.apply_enhanced_vesting_adjustments.call_args
        assert kwargs.get('threshold_factor') == 0.6
        assert kwargs.get('absolute_threshold') == 0.02
        
        # Verify weights were adjusted and sum to 1
        assert abs(sum(weights.values()) - 1.0) < 1e-6
    
    def test_update_redistribution_settings(self, setup):
        """Test updating redistribution settings."""
        scoring_system, vesting_system = setup
        
        # Install integration with standard redistribution
        integration = VestingSystemIntegration(
            scoring_system, 
            vesting_system,
            use_enhanced_redistribution=False
        )
        integration.install()
        
        # Define test data - miners
        uids = [1, 2, 3]
        scores = [10, 20, 30]
        
        # Calculate weights with standard redistribution
        scoring_system.calculate_weights(uids, scores)
        vesting_system.apply_vesting_adjustments.assert_called_once()
        vesting_system.apply_enhanced_vesting_adjustments.assert_not_called()
        
        # Reset mock call counts
        vesting_system.apply_vesting_adjustments.reset_mock()
        vesting_system.apply_enhanced_vesting_adjustments.reset_mock()
        
        # Update to enhanced redistribution
        integration.update_redistribution_settings(
            use_enhanced_redistribution=True,
            threshold_factor=0.7,
            absolute_threshold=0.03
        )
        
        # Calculate weights again, should use enhanced now
        weights = scoring_system.calculate_weights(uids, scores)
        
        # Check that the enhanced method was called with updated parameters
        vesting_system.apply_enhanced_vesting_adjustments.assert_called_once()
        args, kwargs = vesting_system.apply_enhanced_vesting_adjustments.call_args
        assert kwargs.get('threshold_factor') == 0.7
        assert kwargs.get('absolute_threshold') == 0.03
        
        # Verify weights still sum to 1
        assert abs(sum(weights.values()) - 1.0) < 1e-6
    
    def test_weight_calculation_with_empty_data(self, setup):
        """Test handling of empty data sets."""
        scoring_system, vesting_system = setup
        
        # Install integration
        integration = VestingSystemIntegration(
            scoring_system, 
            vesting_system,
            use_enhanced_redistribution=True
        )
        integration.install()
        
        # Test with empty uids
        weights = scoring_system.calculate_weights([], [])
        assert weights == {}
        
        # Neither method should have been called
        vesting_system.apply_vesting_adjustments.assert_not_called()
        vesting_system.apply_enhanced_vesting_adjustments.assert_not_called()


@pytest.mark.asyncio
async def test_integration_with_real_components():
    """Integration test with actual components instead of mocks."""
    # This test requires setting up real VestingSystem and StakeTracker instances
    # which might be complex and require database fixtures
    
    # Mock the database connections
    with patch('validator.utils.vesting.stake_tracker.SubtensorClient'):
        # Create real components
        stake_tracker = StakeTracker(None, None)
        vesting_system = VestingSystem(stake_tracker)
        scoring_system = MockScoringSystem()
        
        # Create and install integration
        integration = VestingSystemIntegration(
            scoring_system,
            vesting_system,
            use_enhanced_redistribution=True
        )
        integration.install()
        
        # Test with sample data - miner UIDs and scores
        uids = [1, 2, 3, 4, 5]
        scores = [10, 20, 15, 5, 25]
        
        # Calculate weights
        weights = scoring_system.calculate_weights(uids, scores)
        
        # Verify weights sum to 1
        assert abs(sum(weights.values()) - 1.0) < 1e-6
        
        # Uninstall integration
        integration.uninstall() 