"""
Tests for the BalanceType integration with SQLAlchemy models.
"""
import pytest
from datetime import datetime

import bittensor as bt

from validator.utils.vesting.blockchain.subtensor_client import SubtensorClient
from validator.utils.vesting.core.stake_tracker import StakeTracker
from validator.utils.vesting.database.models import StakeTransaction, BalanceType
from tests.validator.test_vesting_system import MockMetagraph, MockSubtensorClient, setup_test_db


class TestBalanceType:
    """Test suite for the BalanceType SQLAlchemy custom type."""
    
    @pytest.mark.asyncio
    async def test_balance_type_serialization(self, setup_test_db):
        """Test serialization and deserialization of Balance objects using BalanceType."""
        db_manager = await setup_test_db
        
        # Create a test transaction with a Balance object
        async with db_manager.async_session() as session:
            # Create transaction with Balance object
            tx1 = StakeTransaction(
                extrinsic_hash="0xtest_balance_hash1",
                block_number=100,
                block_timestamp=datetime.utcnow(),
                hotkey="0xhotkey_test",
                coldkey="0xcoldkey_test",
                transaction_type="add_stake",
                amount=bt.Balance.from_tao(12.345)
            )
            
            # Create transaction with float
            tx2 = StakeTransaction(
                extrinsic_hash="0xtest_balance_hash2",
                block_number=101,
                block_timestamp=datetime.utcnow(),
                hotkey="0xhotkey_test",
                coldkey="0xcoldkey_test",
                transaction_type="remove_stake",
                amount=6.789
            )
            
            session.add_all([tx1, tx2])
            await session.commit()
        
        # Verify that the transactions were stored correctly
        async with db_manager.async_session() as session:
            # Query by extrinsic_hash
            result = await session.execute(
                """
                SELECT extrinsic_hash, amount FROM vesting_stake_transactions
                WHERE extrinsic_hash IN (:hash1, :hash2)
                ORDER BY block_number
                """,
                {"hash1": "0xtest_balance_hash1", "hash2": "0xtest_balance_hash2"}
            )
            
            rows = result.fetchall()
            assert len(rows) == 2, "Expected 2 transactions"
            
            # First transaction should have a Balance amount of 12.345
            tx1_hash, tx1_amount = rows[0]
            assert tx1_hash == "0xtest_balance_hash1"
            assert isinstance(tx1_amount, bt.Balance), f"Expected Balance, got {type(tx1_amount)}"
            assert tx1_amount.tao == 12.345, f"Expected 12.345 tao, got {tx1_amount.tao}"
            
            # Second transaction should have a Balance amount of 6.789
            tx2_hash, tx2_amount = rows[1]
            assert tx2_hash == "0xtest_balance_hash2"
            assert isinstance(tx2_amount, bt.Balance), f"Expected Balance, got {type(tx2_amount)}"
            assert tx2_amount.tao == 6.789, f"Expected 6.789 tao, got {tx2_amount.tao}"
    
    @pytest.mark.asyncio
    async def test_record_stake_transaction_with_balance(self, setup_test_db):
        """Test recording stake transactions with Balance objects."""
        db_manager = await setup_test_db
        metagraph = MockMetagraph()
        subtensor_client = MockSubtensorClient(metagraph)
        
        # Create stake tracker
        stake_tracker = StakeTracker(db_manager, metagraph, subtensor_client)
        
        # Record transaction with Balance object
        balance_amount = bt.Balance.from_tao(15.5)
        await stake_tracker.record_stake_transaction(
            transaction_type="add_stake",
            hotkey="0xhotkey_balance",
            coldkey="0xcoldkey_balance",
            amount=balance_amount,
            extrinsic_hash="0xbalance_tx_hash",
            block_number=200,
            block_timestamp=datetime.utcnow(),
            source_hotkey=None
        )
        
        # Record transaction with float
        float_amount = 7.25
        await stake_tracker.record_stake_transaction(
            transaction_type="add_stake",
            hotkey="0xhotkey_float",
            coldkey="0xcoldkey_float",
            amount=float_amount,
            extrinsic_hash="0xfloat_tx_hash",
            block_number=201,
            block_timestamp=datetime.utcnow(),
            source_hotkey=None
        )
        
        # Verify that both transactions were stored correctly
        async with db_manager.async_session() as session:
            result = await session.execute(
                """
                SELECT hotkey, amount FROM vesting_stake_transactions
                WHERE extrinsic_hash IN (:hash1, :hash2)
                ORDER BY block_number
                """,
                {"hash1": "0xbalance_tx_hash", "hash2": "0xfloat_tx_hash"}
            )
            
            rows = result.fetchall()
            assert len(rows) == 2, "Expected 2 transactions"
            
            # Both should be stored as Balance objects
            hotkey1, amount1 = rows[0]
            assert hotkey1 == "0xhotkey_balance"
            assert isinstance(amount1, bt.Balance), f"Expected Balance, got {type(amount1)}"
            
            hotkey2, amount2 = rows[1]
            assert hotkey2 == "0xhotkey_float"
            assert isinstance(amount2, bt.Balance), f"Expected Balance, got {type(amount2)}" 