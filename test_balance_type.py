#!/usr/bin/env python3
"""
Test script for integrating bittensor.Balance with SQLAlchemy
"""
import bittensor as bt
from sqlalchemy.types import TypeDecorator, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, create_engine, String
from sqlalchemy.orm import sessionmaker

# Custom type for Balance objects
class BalanceType(TypeDecorator):
    """SQLAlchemy type for bittensor.Balance objects"""
    impl = Float
    
    def process_bind_param(self, value, dialect):
        """Convert Balance to float when storing in database"""
        if value is None:
            return None
        if isinstance(value, bt.Balance):
            return value.tao
        return float(value)
        
    def process_result_value(self, value, dialect):
        """Convert float to Balance when loading from database"""
        if value is None:
            return None
        return bt.Balance.from_tao(value)

# Create SQLAlchemy base
Base = declarative_base()

# Define test model
class StakeTransactionTest(Base):
    """Test model for stake transactions using Balance type"""
    __tablename__ = 'test_stake_transactions'
    
    id = Column(Integer, primary_key=True)
    hotkey = Column(String(255))
    coldkey = Column(String(255))
    transaction_type = Column(String(32))
    amount = Column(BalanceType)

# Connect to an in-memory SQLite database
engine = create_engine('sqlite:///:memory:')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# Test inserting Balance objects
def test_balance_type():
    """Test inserting and retrieving Balance objects"""
    # Create test transactions
    tx1 = StakeTransactionTest(
        hotkey="0xhotkey1",
        coldkey="0xcoldkey1",
        transaction_type="add_stake",
        amount=bt.Balance.from_tao(10.5)
    )
    
    tx2 = StakeTransactionTest(
        hotkey="0xhotkey2",
        coldkey="0xcoldkey2",
        transaction_type="remove_stake",
        amount=5.25  # Test with float
    )
    
    # Add to session and commit
    session.add_all([tx1, tx2])
    session.commit()
    
    # Query and verify
    txs = session.query(StakeTransactionTest).all()
    for tx in txs:
        print(f"Transaction {tx.id}:")
        print(f"  Hotkey: {tx.hotkey}")
        print(f"  Amount: {tx.amount}")
        print(f"  Amount type: {type(tx.amount)}")
        print(f"  Amount as tao: {tx.amount.tao}")

if __name__ == "__main__":
    test_balance_type()
    print("Test completed successfully!") 