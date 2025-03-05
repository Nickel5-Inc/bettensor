#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import pytest

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

def test_import_bettensor():
    """Test that we can import bettensor."""
    import bettensor
    assert bettensor is not None
    print(f"bettensor.__file__: {bettensor.__file__}")

def test_import_validator():
    """Test that we can import bettensor.validator."""
    import bettensor.validator
    assert bettensor.validator is not None
    print(f"bettensor.validator.__file__: {bettensor.validator.__file__}")

def test_import_database():
    """Test that we can import database modules."""
    from bettensor.validator.utils.database.database_manager import DatabaseManager
    assert DatabaseManager is not None
    print("Successfully imported DatabaseManager")

if __name__ == "__main__":
    pytest.main(["-xvs", __file__]) 