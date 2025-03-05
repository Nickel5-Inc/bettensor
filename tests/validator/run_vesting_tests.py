#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test runner for the vesting system test suite.
Provides a simple command-line interface to run various test configurations.
"""

import argparse
import sys
import os
import pytest

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


def main():
    """Parse arguments and run tests with appropriate options."""
    parser = argparse.ArgumentParser(description="Run vesting system tests")
    
    # Test selection options
    parser.add_argument(
        "--component",
        choices=["all", "tracker", "requirements", "retry", "system", "performance"],
        default="all",
        help="Which component to test (default: all)"
    )
    
    # Output options
    parser.add_argument(
        "--verbose", "-v",
        action="count",
        default=0,
        help="Increase verbosity (can be used multiple times)"
    )
    
    # Coverage options
    parser.add_argument(
        "--coverage",
        action="store_true",
        help="Generate coverage report"
    )
    
    # Performance options
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Run performance benchmarks"
    )
    
    args = parser.parse_args()
    
    # Construct verbosity flag
    verbosity = "-" + "v" * args.verbose if args.verbose > 0 else ""
    
    # Construct test path based on component
    test_path = "tests/validator/test_vesting_system.py"
    
    if args.component != "all":
        component_map = {
            "tracker": "::TestStakeTracker",
            "requirements": "::TestStakeRequirements",
            "retry": "::TestRetryMechanism",
            "system": "::TestVestingSystem",
            "performance": "::TestVestingSystem::test_performance"
        }
        test_path += component_map[args.component]
    
    # Build pytest arguments
    pytest_args = [test_path]
    
    if verbosity:
        pytest_args.append(verbosity)
    
    if args.coverage:
        pytest_args.append("--cov=bettensor.validator.utils.vesting")
        pytest_args.append("--cov-report=term")
    
    if args.benchmark and args.component == "all":
        # Add benchmark plugin args if available
        pytest_args.append("-xvs")
    
    # Run tests
    print(f"Running tests with arguments: {' '.join(pytest_args)}")
    return pytest.main(pytest_args)


if __name__ == "__main__":
    sys.exit(main()) 