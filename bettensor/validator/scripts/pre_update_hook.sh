#!/bin/bash

# Get the directory of the current script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Navigate to the project root (assuming scripts is in the bettensor/validator directory)
PROJECT_ROOT="$SCRIPT_DIR/../../.."
cd "$PROJECT_ROOT" || exit 1

echo "Running pre-update hooks..."

# Run the auto-update pre-update script
python -m bettensor.validator.utils.auto_update pre-update

EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    echo "Error: Pre-update hooks failed with exit code $EXIT_CODE"
    exit $EXIT_CODE
fi

echo "Pre-update hooks completed successfully"
exit 0 