#!/bin/bash

# Get the directory of the current script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Navigate to the project root (assuming scripts is in the bettensor/validator directory)
PROJECT_ROOT="$SCRIPT_DIR/../../.."
cd "$PROJECT_ROOT" || exit 1

echo "Running post-update hooks..."

# Run the auto-update post-update script
python -m bettensor.validator.utils.auto_update post-update

EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    echo "Error: Post-update hooks failed with exit code $EXIT_CODE"
    exit $EXIT_CODE
fi

echo "Post-update hooks completed successfully"
exit 0 