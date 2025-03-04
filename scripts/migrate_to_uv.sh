#!/bin/bash

# This script helps migrate from pip to native uv workflows
# It creates a fresh UV virtual environment and installs dependencies using UV's native approach

# Determine the project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT" || { echo "Failed to navigate to project root directory"; exit 1; }
echo "Project root: $PROJECT_ROOT"

# Check if we're in a virtual environment
if [ -z "$VIRTUAL_ENV" ]; then
    echo "Error: No active virtual environment detected."
    echo "Please activate your virtual environment before running this script."
    echo "Example: source .venv/bin/activate"
    exit 1
fi
echo "Using virtual environment: $VIRTUAL_ENV"

# Install uv package
echo "Installing uv package manager..."
if ! pip install uv; then
    echo "Failed to install uv package manager."
    echo "Please check your internet connection and pip installation."
    exit 1
fi
echo "UV package manager installed successfully."

# Create a backup of current requirements
echo "Creating a backup of current requirements..."
BACKUP_FILE="$PROJECT_ROOT/requirements.backup.$(date +%Y%m%d%H%M%S).txt"
pip freeze > "$BACKUP_FILE"
echo "Current requirements backed up to: $BACKUP_FILE"

# Check which approach should be used for reinstalling dependencies
echo "Checking for dependency files..."

if [ -f "$PROJECT_ROOT/pyproject.toml" ]; then
    echo "Found pyproject.toml - using it for dependency installation."
    
    # Use native UV with prerelease flag for projects with pyproject.toml
    echo "Installing dependencies using UV..."
    if uv pip install -e . --pre; then
        echo "Dependencies installed successfully using UV pip install -e!"
    else
        echo "UV pip install failed, trying UV sync..."
        if uv sync --prerelease=allow; then
            echo "Dependencies synchronized successfully using UV sync."
        else
            echo "UV sync failed. Falling back to pip..."
            if pip install -e .; then
                echo "Dependencies installed successfully using pip."
            else
                echo "All installation methods failed. You may need to reinstall dependencies manually."
                echo "Your original dependencies are backed up at: $BACKUP_FILE"
                exit 1
            fi
        fi
    fi
    
elif [ -f "$PROJECT_ROOT/requirements.txt" ]; then
    echo "Found requirements.txt - using it for dependency installation."
    
    # Use UV with prerelease flag for projects with requirements.txt
    echo "Installing dependencies using UV..."
    if uv pip install -r requirements.txt --pre; then
        echo "Dependencies installed successfully using UV."
    else
        echo "UV installation failed. Falling back to pip..."
        if pip install -r requirements.txt; then
            echo "Dependencies installed successfully using pip."
        else
            echo "All installation methods failed. You may need to reinstall dependencies manually."
            echo "Your original dependencies are backed up at: $BACKUP_FILE"
            exit 1
        fi
    fi
    
else
    echo "No dependency files found. Using backup requirements."
    
    echo "Installing dependencies from backup..."
    if uv pip install -r "$BACKUP_FILE" --pre; then
        echo "Dependencies installed successfully using UV."
    else
        echo "UV installation failed. Falling back to pip..."
        if pip install -r "$BACKUP_FILE"; then
            echo "Dependencies installed successfully using pip."
        else
            echo "All installation methods failed. You may need to reinstall dependencies manually."
            echo "Your original dependencies are backed up at: $BACKUP_FILE"
            exit 1
        fi
    fi
fi

# Install additional common dependencies
echo "Installing additional dependencies..."
if uv pip install redis psycopg2-binary --pre; then
    echo "Additional dependencies installed successfully using UV."
else
    echo "UV installation failed for additional dependencies. Falling back to pip..."
    if pip install redis psycopg2-binary; then
        echo "Additional dependencies installed successfully using pip."
    else
        echo "Failed to install additional dependencies."
    fi
fi

echo "================================================================"
echo "Migration to UV complete!"
echo "You can now use UV commands like:"
echo "  - uv pip install -e . (Install project in editable mode)"
echo "  - uv sync (Synchronize dependencies)"
echo "  - uv venv (UV's virtual environment commands)"
echo "================================================================"
echo "Benefits of using UV:"
echo "  - Faster installations (10-100x faster than pip)"
echo "  - Better dependency resolution"
echo "  - Enhanced performance (Rust-based implementation)"
echo "  - Native virtual environment management"
echo "================================================================"
echo "For more information, see the UV documentation at: https://github.com/astral-sh/uv"
echo "================================================================"

exit 0 