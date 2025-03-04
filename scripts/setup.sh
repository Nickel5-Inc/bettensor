#!/bin/bash

# Change to the directory containing the script
cd "$(dirname "$0")/.." || exit 1

# Check if bash is installed (might be using sh on some systems)
if [ -z "$(command -v bash)" ]; then
    echo "Bash is not installed. Please install bash to continue."
    exit 1
fi

# Check if python3 is installed
if [ -z "$(command -v python3)" ]; then
    echo "Python 3 is not installed. Please install Python 3 to continue."
    exit 1
fi

# Check if pip is installed
if [ -z "$(command -v pip)" ] && [ -z "$(command -v pip3)" ]; then
    echo "Pip is not installed. Please install pip to continue."
    exit 1
fi

# Set pip command based on what's available
if [ -n "$(command -v pip)" ]; then
    PIP_CMD="pip"
elif [ -n "$(command -v pip3)" ]; then
    PIP_CMD="pip3"
fi

echo "Starting Bettensor setup..."
echo "Current working directory: $(pwd)"

# Check if the script is run within an activated Python virtual environment
if [ -z "$VIRTUAL_ENV" ]; then
    echo "Warning: No active virtual environment detected."
    echo "It is recommended to run this script within a virtual environment."
    
    read -p "Would you like to create a new virtual environment? (y/n): " CREATE_VENV
    if [[ "$CREATE_VENV" =~ ^[Yy]$ ]]; then
        echo "Creating a new virtual environment..."
        
        # Try to install virtualenv if it's not available
        if [ -z "$(command -v virtualenv)" ]; then
            echo "Installing virtualenv..."
            $PIP_CMD install virtualenv
        fi
        
        # Create the virtual environment
        python3 -m virtualenv .venv
        
        # Activate the virtual environment
        source .venv/bin/activate
        
        if [ -z "$VIRTUAL_ENV" ]; then
            echo "Failed to create or activate virtual environment. Please create and activate a virtual environment manually."
            exit 1
        else
            echo "Virtual environment created and activated at: $VIRTUAL_ENV"
        fi
    else
        echo "Continuing without a virtual environment..."
    fi
fi

# Install UV - try to use it for faster package installation
echo "Attempting to install UV package manager for faster dependency resolution..."

UV_INSTALLED=false
if $PIP_CMD install uv; then
    echo "UV installed successfully. Will use it for faster package installation."
    UV_INSTALLED=true
else
    echo "Failed to install UV. Will fall back to pip for package installation."
fi

# Try different installation methods with appropriate fallbacks
if [ "$UV_INSTALLED" = true ]; then
    # UV pip install approach first
    echo "Attempting installation with UV..."
    if uv pip install -e . --pre; then
        echo "Installation with UV pip install -e successful!"
    else
        echo "UV pip install failed, trying UV sync..."
        # Try UV sync as a fallback
        if uv sync --prerelease=allow; then
            echo "Installation with UV sync successful!"
        else
            echo "UV sync failed, falling back to traditional pip..."
            # Final fallback to pip
            if $PIP_CMD install -e .; then
                echo "Installation with traditional pip successful!"
                if [ -f requirements.txt ]; then
                    $PIP_CMD install -r requirements.txt
                fi
            else
                echo "All installation methods failed. Please check your environment and try again."
                exit 1
            fi
        fi
    fi
else
    # If UV isn't installed, use pip directly
    echo "Installing with pip..."
    if $PIP_CMD install -e .; then
        echo "Installation with pip successful!"
        if [ -f requirements.txt ]; then
            $PIP_CMD install -r requirements.txt
        fi
    else
        echo "Failed to install with pip. Please check your environment and try again."
        exit 1
    fi
fi

# Install additional dependencies
echo "Installing additional dependencies..."
if [ "$UV_INSTALLED" = true ]; then
    if ! uv pip install redis psycopg2-binary --pre; then
        echo "Failed to install additional dependencies with UV, trying with pip..."
        $PIP_CMD install redis psycopg2-binary
    fi
else
    $PIP_CMD install redis psycopg2-binary
fi

# Check if the installation was successful
if python3 -c "import bittensor" 2>/dev/null; then
    echo "==============================================="
    echo "Bettensor setup completed successfully!"
    if [ "$UV_INSTALLED" = true ]; then
        echo "Using UV package manager for improved performance."
    else
        echo "Using traditional pip. Consider installing UV for better performance."
    fi
    echo "==============================================="
else
    echo "==============================================="
    echo "WARNING: Setup completed but bittensor package may not be properly installed."
    echo "Please check the above log for errors and try again if needed."
    echo "==============================================="
    exit 1
fi

exit 0