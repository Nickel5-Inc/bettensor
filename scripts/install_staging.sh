#!/bin/bash

# Section 1: Build/Install
# This section is for first-time setup and installations.

install_dependencies() {
    # Function to install packages on macOS
    install_mac() {
        which brew > /dev/null
        if [ $? -ne 0 ]; then
            echo "Installing Homebrew..."
            /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        fi
        echo "Updating Homebrew packages..."
        brew update
        echo "Installing required packages..."
        brew install make llvm curl libssl protobuf tmux
    }

    # Function to install packages on Ubuntu/Debian
    install_ubuntu() {
        echo "Updating system packages..."
        sudo apt update
        echo "Installing required packages..."
        sudo apt install --assume-yes make build-essential git clang curl libssl-dev llvm libudev-dev protobuf-compiler tmux
    }

    # Detect OS and call the appropriate function
    if [[ "$OSTYPE" == "darwin"* ]]; then
        install_mac
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        install_ubuntu
    else
        echo "Unsupported operating system."
        exit 1
    fi

    # Install rust and cargo
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

    # Update your shell's source to include Cargo's path
    source "$HOME/.cargo/env"
}

# Call install_dependencies only if it's the first time running the script
if [ ! -f ".dependencies_installed" ]; then
    install_dependencies
    touch .dependencies_installed
fi


# Section 2: Test/Run
# This section is for running and testing the setup.

# Create a coldkey for the owner role
wallet=${1:-owner}

# Logic for setting up and running the environment
setup_environment() {
    # Clone subtensor and enter the directory
    if [ ! -d "subtensor" ]; then
        git clone https://github.com/opentensor/subtensor.git
    fi
    cd subtensor
    git pull

    # Update to the nightly version of rust
    ./scripts/init.sh

    cd ../bittensor-subnet-template

    # Install uv package manager if possible
    echo "Installing uv package manager..."
    if python -m pip install uv; then
        echo "Using uv for faster package installation..."
        uv pip install -e .
    else
        echo "Failed to install uv, falling back to pip..."
        python -m pip install -e .
    fi

    # Create and set up wallets
    # This section can be skipped if wallets are already set up
    if [ ! -f ".wallets_setup" ]; then
        btcli wallet new_coldkey --wallet.name $wallet --no_password --no_prompt
        btcli wallet new_coldkey --wallet.name miner --no_password --no_prompt
        btcli wallet new_hotkey --wallet.name miner --wallet.hotkey default --no_prompt
        btcli wallet new_coldkey --wallet.name validator --no_password --no_prompt
        btcli wallet new_hotkey --wallet.name validator --wallet.hotkey default --no_prompt
        touch .wallets_setup
    fi

}

# Call setup_environment every time
setup_environment 

## Setup localnet
# assumes we are in the bittensor-subnet-template/ directory
# Initialize your local subtensor chain in development mode. This command will set up and run a local subtensor network.
cd ../subtensor

# Start a new tmux session and create a new pane, but do not switch to it
echo "FEATURES='pow-faucet runtime-benchmarks' BT_DEFAULT_TOKEN_WALLET=$(cat ~/.bittensor/wallets/$wallet/coldkeypub.txt | grep -oP '"ss58Address": "\K[^"]+') bash scripts/localnet.sh" >> setup_and_run.sh
chmod +x setup_and_run.sh
tmux new-session -d -s localnet -n 'localnet'
tmux send-keys -t localnet 'bash ../subtensor/setup_and_run.sh' C-m

# Notify the user
echo ">> localnet.sh is running in a detached tmux session named 'localnet'"
echo ">> You can attach to this session with: tmux attach-session -t localnet"

# Register a subnet (this needs to be run each time we start a new local chain)
btcli subnet create --wallet.name $wallet --wallet.hotkey default --subtensor.chain_endpoint ws://127.0.0.1:9946 --no_prompt

# Transfer tokens to miner and validator coldkeys
export BT_MINER_TOKEN_WALLET=$(cat ~/.bittensor/wallets/miner/coldkeypub.txt | grep -oP '"ss58Address": "\K[^"]+')
export BT_VALIDATOR_TOKEN_WALLET=$(cat ~/.bittensor/wallets/validator/coldkeypub.txt | grep -oP '"ss58Address": "\K[^"]+')

btcli wallet transfer --subtensor.network ws://127.0.0.1:9946 --wallet.name $wallet --dest $BT_MINER_TOKEN_WALLET --amount 1000 --no_prompt
btcli wallet transfer --subtensor.network ws://127.0.0.1:9946 --wallet.name $wallet --dest $BT_VALIDATOR_TOKEN_WALLET --amount 10000 --no_prompt

# Register wallet hotkeys to subnet
btcli subnet register --wallet.name miner --netuid 1 --wallet.hotkey default --subtensor.chain_endpoint ws://127.0.0.1:9946 --no_prompt
btcli subnet register --wallet.name validator --netuid 1 --wallet.hotkey default --subtensor.chain_endpoint ws://127.0.0.1:9946 --no_prompt

# Add stake to the validator
btcli stake add --wallet.name validator --wallet.hotkey default --subtensor.chain_endpoint ws://127.0.0.1:9946 --amount 10000 --no_prompt

# Ensure both the miner and validator keys are successfully registered.
btcli subnet list --subtensor.chain_endpoint ws://127.0.0.1:9946
btcli wallet overview --wallet.name validator --subtensor.chain_endpoint ws://127.0.0.1:9946 --no_prompt
btcli wallet overview --wallet.name miner --subtensor.chain_endpoint ws://127.0.0.1:9946 --no_prompt

cd ../bittensor-subnet-template


# Check if inside a tmux session
if [ -z "$TMUX" ]; then
    # Start a new tmux session and run the miner in the first pane
    tmux new-session -d -s bittensor -n 'miner' 'python neurons/miner.py --netuid 1 --subtensor.chain_endpoint ws://127.0.0.1:9946 --wallet.name miner --wallet.hotkey default --logging.debug'
    
    # Split the window and run the validator in the new pane
    tmux split-window -h -t bittensor:miner 'python neurons/validator.py --netuid 1 --subtensor.chain_endpoint ws://127.0.0.1:9946 --wallet.name validator --wallet.hotkey default --logging.debug'
    
    # Attach to the new tmux session
    tmux attach-session -t bittensor
else
    # If already in a tmux session, create two panes in the current window
    tmux split-window -h 'python neurons/miner.py --netuid 1 --subtensor.chain_endpoint ws://127.0.0.1:9946 --wallet.name miner --wallet.hotkey default --logging.debug'
    tmux split-window -v -t 0 'python neurons/validator.py --netuid 1 --subtensor.chain_endpoint ws://127.0.0.1:9946 --wallet.name3 validator --wallet.hotkey default --logging.debug'
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

echo "Installing Bittensor staging subnet template..."

# Try to install UV for faster package management
echo "Attempting to install UV package manager..."
UV_INSTALLED=false
if $PIP_CMD install uv; then
    echo "UV installed successfully. Will use it for faster package installation."
    UV_INSTALLED=true
else
    echo "Failed to install UV. Will use pip for package installation."
fi

# Install the package with multiple fallback options
if [ "$UV_INSTALLED" = true ]; then
    # Try UV pip install first
    echo "Installing with UV..."
    if uv pip install bittensor-subnet-template --pre; then
        echo "Installation with UV pip install successful!"
    else
        # Try UV sync as a fallback
        echo "UV pip install failed, trying UV sync..."
        if uv sync --prerelease=allow; then
            echo "Installation with UV sync successful!"
        else
            # Final fallback to pip
            echo "UV sync failed, falling back to pip..."
            if $PIP_CMD install bittensor-subnet-template; then
                echo "Installation with pip successful!"
            else
                echo "Failed to install bittensor-subnet-template. Please check your environment and try again."
                exit 1
            fi
        fi
    fi
else
    # Use pip directly if UV isn't installed
    echo "Installing with pip..."
    if $PIP_CMD install bittensor-subnet-template; then
        echo "Installation with pip successful!"
    else
        echo "Failed to install bittensor-subnet-template. Please check your environment and try again."
        exit 1
    fi
fi

echo "Installation complete! You can now use the Bittensor subnet template."
exit 0
