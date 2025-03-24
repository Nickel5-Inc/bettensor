#!/bin/bash

# Check and set working directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
REPO_ROOT="$( dirname "$SCRIPT_DIR" )"

if [ "$PWD" != "$REPO_ROOT" ]; then
    echo "Changing working directory to $REPO_ROOT"
    cd "$REPO_ROOT" || { echo "Failed to change directory. Exiting."; exit 1; }
fi

# Make sure all necessary scripts are executable
chmod +x scripts/*.sh
chmod +x neurons/*.py

# Default neuron arguments
DEFAULT_NEURON_ARGS=""
NEURON_TYPE=""
NETWORK=""
WALLET_NAME=""
WALLET_HOTKEY=""
LOGGING_LEVEL=""
USE_BT_API=""
SUBTENSOR_CHAIN_ENDPOINT=""

# Miner-specific variables
AXON_PORT=""
VALIDATOR_MIN_STAKE=""

# Function to prompt for user input if not provided as an argument
prompt_for_input() {
    local prompt="$1"
    local default="$2"
    local var_name="$3"
    if [ -z "${!var_name}" ]; then
        read -p "$prompt [$default]: " user_input
        eval $var_name="${user_input:-$default}"
    fi
}

# Function to prompt for yes/no input if not provided as an argument
prompt_yes_no() {
    local prompt="$1"
    local var_name="$2"
    while true; do
        read -p "$prompt [y/n]: " yn
        case $yn in
            [Yy]* ) eval $var_name="true"; break;;
            [Nn]* ) eval $var_name="false"; break;;
            * ) echo "Please answer yes or no.";;
        esac
    done
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --neuron_type) NEURON_TYPE="$2"; shift 2 ;;
        --network) NETWORK="$2"; shift 2 ;;
        --wallet.name) WALLET_NAME="$2"; shift 2 ;;
        --wallet.hotkey) WALLET_HOTKEY="$2"; shift 2 ;;
        --logging.level) LOGGING_LEVEL="$2"; shift 2 ;;
        --axon.port) AXON_PORT="$2"; shift 2 ;;
        --validator_min_stake) VALIDATOR_MIN_STAKE="$2"; shift 2 ;;
        --subtensor.chain_endpoint) SUBTENSOR_CHAIN_ENDPOINT="$2"; shift 2 ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
        
    esac
done

# Prompt for neuron type if not provided
prompt_for_input "Enter neuron type (miner/validator)" "miner" "NEURON_TYPE"

# Prompt for network if not provided
prompt_for_input "Enter network (local/test/finney)" "finney" "NETWORK"
case $NETWORK in
    test)
        DEFAULT_NEURON_ARGS="$DEFAULT_NEURON_ARGS --subtensor.network test --netuid 181"
        ;;
    finney)
        DEFAULT_NEURON_ARGS="$DEFAULT_NEURON_ARGS --subtensor.network finney --netuid 30"
        ;;
    local)
        DEFAULT_NEURON_ARGS="$DEFAULT_NEURON_ARGS --subtensor.network local --netuid 1"
        ;;
    *)
        DEFAULT_NEURON_ARGS="$DEFAULT_NEURON_ARGS --subtensor.network $NETWORK"
        ;;
esac

# Prompt for wallet name and hotkey if not provided
prompt_for_input "Enter wallet name" "default" "WALLET_NAME"
prompt_for_input "Enter wallet hotkey" "default" "WALLET_HOTKEY"
DEFAULT_NEURON_ARGS="$DEFAULT_NEURON_ARGS --wallet.name $WALLET_NAME --wallet.hotkey $WALLET_HOTKEY"

# Miner-specific configuration
if [ "$NEURON_TYPE" = "miner" ]; then
    prompt_for_input "Enter validator_min_stake:(This should be 1000 for mainnet, 10 for testnet)" "1000" "VALIDATOR_MIN_STAKE"
    DEFAULT_NEURON_ARGS="$DEFAULT_NEURON_ARGS --validator_min_stake $VALIDATOR_MIN_STAKE"
    prompt_for_input "Enter axon port(Ensure that you use separate ports for each miner instance)" "12345" "AXON_PORT"
    DEFAULT_NEURON_ARGS="$DEFAULT_NEURON_ARGS --axon.port $AXON_PORT"
    # Prompt for starting Flask server
    prompt_yes_no "Would you like to start flask server for connecting to bettensor.com?" "FLASK_SERVER"

    # Set up PostgreSQL database parameters
    DB_NAME="bettensor"
    DB_USER="root"
    DB_PASSWORD="bettensor_password"
    DB_HOST="localhost"
    DB_PORT="5432"
    

fi

# Prompt for logging level if not provided
prompt_for_input "Enter logging level (info/debug/trace)" "debug" "LOGGING_LEVEL"
DEFAULT_NEURON_ARGS="$DEFAULT_NEURON_ARGS --logging.$LOGGING_LEVEL"





# Start the neuron with PM2
if [ "$NEURON_TYPE" = "miner" ]; then
    MINER_COUNT=$(pm2 list | grep -c "miner")
    NEURON_NAME="miner$MINER_COUNT"
    NEURON_ARGS="$DEFAULT_NEURON_ARGS --axon.port $AXON_PORT"
else
    VALIDATOR_COUNT=$(pm2 list | grep -c "validator")
    NEURON_NAME="validator$VALIDATOR_COUNT"
    NEURON_ARGS="$DEFAULT_NEURON_ARGS $USE_BT_API"
fi

echo "Starting $NEURON_TYPE with arguments: $NEURON_ARGS"
pm2 start --name "$NEURON_NAME" --instances 1 python -- ./neurons/$NEURON_TYPE.py $NEURON_ARGS

# Check if the neuron started successfully
if pm2 list | grep -q "$NEURON_NAME"; then
    echo "$NEURON_TYPE started successfully with instance name: $NEURON_NAME"
else
    echo "Failed to start $NEURON_TYPE. Check logs for details."
    pm2 logs "$NEURON_NAME" --lines 20
    exit 1
fi

# Start additional services only for miners
if [ "$NEURON_TYPE" = "miner" ]; then
    
    if [ "$FLASK_SERVER" = "true" ]; then
        if ! pm2 list | grep -q "flask-server"; then
            echo "Starting Flask server..."
            pm2 start --name "flask-server" python -- \
                -m bettensor.miner.interfaces.miner_interface_server
            
            sleep 2  # Give the server a moment to start

            if pm2 list | grep -q "flask-server"; then
                echo "Flask server started successfully."
                pm2 logs "flask-server" --lines 20 --nostream
            else
                echo "Failed to start Flask server. Check logs for details."
                pm2 logs "flask-server" --lines 20 --nostream
            fi
        else
            echo "Flask server is already running."
        fi
    else
        echo "Flask server will not be started."
    fi
fi


# Save the PM2 process list
pm2 save --force

# Display running processes
echo "Current PM2 processes:"
pm2 list

# Display logs for all processes
for process in "$NEURON_NAME" "flask-server" "auto-updater"; do
    if pm2 list | grep -q "$process"; then
        echo "Logs for $process:"
        pm2 logs "$process" --lines 20 --nostream
        echo ""
    fi
done