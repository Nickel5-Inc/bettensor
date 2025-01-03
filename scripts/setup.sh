#!/bin/bash

# Check if the script is being sourced
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    echo "This script must be sourced. Please run: source ./$(basename ${BASH_SOURCE[0]}) <flags>"
    exit 1
fi

# Check Ubuntu version
ubuntu_version=$(lsb_release -rs)

if [[ "$ubuntu_version" == "20.04" ]]; then
    echo "Running on Ubuntu 20.04"
    # Add any 20.04-specific adjustments here
elif [[ "$ubuntu_version" == "22.04" ]]; then
    echo "Running on Ubuntu 22.04"
else
    echo "This script is only tested on Ubuntu 20.04 and 22.04"
    echo "Your version: $ubuntu_version"
    echo "The script may not work correctly. Do you want to continue? (y/n)"
    read -r response
    if [[ "$response" != "y" ]]; then
        echo "Exiting script"
        exit 1
    fi
fi

# Default values
LITE_NODE=false
NETWORK="test"
echo "Running setup..."
echo -e "WARNING: If you are running in a containerized service (vast.ai, runpod, etc), or with a firewall, 
         you may need to add the appropriate ports to the firewall rules:
        Ports:
        9944 - Websocket. This port is used by Bittensor. This port only accepts connections from localhost.
               Make sure this port is firewalled off from the public internet domain.
        9933 - RPC. This port should be opened but it is not used.
        30333 - p2p socket. This port should accept connections from other subtensor nodes on the internet. 
                Make sure your firewall allows incoming traffic to this port.
        We assume that your default outgoing traffic policy is ACCEPT. If not, make sure that outbound traffic on port 30333 is allowed.
        If you decide to set up a firewall, be careful about blocking your ssh port (22) and losing access to your server.
        "
# Function to display usage information
usage() {
    echo "Usage: $0 [--lite-node] [--subtensor.network <test|main>] [--redis-host <host>] [--redis-port <port>] [--postgres-host <host>] [--postgres-port <port>] [--postgres-user <user>] [--postgres-password <password>] [--postgres-database <database>]"
    echo "  --lite-node              Set up a lite node (optional)"
    echo "  --subtensor.network      Specify the network: test | main (default: test)"
    echo "  --redis-host             Redis server host (default: localhost)"
    echo "  --redis-port             Redis server port (default: 6379)"
    echo "  --postgres-host          PostgreSQL host (default: localhost)"
    echo "  --postgres-port          PostgreSQL port (default: 5432)"
    echo "  --postgres-user          PostgreSQL user (default: bettensor)"
    echo "  --postgres-password     PostgreSQL password (default: bettensor_password)"
    echo "  --postgres-database     PostgreSQL database (default: bettensor)"
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --lite-node)
            LITE_NODE=true
            shift
            ;;
        --subtensor.network)
            if [[ $2 =~ ^(test|main|local)$ ]]; then
                NETWORK=$2
                shift 2
            else
                echo "Error: Invalid network specified."
                usage
            fi
            ;;
        --redis-host)
            REDIS_HOST=$2
            shift 2
            ;;
        --redis-port)
            REDIS_PORT=$2
            shift 2
            ;;
        --postgres-host)
            POSTGRES_HOST=$2
            shift 2
            ;;
        --postgres-port)
            POSTGRES_PORT=$2
            shift 2
            ;;
        --postgres-user)
            POSTGRES_USER=$2
            shift 2
            ;;
        --postgres-password)
            POSTGRES_PASSWORD=$2
            shift 2
            ;;
        --postgres-database)
            POSTGRES_DATABASE=$2
            shift 2
            ;;
        *)
            echo "Error: Unknown option $1"
            usage
            ;;
    esac
done

# Set default values if not provided
REDIS_HOST=${REDIS_HOST:-localhost}
REDIS_PORT=${REDIS_PORT:-6379}
POSTGRES_HOST=${POSTGRES_HOST:-localhost}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_USER=${POSTGRES_USER:-bettensor}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-bettensor_password}
POSTGRES_DATABASE=${POSTGRES_DATABASE:-bettensor}

echo "Setting up environment for network: $NETWORK"
if [ "$LITE_NODE" = true ]; then
    echo "Setting up as a lite node"
fi

# Update and install dependencies
apt-get update 
apt-get install -y build-essential net-tools clang curl git make libssl-dev protobuf-compiler llvm libudev-dev python-is-python3

# Install Redis
apt-get install -y redis-server
systemctl enable redis-server
systemctl start redis-server

# Install PostgreSQL
apt-get install -y postgresql postgresql-contrib
systemctl enable postgresql
systemctl start postgresql

# Set up PostgreSQL
su - postgres -c "psql -c \"CREATE DATABASE $POSTGRES_DATABASE;\""
su - postgres -c "psql -c \"CREATE USER root WITH SUPERUSER PASSWORD '$POSTGRES_PASSWORD';\""
su - postgres -c "psql -c \"GRANT ALL PRIVILEGES ON DATABASE $POSTGRES_DATABASE TO root;\""

# Modify PostgreSQL configuration to allow root access
sed -i "s/local   all             postgres                                peer/local   all             postgres                                trust/" /etc/postgresql/*/main/pg_hba.conf
sed -i "s/local   all             all                                     peer/local   all             all                                     trust/" /etc/postgresql/*/main/pg_hba.conf

# Restart PostgreSQL to apply changes
systemctl restart postgresql

# Modify Redis configuration to allow connections from anywhere (be cautious with this in production)
sed -i 's/bind 127.0.0.1/bind 0.0.0.0/' /etc/redis/redis.conf
systemctl restart redis-server

# Install Rust and subtensor only if lite node is requested
if [ "$LITE_NODE" = true ]; then
    echo "Installing Rust and subtensor for lite node..."
    
    # Install Rust
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source $HOME/.cargo/env

    # Set up Rust
    rustup default stable
    rustup update
    rustup target add wasm32-unknown-unknown
    rustup toolchain install nightly
    rustup target add --toolchain nightly wasm32-unknown-unknown

    # Clone subtensor repository
    cd $HOME
    git clone https://github.com/opentensor/subtensor.git
    cd subtensor
    git checkout main

    # Build subtensor
    cargo build --release --features=runtime-benchmarks

    # Set up the node based on the network
    if [ "$NETWORK" = "main" ]; then
        echo "Setting up lite node for main network"
        ./target/release/node-subtensor --chain raw_spec.json --base-path /tmp/blockchain --sync=warp --execution wasm --wasm-execution compiled --port 30333 --max-runtime-instances 64 --rpc-max-response-size 2048 --rpc-cors all --rpc-port 9933 --bootnodes /ip4/13.58.175.193/tcp/30333/p2p/12D3KooWDe7g2JbNETiKypcKT1KsCEZJbTzEHCn8hpd4PHZ6pdz5 --no-mdns --in-peers 8000 --out-peers 8000 --prometheus-external --rpc-external
    elif [ "$NETWORK" = "test" ]; then
        echo "Setting up lite node for test network"
        ./target/release/node-subtensor --chain raw_testspec.json --base-path /tmp/blockchain --sync=warp --execution wasm --wasm-execution compiled --port 30333 --max-runtime-instances 64 --rpc-max-response-size 2048 --rpc-cors all --rpc-port 9933 --bootnodes /dns/bootnode.test.finney.opentensor.ai/tcp/30333/p2p/12D3KooWPM4mLcKJGtyVtkggqdG84zWrd7Rij6PGQDoijh1X86Vr --no-mdns --in-peers 8000 --out-peers 8000 --prometheus-external --rpc-external
    else
        echo "Lite node setup is only available for main and test networks."
    fi
else
    echo "Skipping Rust and subtensor installation. Use --lite-node flag to install and set up a lite node."
fi

# Install Python 3.10
apt-get install -y software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update
apt-get install -y python3.10 python3.10-venv python3.10-dev

# Install npm, jq, and pm2
apt-get install -y npm jq libfuzzy-dev
npm install -g pm2

# Determine the correct directory structure
CURRENT_DIR=$(basename "$PWD")
if [ "$CURRENT_DIR" = "bettensor" ]; then
    VENV_DIR="../.venv"
    BETTENSOR_DIR="."
elif [ "$CURRENT_DIR" = "scripts" ]; then
    VENV_DIR="../../.venv"
    BETTENSOR_DIR=".."
else
    echo "Error: Script must be run from either the 'bettensor' or 'scripts' directory."
    exit 1
fi

# Create and activate virtual environment
python3.10 -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"

# Navigate to the Bettensor directory if necessary
cd "$BETTENSOR_DIR"

# Install Bettensor dependencies
pip install -e .
pip install -r requirements.txt

# Install additional dependencies for Redis and PostgreSQL
pip install redis psycopg2-binary

# Verify installation
python -c "import bittensor; print(bittensor.__version__)"

echo "Bettensor setup complete!"
echo "You can now start mining or validating on the Bettensor subnet, after registering your miner/validator with the subnet."