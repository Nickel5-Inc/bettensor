#!/bin/bash

# Configuration
PROJECT_DIR=$(pwd)
LOCKFILE="${PROJECT_DIR}/.update.lock"
MAX_LOCK_TIME=3600  # Max time in seconds to consider a lock stale

echo "Starting auto-update process..."

# Exit if another instance is running (unless lock is stale)
if [ -f "$LOCKFILE" ]; then
    lock_timestamp=$(cat "$LOCKFILE")
    current_timestamp=$(date +%s)
    elapsed=$((current_timestamp - lock_timestamp))
    
    if [ $elapsed -lt $MAX_LOCK_TIME ]; then
        echo "Another update is in progress. Exiting."
        exit 1
    else
        echo "Found stale lock file. Overriding."
    fi
fi

# Create lock file
echo $(date +%s) > "$LOCKFILE"

# Cleanup on exit
trap 'rm -f "$LOCKFILE"; echo "Update process complete."' EXIT

# Create or update post-merge hook
mkdir -p .git/hooks
if [ ! -f .git/hooks/post-merge ] || ! grep -q "uv pip install -e" .git/hooks/post-merge; then
    # Backup existing hook if it exists
    if [ -f .git/hooks/post-merge ]; then
        cp .git/hooks/post-merge .git/hooks/post-merge.bak
        echo "Backed up existing post-merge hook to .git/hooks/post-merge.bak"
    fi
    
    # Create new hook
    cat > .git/hooks/post-merge << 'EOF'
#!/bin/bash
echo "Running post-merge hook for dependency updates..."

# Detect if we're in a virtual environment
if [ -n "$VIRTUAL_ENV" ]; then
    echo "Virtual environment detected at: $VIRTUAL_ENV"
else
    echo "No virtual environment detected. Dependencies won't be updated."
    exit 0
fi

# Check if UV is installed or install it
if ! command -v uv &>/dev/null; then
    echo "UV not found, attempting to install..."
    if ! pip install uv; then
        echo "Failed to install UV. Falling back to pip."
        # Try installing with pip
        if pip install -e .; then
            echo "Dependencies updated using pip."
            exit 0
        else
            echo "Failed to update dependencies with pip."
            exit 1
        fi
    fi
fi

# Install with UV pip install -e
if uv pip install -e . --pre; then
    echo "Dependencies updated using UV pip install -e with pre-releases allowed."
    exit 0
else
    echo "UV pip install failed, trying alternative methods..."
    # Try syncing with UV as fallback
    if uv sync --prerelease=allow; then
        echo "Dependencies updated using UV sync with pre-releases allowed."
        exit 0
    # Final fallback to pip
    elif pip install -e .; then
        echo "Dependencies updated using pip (UV fallback)."
        exit 0
    else
        echo "All dependency update methods failed. Please update manually."
        exit 1
    fi
fi
EOF
    chmod +x .git/hooks/post-merge
    echo "Created/updated post-merge hook."
fi

# Attempt to pull latest changes
echo "Pulling latest changes..."
git pull

echo "Auto-update script completed successfully."
exit 0