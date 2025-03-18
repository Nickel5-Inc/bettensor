#!/usr/bin/env python3
import os
import json
from pathlib import Path
from datetime import datetime, timezone

# Find the database directory
default_db_path = "./bettensor/validator/state/validator.db"
db_dir = os.path.dirname(default_db_path)

# Create state directory if it doesn't exist
state_dir = Path(db_dir) / "state"
state_dir.mkdir(parents=True, exist_ok=True)

# Path to the metadata file
metadata_file = state_dir / "state_metadata.json"

print(f"Looking for metadata file at: {metadata_file}")

# Check if metadata file exists
if metadata_file.exists():
    print(f"Metadata file exists: {metadata_file}")
    with open(metadata_file, 'r') as f:
        metadata = json.load(f)
    print(f"Current metadata: {json.dumps(metadata, indent=2)}")
else:
    print(f"Metadata file does not exist, creating default one")
    # Create default metadata
    current_time = datetime.now(timezone.utc)
    default_metadata = {
        "last_update": current_time.isoformat(),
        "files": {}
    }
    
    # Write to file
    with open(metadata_file, 'w') as f:
        json.dump(default_metadata, f, indent=2)
    print(f"Created default metadata file with timestamp: {current_time.isoformat()}")

print("\nTest completed successfully!") 