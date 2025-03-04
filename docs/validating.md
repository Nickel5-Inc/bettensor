# Guide for Validators

Running a validator is very simple for Bettensor. Use our setup script as follows:

1. Run `source ./scripts/start_neuron.sh` and follow prompts for validator. You can run this script with flags if you prefer not to enter prompts. To use a local subtensor, run `source scripts/start_neuron.sh --subtensor.chain_endpoint <YOUR ENDPOINT>`

## Installation and Package Management

Bettensor now uses `uv`, a faster Python package manager, for improved installation speed. All our scripts are configured to use `uv` automatically, but will fall back to `pip` if `uv` cannot be installed. This ensures a smoother experience and faster dependency installation without breaking backward compatibility.

If you want to manually migrate an existing environment to use `uv`, we provide a migration script:

```bash
source scripts/migrate_to_uv.sh
```

This script will:
1. Install `uv` in your current virtual environment
2. Back up your current requirements
3. Reinstall dependencies using `uv` for improved performance

