# Guide for Validators

Running a validator is very simple for Bettensor. Use our setup script as follows:

1. Run `source ./scripts/start_neuron.sh` and follow prompts for validator. You can run this script with flags if you prefer not to enter prompts. To use a local subtensor, run `source scripts/start_neuron.sh --subtensor.chain_endpoint <YOUR ENDPOINT>`

## General Configuration

Beyond the database settings, several core configurations control the validator's behavior. The `scripts/start_neuron.sh` script provides an interactive way to set these, but they can also be provided as command-line arguments when running `neurons/validator.py` directly (e.g., via PM2).

Key configuration options include:

*   **Network (`--network` or `--subtensor.network`):**
    *   Specifies the Bittensor network to connect to (`finney`, `test`, `local`).
    *   Determines the default Subtensor endpoint and `netuid`.
    *   *Script default:* `finney`.
*   **Subtensor Endpoint (`--subtensor.chain_endpoint`):**
    *   Allows overriding the default Subtensor node endpoint for the selected network.
    *   Useful for connecting to a specific public node or your own local/lite node.
    *   *Script default:* Based on the selected network (e.g., `wss://entrypoint-finney.opentensor.ai:443` for `finney`).
*   **Wallet Name (`--wallet.name`):**
    *   The name of your coldkey wallet stored in `~/.bittensor/wallets/`.
    *   *Script default:* `default`.
*   **Hotkey Name (`--wallet.hotkey`):**
    *   The name of the hotkey associated with your coldkey wallet.
    *   *Script default:* `default`.
*   **Logging Level (`--logging.level` or `--logging.debug`, `--logging.trace`):**
    *   Controls the verbosity of the logs.
    *   Options: `info`, `debug`, `trace`.
    *   *Script default:* `debug`.

Refer to the `scripts/start_neuron.sh` script and the output of `python neurons/validator.py --help` for a full list of arguments.


## Database Configuration

Bettensor validators primarily use a PostgreSQL database to store prediction data, scoring information, and validator state. While SQLite is supported for basic functionality, **PostgreSQL is strongly recommended for performance and reliability**, especially for long-running validators.

The validator includes an automated setup process for PostgreSQL under specific conditions.

### Configuration Options

The database connection is configured using the following priority order:

1.  **Environment Variables (Highest Priority):**
    *   `BETTENSOR_DB_TYPE`: Set to `postgres` or `sqlite`.
    *   `BETTENSOR_DB_HOST`: PostgreSQL host (e.g., `localhost`).
    *   `BETTENSOR_DB_PORT`: PostgreSQL port (e.g., `5432`).
    *   `BETTENSOR_DB_USER`: PostgreSQL user (e.g., `postgres`).
    *   `BETTENSOR_DB_PASSWORD`: PostgreSQL password.
    *   `BETTENSOR_DB_NAME`: PostgreSQL database name (e.g., `bettensor_validator`).
    *   `BETTENSOR_DB_PATH`: Path for the SQLite file (if `BETTENSOR_DB_TYPE` is `sqlite`).
2.  **`./config/database.cfg` or `~/.bettensor/database.cfg` File (JSON Format):**
    A JSON file containing keys like `\"type\"`, `\"host\"`, `\"port\"`, `\"user\"`, `\"password\"`, `\"dbname\"`, `\"path\"`.
3.  **`setup.cfg` File:** Legacy configuration (lower priority).
4.  **Command Line Arguments:** Arguments like `--db_type`, `--postgres_host`, etc., passed during startup.
5.  **Defaults:** If no configuration is found, it defaults to SQLite at `./bettensor/validator/state/validator.db`.

### Automated PostgreSQL Setup

If you configure the validator to use PostgreSQL (e.g., `BETTENSOR_DB_TYPE=postgres`) and run the validator script **as the root user** on a supported Linux distribution (Debian/Ubuntu, Fedora/CentOS), the validator will attempt to automatically:

1.  **Install Packages:** Install `postgresql-client` and `postgresql` server using the detected package manager (`apt-get`, `dnf`, `yum`).
2.  **Start Service:** Start and enable the `postgresql` service using `systemctl` (if available).
3.  **Create Database & User:** Connect to the PostgreSQL server as the admin user (`postgres` by default, or the user specified by `PGADMIN_USER` env var) and:
    *   Create the target database (specified by `BETTENSOR_DB_NAME`, default `bettensor_validator`) if it doesn't exist.
    *   Create the target user (specified by `BETTENSOR_DB_USER`, default `postgres`) if it doesn't exist.
4.  **Set Default Password:** If the target user is created OR if the target user already exists but no password was specified in the configuration (`BETTENSOR_DB_PASSWORD` or `database.cfg`), the automation will set/reset the user's password to the default: **`postgres`**.
5.  **Grant Privileges:** Grant necessary privileges for the target user on the target database.

**Admin Credentials for Automation:**
The automated database/user creation step requires connecting as a PostgreSQL superuser/admin.
*   The script uses the user specified by the `PGADMIN_USER` environment variable (defaults to `postgres`).
*   It attempts passwordless connection first (for `peer`/`trust` auth).
*   If that fails, it uses the password from the `PGADMIN_PASSWORD` environment variable. If `PGADMIN_PASSWORD` is not set, it falls back to trying the main `BETTENSOR_DB_PASSWORD` (or the password from `database.cfg`).
*   **Ensure the specified admin user has privileges like `CREATE DATABASE`, `CREATE USER`, and `ALTER USER`.**

### Manual PostgreSQL Setup

If automated setup is not desired, fails, or you are not running as root, follow these steps:

1.  **Install PostgreSQL:**
    ```bash
    # Debian/Ubuntu
    sudo apt-get update
    sudo apt-get install -y postgresql postgresql-client

    # Fedora/CentOS
    sudo dnf install -y postgresql-server postgresql
    # (May require sudo postgresql-setup --initdb on first install)
    ```
2.  **Start & Enable Service:**
    ```bash
    sudo systemctl start postgresql
    sudo systemctl enable postgresql
    ```
3.  **Create Database User:** (Replace `your_secure_password`)
    ```bash
    sudo -u postgres psql -c "CREATE USER postgres WITH PASSWORD 'your_secure_password';"
    ```
    *   Alternatively, configure passwordless local access (e.g., `peer` auth) in `/etc/postgresql/{version}/main/pg_hba.conf` and reload PostgreSQL (`sudo systemctl reload postgresql`).
4.  **Create Database:**
    ```bash
    sudo -u postgres createdb bettensor_validator
    ```
5.  **Grant Privileges:**
    ```bash
    sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE bettensor_validator TO postgres;"
    ```
6.  **Configure Validator:** Set the environment variables (`BETTENSOR_DB_TYPE=postgres`, `BETTENSOR_DB_USER=postgres`, `BETTENSOR_DB_PASSWORD='your_secure_password'`, etc.) or update `database.cfg` accordingly.

### Data Migration

If you switch from SQLite to PostgreSQL (`migrate_to_postgres = True` in `setup.cfg` or `BETTENSOR_MIGRATE_TO_POSTGRES=true` env var), the validator will automatically attempt to:

1.  Stop the running validator process (if found).
2.  Migrate data from the existing SQLite database (`./bettensor/validator/state/validator.db` by default) to the configured PostgreSQL database.
3.  Update the configuration (`database.cfg`) to permanently use PostgreSQL.
4.  Restart the validator process.

Ensure PostgreSQL is set up (manually or via automation) *before* enabling the migration flag.

