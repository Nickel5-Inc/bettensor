import asyncio
import subprocess
import bittensor as bt
import asyncpg
import sys
import os
import shutil
from pathlib import Path

async def check_postgres_installed():
    """Check if psql command is available."""
    try:
        process = await asyncio.create_subprocess_shell(
            'psql --version',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            bt.logging.debug(f"PostgreSQL client (psql) found: {stdout.decode().strip()}")
            return True
        else:
            bt.logging.error(f"PostgreSQL client (psql) not found or command failed. Error: {stderr.decode()}")
            return False
    except FileNotFoundError:
        bt.logging.error("PostgreSQL client (psql) not found. Please install PostgreSQL client tools.")
        return False
    except Exception as e:
        bt.logging.error(f"Error checking for psql: {e}")
        return False

async def check_postgres_service(db_config):
    """Check if the PostgreSQL service is running, attempting install/start if needed and possible."""
    host = db_config.get('host', 'localhost')
    port = db_config.get('port', 5432)
    user = 'postgres' # Use default user for pg_isready check
    command = f'pg_isready -h {host} -p {port} -U {user} -t 2' 

    async def run_pg_isready():
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await process.communicate()
        return process.returncode, stdout.decode(), stderr.decode()

    try:
        return_code, stdout, stderr = await run_pg_isready()

        if return_code == 0:
            bt.logging.info(f"PostgreSQL service is accepting connections on {host}:{port}.")
            return True
        elif return_code == 1:
             bt.logging.error(f"PostgreSQL service on {host}:{port} is running but rejecting connections.")
             bt.logging.error(f"Check PostgreSQL configuration (pg_hba.conf). Output: {stdout} Stderr: {stderr}")
             bt.logging.error("Automated setup cannot fix configuration issues. Manual intervention required.")
             return False # Cannot fix this automatically
        elif return_code == 2 or return_code == 3:
             # No response or no attempt - Service likely not running
             bt.logging.warning(f"PostgreSQL service on {host}:{port} is not responding (pg_isready code {return_code}).")
             bt.logging.warning("Attempting automated PostgreSQL server installation and startup...")
             
             # --- Attempt Server Install/Start ---
             is_root = os.geteuid() == 0
             if not is_root:
                 bt.logging.error("Automated server installation/startup requires root privileges.")
                 bt.logging.error("Please run as root or install/start PostgreSQL manually.")
                 return False # Cannot proceed without root
             
             # Detect package manager
             install_cmd = None
             server_package = 'postgresql' # Common package name
             package_manager_name = None
            
             if shutil.which('apt-get'):
                 package_manager_name = 'apt-get'
                 install_cmd = f'apt-get update && apt-get install -y {server_package}'
             elif shutil.which('dnf'):
                 package_manager_name = 'dnf'
                 install_cmd = f'dnf install -y {server_package}'
             elif shutil.which('yum'):
                  package_manager_name = 'yum'
                  install_cmd = f'yum install -y {server_package}'
                  
             if install_cmd:
                 bt.logging.info(f"Detected {package_manager_name}. Installing/Updating PostgreSQL server: '{install_cmd}'")
                 install_process = await asyncio.create_subprocess_shell(
                     install_cmd,
                     stdout=asyncio.subprocess.PIPE,
                     stderr=asyncio.subprocess.PIPE
                 )
                 stdout_install, stderr_install = await install_process.communicate()
                 
                 if install_process.returncode == 0:
                     bt.logging.info("PostgreSQL server package installed/updated successfully.")
                     
                     # --- Modify pg_hba.conf for peer auth --- 
                     hba_conf_path = None
                     possible_hba_paths = []
                     # Find potential pg_hba.conf paths based on installed versions
                     if Path('/etc/postgresql/').exists():
                         for version_dir in Path('/etc/postgresql/').iterdir():
                             if version_dir.is_dir(): # e.g., /etc/postgresql/14/
                                 main_dir = version_dir / 'main'
                                 if main_dir.exists():
                                     possible_hba_paths.append(main_dir / 'pg_hba.conf')
                     
                     if not possible_hba_paths:
                          bt.logging.warning(f"Could not find PostgreSQL version directory in /etc/postgresql/ to locate pg_hba.conf automatically.")
                     else:
                          # Use the first found path (usually the latest or only version)
                          hba_conf_path = possible_hba_paths[0]
                          bt.logging.info(f"Attempting to configure peer authentication in {hba_conf_path}")
                          try:
                              with open(hba_conf_path, 'r') as f:
                                  lines = f.readlines()
                              
                              # Check if correct peer auth line already exists
                              peer_line = 'local   all             postgres                                peer\n'
                              found_peer_line = False
                              modified_lines = []
                              inserted = False

                              for line in lines:
                                   stripped_line = line.strip()
                                   # Check if this line defines local auth for postgres user
                                   if stripped_line.startswith('local') and 'postgres' in stripped_line.split()[:4]:
                                        # If it's the correct peer line, mark as found
                                        if stripped_line == peer_line.strip():
                                             found_peer_line = True
                                             modified_lines.append(line) # Keep the correct line
                                        else:
                                             # Comment out incorrect/conflicting postgres local auth lines (e.g., md5)
                                             bt.logging.warning(f"Commenting out existing conflicting line in pg_hba.conf: {stripped_line}")
                                             modified_lines.append(f'# {line}') 
                                   else:
                                        # Keep other lines
                                        modified_lines.append(line)
                                        # Insert the peer line before the first non-comment line if not found yet
                                        if not inserted and not found_peer_line and not stripped_line.startswith('#') and stripped_line:
                                             bt.logging.info("Prepending peer auth line for postgres user.")
                                             modified_lines.insert(-1, peer_line) # Insert before the current line
                                             inserted = True
                                             found_peer_line = True # Assume insertion worked

                              # If loop finished and line wasn't inserted (e.g., empty file or only comments), append it
                              if not found_peer_line:
                                   bt.logging.info("Appending peer auth line for postgres user.")
                                   modified_lines.append(peer_line)
                              
                              # Write the modified config back
                              with open(hba_conf_path, 'w') as f:
                                   f.writelines(modified_lines)
                              bt.logging.info(f"Successfully modified {hba_conf_path} for peer authentication.")
                              
                              # Reload PostgreSQL config
                              if shutil.which('systemctl'):
                                   reload_cmd = 'systemctl reload postgresql'
                                   bt.logging.info(f"Reloading PostgreSQL configuration: '{reload_cmd}' ... ")
                                   reload_proc = await asyncio.create_subprocess_shell(
                                        reload_cmd,
                                        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                                   )
                                   stdout_reload, stderr_reload = await reload_proc.communicate()
                                   if reload_proc.returncode != 0:
                                        bt.logging.warning(f"'systemctl reload postgresql' failed (code {reload_proc.returncode}). Service might need full restart or manual reload.")
                                        if stderr_reload:
                                             bt.logging.warning(f"Reload stderr: {stderr_reload.decode()}")
                                   else:
                                        bt.logging.info("PostgreSQL configuration reloaded successfully.")
                              else:
                                   bt.logging.warning("'systemctl' not found. Cannot automatically reload PostgreSQL config. Manual reload might be required.")
                                   
                          except Exception as hba_exc:
                               bt.logging.error(f"Failed to modify {hba_conf_path} for peer authentication: {hba_exc}")
                               bt.logging.error("Manual configuration of pg_hba.conf required.")
                               # Proceed to service start, maybe it was already configured correctly
                     # --- End Modify pg_hba.conf ---
                     
                     # Attempt to start and enable the service
                     if shutil.which('systemctl'):
                         service_name = 'postgresql' # Common service name
                         bt.logging.info(f"Attempting to start and enable service '{service_name}' using systemctl...")
                         start_cmd = f'systemctl start {service_name} && systemctl enable {service_name}'
                         service_process = await asyncio.create_subprocess_shell(
                             start_cmd,
                             stdout=asyncio.subprocess.PIPE,
                             stderr=asyncio.subprocess.PIPE
                         )
                         stdout_svc, stderr_svc = await service_process.communicate()
                         if service_process.returncode == 0:
                             bt.logging.info(f"Systemctl command for '{service_name}' executed successfully.")
                             await asyncio.sleep(3) # Give service a moment to start
                         else:
                             bt.logging.warning(f"Systemctl command for '{service_name}' failed (code {service_process.returncode}). Service might be masked or already running.")
                             if stderr_svc:
                                  bt.logging.warning(f"Systemctl stderr: {stderr_svc.decode()}")
                     else:
                         bt.logging.warning("'systemctl' not found. Cannot automatically start/enable PostgreSQL service.")

                     # Re-check service status
                     bt.logging.info("Re-checking PostgreSQL service status after installation attempt...")
                     return_code_after, stdout_after, stderr_after = await run_pg_isready()
                     if return_code_after == 0:
                         bt.logging.info(f"PostgreSQL service is now responding on {host}:{port}.")
                         return True
                     else:
                         bt.logging.error(f"PostgreSQL service still not responding correctly after installation/start attempt (pg_isready code {return_code_after}).")
                         bt.logging.error(f"Output: {stdout_after} Stderr: {stderr_after}")
                         bt.logging.error("Manual check of PostgreSQL service status and configuration required.")
                         return False
                 else:
                     bt.logging.error(f"Failed to install PostgreSQL server package using {package_manager_name} (code {install_process.returncode}).")
                     if stderr_install:
                         bt.logging.error(f"Installation stderr: {stderr_install.decode()}")
                     bt.logging.error("Manual installation required.")
                     return False
             else:
                 bt.logging.error("Could not detect a supported package manager (apt-get, dnf, yum) for automated server installation.")
                 bt.logging.error("Manual installation required.")
                 return False
             # --- End Attempt Server Install/Start ---
        else:
             # Unexpected pg_isready return code
             bt.logging.error(f"pg_isready command failed with unexpected code {return_code}. Output: {stdout} Stderr: {stderr}")
             return False
             
    except FileNotFoundError:
        # This implies pg_isready itself wasn't found, which should have been caught by the psql check in check_and_install_dependencies
        bt.logging.error("'pg_isready' command not found, though 'psql' was detected. This is unexpected.")
        bt.logging.error("Ensure PostgreSQL client tools are correctly installed and in PATH.")
        return False
    except Exception as e:
        bt.logging.error(f"Error checking PostgreSQL service status: {e}")
        return False

async def configure_postgres_db(db_config):
    """Attempt to create the database and user if they don't exist."""
    target_user = db_config['user']
    target_dbname = db_config['dbname']
    host = db_config.get('host', 'localhost') # Keep for potential non-root path
    port = db_config.get('port', 5432)     # Keep for potential non-root path
    
    # Determine the password to use/set: Use config password if provided, otherwise default to 'postgres'
    password_to_set = db_config.get('password') or 'postgres'
    if not db_config.get('password'):
        bt.logging.warning(f"No password specified in config for user '{target_user}'. Defaulting to 'postgres' for setup/reset.")

    is_root = os.geteuid() == 0
    # Check if sudo and postgres user exist if we are root
    can_use_sudo = is_root and shutil.which('sudo') and shutil.which('id')
    postgres_os_user_exists = False
    if can_use_sudo:
        try:
            check_user_proc = await asyncio.create_subprocess_shell(
                f'id -u postgres',
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout_usr, stderr_usr = await check_user_proc.communicate()
            if check_user_proc.returncode == 0:
                postgres_os_user_exists = True
            else:
                bt.logging.warning(f"OS user 'postgres' not found. Cannot use 'sudo -u postgres psql'. Error: {stderr_usr.decode()}")
        except Exception as e_check_user:
            bt.logging.warning(f"Error checking for OS user 'postgres': {e_check_user}")

    # --- Use sudo -u postgres psql if running as root --- 
    if is_root and can_use_sudo and postgres_os_user_exists:
        bt.logging.info("Running as root. Using 'sudo -u postgres psql' for database setup.")

        async def run_sudo_psql(command, check_output=False):
            # Escape single quotes in password for shell command safety
            safe_command = command.replace("'", "'\\''")
            # Base command connects to default 'postgres' db, -tA for tuples-only, unaligned output
            full_cmd = f"sudo -u postgres psql -tA -c '{safe_command}' postgres"
            bt.logging.debug(f"Executing: {full_cmd}") 
            process = await asyncio.create_subprocess_shell(
                full_cmd,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            stdout_str = stdout.decode().strip()
            stderr_str = stderr.decode().strip()

            if process.returncode != 0:
                bt.logging.error(f"'sudo -u postgres psql' command failed (code {process.returncode}): {safe_command}")
                if stderr_str:
                    bt.logging.error(f"stderr: {stderr_str}")
                if stdout_str:
                    bt.logging.error(f"stdout: {stdout_str}")
                return None # Indicate failure
            else:
                if stderr_str: # Log warnings/notices from stderr even on success
                    bt.logging.debug(f"psql stderr: {stderr_str}")
                if check_output:
                    return stdout_str
                else:
                    return True # Indicate success

        try:
            # Check if database exists
            db_exists_output = await run_sudo_psql(f"SELECT 1 FROM pg_database WHERE datname='{target_dbname}'", check_output=True)
            if db_exists_output is None: return {'success': False, 'password_used': password_to_set} # Command failed
            
            if db_exists_output != '1':
                bt.logging.info(f"Database '{target_dbname}' does not exist. Creating...")
                create_db_success = await run_sudo_psql(f'CREATE DATABASE "{target_dbname}"')
                if not create_db_success:
                    bt.logging.error("Failed to create database using sudo. Check logs and permissions.")
                    return {'success': False, 'password_used': password_to_set}
                bt.logging.info(f"Database '{target_dbname}' created successfully.")
            else:
                bt.logging.info(f"Database '{target_dbname}' already exists.")

            # Check if user exists
            user_exists_output = await run_sudo_psql(f"SELECT 1 FROM pg_roles WHERE rolname='{target_user}'", check_output=True)
            if user_exists_output is None: return {'success': False, 'password_used': password_to_set} # Command failed
            
            safe_password_to_set = password_to_set.replace("'", "''") # Escape single quotes for SQL
            
            if user_exists_output != '1':
                bt.logging.info(f"User '{target_user}' does not exist. Creating with specified/default password...")
                create_user_success = await run_sudo_psql(f'CREATE USER "{target_user}" WITH PASSWORD \'{safe_password_to_set}\'')
                if not create_user_success:
                     bt.logging.error("Failed to create user using sudo. Check logs and permissions.")
                     return {'success': False, 'password_used': password_to_set}
                bt.logging.info(f"User '{target_user}' created successfully.")
            else:
                 bt.logging.info(f"User '{target_user}' already exists.")
                 # If user exists AND no password was specified in config, reset to default.
                 if not db_config.get('password'):
                     bt.logging.warning(f"Resetting password for existing user '{target_user}' to default ('{password_to_set}')...")
                     alter_user_success = await run_sudo_psql(f'ALTER USER "{target_user}" WITH PASSWORD \'{safe_password_to_set}\'')
                     if not alter_user_success:
                         bt.logging.error("Failed to alter user password using sudo. Check logs and permissions.")
                         # Don't return False, maybe grants will still work
                     else:
                         bt.logging.info(f"Password for user '{target_user}' reset to default.")

            # Grant privileges
            bt.logging.info(f"Granting privileges on database '{target_dbname}' to user '{target_user}'...")
            grant_all_success = await run_sudo_psql(f'GRANT ALL PRIVILEGES ON DATABASE "{target_dbname}" TO "{target_user}"')
            grant_connect_success = await run_sudo_psql(f'GRANT CONNECT ON DATABASE "{target_dbname}" TO "{target_user}"')

            if grant_all_success and grant_connect_success:
                bt.logging.info(f"Privileges granted successfully.")
            else:
                bt.logging.error(f"Failed to grant privileges on database '{target_dbname}' to user '{target_user}' using sudo.")
                # Don't return False, application might still work if grants existed previously.

            return {'success': True, 'password_used': password_to_set} # Sudo path completed
        except Exception as e_sudo:
            bt.logging.error(f"An error occurred during 'sudo -u postgres psql' execution: {e_sudo}")
            return {'success': False, 'password_used': password_to_set}
            
    # --- Fallback/Non-Root Path (Original asyncpg logic) --- 
    else:
        if is_root:
            bt.logging.warning("Running as root, but 'sudo' or 'postgres' OS user unavailable. Falling back to direct connection attempt.")
        else:
             bt.logging.info("Not running as root. Attempting direct admin connection (requires pre-configured auth).")
             
        conn = None
        try:
            # Determine admin password to try (prefer env var, fall back to main config password)
            admin_user = os.environ.get('PGADMIN_USER', 'postgres')
            admin_db = 'postgres' # Standard admin database
            admin_password_to_try = os.environ.get('PGADMIN_PASSWORD', db_config.get('password'))
            
            # Try connecting without password first (for peer/trust auth configured for this user)
            bt.logging.info(f"Attempting admin connection to '{admin_db}' as '{admin_user}' (no password)... ")
            try:
                conn = await asyncpg.connect(
                    user=admin_user,
                    password=None, # Explicitly None
                    database=admin_db,
                    host=host,
                    port=port,
                    timeout=10 # Add a timeout
                )
                bt.logging.info("Admin connection successful (without password). Proceeding with configuration.")
            except (asyncpg.exceptions.InvalidPasswordError, asyncpg.exceptions.ClientCannotConnectError, ConnectionRefusedError) as e_nopass_connect:
                 bt.logging.warning(f"Admin connection without password failed ({type(e_nopass_connect).__name__}). Will try with password if available.")
                 conn = None # Ensure conn is None before next try
            except Exception as e_no_pass:
                 bt.logging.warning(f"Admin connection without password failed unexpectedly: {e_no_pass}")
                 conn = None
    
            # If no-password connection failed, try with password
            if conn is None and admin_password_to_try:
                bt.logging.info(f"Attempting admin connection to '{admin_db}' as '{admin_user}' (with password)... ")
                try:
                    conn = await asyncpg.connect(
                        user=admin_user,
                        password=admin_password_to_try,
                        database=admin_db,
                        host=host,
                        port=port,
                        timeout=10 # Add a timeout
                    )
                    bt.logging.info("Admin connection successful (with password). Proceeding with configuration.")
                except asyncpg.exceptions.InvalidPasswordError:
                     bt.logging.error(f"Invalid password provided for admin user '{admin_user}'. Cannot configure database.")
                     bt.logging.error("Check PGADMIN_PASSWORD env var or the main DB password if it's intended for the admin.")
                     sys.exit(1)
                except (asyncpg.exceptions.ClientCannotConnectError, ConnectionRefusedError) as e_pw_connect:
                     bt.logging.error(f"Could not connect to database at {host}:{port} as admin '{admin_user}' even with password: {e_pw_connect}")
                     bt.logging.error("Ensure the database service is running and accessible.")
                     sys.exit(1)
                except Exception as e_with_pass:
                     bt.logging.error(f"Admin connection with password failed unexpectedly: {e_with_pass}")
                     sys.exit(1)
            elif conn is None: # No password was available to try or no-password attempt failed
                 bt.logging.error(f"Admin connection failed. Neither passwordless nor password-based connection for admin user '{admin_user}' succeeded.")
                 bt.logging.error("Ensure PostgreSQL is running and check authentication methods in pg_hba.conf.")
                 bt.logging.error("If running non-root, ensure you have permissions or set PGADMIN_USER/PGADMIN_PASSWORD.")
                 sys.exit(1)
                 
            # --- If connection succeeded (either way), proceed --- 
            # (Existing asyncpg logic for DB/User/Grant checks and operations)
            # Check if target database exists
            db_exists = await conn.fetchval(f"SELECT 1 FROM pg_database WHERE datname = $1", target_dbname)
            if not db_exists:
                bt.logging.info(f"Database '{target_dbname}' does not exist. Creating...")
                try:
                    await conn.execute(f'CREATE DATABASE "{target_dbname}"')
                    bt.logging.info(f"Database '{target_dbname}' created successfully.")
                except Exception as create_db_e:
                    bt.logging.error(f"Failed to create database '{target_dbname}': {create_db_e}")
                    bt.logging.error("Please ensure the admin user has CREATE DATABASE privileges.")
                    # return False # Don't return false here, let it fail later if needed
                    sys.exit(1)
            else:
                bt.logging.info(f"Database '{target_dbname}' already exists.")
    
            # Check if target user exists
            user_exists = await conn.fetchval(f"SELECT 1 FROM pg_roles WHERE rolname = $1", target_user)
            
            safe_password_to_set = password_to_set # asyncpg handles parameterization safely

            if not user_exists:
                bt.logging.info(f"User '{target_user}' does not exist. Creating with specified/default password...")
                try:
                    await conn.execute(f'CREATE USER "{target_user}" WITH PASSWORD $1', safe_password_to_set)
                    bt.logging.info(f"User '{target_user}' created successfully.")
                except Exception as create_user_e:
                    bt.logging.error(f"Failed to create user '{target_user}': {create_user_e}")
                    bt.logging.error("Please ensure the admin user has CREATE USER privileges.")
                    sys.exit(1)
            else:
                 bt.logging.info(f"User '{target_user}' already exists.")
                 # If user exists AND no password was specified in config, reset to default.
                 if not db_config.get('password'):
                     bt.logging.warning(f"Resetting password for existing user '{target_user}' to default ('{password_to_set}')...")
                     try:
                         await conn.execute(f'ALTER USER "{target_user}" WITH PASSWORD $1', safe_password_to_set) 
                         bt.logging.info(f"Password for user '{target_user}' reset to default.")
                     except Exception as alter_user_e:
                         bt.logging.error(f"Failed to reset password for user '{target_user}': {alter_user_e}")
                         bt.logging.error("Check admin privileges. Manual password reset might be required.")
                         # Continue, maybe grants will still work.
    
            # Grant privileges on the target database to the target user
            try:
                bt.logging.info(f"Granting privileges on database '{target_dbname}' to user '{target_user}'...")
                await conn.execute(f'GRANT ALL PRIVILEGES ON DATABASE "{target_dbname}" TO "{target_user}"')
                await conn.execute(f'GRANT CONNECT ON DATABASE "{target_dbname}" TO "{target_user}"')
                bt.logging.info(f"Privileges granted successfully.")
            except Exception as grant_e:
                bt.logging.error(f"Failed to grant privileges on database '{target_dbname}' to user '{target_user}': {grant_e}")
                bt.logging.error("Manual privilege grant might be required.")
    
            # Return success and the password originally passed in (or default if none)
            return {'success': True, 'password_used': password_to_set} # Configuration attempted, proceed with normal connection
    
        except Exception as e:
            # General catch-all for other unexpected errors during config
            bt.logging.error(f"Failed during PostgreSQL auto-configuration steps: {e}")
            # Return failure but still include the password we were trying to use
            return {'success': False, 'password_used': password_to_set}
    
        finally:
            if conn:
                await conn.close() 