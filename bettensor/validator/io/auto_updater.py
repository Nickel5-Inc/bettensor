import subprocess
import traceback
import bittensor as bt
import os
import json
import configparser
import time
import asyncio
import sys
from pathlib import Path
import shutil

async def check_and_install_dependencies():
    """Check and install required dependencies before code execution"""
    
    # --- Check/Install PostgreSQL Client --- 
    psql_found = False
    try:
        # Check if psql command is available
        process = await asyncio.create_subprocess_shell(
            'psql --version',
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL)
        await process.wait() 
        if process.returncode == 0:
            psql_found = True
            #bt.logging.debug("PostgreSQL client ('psql') found.")
        #else: psql not found, proceed to attempt install
            
    except Exception as e:
        bt.logging.warning(f"Error during initial psql check (will attempt install if possible): {e}")
        # Don't exit, attempt installation below

    if not psql_found:
        bt.logging.warning("PostgreSQL client ('psql') not found. Attempting automated installation...")
        
        try:
            is_root = os.geteuid() == 0
            if not is_root:
                bt.logging.error("Automated installation requires root privileges.")
                bt.logging.error("Please run the validator as root or install 'postgresql-client' manually.")
                # Print manual instructions before exiting
                bt.logging.error("Manual Installation Instructions:")
                bt.logging.error("  Debian/Ubuntu: sudo apt-get update && sudo apt-get install -y postgresql-client")
                bt.logging.error("  Fedora/CentOS: sudo dnf install -y postgresql")
                bt.logging.error("  macOS (Homebrew): brew install postgresql")
                sys.exit(1)
                
            # Running as root, detect package manager
            install_cmd = None
            package_manager_name = None
            
            if shutil.which('apt-get'):
                package_manager_name = 'apt-get'
                install_cmd = 'apt-get update && apt-get install -y postgresql-client'
            elif shutil.which('dnf'):
                package_manager_name = 'dnf'
                # Assuming dnf doesn't typically need an update command first for this scenario
                install_cmd = 'dnf install -y postgresql' 
            elif shutil.which('yum'):
                 package_manager_name = 'yum'
                 # Assuming yum doesn't typically need an update command first for this scenario
                 install_cmd = 'yum install -y postgresql'
                 
            if install_cmd:
                bt.logging.info(f"Detected {package_manager_name}. Running installation command: '{install_cmd}'")
                install_process = await asyncio.create_subprocess_shell(
                    install_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await install_process.communicate()
                
                if install_process.returncode == 0:
                    bt.logging.info("Automated installation command executed successfully.")
                    # Verify psql is now available
                    check_process = await asyncio.create_subprocess_shell(
                        'psql --version',
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL)
                    await check_process.wait()
                    if check_process.returncode == 0:
                        bt.logging.info("Successfully installed and verified PostgreSQL client ('psql').")
                        psql_found = True # Continue with other dependencies
                    else:
                        bt.logging.error("Installation command ran, but 'psql --version' still fails. Manual installation required.")
                        # stderr from install_process might have clues
                        if stderr:
                            bt.logging.error(f"Installation stderr: {stderr.decode()}")
                        # Print manual instructions again
                        bt.logging.error("Manual Installation Instructions:")
                        bt.logging.error("  Debian/Ubuntu: sudo apt-get update && sudo apt-get install -y postgresql-client")
                        bt.logging.error("  Fedora/CentOS: sudo dnf install -y postgresql")
                        bt.logging.error("  macOS (Homebrew): brew install postgresql")
                        sys.exit(1)
                else:
                    bt.logging.error(f"Automated installation command failed with exit code {install_process.returncode}.")
                    if stdout:
                         bt.logging.error(f"Installation stdout: {stdout.decode()}")
                    if stderr:
                        bt.logging.error(f"Installation stderr: {stderr.decode()}")
                    bt.logging.error("Manual installation required.")
                    # Print manual instructions
                    bt.logging.error("Manual Installation Instructions:")
                    bt.logging.error("  Debian/Ubuntu: sudo apt-get update && sudo apt-get install -y postgresql-client")
                    bt.logging.error("  Fedora/CentOS: sudo dnf install -y postgresql")
                    bt.logging.error("  macOS (Homebrew): brew install postgresql")
                    sys.exit(1)
            else:
                bt.logging.error("Could not detect a supported package manager (apt-get, dnf, yum) for automated installation.")
                # Print manual instructions
                bt.logging.error("Manual Installation Instructions:")
                bt.logging.error("  Debian/Ubuntu: sudo apt-get update && sudo apt-get install -y postgresql-client")
                bt.logging.error("  Fedora/CentOS: sudo dnf install -y postgresql")
                bt.logging.error("  macOS (Homebrew): brew install postgresql")
                sys.exit(1)

        except Exception as install_exc:
            bt.logging.error(f"An unexpected error occurred during the automated installation attempt: {install_exc}")
            # Print manual instructions
            bt.logging.error("Manual Installation Instructions:")
            bt.logging.error("  Debian/Ubuntu: sudo apt-get update && sudo apt-get install -y postgresql-client")
            bt.logging.error("  Fedora/CentOS: sudo dnf install -y postgresql")
            bt.logging.error("  macOS (Homebrew): brew install postgresql")
            sys.exit(1)
    # --- End PostgreSQL Client Check/Install ---

    # --- Original Core Dependency Checks --- 
    # (Only proceed if psql check/install was successful or not needed)
    if not psql_found: # Should have exited above if install failed
         bt.logging.critical("Internal logic error: psql check failed but script did not exit.")
         sys.exit(1) 
         
    try:
        # Core dependencies needed for update process
        core_deps = {
            "pip": "--upgrade pip",
            "setuptools": "--upgrade setuptools",
            "wheel": "--upgrade wheel"
        }
        
        for package, install_args in core_deps.items():
            try:
                await asyncio.to_thread(
                    lambda: subprocess.check_call(
                        [sys.executable, "-m", "pip", "show", package],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                )
            except subprocess.CalledProcessError:
                bt.logging.info(f"Installing core dependency: {package}")
                try:
                    await asyncio.to_thread(
                        lambda: subprocess.check_call(
                            [sys.executable, "-m", "pip", "install"] + install_args.split(),
                            stdout=subprocess.DEVNULL
                        )
                    )
                except subprocess.CalledProcessError as e:
                    bt.logging.error(f"Failed to install {package}: {e}")
                    return False
        
        return True
    except Exception as e:
        bt.logging.error(f"Error in dependency check: {e}")
        return False

async def install_requirements():
    """Install project requirements"""
    try:
        requirements_path = Path("requirements.txt")
        if not requirements_path.exists():
            bt.logging.error("requirements.txt not found")
            return False
            
        bt.logging.info("Installing project requirements...")
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "pip", "install", "-r", "requirements.txt",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            bt.logging.error(f"Failed to install requirements: {stderr.decode()}")
            return False
            
        return True
    except Exception as e:
        bt.logging.error(f"Error installing requirements: {e}")
        return False

async def get_current_branch():
    try:
        branch = await asyncio.to_thread(
            lambda: subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"]
            ).decode().strip()
        )
        return branch
    except subprocess.CalledProcessError as e:
        bt.logging.error(f"Failed to get current git branch: {e}")
        return None

async def get_remote_hash(branch):
    try:
        remote_hash = await asyncio.to_thread(
            lambda: subprocess.check_output(
                ["git", "rev-parse", f"origin/{branch}"]
            ).decode().strip()
        )
        return remote_hash
    except subprocess.CalledProcessError as e:
        bt.logging.error(f"Failed to get remote hash for branch {branch}: {e}")
        return None

async def get_local_hash():
    try:
        local_hash = await asyncio.to_thread(
            lambda: subprocess.check_output(
                ["git", "rev-parse", "HEAD"]
            ).decode().strip()
        )
        return local_hash
    except subprocess.CalledProcessError as e:
        bt.logging.error(f"Failed to get local git hash: {e}")
        return None

async def pull_latest_changes():
    try:
        current_branch = await get_current_branch()
        if not current_branch:
            bt.logging.error("Failed to get current branch.")
            return False
        
        # Fetch the latest changes
        await asyncio.to_thread(
            lambda: subprocess.check_call(["git", "fetch", "origin"])
        )
        
        # Get the hash of the current HEAD
        local_hash = await asyncio.to_thread(
            lambda: subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
        )
        
        # Get the hash of the remote branch
        remote_hash = await asyncio.to_thread(
            lambda: subprocess.check_output(["git", "rev-parse", f"origin/{current_branch}"]).decode().strip()
        )
        
        # Compare the hashes
        if local_hash == remote_hash:
            bt.logging.info("No new changes to pull.")
            return False
        
        # Stash any changes
        await asyncio.to_thread(
            lambda: subprocess.check_call(["git", "stash"])
        )
        
        # If hashes are different, pull the changes
        await asyncio.to_thread(
            lambda: subprocess.check_call(["git", "pull", "origin", current_branch])
        )
        
        # Get the new local hash after pulling
        new_local_hash = await asyncio.to_thread(
            lambda: subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
        )
        
        # Verify that changes were actually pulled
        if new_local_hash != local_hash:
            bt.logging.info(f"Successfully pulled latest changes from {current_branch} branch.")
            return True
        else:
            bt.logging.warning("Git pull was executed, but no changes were applied.")
            return False
        
    except subprocess.CalledProcessError as e:
        bt.logging.error(f"Failed to pull latest changes: {e}")
        return False

async def get_pm2_process_name():
    try:
        # Get the PM2 process ID from the environment variable
        pm_id = os.environ.get('pm_id')
        
        if pm_id is None:
            return None  # Not running under PM2
        
        # Use PM2 to get process info
        result = await asyncio.to_thread(
            lambda: subprocess.run(['pm2', 'jlist'], capture_output=True, text=True)
        )
        processes = json.loads(result.stdout)
        
        # Find the process with matching pm_id
        for process in processes:
            if str(process.get('pm_id')) == pm_id:
                return process.get('name')
        
        return None  # Process not found
    except Exception as e:
        print(f"Error getting PM2 process name: {e}")
        return None

async def perform_update(validator):
    """Enhanced update process with dependency management"""
    bt.logging.info("Starting update process...")
    
    # First check and install core dependencies
    if not await check_and_install_dependencies():
        bt.logging.error("Failed to install core dependencies")
        return False
        
    if await pull_latest_changes():
        try:
            # Install updated requirements
            if not await install_requirements():
                bt.logging.error("Failed to install updated requirements")
                return False
                
            # Check if scoring reset is needed
            if check_version_and_reset():
                try:
                    await validator.reset_scoring_system()
                    bt.logging.info("Scoring system reset successfully")
                    
                    config = configparser.ConfigParser()
                    config.read('setup.cfg')
                    config.set('metadata', 'reset_validator_scores', 'False')
                    with open('setup.cfg', 'w') as f:
                        config.write(f)
                        
                except Exception as e:
                    bt.logging.error(f"Failed to reset scoring system: {e}")
                    bt.logging.error(traceback.format_exc())
            
            # Handle PM2 restart
            process_name = await get_pm2_process_name()
            if process_name:
                bt.logging.info(f"Restarting PM2 process: {process_name}")
                success = await restart_pm2_process(process_name)
                if not success:
                    bt.logging.error("Failed to restart via PM2")
                    return False
            else:
                bt.logging.warning("Not running under PM2, manual restart may be required")
            
            return True
            
        except Exception as e:
            bt.logging.error(f"Error during update process: {e}")
            bt.logging.error(traceback.format_exc())
            return False
    
    return False

async def restart_pm2_process(process_name):
    """Handle PM2 process restart with retries"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            result = await asyncio.wait_for(
                asyncio.create_subprocess_exec(
                    "pm2", "restart", process_name, "--update-env",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                ),
                timeout=30
            )
            
            stdout, stderr = await result.communicate()
            
            if result.returncode == 0:
                bt.logging.info(f"PM2 restart successful on attempt {attempt + 1}")
                return True
                
            bt.logging.warning(f"PM2 restart failed on attempt {attempt + 1}")
            if attempt < max_retries - 1:
                await asyncio.sleep(5 * (attempt + 1))
                
        except asyncio.TimeoutError:
            bt.logging.error(f"PM2 restart timed out on attempt {attempt + 1}")
            if attempt < max_retries - 1:
                await asyncio.sleep(5 * (attempt + 1))
                
    return False

def check_version_and_reset():
    config = configparser.ConfigParser()
    try:
        config.read('setup.cfg')
        reset_scores = config.getboolean('metadata', 'reset_validator_scores', fallback=False)
        return reset_scores
    except configparser.Error as e:
        bt.logging.error(f"Error reading setup.cfg: {e}")
        return False
