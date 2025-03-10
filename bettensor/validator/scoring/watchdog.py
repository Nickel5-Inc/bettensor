import threading
import os
import subprocess
import json
import bittensor as bt
import time
import traceback
import signal
from threading import Lock


class Watchdog:
    """
    Handles random hanging issues by restarting the PM2 process.
    """

    def __init__(self, validator, timeout):
        self.validator = validator
        self.timeout = timeout
        self.last_reset = time.time()
        self.lock = Lock()
        self.timer = threading.Timer(timeout, self._timeout_handler)
        self.timer.daemon = True
        self.timer.start()
        self.pm2_process_name = self.get_pm2_process_name()
        self.is_running = True

    def reset(self):
        """Reset the watchdog timer with thread safety"""
        with self.lock:
            if not self.is_running:
                return
            self.timer.cancel()
            self.last_reset = time.time()
            self.timer = threading.Timer(self.timeout, self._timeout_handler)
            self.timer.daemon = True
            self.timer.start()
            bt.logging.debug("Watchdog timer reset")

    def extend_timeout(self, additional_seconds):
        """Extends the current timeout by the specified number of seconds"""
        with self.lock:
            if not self.is_running:
                return
            current_time = time.time()
            time_elapsed = current_time - self.last_reset
            remaining_time = self.timeout - time_elapsed
            
            # Only extend if we have less than 5 minutes remaining
            if remaining_time < 300:
                self.timer.cancel()
                new_timeout = remaining_time + additional_seconds
                self.last_reset = current_time
                self.timer = threading.Timer(new_timeout, self._timeout_handler)
                self.timer.daemon = True
                self.timer.start()
                bt.logging.debug(f"Extended watchdog timeout by {additional_seconds} seconds")

    def handle_timeout(self):
        bt.logging.error("Watchdog timer expired. Restarting process...")
        self.restart_process()

    def restart_process(self):
        """Restart the PM2 process."""
        try:
            process_name = self.get_pm2_process_name()
            if not process_name:
                bt.logging.error("Could not determine PM2 process name, attempting default names...")
                # Try common validator process names
                for name in ["validator", "validator0", "validator1"]:
                    try:
                        result = subprocess.run(["pm2", "describe", name], check=True, capture_output=True, text=True)
                        if result.returncode == 0:
                            process_name = name
                            break
                    except subprocess.CalledProcessError:
                        continue

            if process_name:
                bt.logging.info(f"Attempting to restart PM2 process: {process_name}")
                
                # Simply restart the process
                try:
                    subprocess.run(["pm2", "restart",process_name,"--update-env"], check=True, timeout=30, capture_output=True)
                    bt.logging.info(f"PM2 process {process_name} restarted successfully.")
                    
                    # Give PM2 a moment to restart
                    time.sleep(2)
                    
                    # Verify process restarted
                    result = subprocess.run(["pm2", "pid", process_name], capture_output=True, text=True)
                    if result.stdout.strip():
                        bt.logging.info("Process restarted successfully.")
                        os._exit(0)
                    else:
                        bt.logging.error("Process failed to restart.")
                except subprocess.CalledProcessError as e:
                    bt.logging.error(f"Failed to restart PM2 process {process_name}: {e}")
                    raise e
            else:
                bt.logging.error("Could not determine PM2 process name. Manual restart required.")
                
        except Exception as e:
            bt.logging.error(f"Failed to restart PM2 process: {str(e)}")
            bt.logging.error(traceback.format_exc())

        # If we get here, something went wrong with the restart
        bt.logging.error("Process restart failed, attempting force kill...")
        os.kill(os.getpid(), signal.SIGKILL)

    def get_pm2_process_name(self):
        """Get the current PM2 process name by checking all validator processes"""
        try:
            result = subprocess.run(["pm2", "jlist"], capture_output=True, text=True, check=True)
            processes = json.loads(result.stdout)
            
            # Get current process ID
            current_pid = os.getpid()
            
            for proc in processes:
                # Check if this is a validator process and matches our PID
                if (proc.get("name", "").startswith("validator") and 
                    str(proc.get("pid")) == str(current_pid)):
                    return proc.get("name")
                    
            bt.logging.error(f"Could not find matching PM2 process for PID {current_pid}")
            return None
            
        except Exception as e:
            bt.logging.error(f"Error fetching PM2 process list: {e}")
            bt.logging.error(traceback.format_exc())
            return None

    def _timeout_handler(self):
        """Internal timeout handler that checks running state"""
        with self.lock:
            if not self.is_running:
                return
            bt.logging.error("Watchdog timer expired. Cleaning up resources...")
            self.restart_process()

    def cleanup(self):
        """Cleanup method for proper shutdown"""
        with self.lock:
            self.is_running = False
            if self.timer:
                self.timer.cancel()
                self.timer = None
            bt.logging.debug("Watchdog cleaned up")
