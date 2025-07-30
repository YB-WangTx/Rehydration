#!/usr/bin/env python3
import subprocess
import logging
import time
import json
import yaml
import sys
import argparse
import tempfile
import os
from datetime import datetime

# Load configuration
try:
    with open("yba_config.yaml", "r") as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    print("Error: yba_config.yaml not found. Please ensure it's in the same directory.", file=sys.stderr)
    sys.exit(1)
except yaml.YAMLError as e:
    print(f"Error parsing yba_config.yaml: {e}", file=sys.stderr)
    sys.exit(1)

# Global configuration - Directly from yba_config.yaml template
PROJECT = config["project_id"] 
NEW_IMAGE = config.get("new_image") 
IMAGE_PROJECT = config.get("image_project")
DISK_TYPE = config.get("disk_type")
DISK_SIZE = config.get("disk_size")
SSH_USER = config["ssh_user"] # Global SSH user from top-level config
WAIT_TIME = config["ssh_wait_time"] # Global SSH wait time from top-level config

YBA_CLI = config["yba"]["yba_cli_path"]
YBA_HOST = config["yba"]["yba_host"]
YBA_TOKEN = config["yba"]["yba_api_token"]
YBA_UNIVERSE = config["yba"]["universe_name"]

# Setup logging
log_file = f"yba_control_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file)]
)

def validate_config(config, action):
    """Validate the configuration file based on the requested action."""
    # Base YBA CLI fields
    yba_base_fields = [
        "yba_host", "yba_api_token", "universe_name", "yba_cli_path"
    ]
    if "yba" not in config:
        raise ValueError("Missing 'yba' section in yba_config.yaml")
    for field in yba_base_fields:
        if field not in config["yba"]:
            raise ValueError(f"Missing required YBA field: {field} in yba_config.yaml")

    # Top-level required fields
    required_top_level = ["project_id", "ssh_user", "ssh_wait_time"]
    for field in required_top_level:
        if field not in config or not config[field]:
            raise ValueError(f"Missing or empty top-level field: '{field}' in yba_config.yaml.")

    # Node list for pause/resume
    if action in ["pause", "resume"]:
        if "node_list" not in config["yba"] or not config["yba"]["node_list"]:
            raise ValueError("Missing or empty 'yba.node_list' in yba_config.yaml, required for pause/resume actions.")
        for i, node in enumerate(config["yba"]["node_list"]):
            required_node_fields = ["ip", "yba_node_name", "zone"]
            if not isinstance(node, dict): # Check for empty list item (like '- ')
                raise ValueError(f"Invalid entry at index {i} in 'yba.node_list'. Expected a dictionary, found: {node}")
            for field in required_node_fields:
                if field not in node or not node[field]:
                    raise ValueError(f"Node {i} in 'yba.node_list' missing or empty required field: {field}")

    # Node management for add/remove/precheck
    if action in ["add-node", "remove-node", "precheck-nodes"]: # Added precheck-nodes
        if "node_management" not in config["yba"]:
            raise ValueError("Missing 'yba.node_management' section in yba_config.yaml required for node operations.")

        if action == "add-node":
            add_nodes_list = config["yba"]["node_management"].get("add")
            if not isinstance(add_nodes_list, list) or not add_nodes_list:
                raise ValueError("Expected 'yba.node_management.add' to be a non-empty list of nodes for add-node/precheck-nodes action.")
            for i, node in enumerate(add_nodes_list):
                if not isinstance(node, dict): # Check for empty list item (like '- ')
                    raise ValueError(f"Invalid entry at index {i} in 'yba.node_management.add'. Expected a dictionary, found: {node}")
                # Required fields for preparing the VM (not for YBA CLI 'add' command anymore)
                required_add_fields_for_vm_prep = [
                    "ip", "yba_node_name", "zone" 
                ] 
                for field in required_add_fields_for_vm_prep:
                    if field not in node or (isinstance(node[field], str) and not node[field].strip()): # Check for empty string
                        raise ValueError(f"Node {i} in 'yba.node_management.add' missing or empty required field: {field} for VM preparation.")

        elif action == "remove-node":
            remove_nodes_list = config["yba"]["node_management"].get("remove", []) # Now expects a list
            if not isinstance(remove_nodes_list, list) or not remove_nodes_list:
                raise ValueError("Expected 'yba.node_management.remove' to be a non-empty list of nodes for remove-node action.")
            for i, node in enumerate(remove_nodes_list):
                if not isinstance(node, dict):
                    raise ValueError(f"Invalid entry at index {i} in 'yba.node_management.remove'. Expected a dictionary, found: {node}")
                if "yba_node_name" not in node or not node["yba_node_name"]:
                    raise ValueError(f"Node {i} in 'yba.node_management.remove' missing or empty 'yba_node_name' field.")
            
    logging.info(f"✅ Configuration validation passed for action: {action}")

def run(cmd, capture_output=False):
    """Run a shell command and return the CompletedProcess object."""
    logging.info(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, text=True, capture_output=capture_output, check=True)
        return result
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed: {e.cmd}")
        logging.error(f"Stdout: {e.stdout}")
        logging.error(f"Stderr: {e.stderr}")
        raise

# --- GCP VM Control Functions ---
def resolve_instance_name_and_zone(ip):
    """Resolve instance name and zone from IP address."""
    completed_process = run([
        "gcloud", "compute", "instances", "list",
        "--filter", f"networkInterfaces[0].networkIP={ip}",
        "--format", "json",
        "--project", PROJECT
    ], capture_output=True)
    data = json.loads(completed_process.stdout)
    if not data:
        raise RuntimeError(f"No instance found for IP {ip}")
    return data[0]["name"], data[0]["zone"].split("/")[-1]

def wait_for_status(instance_name, zone, expected, timeout=300):
    """Wait for instance to reach expected status."""
    logging.info(f"Waiting for instance '{instance_name}' to reach status: {expected} (timeout: {timeout}s)")
    start_time = time.time()
    while time.time() - start_time < timeout:
        completed_process = run([
            "gcloud", "compute", "instances", "describe", instance_name,
            "--zone", zone, "--project", PROJECT, "--format=json"
        ], capture_output=True)
        current_status = json.loads(completed_process.stdout).get("status")
        if current_status == expected:
            logging.info(f"Instance '{instance_name}' reached status: {expected}")
            return
        logging.info(f"Current status: {current_status}. Retrying in 10 seconds...")
        time.sleep(10)
    raise TimeoutError(f"Timeout waiting for instance status '{expected}' for {instance_name}")

def stop_instance_gcp(instance_name, zone):
    """Stops a GCP VM instance."""
    logging.info(f"Stopping GCP instance '{instance_name}' in zone '{zone}'...")
    run(["gcloud", "compute", "instances", "stop", instance_name, "--zone", zone, "--project", PROJECT])
    wait_for_status(instance_name, zone, "TERMINATED")
    logging.info(f"✅ GCP instance '{instance_name}' is TERMINATED.")

def start_instance_gcp(instance_name, zone):
    """Starts a GCP VM instance."""
    logging.info(f"Starting GCP instance '{instance_name}' in zone '{zone}'...")
    run(["gcloud", "compute", "instances", "start", instance_name, "--zone", zone, "--project", PROJECT])
    wait_for_status(instance_name, zone, "RUNNING")
    logging.info(f"✅ GCP instance '{instance_name}' is RUNNING.")

# --- YBA CLI Operations (Original) ---
def pause_universe():
    """Pauses the YugabyteDB universe by stopping its underlying GCP VMs in parallel."""
    logging.info(f"Attempting to pause universe '{YBA_UNIVERSE}' by stopping GCP VMs in parallel...")
    
    node_list = config["yba"]["node_list"]
    processes = []
    
    # 1. Initiate stop commands in parallel
    for node_info in node_list:
        instance_name, zone = resolve_instance_name_and_zone(node_info['ip'])
        logging.info(f"Initiating stop for instance '{instance_name}' (IP: {node_info['ip']}) in zone '{zone}'...")
        cmd = ["gcloud", "compute", "instances", "stop", instance_name, "--zone", zone, "--project", PROJECT, "--quiet"]
        process = subprocess.Popen(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        processes.append({'name': instance_name, 'zone': zone, 'process': process})
    
    # 2. Wait for all stop commands to complete
    for p_info in processes:
        stdout, stderr = p_info['process'].communicate()
        if p_info['process'].returncode != 0:
            logging.error(f"❌ Failed to send stop command for '{p_info['name']}': {stderr}")
            raise RuntimeError(f"Failed to send stop command for {p_info['name']}")
        else:
            logging.info(f"Command to stop instance '{p_info['name']}' sent successfully.")

    # 3. Wait for all instances to reach TERMINATED status
    for p_info in processes:
        wait_for_status(p_info['name'], p_info['zone'], "TERMINATED")
    
    logging.info(f"✅ Universe '{YBA_UNIVERSE}' successfully paused (all VMs terminated).")

def resume_universe():
    """Resumes the YugabyteDB universe by starting its underlying GCP VMs in parallel."""
    logging.info(f"Attempting to resume universe '{YBA_UNIVERSE}' by starting GCP VMs in parallel...")
    
    node_list = config["yba"]["node_list"]
    processes = []
    
    # 1. Initiate start commands in parallel
    for node_info in node_list:
        instance_name, zone = resolve_instance_name_and_zone(node_info['ip'])
        logging.info(f"Initiating start for instance '{instance_name}' (IP: {node_info['ip']}) in zone '{zone}'...")
        cmd = ["gcloud", "compute", "instances", "start", instance_name, "--zone", zone, "--project", PROJECT, "--quiet"]
        process = subprocess.Popen(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        processes.append({'name': instance_name, 'zone': zone, 'process': process})
    
    # 2. Wait for all start commands to complete
    for p_info in processes:
        stdout, stderr = p_info['process'].communicate()
        if p_info['process'].returncode != 0:
            logging.error(f"❌ Failed to send start command for '{p_info['name']}': {stderr}")
            raise RuntimeError(f"Failed to send start command for {p_info['name']}")
        else:
            logging.info(f"Command to start instance '{p_info['name']}' sent successfully.")

    # 3. Wait for all instances to reach RUNNING status
    for p_info in processes:
        wait_for_status(p_info['name'], p_info['zone'], "RUNNING")
    
    logging.info(f"✅ Universe '{YBA_UNIVERSE}' successfully resumed (all VMs running).")


def reprovision_and_start_node(node_name, node_ip=None, node_zone=None, node_ssh_user=None):
    """
    Reprovisions (configures/installs YB on) and starts a YB node via YBA CLI.
    Used for both existing nodes (rehydration) and adding new pre-existing VMs.
    """
    logging.info(f"Initiating YBA reprovision for node '{node_name}'...")
    cmd_reprovision = [
        YBA_CLI, "universe", "node", "reprovision",
        "-H", YBA_HOST, "-a", YBA_TOKEN,
        "--name", YBA_UNIVERSE,
        "--node-name", node_name
    ]
    if node_ip and node_zone and node_ssh_user:
        logging.info(f"Adding new node '{node_name}' at IP '{node_ip}' in zone '{node_zone}' with SSH user '{node_ssh_user}'.")
        cmd_reprovision.extend([
            "--node-ip", node_ip,
            "--zone", node_zone,
            "--ssh-user", node_ssh_user
        ])
    
    # Handle --insecure or --cacert for YBA CLI calls
    if config["yba"].get("use_insecure_cli", False):
        cmd_reprovision.append("--insecure")
    elif config["yba"].get("yba_ca_cert_path"):
        cmd_reprovision.extend(["--cacert", config["yba"]["yba_ca_cert_path"]])

    try:
        run(cmd_reprovision)
        logging.info(f"Reprovision command sent for node '{node_name}'. Waiting 10 seconds for it to settle.")
        time.sleep(10)
        
        logging.info(f"Starting node '{node_name}' via YBA CLI.")
        cmd_start = [
            YBA_CLI, "universe", "node", "start",
            "-H", YBA_HOST, "-a", YBA_TOKEN,
            "--name", YBA_UNIVERSE,
            "--node-name", node_name
        ]
        if config["yba"].get("use_insecure_cli", False):
            cmd_start.append("--insecure")
        elif config["yba"].get("yba_ca_cert_path"):
            cmd_start.extend(["--cacert", config["yba"]["yba_ca_cert_path"]])

        run(cmd_start)
        logging.info(f"✅ Reprovision and start command sent for node '{node_name}'.")
    except subprocess.CalledProcessError as e:
        error_msg = f"❌ Failed to reprovision/start node '{node_name}': {e.stderr}"
        logging.error(error_msg)
        raise RuntimeError(error_msg)


def provision_node_agent_via_ssh_as_user_with_sudo(instance_name, zone):
    """
    Provisions the node agent on a new VM via SSH as the global SSH_USER, using sudo.
    This function performs a *minimal* setup, assuming yugabyte user/group/home are pre-configured.
    Assumes: /data is mounted, /data/2024.2.2.2-b2/scripts/node-agent-provision.sh exists and is executable.
    """
    logging.info(f"Attempting to provision node agent for {instance_name} via SSH as user '{SSH_USER}' (using sudo - minimal setup)...")
    
    NODE_AGENT_SCRIPT_DIR = "/data/2024.2.2.2-b2/scripts"
    NODE_AGENT_SCRIPT_NAME = "node-agent-provision.sh"
    FULL_NODE_AGENT_SCRIPT_PATH = f"{NODE_AGENT_SCRIPT_DIR}/{NODE_AGENT_SCRIPT_NAME}"

    cmd = " && ".join([
        "echo '🚀 Running node-agent-provision.sh (minimal setup)...'",
        # Basic checks before CD and execution
        f"if ! mountpoint -q /data; then echo 'ERROR: /data is not mounted, cannot provision agent.' >&2; exit 1; fi",
        f"if [ ! -d '{NODE_AGENT_SCRIPT_DIR}' ]; then echo 'ERROR: {NODE_AGENT_SCRIPT_DIR} not found. Cannot provision agent.' >&2; exit 1; fi",
        f"if [ ! -f '{FULL_NODE_AGENT_SCRIPT_PATH}' ]; then echo 'ERROR: {FULL_NODE_AGENT_SCRIPT_PATH} not found. Cannot run provision script.' >&2; exit 1; fi",
        
        # Execute the script using sudo sh -c to handle cd and relative path in one sudo call
        f"sudo sh -c 'cd {NODE_AGENT_SCRIPT_DIR} && ./{NODE_AGENT_SCRIPT_NAME}'"
    ])
    try:
        run([
            "gcloud", "compute", "ssh", f"{SSH_USER}@{instance_name}",
            "--zone", zone, "--project", PROJECT,
            "--internal-ip", "--command", cmd
        ])
        logging.info(f"✅ Node agent provisioning command sent for {instance_name} via SSH as user '{SSH_USER}' (minimal setup).")
    except subprocess.CalledProcessError as e:
        error_msg = f"❌ Failed to provision node agent for {instance_name} via SSH as user '{SSH_USER}' (minimal setup): {e.stderr}"
        logging.error(error_msg)
        raise RuntimeError(error_msg)

def provision_new_node_full_setup_via_ssh(instance_name, zone):
    """
    Provisions a new node by setting up the yugabyte user/group/home,
    and then running node-agent-provision.sh via SSH as the global SSH_USER (with sudo).
    This assumes a relatively clean OS image and performs full user/dir setup.
    This function is kept separate for potential rehydration use cases.
    """
    logging.info(f"Attempting full setup and agent provisioning for {instance_name} via SSH as user '{SSH_USER}' (using sudo - full setup)...")
    
    NODE_AGENT_SCRIPT_DIR = "/data/2024.2.2.2-b2/scripts"
    NODE_AGENT_SCRIPT_NAME = "node-agent-provision.sh"
    FULL_NODE_AGENT_SCRIPT_PATH = f"{NODE_AGENT_SCRIPT_DIR}/{NODE_AGENT_SCRIPT_NAME}"

    # Full set of commands for user/group/directory setup, then script execution
    cmd = " && ".join([
        "echo '🚀 Performing full Yugabyte user/group setup and running node-agent-provision.sh...'",
        "sudo pkill -u yugabyte || true", # Kill any old YB processes
        "sudo id yugabyte && sudo userdel -r yugabyte || true", # Delete user if exists
        "sudo getent group yugabyte && (sudo getent passwd | awk -F: '$4 == \"$(sudo getent group yugabyte | cut -d: -f3)\"' | grep -q . || sudo groupdel yugabyte) || true", # Delete group if exists and no other user in it
        "sudo useradd -m -d /data/home/yugabyte -s /bin/bash yugabyte", # Create user
        "sudo mkdir -p /data/home/yugabyte", # Ensure home dir exists (useradd -m does this, but for idempotency)
        "sudo chown -R yugabyte:yugabyte /data/home/yugabyte", # Set ownership
        "sudo chmod 755 /data/home/yugabyte", # Set permissions

        # Assuming /data is mounted and contains YB binaries for node-agent-provision.sh
        f"if ! mountpoint -q /data; then echo 'ERROR: /data is not mounted, cannot provision agent.' >&2; exit 1; fi",
        f"if [ ! -d '{NODE_AGENT_SCRIPT_DIR}' ]; then echo 'ERROR: {NODE_AGENT_SCRIPT_DIR} not found. Cannot provision agent.' >&2; exit 1; fi",
        f"if [ ! -f '{FULL_NODE_AGENT_SCRIPT_PATH}' ]; then echo 'ERROR: {FULL_NODE_AGENT_SCRIPT_PATH} not found. Cannot run provision script.' >&2; exit 1; fi",
        
        # Execute the script: sudo sh -c to ensure cd and relative path execution under sudo context
        f"sudo sh -c 'cd {NODE_AGENT_SCRIPT_DIR} && ./{NODE_AGENT_SCRIPT_NAME}'"
    ])
    try:
        run([
            "gcloud", "compute", "ssh", f"{SSH_USER}@{instance_name}",
            "--zone", zone, "--project", PROJECT,
            "--internal-ip", "--command", cmd
        ])
        logging.info(f"✅ Node agent full setup and provisioning command sent for {instance_name} via SSH as user '{SSH_USER}'.")
    except subprocess.CalledProcessError as e:
        error_msg = f"❌ Failed to perform full node agent setup and provisioning for {instance_name} via SSH as user '{SSH_USER}': {e.stderr}"
        logging.error(error_msg)
        raise RuntimeError(error_msg)


def _get_current_universe_node_count():
    """Retrieves the current number of nodes in the YBA universe."""
    logging.info(f"Retrieving current node count for universe '{YBA_UNIVERSE}'...")
    cmd = [
        YBA_CLI, "universe", "get",
        "-H", YBA_HOST, "-a", YBA_TOKEN,
        "--name", YBA_UNIVERSE,
        "--output", "json"
    ]
    # Handle --insecure or --cacert for YBA CLI calls
    if config["yba"].get("use_insecure_cli", False):
        cmd.append("--insecure")
    elif config["yba"].get("yba_ca_cert_path"):
        cmd.extend(["--cacert", config["yba"]["yba_ca_cert_path"]])

    try:
        completed_process = run(cmd, capture_output=True)
        raw_json_output = completed_process.stdout.strip()
        universe_info = json.loads(raw_json_output)
        
        # Access numNodes which is explicitly shown under 'resources' in the provided JSON output
        if 'resources' in universe_info and 'numNodes' in universe_info['resources']:
            node_count = universe_info['resources']['numNodes']
            logging.info(f"Current universe '{YBA_UNIVERSE}' has {node_count} nodes (from 'resources.numNodes').")
            return node_count
        else:
            # Provide more specific error message and dump only the relevant part of output
            error_details = {
                "top_level_keys": list(universe_info.keys()),
                "resources_keys": list(universe_info.get('resources', {}).keys()) if 'resources' in universe_info else []
            }
            raise RuntimeError(f"Could not determine current node count from YBA universe get output for '{YBA_UNIVERSE}'. "
                               f"Expected 'resources.numNodes'. Actual top-level keys: {error_details['top_level_keys']}. "
                               f"Actual resources keys: {error_details['resources_keys']}. "
                               f"Full output start: {raw_json_output[:200]}... full output end: {raw_json_output[-200:] if len(raw_json_output) > 200 else raw_json_output}")
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        error_msg = f"❌ Failed to get current universe node count: {e.stderr if isinstance(e, subprocess.CalledProcessError) else e}"
        logging.error(error_msg)
        raise RuntimeError(error_msg)


def run_preflight_check_via_ssh(instance_name, zone):
    """
    Runs the preflight_check.sh script on the specified VM via SSH as global SSH_USER (with sudo).
    It will pass required arguments to the preflight script.
    """
    logging.info(f"Attempting to run preflight_check.sh on {instance_name} via SSH as user '{SSH_USER}' (using sudo)...")
    
    NODE_AGENT_SCRIPT_DIR = "/data/2024.2.2.2-b2/scripts" # Same directory as node-agent-provision.sh
    PREFLIGHT_SCRIPT_NAME = "preflight_check.sh"
    FULL_PREFLIGHT_SCRIPT_PATH = f"{NODE_AGENT_SCRIPT_DIR}/{PREFLIGHT_SCRIPT_NAME}"

    # Define arguments for preflight_check.sh
    # Assuming '/data' is the primary mount point and '/data/home/yugabyte' is the YB home dir.
    PREFLIGHT_ARGS = "--type provision --mount_points /data --yb_home_dir /data/home/yugabyte --ssh_port 22" 

    # Construct the remote command. All checks and execution will be within the sudo sh -c block.
    # This ensures consistency of environment and privileges.
    cmd = f"""sudo sh -c '
        echo "🚀 Running preflight_check.sh..."

        if ! mountpoint -q /data; then
            echo "ERROR: /data is not mounted, cannot run preflight check." >&2
            exit 1
        fi

        if [ ! -d "{NODE_AGENT_SCRIPT_DIR}" ]; then
            echo "ERROR: {NODE_AGENT_SCRIPT_DIR} not found. Cannot run preflight check." >&2
            exit 1
        fi

        if [ ! -f "{FULL_PREFLIGHT_SCRIPT_PATH}" ]; then
            echo "ERROR: {FULL_PREFLIGHT_SCRIPT_PATH} not found. Cannot run provision script." >&2
            exit 1
        fi
        
        # Execute the script with its required arguments
        bash -c "cd {NODE_AGENT_SCRIPT_DIR} && ./{PREFLIGHT_SCRIPT_NAME} {PREFLIGHT_ARGS}"
    '""" # The entire block is wrapped in sudo sh -c '...'

    try:
        completed_process = run([ # Capture output to report pre-check results
            "gcloud", "compute", "ssh", f"{SSH_USER}@{instance_name}",
            "--zone", zone, "--project", PROJECT,
            "--internal-ip", "--command", cmd
        ], capture_output=True)
        
        logging.info(f"✅ Preflight check command sent for {instance_name}. Output:\n{completed_process.stdout}")
        
    except subprocess.CalledProcessError as e:
        error_msg = f"❌ Preflight check FAILED for {instance_name} via SSH as user '{SSH_USER}'. " \
                    f"Stdout: {e.stdout.strip() if e.stdout else 'None'}. " \
                    f"Stderr: {e.stderr.strip() if e.stderr else 'None'}."
        logging.error(error_msg)
        raise RuntimeError(error_msg)


def add_nodes_with_scaling(): # Renamed from add_node
    """Adds new pre-existing VMs as nodes to the YugabyteDB universe and scales the universe."""
    add_nodes_list = config["yba"]["node_management"].get("add", [])
    
    if not add_nodes_list:
        logging.info("No nodes specified in 'yba.node_management.add' to add. Skipping add-node operation.")
        return

    logging.info(f"Attempting to add {len(add_nodes_list)} new pre-existing VM(s) to universe '{YBA_UNIVERSE}'.")
    
    # Store successfully prepared node names for the scaling step
    successfully_prepared_this_run = []

    for node_config in add_nodes_list:
        if not node_config or not isinstance(node_config, dict):
            logging.warning(f"Skipping invalid (likely empty) node entry in 'yba.node_management.add': {node_config}")
            continue

        node_ip = node_config["ip"]
        node_name = node_config["yba_node_name"]
        node_zone = node_config["zone"]
        # ssh_user is taken from global SSH_USER for all SSH ops in this script

        logging.info(f"\n--- Processing new node '{node_name}' (IP: {node_ip}) ---")
        
        try:
            instance_name, _ = resolve_instance_name_and_zone(node_ip)
            
            # 1. Wait for instance to become RUNNING and SSH ready for the user
            logging.info(f"Waiting for '{instance_name}' to be RUNNING and SSH ready for user '{SSH_USER}'...")
            wait_for_status(instance_name, node_zone, "RUNNING", timeout=WAIT_TIME)

            # 2. Run node-agent-provision.sh via SSH as the global SSH_USER, using sudo (minimal setup)
            # This is the point where the `node-agent-provision.sh` is executed on the new VM.
            logging.info(f"Executing node-agent-provision.sh on '{instance_name}' via SSH as user '{SSH_USER}' (minimal setup)...")
            provision_node_agent_via_ssh_as_user_with_sudo(instance_name, node_zone) # CALLING THE MINIMAL SETUP FUNCTION

            # No explicit 'yba universe node add' command here, as the scaling command handles discovery.
            
            logging.info(f"✅ Node '{node_name}' prepared and provision script executed.")
            successfully_prepared_this_run.append(node_name) # Add to list for scaling
        except Exception as e:
            logging.error(f"❌ Failed to prepare node '{node_name}' (IP: {node_ip}): {e}")
            raise RuntimeError(f"Failed to prepare node {node_name}: {e}")

    # --- Preflight Checks for newly prepared nodes ---
    if successfully_prepared_this_run:
        logging.info("\n--- Running preflight checks on newly prepared nodes ---")
        for node_name in successfully_prepared_this_run:
            # Need node_ip and node_zone for precheck
            # Retrieve node_config again from the original list as it contains IP
            node_config = next((item for item in config["yba"]["node_management"]["add"] if item["yba_node_name"] == node_name), None)
            if node_config:
                instance_name, zone = resolve_instance_name_and_zone(node_config['ip'])
                try:
                    run_preflight_check_via_ssh(instance_name, zone)
                    logging.info(f"✅ Preflight check passed for node '{node_name}'.")
                except RuntimeError as e:
                    logging.error(f"❌ Preflight check failed for node '{node_name}': {e}")
                    raise RuntimeError(f"Preflight check failed for node '{node_name}'. Aborting scaling.")
            else:
                logging.error(f"Could not find configuration for node '{node_name}' for preflight check.")
                raise RuntimeError(f"Configuration missing for prepared node {node_name} during preflight check.")

    # --- Universe Scaling Step after all new nodes are prepared and pre-checked ---
    if successfully_prepared_this_run:
        logging.info(f"\n--- Scaling universe '{YBA_UNIVERSE}' after preparing new nodes ---")
        try:
            current_node_count = _get_current_universe_node_count() # Get current count
            nodes_to_add_this_run = len(successfully_prepared_this_run)
            new_total_nodes = current_node_count + nodes_to_add_this_run
            
            logging.info(f"Current nodes: {current_node_count}. Nodes prepared in this run: {nodes_to_add_this_run}. New target total: {new_total_nodes}.")

            scale_cmd = [
                YBA_CLI, "universe", "edit", "cluster",
                "-H", YBA_HOST, "-a", YBA_TOKEN,
                "--name", YBA_UNIVERSE,
                "primary", # Assuming "primary" cluster
                "--num-nodes", str(new_total_nodes),
                "--force" # Use --force as requested, but be aware of its implications
            ]
            
            # Handle --insecure or --cacert for YBA CLI calls
            if config["yba"].get("use_insecure_cli", False):
                scale_cmd.append("--insecure")
            elif config["yba"].get("yba_ca_cert_path"):
                scale_cmd.extend(["--cacert", config["yba"]["yba_ca_cert_path"]])

            # Set environment variable for this specific command
            env = os.environ.copy()
            env["YBA_FF_PREVIEW"] = "true"
            
            logging.info(f"Running scaling command: {' '.join(scale_cmd)}")
            subprocess.run(scale_cmd, text=True, check=True, env=env) # Use subprocess.run directly here for env
            
            logging.info(f"✅ Successfully initiated universe scaling to {new_total_nodes} nodes.")
            logging.info("Please monitor YBA UI or 'yba task get' for scaling progress.")
        except Exception as e:
            logging.error(f"❌ Failed to scale universe '{YBA_UNIVERSE}': {e}")
            raise RuntimeError(f"Failed to scale universe {YBA_UNIVERSE}: {e}")
    else:
        logging.info("No nodes were successfully prepared in this run, skipping scaling step.")

    logging.info(f"\n✅ All specified nodes initiated for addition to universe '{YBA_UNIVERSE}'.")
    logging.info("Please monitor YBA UI or 'yba task get' for final status of these nodes.")


def remove_node():
    """
    Decommissions nodes from the YugabyteDB universe and then scales down the universe.
    It takes nodes from the "remove" list in config and processes them one at a time.
    """
    remove_nodes_list = config["yba"]["node_management"].get("remove", [])
    
    if not remove_nodes_list: # Check if the list is empty after retrieval
        logging.warning("No nodes specified in 'yba.node_management.remove' to remove. Skipping remove-node operation.")
        return

    logging.info(f"Attempting to remove {len(remove_nodes_list)} node(s) from universe '{YBA_UNIVERSE}'...")
    
    # Store successfully decommissioned node names for logging
    successfully_decommissioned_this_run = []

    for node_config in remove_nodes_list: # Iterate through the list of nodes to remove
        # Validate node_config item, though validate_config should handle most cases
        if not isinstance(node_config, dict) or "yba_node_name" not in node_config or not node_config["yba_node_name"]:
            logging.error(f"❌ Invalid node entry in 'yba.node_management.remove': {node_config}. Skipping this node.")
            continue # Skip to the next node if this one is malformed

        node_name_to_decommission = node_config["yba_node_name"] # Extract the node name
        
        logging.info(f"\n--- Processing node '{node_name_to_decommission}' for decommissioning ---")
        
        # Construct the decommission command
        # This part is removed as per your request.
        # cmd_decommission = [
        #     YBA_CLI, "universe", "node", "decommission", 
        #     "-H", YBA_HOST, "-a", YBA_TOKEN,
        #     "--name", YBA_UNIVERSE,
        #     "--node-name", node_name_to_decommission
        # ]
        # ... (handle --insecure/--cacert and run cmd_decommission) ...
        # successfully_decommissioned_this_run.append(node_name_to_decommission)

        logging.info(f"NOTE: Decommissioning of specific node '{node_name_to_decommission}' is skipped as per script configuration. Scaling down will let YBA choose nodes.")
        successfully_decommissioned_this_run.append(node_name_to_decommission) # Still count it for scale down calculation

    # --- Universe Scaling Down Step after all nodes are "marked for removal" (implicitly) ---
    if successfully_decommissioned_this_run:
        logging.info(f"\n--- Scaling down universe '{YBA_UNIVERSE}' after preparing nodes for removal ---")
        try:
            current_node_count = _get_current_universe_node_count() # Get current count
            nodes_to_remove_this_run = len(successfully_decommissioned_this_run)
            number_tservers = current_node_count - nodes_to_remove_this_run # Calculate new total
            
            if number_tservers < 0:
                logging.error(f"❌ Calculated number_tservers is negative ({number_tservers}). This indicates an error in node count. Aborting scale down.")
                raise RuntimeError("Calculated number of tservers cannot be negative. Check current universe status.")
            if number_tservers == 0:
                logging.warning("⚠️ Calculated number_tservers is 0. This will decommission all nodes. Ensure this is intended.")
            
            logging.info(f"Current nodes: {current_node_count}. Nodes to remove (by count): {nodes_to_remove_this_run}. New target total: {number_tservers}.")

            scale_down_cmd = [
                YBA_CLI, "universe", "edit", "cluster",
                "-H", YBA_HOST, "-a", YBA_TOKEN,
                "--name", YBA_UNIVERSE,
                "primary", # Assuming "primary" cluster
                "--num-nodes", str(number_tservers),
                "--force" # Use --force as requested, but be aware of its implications
            ]
            
            # Handle --insecure or --cacert for YBA CLI calls
            if config["yba"].get("use_insecure_cli", False):
                scale_down_cmd.append("--insecure")
            elif config["yba"].get("yba_ca_cert_path"):
                scale_down_cmd.extend(["--cacert", config["yba"]["yba_ca_cert_path"]])

            # Set environment variable for this specific command
            env = os.environ.copy()
            env["YBA_FF_PREVIEW"] = "true"
            
            logging.info(f"Running scaling down command: {' '.join(scale_down_cmd)}")
            subprocess.run(scale_down_cmd, text=True, check=True, env=env)
            
            logging.info(f"✅ Successfully initiated universe scaling down to {number_tservers} nodes.")
            logging.info("Please monitor YBA UI or 'yba task get' for scaling progress.")
        except Exception as e:
            logging.error(f"❌ Failed to scale down universe '{YBA_UNIVERSE}': {e}")
            raise RuntimeError(f"Failed to scale down universe {YBA_UNIVERSE}: {e}")
    else:
        logging.info("No nodes were processed for removal in this run, skipping scale down step.")

    logging.info(f"\n✅ All specified nodes initiated for removal from universe '{YBA_UNIVERSE}'.")
    logging.info("Please monitor YBA UI or 'yba task get' for final status of these nodes.")


def main():
    """Main function to control YugabyteDB universe operations."""
    parser = argparse.ArgumentParser(
        description="Control YugabyteDB universe operations (pause, resume, add-node, remove-node, precheck-nodes)."
    )
    parser.add_argument(
        "action",
        choices=["pause", "resume", "add-node", "remove-node", "precheck-nodes"], # Added precheck-nodes
        help="Action to perform on the YugabyteDB universe."
    )
    args = parser.parse_args()

    try:
        # Pass the action to validate_config
        validate_config(config, args.action)

        logging.info(f"\n{'='*80}\nStarting YugabyteDB Universe Control Process: {args.action}\n{'='*80}")
        
        if args.action == "pause":
            pause_universe()
        elif args.action == "resume":
            resume_universe()
        elif args.action == "add-node":
            add_nodes_with_scaling() # Call the renamed function
        elif args.action == "remove-node":
            remove_node()
        elif args.action == "precheck-nodes": # NEW: Handle precheck-nodes action
            precheck_nodes()

        logging.info(f"\n{'='*80}\nUniverse Control Process '{args.action}' Completed Successfully\n{'='*80}")

    except Exception as e:
        logging.error(f"\n{'='*80}\n❌ An error occurred during the universe control process for action '{args.action}': {e}\n{'='*80}")
        sys.exit(1)

if __name__ == "__main__":
    main()
