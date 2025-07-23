#!/usr/bin/env python3
import subprocess
import logging
import time
import json
import yaml
import sys
import tempfile
import os
from datetime import datetime

# Load configuration
with open("yba_config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Global configuration (zone is now per-node)
PROJECT = config["project_id"]
NEW_IMAGE = config["new_image"]
IMAGE_PROJECT = config["image_project"]
DISK_TYPE = config["disk_type"]
DISK_SIZE = str(config["disk_size"]) + "GB"
SSH_USER = config["sh_user"] # Kept as a global, but not used for direct root commands now
WAIT_TIME = config["ssh_wait_time"] # May need adjustment if operations become slower due to reboots
YBA_CLI = config["yba"]["yba_cli_path"]
YBA_HOST = config["yba"]["yba_host"]
YBA_TOKEN = config["yba"]["yba_api_token"]
YBA_UNIVERSE = config["yba"]["universe_name"]

# Setup logging
log_file = f"rehydration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file)]
)

def validate_config(config):
    """Validate the configuration file."""
    required_fields = [
        "project_id", "new_image", "image_project",
        "disk_type", "disk_size", "sh_user", "ssh_wait_time"
    ]

    yba_required_fields = [
        "yba_host", "customer_id", "yba_api_token",
        "universe_name", "node_list", "yba_cli_path"
    ]

    # Check top-level fields
    for field in required_fields:
        if field not in config:
            raise ValueError(f"Missing required field: {field}")

    # Check YBA section
    if "yba" not in config:
        raise ValueError("Missing 'yba' section")

    for field in yba_required_fields:
        if field not in config["yba"]:
            raise ValueError(f"Missing required YBA field: {field}")

    # Check node_list
    if not config["yba"]["node_list"]:
        raise ValueError("node_list cannot be empty")

    for i, node in enumerate(config["yba"]["node_list"]):
        required_node_fields = ["ip", "yba_node_name", "zone"]
        for field in required_node_fields:
            if field not in node:
                raise ValueError(f"Node {i} missing required field: {field}")

    logging.info("✅ Configuration validation passed")

def run(cmd, capture_output=False):
    """Run a shell command and return its output."""
    logging.info(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, text=True, capture_output=capture_output, check=True)
        return result.stdout.strip() if capture_output else None
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed: {e.cmd}")
        logging.error(f"Stdout: {e.stdout}")
        logging.error(f"Stderr: {e.stderr}")
        raise

def resolve_instance_name_and_zone(ip):
    """Resolve instance name and zone from IP address."""
    result = run([
        "gcloud", "compute", "instances", "list",
        "--filter", f"networkInterfaces[0].networkIP={ip}",
        "--format", "json",
        "--project", PROJECT
    ], capture_output=True)
    data = json.loads(result)
    if not data:
        raise RuntimeError(f"No instance found for IP {ip}")
    return data[0]["name"], data[0]["zone"].split("/")[-1]

def stop_yugabyte_processes(node_name):
    """Stop Yugabyte processes on the node."""
    try:
        run([
            YBA_CLI, "universe", "node", "stop",
            "-H", YBA_HOST, "-a", YBA_TOKEN,
            "--name", YBA_UNIVERSE,
            "--node-name", node_name
        ])
        logging.info(f"✅ Stopped node {node_name} using YBA CLI")
    except subprocess.CalledProcessError as e:
        error_msg = f"❌ Failed to stop node {node_name} using YBA CLI: {e.stderr}"
        logging.error(error_msg)
        raise RuntimeError(error_msg)

def reprovision_and_start_node(node_name):
    """Reprovision and start the node."""
    logging.info(f"Initiating reprovision for node {node_name}")
    run([
        YBA_CLI, "universe", "node", "reprovision",
        "-H", YBA_HOST, "-a", YBA_TOKEN,
        "--name", YBA_UNIVERSE,
        "--node-name", node_name
    ])
    logging.info(f"Waiting 10 seconds for reprovision to settle.")
    time.sleep(10)
    logging.info(f"Starting node {node_name} via YBA CLI.")
    run([
        YBA_CLI, "universe", "node", "start",
        "-H", YBA_HOST, "-a", YBA_TOKEN,
        "--name", YBA_UNIVERSE,
        "--node-name", node_name
    ])
    logging.info(f"✅ Reprovision and start command sent for node {node_name}")


def get_boot_disk(instance_name, zone):
    """Get the boot disk name for an instance."""
    result = run([
        "gcloud", "compute", "instances", "describe", instance_name,
        "--zone", zone, "--project", PROJECT, "--format=json"
    ], capture_output=True)
    info = json.loads(result)
    for disk in info["disks"]:
        if disk.get("boot"):
            return disk["source"].split("/")[-1]
    raise RuntimeError("Boot disk not found")

def wait_for_status(instance_name, zone, expected, timeout=300):
    """Wait for instance to reach expected status."""
    logging.info(f"Waiting for instance {instance_name} to reach status: {expected} (timeout: {timeout}s)")
    start_time = time.time()
    while time.time() - start_time < timeout:
        result = run([
            "gcloud", "compute", "instances", "describe", instance_name,
            "--zone", zone, "--project", PROJECT, "--format=json"
        ], capture_output=True)
        current_status = json.loads(result).get("status")
        if current_status == expected:
            logging.info(f"Instance {instance_name} reached status: {expected}")
            return
        logging.info(f"Current status: {current_status}. Retrying in 10 seconds...")
        time.sleep(10)
    raise TimeoutError(f"Timeout waiting for instance status '{expected}' for {instance_name}")

def stop_instance(instance_name, zone):
    """Stop the instance."""
    logging.info(f"Stopping instance {instance_name} in zone {zone}...")
    run(["gcloud", "compute", "instances", "stop", instance_name, "--zone", zone, "--project", PROJECT])
    wait_for_status(instance_name, zone, "TERMINATED")
    logging.info(f"✅ Instance {instance_name} is TERMINATED.")

def start_instance(instance_name, zone):
    """Start the instance."""
    logging.info(f"Starting instance {instance_name} in zone {zone}...")
    run(["gcloud", "compute", "instances", "start", instance_name, "--zone", zone, "--project", PROJECT])
    wait_for_status(instance_name, zone, "RUNNING")
    logging.info(f"✅ Instance {instance_name} is RUNNING.")

def replace_boot_disk(instance_name, zone):
    """Replace the boot disk with a new one, or attach if it already exists."""
    logging.info(f"Replacing boot disk for {instance_name}...")
    old_disk = get_boot_disk(instance_name, zone)
    date_stamp = datetime.now().strftime("%m%d")
    new_disk = f"{instance_name}-boot-{date_stamp}"

    try:
        logging.info(f"Attempting to create new disk {new_disk} from image {NEW_IMAGE}...")
        run([
            "gcloud", "compute", "disks", "create", new_disk,
            "--image", NEW_IMAGE, "--image-project", IMAGE_PROJECT,
            "--size", DISK_SIZE, "--type", DISK_TYPE,
            "--zone", zone, "--project", PROJECT
        ])
        logging.info(f"✅ Created new disk {new_disk}")
    except subprocess.CalledProcessError as e:
        if "already exists" in str(e):
            logging.warning(f"⚠️ Disk {new_disk} already exists, will attempt to attach it.")
        else:
            logging.error(f"❌ Failed to create disk {new_disk}: {e.stderr}")
            raise

    logging.info(f"Detaching old boot disk {old_disk} from {instance_name}...")
    run([
        "gcloud", "compute", "instances", "detach-disk", instance_name,
        "--disk", old_disk, "--zone", zone, "--project", PROJECT
    ])
    logging.info(f"✅ Detached old boot disk {old_disk}.")

    logging.info(f"Attaching new boot disk {new_disk} to {instance_name}...")
    run([
        "gcloud", "compute", "instances", "attach-disk", instance_name,
        "--disk", new_disk, "--zone", zone, "--project", PROJECT,
        "--boot"
    ])
    logging.info(f"✅ Attached new boot disk {new_disk}.")
    return new_disk # Return new disk name, though not strictly used later


def parse_size_to_gb(size_str):
    """Convert size string (e.g., '400G', '1T') to GB."""
    size_str = size_str.upper()
    if 'T' in size_str:
        return float(size_str.replace('T', '')) * 1024
    elif 'G' in size_str:
        return float(size_str.replace('G', ''))
    elif 'M' in size_str:
        return float(size_str.replace('M', '')) / 1024
    else:
        return float(size_str) / (1024 * 1024 * 1024)  # Assume bytes

# --- NEW HELPER FUNCTIONS FOR NO-SSH APPROACH ---
def _set_vm_metadata(instance_name, zone, project, metadata_key, metadata_value, from_file=False):
    """Sets VM metadata for a given key-value pair, optionally from a file."""
    logging.info(f"Setting metadata '{metadata_key}' for instance {instance_name}...")
    try:
        command = [
            "gcloud", "compute", "instances", "set-metadata", instance_name,
            "--zone", zone, "--project", project
        ]
        if from_file:
            # Create a temporary file for the script content
            with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_script_file:
                temp_script_file.write(metadata_value)
                temp_script_path = temp_script_file.name
            command.append(f"--metadata-from-file={metadata_key}={temp_script_path}")
        else:
            command.append(f"--metadata={metadata_key}={metadata_value}")

        result = run(command, capture_output=True)
        logging.info(f"Metadata set result: {result}")
    except Exception as e:
        logging.error(f"Error setting metadata '{metadata_key}': {e}")
        raise
    finally:
        if from_file and 'temp_script_path' in locals() and os.path.exists(temp_script_path):
            os.remove(temp_script_path) # Clean up temp file

def _get_log_entry_from_cloud_logging(instance_name, zone, project, log_tag, timeout_seconds=480): # Increased timeout
    """
    Retrieves the most recent log entry with a specific tag for an instance.
    Waits up to timeout_seconds for the log entry to appear.
    """
    logging.info(f"Searching Cloud Logging for tag '{log_tag}' from instance '{instance_name}'...")
    end_time = time.time() + timeout_seconds
    
    # First, get the instance ID (needed for specific log filtering)
    try:
        instance_details = json.loads(run([
            "gcloud", "compute", "instances", "describe", instance_name,
            "--zone", zone, "--project", project, "--format=json"
        ], capture_output=True))
        instance_id = instance_details["id"]
        logging.info(f"Instance {instance_name} has ID: {instance_id}")
    except Exception as e:
        logging.error(f"Could not get instance ID for {instance_name}: {e}")
        raise RuntimeError(f"Failed to get instance ID for logging filter.")

    while time.time() < end_time:
        try:
            log_filter = f'resource.type="gce_instance" resource.labels.instance_id="{instance_id}" syslogTag="{log_tag}"'
            command = [
                "gcloud", "logging", "read", log_filter,
                "--project", project,
                "--limit=1", "--format=json",
                "--order=desc" # Get the most recent one
            ]
            result = run(command, capture_output=True)
            log_entries = json.loads(result)

            if log_entries:
                log_message = log_entries[0].get("textPayload", "")
                logging.info(f"Found log entry: {log_message}")
                return log_message
            else:
                logging.info("No log entry found yet. Waiting...")
                time.sleep(15) # Increased wait to reduce API calls
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            logging.warning(f"Error reading logs: {e}. Retrying...")
            time.sleep(15)
        except Exception as e:
            logging.error(f"An unexpected error occurred while polling logs: {e}")
            raise

    raise TimeoutError(f"Timeout: Could not retrieve log entry for tag '{log_tag}' from instance '{instance_name}'.")

# ----------------------------------------

def get_data_disk_uuid_no_ssh(instance_name, zone):
    """
    Triggers a VM reboot with a startup script to find and log the XFS data disk UUID,
    then retrieves it from Cloud Logging.
    Returns (device_path, uuid) for the largest XFS partition not on the root disk.
    """
    logging.info(f"Attempting to get data disk UUID for {instance_name} using startup script and Cloud Logging.")
    log_tag = "data-disk-uuid-finder"
    startup_script_content = f"""#!/bin/bash
# Startup script to find and log data disk UUID to Cloud Logging
echo "Starting data disk UUID retrieval script..." | logger -t {log_tag}

# Run lsblk and process output
LSBLK_OUTPUT=$(lsblk -b -J -o NAME,MOUNTPOINT,FSTYPE,UUID,SIZE,TYPE)
echo "$LSBLK_OUTPUT" > /tmp/lsblk_output_raw.json # For debugging on VM

# Use python3 to parse JSON and identify the data disk
python3 -c '
import json
import sys

try:
    blk = json.loads(sys.stdin.read())
    root_devs = set()
    candidates = []

    for disk in blk["blockdevices"]:
        stack = [disk] + disk.get("children", [])
        for n in stack:
            name = n["name"]
            mp = (n.get("mountpoint") or "").strip()
            fs = n.get("fstype")
            uuid = n.get("uuid")
            size = int(n.get("size") or 0)

            if mp == "/":
                # Handle cases like /dev/sda or /dev/nvme0n1 for boot disk
                root_devs.add(name.split("p")[0].split("n")[0]) # "p" for partition, "n" for nvme
            elif fs == "xfs" and uuid:
                candidates.append((size, f"/dev/{{name}}", uuid))

    # Keep only those not on the root disk
    data_disks = [
        (s, p, u)
        for s, p, u in candidates
        if p.split("/")[-1].split("p")[0].split("n")[0] not in root_devs
    ]

    if not data_disks:
        print("NO_XFS_DATA_DISK_FOUND", file=sys.stderr)
        sys.exit(1)

    data_disks.sort(reverse=True)
    _, device_path, disk_uuid = data_disks[0]
    print(f"UUID_FOUND__{device_path}__{disk_uuid}")

except Exception as e:
    print(f"ERROR_IN_SCRIPT: {{e}}", file=sys.stderr)
    sys.exit(1)
' <<< "$LSBLK_OUTPUT" | logger -t {log_tag}

echo "Data disk UUID script finished." | logger -t {log_tag}
"""
    # 1. Set the startup script. This will replace any existing startup-script.
    _set_vm_metadata(instance_name, zone, PROJECT, "startup-script", startup_script_content, from_file=True)

    # 2. Reboot the VM to trigger the script.
    # The main function already reboots after replace_boot_disk, so this step might be redundant
    # if this function is called immediately after a boot disk replacement.
    # For standalone UUID retrieval, you would call:
    # _reboot_vm(instance_name, zone, PROJECT)
    # For now, we assume a reboot will occur later in the main flow.
    logging.info(f"Startup script for UUID retrieval set. Instance {instance_name} will execute it on next boot.")

    # 3. Retrieve the UUID from Cloud Logging
    # Wait for the VM to boot and the script to run. This might be after a primary reboot in main.
    logging.info(f"Waiting for {instance_name} to finish booting and log UUID...")
    log_message = _get_log_entry_from_cloud_logging(instance_name, zone, PROJECT, log_tag)

    if "UUID_FOUND__" in log_message:
        parts = log_message.split("UUID_FOUND__")[1].strip().split("__")
        if len(parts) == 2:
            device_path, disk_uuid = parts
            logging.info(f"Retrieved UUID: {disk_uuid} for device: {device_path}")
            # Clear the startup script metadata after use to prevent re-execution on subsequent reboots
            _set_vm_metadata(instance_name, zone, PROJECT, "startup-script", "")
            return device_path, disk_uuid
        else:
            raise RuntimeError(f"Unexpected log format for UUID from {instance_name}: {log_message}")
    elif "NO_XFS_DATA_DISK_FOUND" in log_message:
        raise RuntimeError(f"No XFS data disk found (root excluded) on {instance_name} after reboot.")
    elif "ERROR_IN_SCRIPT" in log_message:
         raise RuntimeError(f"Error detected in UUID retrieval script on {instance_name}: {log_message}")
    else:
        raise RuntimeError(f"Could not parse UUID from log for {instance_name}: {log_message}")

def mount_data_disk_no_ssh(instance_name, zone, uuid):
    """
    Triggers a VM reboot with a startup script to mount the data disk.
    """
    logging.info(f"Attempting to mount data disk with UUID {uuid} for {instance_name} using startup script.")
    log_tag = "data-disk-mount"
    mount_script_content = f"""#!/bin/bash
# Startup script to mount data disk and configure fstab
echo "Starting data disk mount script for UUID={uuid}..." | logger -t {log_tag}

MOUNT_POINT="/data"
FS_TYPE="xfs" # Assuming XFS based on your get_data_disk_uuid function

# Ensure mount point exists
mkdir -p ${{MOUNT_POINT}} | logger -t {log_tag} 2>&1

# Add or update fstab entry using UUID
FSTAB_ENTRY="UUID={uuid} ${{MOUNT_POINT}} ${{FS_TYPE}} defaults,nofail 0 2"
if ! grep -q "UUID={uuid}" /etc/fstab; then
    echo "${{FSTAB_ENTRY}}" | tee -a /etc/fstab | logger -t {log_tag} 2>&1
    echo "Added fstab entry for UUID={uuid}." | logger -t {log_tag}
else
    echo "fstab entry already exists for UUID={uuid}." | logger -t {log_tag}
fi

# Attempt to mount all filesystems in fstab
mount -a | logger -t {log_tag} 2>&1
if [ $? -eq 0 ]; then
    echo "Data disk mounted successfully to ${{MOUNT_POINT}}." | logger -t {log_tag}
else
    echo "Failed to mount data disk to ${{MOUNT_POINT}}." | logger -t {log_tag}
    # Log more details from mount command if possible, e.g., dmesg | tail
fi

# Set permissions (consider if 777 is truly desired for security)
chmod 777 ${{MOUNT_POINT}} | logger -t {log_tag} 2>&1

echo "Data disk mount script finished." | logger -t {log_tag}
"""
    # 1. Set the startup script. This will replace the UUID retrieval script if it was set.
    _set_vm_metadata(instance_name, zone, PROJECT, "startup-script", mount_script_content, from_file=True)

    # 2. Reboot the VM to trigger the script.
    # This reboot will happen as part of the main workflow or can be triggered explicitly if needed.
    logging.info(f"Startup script for mounting data disk set. Instance {instance_name} will execute it on next boot.")
    # No direct return value, success is implied by lack of exception and expected future state.

# ----------------------------------------

def provision_node_agent(instance_name, zone):
    """Provision the node agent. This function still uses gcloud compute ssh as it's complex to port to startup script without knowing the content of node-agent-provision.sh."""
    cmd = " && ".join([
        "echo '🚀 Reprovisioning yugabyte user and running node-agent-provision.sh...'",
        "sudo pkill -u yugabyte || true",
        "id yugabyte && sudo userdel -r yugabyte || true",
        "getent group yugabyte && (getent passwd | awk -F: '$4 == \"$(getent group yugabyte | cut -d: -f3)\"' | grep -q . || sudo groupdel yugabyte) || true",
        "sudo useradd -m -d /data/home/yugabyte -s /bin/bash yugabyte",
        "sudo mkdir -p /data/home/yugabyte",
        "sudo chown -R yugabyte:yugabyte /data/home/yugabyte",
        "sudo chmod 755 /data/home/yugabyte",
        "cd /data/2024.2.2.2-b2/scripts",
        "sudo ./node-agent-provision.sh"
    ])
    logging.info(f"Attempting to provision node agent for {instance_name} via gcloud compute ssh (requires SSH_USER with sudo).")
    run([
        "gcloud", "compute", "ssh", f"{SSH_USER}@{instance_name}",
        "--zone", zone, "--project", PROJECT,
        "--internal-ip", "--command", cmd
    ])
    logging.info(f"✅ Node agent provisioning command sent for {instance_name}.")


def generate_summary(processed_nodes):
    """Generate a summary of the rehydration process."""
    summary = f"""
{'='*80}
REHYDRATION PROCESS SUMMARY
{'='*80}

Total Nodes Processed: {len(processed_nodes)}
Successful Nodes: {sum(1 for node in processed_nodes if node['success'])}
Failed Nodes: {sum(1 for node in processed_nodes if not node['success'])}

{'='*80}
NODE DETAILS
{'='*80}
"""

    for node in processed_nodes:
        status = "✅ SUCCESS" if node['success'] else "❌ FAILED"
        summary += f"""
Node: {node['yba_node_name']}
IP: {node['ip']}
Instance: {node['instance_name']}
Zone: {node['zone']}
Status: {status}
"""
        if not node['success'] and node.get('error'):
            summary += f"Error: {node['error']}\n"
        summary += "-"*80 + "\n"

    return summary

def main():
    """Main function to process all nodes."""
    # Validate configuration first
    validate_config(config)

    processed_nodes = []

    for node_info in config["yba"]["node_list"]:
        INSTANCE_IP = node_info['ip']
        NODE_NAME = node_info['yba_node_name']
        ZONE = node_info['zone']

        try:
            INSTANCE_NAME, _ = resolve_instance_name_and_zone(INSTANCE_IP)
            logging.info(f"\n{'#'*80}\nProcessing node {NODE_NAME} (IP: {INSTANCE_IP}, Zone: {ZONE})\n{'#'*80}")

            # Step 1: Stop Yugabyte processes
            stop_yugabyte_processes(NODE_NAME)

            # Step 2: Get data disk UUID (requires a VM reboot for the startup script to run)
            # We will use the existing stop/start sequence around boot disk replacement to trigger this
            # Set the UUID retrieval script BEFORE stopping for boot disk replacement
            logging.info("Setting UUID retrieval startup script before instance stop for boot disk replacement.")
            get_data_disk_uuid_no_ssh(INSTANCE_NAME, ZONE) # This sets the script, doesn't reboot yet

            # Step 3: Stop instance, replace boot disk, and start instance
            stop_instance(INSTANCE_NAME, ZONE)
            replace_boot_disk(INSTANCE_NAME, ZONE)
            start_instance(INSTANCE_NAME, ZONE) # This reboot triggers the UUID retrieval script

            # Step 4: Retrieve the UUID after the instance has booted with the new disk
            logging.info(f"Waiting for {INSTANCE_NAME} to boot and log UUID...")
            _, uuid = get_data_disk_uuid_no_ssh(INSTANCE_NAME, ZONE) # This now just retrieves from logs

            # Step 5: Mount data disk (requires another VM reboot for its startup script)
            # Set the mount script immediately. The reprovisioning will trigger another reboot.
            logging.info(f"Setting data disk mount startup script for UUID {uuid}.")
            mount_data_disk_no_ssh(INSTANCE_NAME, ZONE, uuid)

            # Step 6: Provision node agent
            # This step still uses gcloud compute ssh, assumes SSH_USER can sudo.
            # If `provision_node_agent` also needs to be "no ssh", it would need
            # to be refactored similar to the UUID and mount functions, likely
            # setting another startup script and triggering another reboot, or
            # making it part of the mount script.
            provision_node_agent(INSTANCE_NAME, ZONE)

            # Step 7: Reprovision and start node (this also involves a VM reboot by YBA CLI)
            reprovision_and_start_node(NODE_NAME)

            processed_nodes.append({
                'yba_node_name': NODE_NAME,
                'ip': INSTANCE_IP,
                'instance_name': INSTANCE_NAME,
                'zone': ZONE,
                'success': True
            })
            logging.info(f"✅ Rehydration complete for node {NODE_NAME}")

        except Exception as e:
            error_msg = str(e)
            logging.error(f"❌ Error processing node {NODE_NAME}: {error_msg}")
            processed_nodes.append({
                'yba_node_name': NODE_NAME,
                'ip': INSTANCE_IP,
                'instance_name': INSTANCE_NAME if 'INSTANCE_NAME' in locals() else 'Unknown',
                'zone': ZONE if 'ZONE' in locals() else 'Unknown',
                'success': False,
                'error': error_msg
            })
            # Exit on first failure
            sys.exit(1)

        # Add delay between nodes
        if config["yba"]["node_list"].index(node_info) < len(config["yba"]["node_list"]) - 1:
            logging.info("Waiting 60 seconds before processing next node...")
            time.sleep(60)

    # Generate and save summary
    summary = generate_summary(processed_nodes)
    logging.info(summary)

    # Save summary to file
    summary_file = f"rehydration_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(summary_file, 'w') as f:
        f.write(summary)
    logging.info(f"Summary saved to {summary_file}")

    # Exit with error if any node failed
    if any(not node['success'] for node in processed_nodes):
        sys.exit(1)

if __name__ == "__main__":
    main()
