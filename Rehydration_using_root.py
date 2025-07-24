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
SSH_USER = config["sh_user"] # User for SSH, will still be used for get_data_disk_uuid_via_ssh (pre-replacement)
WAIT_TIME = config["ssh_wait_time"]
YBA_CLI = config["yba"]["yba_cli_path"]
YBA_HOST = config["yba"]["yba_host"]
YBA_TOKEN = config["yba"]["yba_api_token"]
YBA_UNIVERSE = config["yba"]["universe_name"]
ROOT_PUBLIC_KEY = config["root_public_key"] # !!! NEW: Public SSH key string for root access

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
        "disk_type", "disk_size", "sh_user", "ssh_wait_time", "root_public_key" # Added root_public_key
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
    run([
        YBA_CLI, "universe", "node", "start",
        "-H", YBA_HOST, "-a", YBA_TOKEN,
        "--name", YBA_UNIVERSE,
        "--node-name", node_name
    ])
    logging.info(f"✅ Reprovision and start command sent for node {node_name}")


def get_boot_disk(instance_name, zone):
    """Get the boot disk name for an instance."""
    completed_process = run([
        "gcloud", "compute", "instances", "describe", instance_name,
        "--zone", zone, "--project", PROJECT, "--format=json"
    ], capture_output=True)
    info = json.loads(completed_process.stdout)
    for disk in info["disks"]:
        if disk.get("boot"):
            return disk["source"].split("/")[-1]
    raise RuntimeError("Boot disk not found")

def wait_for_status(instance_name, zone, expected, timeout=300):
    """Wait for instance to reach expected status."""
    logging.info(f"Waiting for instance {instance_name} to reach status: {expected} (timeout: {timeout}s)")
    start_time = time.time()
    while time.time() - start_time < timeout:
        completed_process = run([
            "gcloud", "compute", "instances", "describe", instance_name,
            "--zone", zone, "--project", PROJECT, "--format=json"
        ], capture_output=True)
        current_status = json.loads(completed_process.stdout).get("status")
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

# --- Data disk UUID retrieval via SSH (PRE-REPLACEMENT) ---
def get_data_disk_uuid_via_ssh(instance_name, zone):
    """
    Return (device_path, uuid) for the XFS partition that is *not* the one
    mounted at '/' (works even when /data isn’t mounted yet).
    Uses gcloud compute ssh to run lsblk. This is called BEFORE boot disk replacement.
    """
    logging.info(f"Attempting to get data disk UUID for {instance_name} via SSH (pre-replacement)...")
    remote_cmd = (
        "sudo lsblk -b -J -o NAME,MOUNTPOINT,FSTYPE,UUID,SIZE,TYPE"
    )

    # This relies on SSH_USER having sudo privileges and SSH being enabled on the instance.
    completed_process = run(
        [
            "gcloud", "compute", "ssh", f"{SSH_USER}@{instance_name}",
            "--zone", zone, "--project", PROJECT,
            "--internal-ip", "--command", remote_cmd
        ],
        capture_output=True
    )
    raw_json = completed_process.stdout # Get stdout from the completed process

    blk = json.loads(raw_json)          # whole lsblk tree
    root_devs  = set()
    candidates = []                     # (size, path, uuid)

    # walk disks and any children (partitions)
    for disk in blk["blockdevices"]:
        stack = [disk] + disk.get("children", [])
        for n in stack:
            name = n["name"]
            mp   = (n.get("mountpoint") or "").strip()
            fs   = n.get("fstype")
            uuid = n.get("uuid")
            size = int(n.get("size") or 0)

            if mp == "/":
                # Handle cases like /dev/sda or /dev/nvme0n1 for boot disk
                root_dev_base = name.split("p")[0]
                if "nvme" in root_dev_base and len(root_dev_base.split("n")) > 1:
                    root_dev_base = root_dev_base.split("n")[0] + "n" + root_dev_base.split("n")[1][0]
                root_devs.add(root_dev_base)
            elif fs == "xfs" and uuid:
                candidates.append((size, f"/dev/{name}", uuid))

    # keep only those not on the root disk
    data_disks = [
        (s, p, u)
        for s, p, u in candidates
        if p.split('/')[-1].split('p')[0].split("n")[0] not in root_devs # Corrected filtering for NVMe
    ]

    if not data_disks:
        raise RuntimeError("No XFS data disk found (root excluded)")

    # choose largest; adjust if you want a different policy
    data_disks.sort(reverse=True)
    _, device_path, disk_uuid = data_disks[0]
    logging.info(f"✅ Retrieved data disk: {device_path}, UUID: {disk_uuid} via SSH.")
    return device_path, disk_uuid

# --- Data disk mounting (NO SSH, uses startup script) ---
def _set_vm_metadata(instance_name, zone, project, metadata_key, metadata_value, from_file=False):
    """Sets VM metadata for a given key-value pair, optionally from a file."""
    logging.info(f"Setting metadata '{metadata_key}' for instance {instance_name}...")
    try:
        command = [
            "gcloud", "compute", "instances", "add-metadata", instance_name, # Corrected subcommand
            "--zone", zone, "--project", project
        ]
        if from_file:
            with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_script_file:
                temp_script_file.write(metadata_value)
                temp_script_path = temp_script_file.name
            command.append(f"--metadata-from-file={metadata_key}={temp_script_path}")
        else:
            command.append(f"--metadata={metadata_key}={metadata_value}")

        run(command) # No capture_output needed, just run the command
        logging.info(f"Metadata set successfully for '{metadata_key}' on {instance_name}.")
    except Exception as e:
        logging.error(f"Error setting metadata '{metadata_key}': {e}")
        raise
    finally:
        if from_file and 'temp_script_path' in locals() and os.path.exists(temp_script_path):
            os.remove(temp_script_path) # Clean up temp file

def mount_data_disk_via_startup_script(instance_name, zone, project, uuid):
    """
    Sets a startup script to mount the data disk using its UUID.
    This script is set via metadata and executed on the VM's next boot.
    """
    logging.info(f"Setting startup script to mount data disk with UUID {uuid} for {instance_name}.")
    log_tag = "data-disk-mount" 
    mount_script_content = f"""#!/bin/bash
set -euxo pipefail # Strict execution for debugging
exec > >(logger -t startup-script-mount 2>&1) # Redirect all output to logger

MOUNT_POINT="/data" 
FS_TYPE="xfs" # Assuming XFS, adjust if your filesystem is different

echo "--- Starting data disk mount setup (UUID={uuid}) ---"
mkdir -p ${{MOUNT_POINT}}

FSTAB_ENTRY="UUID={uuid} ${{MOUNT_POINT}} ${{FS_TYPE}} defaults,nofail 0 2"

if ! grep -Fq "${{FSTAB_ENTRY}}" /etc/fstab; then
    echo "${{FSTAB_ENTRY}}" | tee -a /etc/fstab
    echo "Added fstab entry to /etc/fstab."
else
    echo "Fstab entry already exists: ${{FSTAB_ENTRY}}."
fi

echo "Attempting mount -a..."
mount_output=$(mount -a 2>&1)
if [ $? -eq 0 ]; then
    echo "Data disk mounted successfully to ${{MOUNT_POINT}}."
else
    echo "ERROR: Failed to mount data disk. mount -a output: ${{mount_output}}"
    echo "Checking dmesg for mount errors..."
    dmesg | tail -n 20
    exit 1 # Exit on mount failure, as agent provisioning will likely fail without it
fi

chmod 777 ${{MOUNT_POINT}} # Set permissions
echo "--- Data disk mount setup finished. ---"
"""
    _set_vm_metadata(instance_name, zone, project, "startup-script", mount_script_content, from_file=True)
    logging.info(f"Startup script for mounting data disk set for {instance_name}. It will execute on the next boot.")

# --- Root SSH Key Injection (using Cloud-init user-data) ---
def set_root_ssh_key_via_cloud_init(instance_name, zone, project, root_public_key_string):
    """
    Sets Cloud-init user-data metadata to inject a public SSH key for the root user.
    This runs very early in the boot process.
    """
    logging.info(f"Setting Cloud-init user-data for root SSH key for {instance_name}.")
    cloud_init_content = f"""#cloud-config
users:
  - name: root
    ssh_authorized_keys:
      - {root_public_key_string}
"""
    _set_vm_metadata(instance_name, zone, project, "user-data", cloud_init_content, from_file=True)
    logging.info(f"Cloud-init user-data for root SSH key set for {instance_name}.")

def wait_for_root_ssh_ready(instance_name, zone, project, timeout=180):
    """
    Waits for root SSH access to become available by repeatedly trying a simple SSH command.
    """
    logging.info(f"Waiting for root SSH access to {instance_name} to become ready (timeout: {timeout}s)...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # Try a simple SSH command as root
            run([
                "gcloud", "compute", "ssh", f"root@{instance_name}",
                "--zone", zone, "--project", PROJECT,
                "--internal-ip", "--command", "echo 'SSH_READY'",
                "--quiet" # Suppress gcloud's own verbose output for polling
            ], capture_output=True)
            logging.info(f"✅ Root SSH access to {instance_name} is ready.")
            return
        except subprocess.CalledProcessError as e:
            # SSH might fail with 255 if not ready, or other codes for network issues
            logging.debug(f"Root SSH not yet ready ({e.returncode}). Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            logging.warning(f"Unexpected error while waiting for root SSH: {e}. Retrying...")
            time.sleep(5)
    raise TimeoutError(f"Timeout waiting for root SSH access to {instance_name}.")

# --- Node agent provisioning (direct SSH AS ROOT) ---
def provision_node_agent_via_ssh_as_root(instance_name, zone):
    """Provision the node agent using direct SSH as root."""
    logging.info(f"Attempting to provision node agent for {instance_name} via SSH (as root, post-replacement)...")
    cmd = " && ".join([
        "echo '🚀 Reprovisioning yugabyte user and running
