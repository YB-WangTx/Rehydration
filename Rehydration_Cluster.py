#!/usr/bin/env python3
import subprocess
import logging
import time
import json
import yaml
import sys
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
SSH_USER = config["sh_user"]
WAIT_TIME = config["ssh_wait_time"]
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
    result = subprocess.run(cmd, text=True, capture_output=capture_output, check=True)
    return result.stdout.strip() if capture_output else None

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
        error_msg = f"❌ Failed to stop node {node_name} using YBA CLI: {e}"
        logging.error(error_msg)
        raise RuntimeError(error_msg)  # This will cause the script to exit

def reprovision_and_start_node(node_name):
    """Reprovision and start the node."""
    run([
        YBA_CLI, "universe", "node", "reprovision",
        "-H", YBA_HOST, "-a", YBA_TOKEN,
        "--name", YBA_UNIVERSE,
        "--node-name", node_name
    ])
    time.sleep(10)
    run([
        YBA_CLI, "universe", "node", "start",
        "-H", YBA_HOST, "-a", YBA_TOKEN,
        "--name", YBA_UNIVERSE,
        "--node-name", node_name
    ])

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
    for _ in range(timeout // 10):
        result = run([
            "gcloud", "compute", "instances", "describe", instance_name,
            "--zone", zone, "--project", PROJECT, "--format=json"
        ], capture_output=True)
        if json.loads(result).get("status") == expected:
            return
        time.sleep(10)
    raise TimeoutError(f"Timeout waiting for instance status: {expected}")

def stop_instance(instance_name, zone):
    """Stop the instance."""
    run(["gcloud", "compute", "instances", "stop", instance_name, "--zone", zone, "--project", PROJECT])
    wait_for_status(instance_name, zone, "TERMINATED")

def start_instance(instance_name, zone):
    """Start the instance."""
    run(["gcloud", "compute", "instances", "start", instance_name, "--zone", zone, "--project", PROJECT])
    wait_for_status(instance_name, zone, "RUNNING")

def replace_boot_disk(instance_name, zone):
    """Replace the boot disk with a new one, or attach if it already exists."""
    old_disk = get_boot_disk(instance_name, zone)
    # 👉 append the current month-day stamp
    date_stamp = datetime.now().strftime("%m%d")        # e.g., 0710 for July 10
    new_disk   = f"{instance_name}-boot-{date_stamp}"

    # Try to create the new disk
    try:
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
            logging.error(f"❌ Failed to create disk {new_disk}: {e}")
            raise

    # Detach the old boot disk
    run([
        "gcloud", "compute", "instances", "detach-disk", instance_name,
        "--disk", old_disk, "--zone", zone, "--project", PROJECT
    ])

    # Attach the new (or existing) boot disk
    run([
        "gcloud", "compute", "instances", "attach-disk", instance_name,
        "--disk", new_disk, "--zone", zone, "--project", PROJECT,
        "--boot"
    ])

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

# This makes the routine indifferent to whether the kernel calls that disk sda, sdb, nvme0n1, etc.
# Collect all XFS partitions with a UUID whose device isn’t on the same disk as root. If several remain (e.g., extra data disks), choose the largest one.

def get_data_disk_uuid(instance_name, zone):
    """
    Return (device_path, uuid) for the XFS partition that is *not* the one
    mounted at '/' (works even when /data isn’t mounted yet).  No jq needed.
    """
    remote_cmd = (
        # plain JSON; no jq
        "lsblk -b -J -o NAME,MOUNTPOINT,FSTYPE,UUID,SIZE,TYPE"
    )

    raw_json = run(
        [
            "gcloud", "compute", "ssh", f"{SSH_USER}@{instance_name}",
            "--zone", zone, "--project", PROJECT,
            "--internal-ip", "--command", remote_cmd
        ],
        capture_output=True
    )

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
                root_devs.add(name.split("p")[0])  # base disk name
            elif fs == "xfs" and uuid:
                candidates.append((size, f"/dev/{name}", uuid))

    # keep only those not on the root disk
    data_disks = [
        (s, p, u)
        for s, p, u in candidates
        if p.split('/')[-1].split('p')[0] not in root_devs
    ]

    if not data_disks:
        raise RuntimeError("No XFS data disk found (root excluded)")

    # choose largest; adjust if you want a different policy
    data_disks.sort(reverse=True)
    _, device_path, disk_uuid = data_disks[0]
    return device_path, disk_uuid

def mount_data_disk(instance_name, zone, uuid):
    """Mount the data disk."""
    cmd = " && ".join([
        "sudo mkdir -p /data",
        f"sudo mount UUID={uuid} /data",
        "sudo chmod 777 /data",
        f"grep -q UUID={uuid} /etc/fstab || echo 'UUID={uuid} /data xfs defaults,nofail 0 2' | sudo tee -a /etc/fstab"
    ])
    run([
        "gcloud", "compute", "ssh", f"{SSH_USER}@{instance_name}",
        "--zone", zone, "--project", PROJECT,
        "--internal-ip", "--command", cmd
    ])

def provision_node_agent(instance_name, zone):
    """Provision the node agent."""
    cmd = " && ".join([
        "echo '�� Reprovisioning yugabyte user and running node-agent-provision.sh...'",
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
    run([
        "gcloud", "compute", "ssh", f"{SSH_USER}@{instance_name}",
        "--zone", zone, "--project", PROJECT,
        "--internal-ip", "--command", cmd
    ])
/*
def provision_node_agent(instance_name, zone):
    """Provision the node agent with explicit UID/GID, home, and mount logic."""
    cmd = " && ".join([
        # Mount /dev/sdb to /mnt/d0 if not already mounted
        "sudo mkdir -p /mnt/d0",
        "mountpoint -q /mnt/d0 || sudo mount /dev/sdb /mnt/d0",
        # Create group yugabyte with GID 3005 if not exists
        "getent group yugabyte || sudo groupadd -g 3005 yugabyte",
        # Create user yugabyte with UID 3004, GID 3005, home /mnt/d0/yugabyte, shell /bin/bash if not exists
        "id yugabyte || sudo useradd -u 3004 -g 3005 -d /mnt/d0/yugabyte -m -s /bin/bash yugabyte",
        # If user exists, ensure correct home and group
        "id yugabyte && sudo usermod -d /mnt/d0/yugabyte -g 3005 yugabyte",
        "sudo mkdir -p /mnt/d0/yugabyte",
        "sudo chown -R yugabyte:yugabyte /mnt/d0/yugabyte",
        "sudo chmod 755 /mnt/d0/yugabyte",
        # Add yugabyte to systemd-journal group
        "sudo usermod -aG systemd-journal yugabyte",
        # Optional: verify
        "id yugabyte",
        "getent group yugabyte",
        "ls -ld /mnt/d0/yugabyte"
        # You can add your node-agent-provision.sh call here if needed
        # "cd /data/2024.2.2.2-b2/scripts",
        # "sudo ./node-agent-provision.sh"
    ])
    run([
        "gcloud", "compute", "ssh", f"{SSH_USER}@{instance_name}",
        "--zone", zone, "--project", PROJECT,
        "--internal-ip", "--command", cmd
    ])
*/

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
        ZONE = node_info['zone']  # Read zone from node configuration
        
        try:
            INSTANCE_NAME, _ = resolve_instance_name_and_zone(INSTANCE_IP)  # We'll use the zone from config
            logging.info(f"Processing node {NODE_NAME} (IP: {INSTANCE_IP}, Zone: {ZONE})")
            
            # Stop Yugabyte processes - this will exit if it fails
            stop_yugabyte_processes(NODE_NAME)
            
            # Only proceed if Yugabyte processes were stopped successfully
            stop_instance(INSTANCE_NAME, ZONE)
            replace_boot_disk(INSTANCE_NAME, ZONE)
            start_instance(INSTANCE_NAME, ZONE)
            time.sleep(WAIT_TIME)
            _, uuid = get_data_disk_uuid(INSTANCE_NAME, ZONE)
            mount_data_disk(INSTANCE_NAME, ZONE, uuid)
            provision_node_agent(INSTANCE_NAME, ZONE)
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
            # Exit on first failure (including Yugabyte stop failure)
            break
        
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
