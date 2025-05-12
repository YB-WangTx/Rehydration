#!/usr/bin/env python3
import subprocess
import logging
import time
import json
import yaml
from datetime import datetime

# Load configuration
with open("yba_config.yaml", "r") as f:
    config = yaml.safe_load(f)

node_map = config["yba"]["node_list"][0]
INSTANCE_IP = list(node_map.keys())[0]
INSTANCE_NAME = list(node_map.values())[0]
NODE_NAME = INSTANCE_NAME

ZONE = config["zone"]
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

log_file = f"rehydration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file)]
)

def run(cmd, capture_output=False):
    logging.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, text=True, capture_output=capture_output, check=True)
    return result.stdout.strip() if capture_output else None

def resolve_instance_name_and_zone(ip):
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

def stop_yugabyte_processes():
    try:
        run([
            YBA_CLI, "universe", "node", "stop",
            "-H", YBA_HOST, "-a", YBA_TOKEN,
            "--name", YBA_UNIVERSE,
            "--node-name", NODE_NAME
        ])
        logging.info(f"✅ Stopped node {NODE_NAME} using YBA CLI")
    except subprocess.CalledProcessError as e:
        logging.warning(f"⚠️ Failed to stop node using YBA CLI: {e}")

def reprovision_and_start_node():
    run([
        YBA_CLI, "universe", "node", "reprovision",
        "-H", YBA_HOST, "-a", YBA_TOKEN,
        "--name", YBA_UNIVERSE,
        "--node-name", NODE_NAME
    ])
    time.sleep(10)
    run([
        YBA_CLI, "universe", "node", "start",
        "-H", YBA_HOST, "-a", YBA_TOKEN,
        "--name", YBA_UNIVERSE,
        "--node-name", NODE_NAME
    ])

def get_boot_disk():
    result = run([
        "gcloud", "compute", "instances", "describe", INSTANCE_NAME,
        "--zone", ZONE, "--project", PROJECT, "--format=json"
    ], capture_output=True)
    info = json.loads(result)
    for disk in info["disks"]:
        if disk.get("boot"):
            return disk["source"].split("/")[-1]
    raise RuntimeError("Boot disk not found")

def wait_for_status(expected, timeout=300):
    for _ in range(timeout // 10):
        result = run([
            "gcloud", "compute", "instances", "describe", INSTANCE_NAME,
            "--zone", ZONE, "--project", PROJECT, "--format=json"
        ], capture_output=True)
        if json.loads(result).get("status") == expected:
            return
        time.sleep(10)
    raise TimeoutError(f"Timeout waiting for instance status: {expected}")

def stop_instance():
    run(["gcloud", "compute", "instances", "stop", INSTANCE_NAME, "--zone", ZONE, "--project", PROJECT])
    wait_for_status("TERMINATED")

def start_instance():
    run(["gcloud", "compute", "instances", "start", INSTANCE_NAME, "--zone", ZONE, "--project", PROJECT])
    wait_for_status("RUNNING")

def replace_boot_disk():
    old_disk = get_boot_disk()
    new_disk = f"{INSTANCE_NAME}-boot-{int(time.time())}"
    run([
        "gcloud", "compute", "disks", "create", new_disk,
        "--image", NEW_IMAGE, "--image-project", IMAGE_PROJECT,
        "--size", DISK_SIZE, "--type", DISK_TYPE,
        "--zone", ZONE, "--project", PROJECT
    ])
    run([
        "gcloud", "compute", "instances", "detach-disk", INSTANCE_NAME,
        "--disk", old_disk, "--zone", ZONE, "--project", PROJECT
    ])
    run([
        "gcloud", "compute", "instances", "attach-disk", INSTANCE_NAME,
        "--disk", new_disk, "--zone", ZONE, "--project", PROJECT,
        "--boot"
    ])

def get_data_disk_uuid():
    cmd = [
        "gcloud", "compute", "ssh", f"{SSH_USER}@{INSTANCE_NAME}",
        "--zone", ZONE, "--project", PROJECT,
        "--internal-ip", "--command",
        "lsblk -o NAME,FSTYPE,UUID | grep xfs | grep -v sda"
    ]
    output = run(cmd, capture_output=True)
    device, _, uuid = output.strip().split()
    return f"/dev/{device}", uuid

def mount_data_disk(uuid):
    cmd = " && ".join([
        "sudo mkdir -p /data",
        f"sudo mount UUID={uuid} /data",
        "sudo chmod 777 /data",
        f"grep -q UUID={uuid} /etc/fstab || echo 'UUID={uuid} /data xfs defaults,nofail 0 2' | sudo tee -a /etc/fstab"
    ])
    run([
        "gcloud", "compute", "ssh", f"{SSH_USER}@{INSTANCE_NAME}",
        "--zone", ZONE, "--project", PROJECT,
        "--internal-ip", "--command", cmd
    ])


def provision_node_agent():
    cmd = " && ".join([
        "echo '🛠 Reprovisioning yugabyte user and running node-agent-provision.sh...'",
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
        "gcloud", "compute", "ssh", f"{SSH_USER}@{INSTANCE_NAME}",
        "--zone", ZONE, "--project", PROJECT,
        "--internal-ip", "--command", cmd
    ])
def main():
    global INSTANCE_NAME, ZONE
    INSTANCE_NAME, ZONE = resolve_instance_name_and_zone(INSTANCE_IP)
    stop_yugabyte_processes()
    stop_instance()
    replace_boot_disk()
    start_instance()
    time.sleep(WAIT_TIME)
    _, uuid = get_data_disk_uuid()
    mount_data_disk(uuid)
    provision_node_agent()
    reprovision_and_start_node()
    logging.info("✅ Rehydration complete with home bind-mount fix.")

if __name__ == "__main__":
    main()