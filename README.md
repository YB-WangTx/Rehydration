# Yugabyte Database Node Rehydration Procedure on GCP

This script automates the process of rehydrating Yugabyte nodes in a Google Cloud Platform (GCP) environment. It handles the complete workflow of stopping nodes, replacing boot disks, remounting data disks, and reprovisioning node agents.

## Prerequisites

- Python 3.6 or higher
- Google Cloud SDK (gcloud) installed and configured
- Yugabyte Platform (YBA) CLI installed
- Access to GCP project with appropriate permissions
- Access to Yugabyte Platform with API token

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd <repository-name>
```

2. Install required Python packages:
```bash
pip install pyyaml
```

3. Create a `yba_config.yaml` file with your configuration (see Configuration section below).

## Configuration

Create a `yba_config.yaml` file with the following structure:

```yaml
project_id: "your-gcp-project"
instance_type: "n2-standard-4"
zone: "your-gcp-zone"
image_family: "rhel-8"
image_project: "rhel-cloud"
disk_size: 150
extra_disk_size: 250
new_disk_name: "new-boot-disk"
new_image: "rhel-9-v20240415"
disk_type: "pd-balanced"
sh_user: "your-ssh-user"
ssh_wait_time: 30

yba:
  yba_host: "https://your-yba-host/"
  customer_id: "your-customer-id"
  yba_api_token: "your-api-token"
  universe_name: "your-universe-name"
  node_list:
    - ip: "node1-ip"
      yba_node_name: "yb-dev-universe-n1"
    - ip: "node2-ip"
      yba_node_name: "yb-dev-universe-n2"
    - ip: "node3-ip"
      yba_node_name: "yb-dev-universe-n3"
  yba_cli_path: "/path/to/yba-cli"
```

### Configuration Parameters

- `project_id`: Your GCP project ID
- `instance_type`: GCP instance type
- `zone`: GCP zone where instances are located
- `image_family`: Base image family
- `image_project`: Project containing the base image
- `disk_size`: Boot disk size in GB
- `extra_disk_size`: Additional disk size in GB
- `new_disk_name`: Name for the new boot disk
- `new_image`: New image to use for boot disk
- `disk_type`: GCP disk type (e.g., pd-balanced)
- `sh_user`: SSH user for instance access
- `ssh_wait_time`: Seconds to wait for SSH availability
- `yba`: Yugabyte Platform configuration
  - `yba_host`: YBA host URL
  - `customer_id`: YBA customer ID
  - `yba_api_token`: YBA API token
  - `universe_name`: Name of the Yugabyte universe
  - `node_list`: List of nodes to process
    - `ip`: Node IP address
    - `yba_node_name`: Node name in YBA
  - `yba_cli_path`: Path to YBA CLI executable

## Usage

1. Make the script executable:
```bash
chmod +x rehydration.py
```

2. Run the script:
```bash
./rehydration.py
```

The script will:
1. Process each node in the configuration sequentially
2. Stop Yugabyte processes
3. Replace the boot disk
4. Remount the data disk
5. Reprovision the node agent
6. Generate a detailed summary

## Output

The script generates two files:
1. `rehydration_<timestamp>.log`: Detailed log of the rehydration process
2. `rehydration_summary_<timestamp>.txt`: Summary of the rehydration process, including:
   - Total nodes processed
   - Success/failure counts
   - Detailed status for each node
   - Any errors encountered

## Error Handling

- The script stops processing on the first node failure
- Detailed error messages are logged
- A summary is generated even if the process fails
- Exit code 1 is returned if any node fails

## Best Practices

1. Always backup your configuration before running the script
2. Test the script in a non-production environment first
3. Ensure you have sufficient permissions in GCP and YBA
4. Monitor the logs during execution
5. Review the summary after completion
## Notes: With Terraformed managed infrastructure, once you use this script to rehydrate nodes by replacing the boot disk, the infrastructure (specifically the GCE instances) is no longer in the same state as what Terraform originally provisioned.
What Breaks or Becomes Risky:
Terraform will detect drift:
Since the boot disk has been replaced outside of Terraform, a future terraform plan or apply may try to "correct" it, i.e., recreate the instance using the old AMI.
This is especially true if boot_disk.image is tracked in Terraform.
Drifted state:
The Terraform state file no longer matches reality. The disk resource (google_compute_disk) that was replaced is now stale in the .tfstate file.
Terraform taint or ignore_changes may be needed:
You’d have to use something like:
lifecycle {
  ignore_changes = [boot_disk]
}
## Recommendations:
Given this boot disk rehydration process is part of a lifecycle (e.g., every 45 days), it’s best to treat these nodes as semi-managed by Terraform.

Consider separating your Terraform stack
- Core infra (VPC, IAM, etc.): fully Terraform-managed.
- Ephemeral compute resources (like these YugabyteDB nodes): provisioned once, then maintained with scripts like this.


## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Your chosen license]

## Support

For support, please [create an issue](repository-issues-url) in the repository.
