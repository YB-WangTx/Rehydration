# Yugabyte Rehydration Procedure

This repository contains a script for rehydrating Yugabyte nodes by replacing boot disks and reprovisioning node agents.

## Features

- Automatic boot disk replacement with new image
- Data disk preservation and remounting
- Yugabyte user reprovisioning
- Node agent reprovisioning
- YBA CLI integration for node management
- Comprehensive logging
- Error handling and verification steps

## Prerequisites

- Python 3.6+
- Google Cloud SDK (gcloud)
- Yugabyte Anywhere CLI (yba)
- Access to GCP project
- Access to Yugabyte Anywhere instance

## Installation

1. Clone the repository:
```bash
git clone https://github.com/your-org/yugabyte-rehydration.git
cd yugabyte-rehydration
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Copy the example config and update with your values:
```bash
cp config/yba_config.yaml.example config/yba_config.yaml
```

## Usage

1. Update the configuration in `config/yba_config.yaml` with your specific values.

2. Run the script:
```bash
./scripts/rehydration.py
```

## Configuration

The script uses a YAML configuration file (`yba_config.yaml`) with the following parameters:

- `yba`: Yugabyte Anywhere configuration
  - `node_list`: List of nodes to process
  - `yba_host`: YBA host URL
  - `yba_api_token`: API token
  - `universe_name`: Universe name
  - `yba_cli_path`: Path to YBA CLI
- `zone`: GCP zone
- `project_id`: GCP project ID
- `new_image`: New boot disk image
- `image_project`: Image project
- `disk_type`: Disk type
- `disk_size`: Disk size in GB
- `sh_user`: SSH user
- `ssh_wait_time`: Wait time after instance start

## Logging

The script creates a timestamped log file (`rehydration_YYYYMMDD_HHMMSS.log`) with detailed information about each step of the process.

## Error Handling

The script includes comprehensive error handling for:
- Instance status verification
- Disk operations
- SSH commands
- YBA CLI operations

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
