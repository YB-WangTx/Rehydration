# Configuration

This directory contains configuration files for the rehydration script.

## Files

- `yba_config.yaml.example`: Example configuration file
- `yba_config.yaml`: Actual configuration file (not tracked in git)

## Configuration Parameters

See the main README.md for detailed parameter descriptions.

## yba_config.yaml
yba:
  node_list:
    - "xx.xxx.xx.xxx": "yb-node-1"
  yba_host: "https://your-yba-host"
  yba_api_token: "your-api-token"
  universe_name: "your-universe"
  yba_cli_path: "/path/to/yba"

zone: "us-central1-a"
project_id: "your-project"
new_image: "your-image"
image_project: "your-image-project"
disk_type: "pd-standard"
disk_size: 100
sh_user: "sh_user"
ssh_wait_time: 60
