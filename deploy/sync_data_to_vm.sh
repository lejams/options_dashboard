#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <ssh-target> <remote-data-dir>"
  echo "Example: $0 azureuser@1.2.3.4 /opt/options_data"
  exit 1
fi

SSH_TARGET="$1"
REMOTE_DATA_DIR="$2"
LOCAL_DATA_DIR="${LOCAL_DATA_DIR:-/Users/ismailje/Documents/dashboard_macro/data/vm_option_data_full_20260322}"

ssh "$SSH_TARGET" "sudo mkdir -p '$REMOTE_DATA_DIR' && sudo chown -R \"\$USER\":\"\$USER\" '$REMOTE_DATA_DIR'"

rsync -avh --progress --delete \
  --exclude ".DS_Store" \
  "$LOCAL_DATA_DIR"/ \
  "$SSH_TARGET":"$REMOTE_DATA_DIR"/

ssh "$SSH_TARGET" "find '$REMOTE_DATA_DIR' -maxdepth 2 -type d | sort && du -sh '$REMOTE_DATA_DIR'"
