#!/bin/bash

# This script establishes an SSH tunnel to the remote database server.

# --- Configuration ---
LOCAL_PORT=5433
REMOTE_PORT=5432
REMOTE_HOST="173.249.24.215" # Hostname or IP address of your remote server
SSH_USER="emilios"           # Your SSH username

# --- Script Logic ---
echo "Attempting to establish an SSH tunnel..."
echo "Local Port: $LOCAL_PORT"
echo "Remote Port: $REMOTE_PORT"
echo "Remote Host: $REMOTE_HOST"
echo "SSH User: $SSH_USER"
echo "--------------------------------------------------"

# Check if a process is already listening on the local port
if lsof -i :$LOCAL_PORT > /dev/null; then
    echo "A process is already listening on port $LOCAL_PORT."
    echo "Assuming the tunnel is already active. If you have connection issues, please kill the existing process."
    exit 1
fi

# Establish the SSH tunnel in the background
ssh -f -N -L ${LOCAL_PORT}:localhost:${REMOTE_PORT} ${SSH_USER}@${REMOTE_HOST}

# Check if the tunnel was successfully established
if [ $? -eq 0 ]; then
    echo "SSH tunnel established successfully."
    echo "You can now connect to the database via localhost:$LOCAL_PORT"
    echo "The tunnel is running in the background. To close it, find the process with 'ps aux | grep ssh' and kill it."
else
    echo "Failed to establish SSH tunnel. Please check your SSH credentials and host."
fi