#!/bin/bash

# Directories to set ownership
DIRECTORIES=("dags" "logs" "plugins" "scripts" "utils")

# Owner and group
OWNER=50000
GROUP=50000

# Setting ownership
for DIR in "${DIRECTORIES[@]}"; do
    echo "Setting ownership for $DIR..."
    sudo chown -R $OWNER:$GROUP $DIR
done

# Setting permissions
for DIR in "${DIRECTORIES[@]}"; do
    echo "Setting permissions for $DIR..."
    sudo chmod -R 755 $DIR
done

echo "Permissions and ownership have been set."
