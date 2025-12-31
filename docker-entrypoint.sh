#!/bin/bash
set -e

# Ensure required directories exist with proper ownership
# These will be created in the mounted volumes if they don't exist
REQUIRED_DIRS=(
    "/opt/discord/cnf"
    "/opt/discord/downloads"
    "/var/log/discord"
)

echo "Discord Bot Container Starting..."
echo "Running as user: $(id -u):$(id -g)"

for dir in "${REQUIRED_DIRS[@]}"; do
    if [ ! -d "$dir" ]; then
        echo "Creating directory: $dir"
        mkdir -p "$dir" 2>/dev/null || {
            echo "Warning: Could not create $dir (may need to set host permissions)"
        }
    fi
done

# Check write permissions on critical directories
echo "Checking write permissions..."
for dir in "/opt/discord" "/var/log/discord"; do
    if [ -w "$dir" ]; then
        echo "✓ Write access to $dir"
    else
        echo "✗ WARNING: No write access to $dir - check volume mount permissions!"
        echo "  Run: sudo chown -R 1000:1000 /path/to/host/$dir"
    fi
done

echo "Starting discord-bot..."
exec "$@"
