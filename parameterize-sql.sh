#!/bin/bash

# Load configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/deploy.config"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Configuration file not found: $CONFIG_FILE"
    exit 1
fi

source "$CONFIG_FILE"

if [ -z "$PROJECT_ID" ]; then
    echo "Error: PROJECT_ID not set in deploy.config"
    exit 1
fi

echo "Parameterizing SQL files for project: $PROJECT_ID"

# Files to process
SQL_FILES=("setup_scd_resources.sql" "config_tables_setup.sql")

for FILE in "${SQL_FILES[@]}"; do
    if [ -f "$FILE" ]; then
        OUTPUT_FILE="${FILE%.sql}.generated.sql"
        sed "s/{{PROJECT_ID}}/$PROJECT_ID/g" "$FILE" > "$OUTPUT_FILE"
        echo "✓ Generated $OUTPUT_FILE"
    fi
done

echo "Done. Use the .generated.sql files in BigQuery Console."
