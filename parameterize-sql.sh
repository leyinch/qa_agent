# SQL Parameterization Helper Script
# This script automates the process of replacing placeholders in SQL files 
# with the actual project configuration.

# 1. Load configuration from the single source of truth
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/deploy.config"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Configuration file not found: $CONFIG_FILE"
    exit 1
fi

source "$CONFIG_FILE"

# 2. Validate that PROJECT_ID is defined
if [ -z "$PROJECT_ID" ]; then
    echo "Error: PROJECT_ID not set in deploy.config"
    exit 1
fi

echo "Parameterizing SQL files for project: $PROJECT_ID"

# 3. Define the list of SQL templates to process
# These files should contain {{PROJECT_ID}} placeholders
SQL_FILES=("setup_scd_resources.sql" "config_tables_setup.sql")

# 4. Loop through each file and generate a parameterized version
for FILE in "${SQL_FILES[@]}"; do
    if [ -f "$FILE" ]; then
        # Create a new file with .generated.sql suffix
        OUTPUT_FILE="${FILE%.sql}.generated.sql"
        
        # Replace {{PROJECT_ID}} with the value from deploy.config
        sed "s/{{PROJECT_ID}}/$PROJECT_ID/g" "$FILE" > "$OUTPUT_FILE"
        
        echo "✓ Generated $OUTPUT_FILE (Ready to run in BigQuery)"
    fi
done

echo "Done. Please copy and run the contents of the .generated.sql files in the BigQuery Console."
