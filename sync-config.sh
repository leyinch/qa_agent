#!/bin/bash

# Sync Configuration Script
# Reads deploy.config and updates all cloudbuild.yaml files
# Usage: ./sync-config.sh

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}Syncing deployment configuration...${NC}"

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/deploy.config"

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo -e "${RED}Error: deploy.config not found${NC}"
    exit 1
fi

# Source the configuration
source "$CONFIG_FILE"

# Validate required variables
if [ -z "$PROJECT_ID" ] || [ -z "$REGION" ] || [ -z "$FRONTEND_SERVICE" ] || [ -z "$BACKEND_SERVICE" ]; then
    echo -e "${RED}Error: Missing required configuration in deploy.config${NC}"
    exit 1
fi

echo "Configuration loaded:"
echo "  PROJECT_ID: $PROJECT_ID"
echo "  REGION: $REGION"
echo "  FRONTEND_SERVICE: $FRONTEND_SERVICE"
echo "  BACKEND_SERVICE: $BACKEND_SERVICE"
echo ""

# Function to update YAML file
update_yaml() {
    local file=$1
    local service_name=$2
    
    if [ ! -f "$file" ]; then
        echo -e "${RED}Warning: $file not found, skipping${NC}"
        return
    fi
    
    echo "Updating $file..."
    
    # Create backup
    cp "$file" "$file.bak"
    
    # Update substitutions using sed
    # This preserves the YAML structure and only updates the values
    sed -i.tmp \
        -e "s/_SERVICE_NAME:.*/_SERVICE_NAME: $service_name/" \
        -e "s/_REGION:.*/_REGION: $REGION/" \
        -e "s/_PROJECT_ID:.*/_PROJECT_ID: $PROJECT_ID/" \
        "$file"
    
    # Remove temporary file
    rm -f "$file.tmp"
    
    echo -e "${GREEN}✓ Updated $file${NC}"
}

# Update frontend cloudbuild.yaml
update_yaml "${SCRIPT_DIR}/cloudbuild.yaml" "$FRONTEND_SERVICE"

# Update backend cloudbuild.yaml
update_yaml "${SCRIPT_DIR}/backend/cloudbuild.yaml" "$BACKEND_SERVICE"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Configuration sync complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Updated files:"
echo "  - cloudbuild.yaml (frontend)"
echo "  - backend/cloudbuild.yaml (backend)"
echo ""
echo "Backup files created:"
echo "  - cloudbuild.yaml.bak"
echo "  - backend/cloudbuild.yaml.bak"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Review the changes in the YAML files"
echo "2. Commit the updated files to git"
echo "3. Deploy with ./deploy-all.sh or git push"
