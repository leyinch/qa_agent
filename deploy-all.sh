#!/bin/bash

# Unified Deployment Script for QA Agent
# Automatically detects first-time vs redeployment
# Usage:
#   ./deploy-all.sh                 # Auto-detect and deploy
#   ./deploy-all.sh --backend       # Deploy backend only
#   ./deploy-all.sh --frontend      # Deploy frontend only
#   ./deploy-all.sh --force-oauth   # Force OAuth reconfiguration

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Load configuration from deploy.config
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/deploy.config"

if [ ! -f "$CONFIG_FILE" ]; then
    echo -e "${RED}Error: Configuration file not found: $CONFIG_FILE${NC}"
    echo "Please create deploy.config with required parameters."
    exit 1
fi

# Source the configuration file
source "$CONFIG_FILE"

# Validate required configuration
if [ -z "$PROJECT_ID" ] || [ -z "$REGION" ] || [ -z "$FRONTEND_SERVICE" ] || [ -z "$BACKEND_SERVICE" ]; then
    echo -e "${RED}Error: Missing required configuration in deploy.config${NC}"
    echo "Required: PROJECT_ID, REGION, FRONTEND_SERVICE, BACKEND_SERVICE"
    exit 1
fi

# Function to sync YAML files with deploy.config
sync_yaml_files() {
    echo -e "${BLUE}Syncing cloudbuild.yaml files with deploy.config...${NC}"
    
    # Update frontend cloudbuild.yaml
    if [ -f "${SCRIPT_DIR}/cloudbuild.yaml" ]; then
        sed -i.bak \
            -e "s/_SERVICE_NAME:.*/_SERVICE_NAME: $FRONTEND_SERVICE/" \
            -e "s/_REGION:.*/_REGION: $REGION/" \
            -e "s/_PROJECT_ID:.*/_PROJECT_ID: $PROJECT_ID/" \
            "${SCRIPT_DIR}/cloudbuild.yaml"
        echo -e "${GREEN}✓ Synced cloudbuild.yaml${NC}"
    fi
    
    # Update backend cloudbuild.yaml
    if [ -f "${SCRIPT_DIR}/backend/cloudbuild.yaml" ]; then
        sed -i.bak \
            -e "s/_SERVICE_NAME:.*/_SERVICE_NAME: $BACKEND_SERVICE/" \
            -e "s/_REGION:.*/_REGION: $REGION/" \
            -e "s/_PROJECT_ID:.*/_PROJECT_ID: $PROJECT_ID/" \
            "${SCRIPT_DIR}/backend/cloudbuild.yaml"
        echo -e "${GREEN}✓ Synced backend/cloudbuild.yaml${NC}"
    fi
}

# Automatically sync YAML files before deployment
sync_yaml_files

# Parse arguments
FORCE_OAUTH=false
DEPLOY_BACKEND=true
DEPLOY_FRONTEND=true

while [[ $# -gt 0 ]]; do
    case $1 in
        --force-oauth)
            FORCE_OAUTH=true
            shift
            ;;
        --backend)
            DEPLOY_FRONTEND=false
            shift
            ;;
        --frontend)
            DEPLOY_BACKEND=false
            shift
            ;;
        --first-time)
            echo -e "${YELLOW}Note: --first-time flag is deprecated. Script auto-detects deployment type.${NC}"
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Usage: $0 [--backend|--frontend] [--force-oauth]"
            exit 1
            ;;
    esac
done

# Function to check if service exists
check_service_exists() {
    local service_name=$1
    if gcloud run services describe "$service_name" \
        --platform managed \
        --region "$REGION" \
        --project "$PROJECT_ID" \
        --format="value(metadata.name)" &>/dev/null; then
        return 0  # Service exists
    else
        return 1  # Service does not exist
    fi
}

# Function to check if OAuth is configured
check_oauth_configured() {
    local service_name=$1
    local env_vars=$(gcloud run services describe "$service_name" \
        --platform managed \
        --region "$REGION" \
        --project "$PROJECT_ID" \
        --format="value(spec.template.spec.containers[0].env)" 2>/dev/null || echo "")
    
    if echo "$env_vars" | grep -q "GOOGLE_CLIENT_ID"; then
        return 0  # OAuth is configured
    else
        return 1  # OAuth not configured
    fi
}

# Auto-detect deployment type
BACKEND_EXISTS=false
FRONTEND_EXISTS=false
OAUTH_CONFIGURED=false
FIRST_TIME_DEPLOYMENT=false

if [ "$DEPLOY_BACKEND" = true ]; then
    if check_service_exists "$BACKEND_SERVICE"; then
        BACKEND_EXISTS=true
    fi
fi

if [ "$DEPLOY_FRONTEND" = true ]; then
    if check_service_exists "$FRONTEND_SERVICE"; then
        FRONTEND_EXISTS=true
        if check_oauth_configured "$FRONTEND_SERVICE"; then
            OAUTH_CONFIGURED=true
        fi
    fi
fi

# Determine if this is first-time deployment
if [ "$BACKEND_EXISTS" = false ] || [ "$FRONTEND_EXISTS" = false ]; then
    FIRST_TIME_DEPLOYMENT=true
fi

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}QA Agent Deployment${NC}"
echo -e "${GREEN}========================================${NC}"
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo ""
echo -e "${BLUE}Service Status:${NC}"
echo "Backend:  $([ "$DEPLOY_BACKEND" = true ] && echo "✓ Deploy" || echo "✗ Skip") $([ "$BACKEND_EXISTS" = true ] && echo "(exists)" || echo "(new)")"
echo "Frontend: $([ "$DEPLOY_FRONTEND" = true ] && echo "✓ Deploy" || echo "✗ Skip") $([ "$FRONTEND_EXISTS" = true ] && echo "(exists)" || echo "(new)")"
echo ""
echo -e "${BLUE}Deployment Type:${NC}"
if [ "$FIRST_TIME_DEPLOYMENT" = true ]; then
    echo -e "${YELLOW}First-time deployment detected${NC}"
    echo "- Will enable required APIs"
    if [ "$DEPLOY_FRONTEND" = true ] && [ "$OAUTH_CONFIGURED" = false ]; then
        echo "- Will prompt for OAuth configuration (frontend)"
    fi
else
    echo -e "${GREEN}Redeployment detected${NC}"
    echo "- Skipping API enablement"
    if [ "$OAUTH_CONFIGURED" = true ]; then
        echo "- OAuth already configured (skipping)"
    fi
fi
echo ""

# Enable required APIs (only on first deployment)
if [ "$FIRST_TIME_DEPLOYMENT" = true ]; then
    echo -e "${YELLOW}Enabling required APIs...${NC}"
    gcloud services enable \
        run.googleapis.com \
        cloudbuild.googleapis.com \
        artifactregistry.googleapis.com \
        bigquery.googleapis.com \
        aiplatform.googleapis.com \
        storage.googleapis.com
    echo -e "${GREEN}✓ APIs enabled${NC}"
fi

# Deploy Backend
if [ "$DEPLOY_BACKEND" = true ]; then
    echo ""
    echo -e "${YELLOW}========================================${NC}"
    echo -e "${YELLOW}Deploying Backend Service${NC}"
    echo -e "${YELLOW}========================================${NC}"
    
    cd backend
    
    echo "Building and deploying backend..."
    gcloud run deploy $BACKEND_SERVICE \
        --source . \
        --platform managed \
        --region $REGION \
        --allow-unauthenticated \
        --project $PROJECT_ID
    
    BACKEND_URL=$(gcloud run services describe $BACKEND_SERVICE \
        --platform managed \
        --region $REGION \
        --project $PROJECT_ID \
        --format 'value(status.url)')
    
    echo -e "${GREEN}✓ Backend deployed successfully${NC}"
    echo "Backend URL: $BACKEND_URL"
    
    cd ..
fi

# Deploy Frontend
if [ "$DEPLOY_FRONTEND" = true ]; then
    echo ""
    echo -e "${YELLOW}========================================${NC}"
    echo -e "${YELLOW}Deploying Frontend Service${NC}"
    echo -e "${YELLOW}========================================${NC}"
    
    echo "Building and deploying frontend..."
    gcloud run deploy $FRONTEND_SERVICE \
        --source . \
        --platform managed \
        --region $REGION \
        --allow-unauthenticated \
        --project $PROJECT_ID
    
    FRONTEND_URL=$(gcloud run services describe $FRONTEND_SERVICE \
        --platform managed \
        --region $REGION \
        --project $PROJECT_ID \
        --format 'value(status.url)')
    
    echo -e "${GREEN}✓ Frontend deployed successfully${NC}"
    echo "Frontend URL: $FRONTEND_URL"
    
    # OAuth Configuration (only if needed)
    NEED_OAUTH=false
    if [ "$OAUTH_CONFIGURED" = false ] || [ "$FORCE_OAUTH" = true ]; then
        NEED_OAUTH=true
    fi
    
    if [ "$NEED_OAUTH" = true ]; then
        echo ""
        echo -e "${YELLOW}========================================${NC}"
        echo -e "${YELLOW}OAuth Configuration (Frontend Only)${NC}"
        echo -e "${YELLOW}========================================${NC}"
        echo ""
        
        if [ "$FORCE_OAUTH" = true ]; then
            echo -e "${YELLOW}Forcing OAuth reconfiguration...${NC}"
        else
            echo -e "${BLUE}OAuth not configured. Setting up for first time...${NC}"
        fi
        
        echo ""
        echo "To enable Google OAuth authentication:"
        echo "1. Go to: https://console.cloud.google.com/apis/credentials?project=$PROJECT_ID"
        echo "2. Create OAuth 2.0 Client ID (Web Application)"
        echo "3. Add Authorized Redirect URI: $FRONTEND_URL/api/auth/callback/google"
        echo ""
        
        read -p "Do you want to configure OAuth now? (y/n) " -n 1 -r
        echo ""
        
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            read -p "Enter Google OAuth Client ID: " GOOGLE_CLIENT_ID
            read -p "Enter Google OAuth Client Secret: " GOOGLE_CLIENT_SECRET
            
            # Generate NextAuth secret
            NEXTAUTH_SECRET=$(openssl rand -base64 32)
            
            echo ""
            echo "Updating frontend service with OAuth configuration..."
            gcloud run services update $FRONTEND_SERVICE \
                --platform managed \
                --region $REGION \
                --project $PROJECT_ID \
                --set-env-vars "GOOGLE_CLIENT_ID=$GOOGLE_CLIENT_ID,GOOGLE_CLIENT_SECRET=$GOOGLE_CLIENT_SECRET,NEXTAUTH_SECRET=$NEXTAUTH_SECRET,NEXTAUTH_URL=$FRONTEND_URL"
            
            echo -e "${GREEN}✓ OAuth configured successfully${NC}"
        else
            echo -e "${YELLOW}⚠ OAuth configuration skipped${NC}"
            echo "You can configure it later with:"
            echo "gcloud run services update $FRONTEND_SERVICE \\"
            echo "  --region $REGION \\"
            echo "  --set-env-vars \"GOOGLE_CLIENT_ID=...,GOOGLE_CLIENT_SECRET=...,NEXTAUTH_SECRET=...,NEXTAUTH_URL=$FRONTEND_URL\""
        fi
    else
        echo ""
        echo -e "${GREEN}✓ OAuth already configured (skipping)${NC}"
        echo "Use --force-oauth to reconfigure"
    fi
fi

# Summary
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Deployment Complete! 🎉${NC}"
echo -e "${GREEN}========================================${NC}"

if [ "$DEPLOY_BACKEND" = true ]; then
    echo "Backend:  $BACKEND_URL"
fi

if [ "$DEPLOY_FRONTEND" = true ]; then
    echo "Frontend: $FRONTEND_URL"
fi

echo ""
echo -e "${YELLOW}Next Steps:${NC}"
if [ "$FIRST_TIME_DEPLOYMENT" = true ]; then
    echo "1. Test the application at the frontend URL"
    if [ "$OAUTH_CONFIGURED" = false ] && [ "$NEED_OAUTH" = false ]; then
        echo "2. Configure OAuth when ready (see instructions above)"
    fi
    echo "3. For future deployments, just run: ./deploy-all.sh"
else
    echo "1. Test your new features at the URLs above"
    echo "2. Monitor logs if needed:"
    if [ "$DEPLOY_FRONTEND" = true ]; then
        echo "   gcloud run logs tail $FRONTEND_SERVICE --region $REGION"
    fi
    if [ "$DEPLOY_BACKEND" = true ]; then
        echo "   gcloud run logs tail $BACKEND_SERVICE --region $REGION"
    fi
fi
echo ""
