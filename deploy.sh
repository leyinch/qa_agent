#!/bin/bash

# Deployment Script for QA Agent
# Run this in Google Cloud Shell

# Extract configuration from cloudbuild.yaml (single source of truth)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME=$(grep '_SERVICE_NAME:' "${SCRIPT_DIR}/cloudbuild.yaml" | awk '{print $2}')
REGION=$(grep '_REGION:' "${SCRIPT_DIR}/cloudbuild.yaml" | awk '{print $2}')

PROJECT_ID=$(grep '_PROJECT_ID:' "${SCRIPT_DIR}/cloudbuild.yaml" | awk '{print $2}')
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"

echo "Starting deployment for ${SERVICE_NAME}..."
echo "Using Project ID: ${PROJECT_ID}"

# 1. Set Project
# echo "Setting project to ${PROJECT_ID}..."
# gcloud config set project $PROJECT_ID


# 2. Enable APIs
echo "Enabling necessary APIs..."
gcloud services enable run.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com

# 3. Build Image
echo "Building Docker image..."
gcloud builds submit --tag $IMAGE_NAME .

# 4. Deploy
echo "Deploying to Cloud Run..."
# NOTE: Replace the values below or set them as environment variables before running, 
# or configure them in the Google Cloud Console after deployment.
gcloud run deploy $SERVICE_NAME \
  --image $IMAGE_NAME \
  --platform managed \
  --region $REGION \
# 5. Configure OAuth (Interactive)
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --platform managed --region $REGION --format 'value(status.url)')

echo ""
echo "--------------------------------------------------------"
echo "Deployment Complete! 🎉"
echo "Service URL: $SERVICE_URL"
echo "--------------------------------------------------------"
echo ""
echo "To finish setup, you need to configure Google OAuth."
echo "1. Go to https://console.cloud.google.com/apis/credentials?project=$PROJECT_ID"
echo "2. Create OAuth Client ID (Web Application)."
echo "3. Add Redirect URI: $SERVICE_URL/api/auth/callback/google"
echo ""
read -p "Do you want to configure OAuth now? (y/n) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    read -p "Enter Google/OAuth Client ID: " GOOGLE_CLIENT_ID
    read -p "Enter Google/OAuth Client Secret: " GOOGLE_CLIENT_SECRET
    
    # Generate a random secret for NextAuth if not provided
    NEXTAUTH_SECRET=$(openssl rand -base64 32)
    
    echo "Updating Cloud Run service with environment variables..."
    gcloud run services update $SERVICE_NAME \
      --platform managed \
      --region $REGION \
      --set-env-vars "GOOGLE_CLIENT_ID=$GOOGLE_CLIENT_ID,GOOGLE_CLIENT_SECRET=$GOOGLE_CLIENT_SECRET,NEXTAUTH_SECRET=$NEXTAUTH_SECRET,NEXTAUTH_URL=$SERVICE_URL"
      
    echo "Configuration Complete!"
else
    echo "Skipping OAuth configuration. You can do this later using the 'gcloud run services update' command."
fi
