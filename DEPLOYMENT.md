# Deployment Guide

## Overview

This project features an **intelligent auto-deployment system** with:
- ✅ **Auto-detection** of first-time vs redeployment
- ✅ **Centralized configuration** via `deploy.config`
- ✅ **Automatic YAML sync** before deployment
- ✅ **Smart OAuth prompting** (only when needed)

Two deployment methods:
1. **Manual** - Run `deploy-all.sh` locally
2. **CI/CD** - Automated via Cloud Build on git push

## Quick Start

### Deploy Services

```bash
# Just run this - script auto-detects everything!
./deploy-all.sh

# Deploy backend only
./deploy-all.sh --backend

# Deploy frontend only
./deploy-all.sh --frontend

# Force OAuth reconfiguration
./deploy-all.sh --force-oauth
```

**No flags needed!** The script automatically:
- Detects if services exist
- Detects if OAuth is configured
- Syncs YAML files from deploy.config
- Prompts for OAuth only when needed

## Configuration

### Single Source of Truth: `deploy.config`

All deployment parameters are centralized in one file:

```bash
# deploy.config
PROJECT_ID=leyin-sandpit
REGION=us-central1
FRONTEND_SERVICE=data-qa-agent-frontend
BACKEND_SERVICE=data-qa-agent-backend
```

**To change configuration:**
1. Edit `deploy.config`
2. Run `./deploy-all.sh` (auto-syncs YAML files)
3. Done!

## Services

### Backend Service
- **Name**: Configured in `deploy.config` (default: `data-qa-agent-backend`)
- **Port**: 8080
- **Authentication**: Application Default Credentials (automatic)
- **Environment Variables**: None required
- **OAuth**: ❌ Not needed

### Frontend Service
- **Name**: Configured in `deploy.config` (default: `data-qa-agent-frontend`)
- **Port**: 8080
- **Authentication**: Google OAuth (NextAuth.js)
- **Environment Variables** (auto-prompted on first deployment):
  - `GOOGLE_CLIENT_ID`
  - `GOOGLE_CLIENT_SECRET`
  - `NEXTAUTH_SECRET` (auto-generated)
  - `NEXTAUTH_URL` (auto-detected)
- **OAuth**: ✅ Required (first deployment only)

## Auto-Detection Features

### What Gets Auto-Detected

The `deploy-all.sh` script automatically detects:

1. **Service Existence**
   - Checks if backend service exists in Cloud Run
   - Checks if frontend service exists in Cloud Run

2. **OAuth Configuration**
   - Checks if frontend has OAuth environment variables set

3. **Deployment Type**
   - First-time: If any service doesn't exist
   - Redeployment: If all services exist

4. **YAML Sync**
   - Automatically syncs `cloudbuild.yaml` files from `deploy.config`

### Example Output

```bash
$ ./deploy-all.sh

Syncing cloudbuild.yaml files with deploy.config...
✓ Synced cloudbuild.yaml
✓ Synced backend/cloudbuild.yaml

========================================
QA Agent Deployment
========================================
Project: leyin-sandpit
Region: us-central1

Service Status:
Backend:  ✓ Deploy (exists)
Frontend: ✓ Deploy (exists)

Deployment Type:
Redeployment detected
- Skipping API enablement
- OAuth already configured (skipping)
```

## OAuth Configuration

### When is OAuth Needed?

- ✅ **First-time frontend deployment** (auto-detected)
- ❌ **NOT needed for backend**
- ❌ **NOT needed for redeployments** (env vars persist)
- ✅ **Use `--force-oauth` to reconfigure**

### OAuth Setup Steps

1. **Create OAuth Credentials**:
   - Go to [Google Cloud Console - Credentials](https://console.cloud.google.com/apis/credentials?project=leyin-sandpit)
   - Click "Create Credentials" → "OAuth 2.0 Client ID"
   - Application type: "Web application"
   - Name: "QA Agent Frontend"

2. **Add Authorized Redirect URI**:
   ```
   https://[SERVICE-URL]/api/auth/callback/google
   ```
   (Script will display the exact URL)

3. **Enter Credentials**:
   - Script will prompt for Client ID and Client Secret
   - NEXTAUTH_SECRET is auto-generated
   - NEXTAUTH_URL is auto-detected

### Manual OAuth Update

```bash
gcloud run services update data-qa-agent-frontend \
  --region us-central1 \
  --set-env-vars "GOOGLE_CLIENT_ID=...,GOOGLE_CLIENT_SECRET=...,NEXTAUTH_SECRET=$(openssl rand -base64 32),NEXTAUTH_URL=https://..."
```

## Deployment Workflows

### Scenario 1: First-Time Deployment

```bash
# 1. Clone repository
git clone https://github.com/mirunasuresh23/qa_agent.git
cd qa_agent

# 2. Set gcloud project (IMPORTANT!)
gcloud config set project leyin-sandpit

# 3. Make scripts executable
chmod +x deploy-all.sh sync-config.sh

# 4. Run deployment (auto-detects first-time)
./deploy-all.sh

# 5. Follow OAuth prompts for frontend
```

**What happens:**
- Script detects services don't exist
- Enables required APIs
- Syncs YAML files
- Deploys both services
- Prompts for OAuth configuration

### Scenario 2: Feature Redeployment

```bash
# 1. Pull latest changes
git pull origin feature_leyin2

# 2. Deploy (auto-detects redeployment)
./deploy-all.sh
```

**What happens:**
- Script detects services exist
- Detects OAuth already configured
- Syncs YAML files
- Deploys updated code
- **No prompts!**

### Scenario 3: Change Configuration

```bash
# 1. Edit configuration
vim deploy.config
# Change PROJECT_ID, REGION, or service names

# 2. Deploy with new configuration
./deploy-all.sh
```

**What happens:**
- YAML files auto-sync with new values
- Deploys to new project/region/services

### Scenario 4: CI/CD Deployment

```bash
# 1. Edit code
# 2. Commit and push
git push origin feature_leyin2
```

**What happens:**
- Cloud Build trigger detects push
- Runs `cloudbuild.yaml` (frontend) or `backend/cloudbuild.yaml` (backend)
- Automatically deploys to Cloud Run

## Cloud Build Triggers (Optional)

### Setup Automated Deployment

**Backend Trigger:**
```bash
gcloud builds triggers create github \
  --repo-name=qa_agent \
  --repo-owner=mirunasuresh23 \
  --branch-pattern="^feature_leyin2$" \
  --build-config=backend/cloudbuild.yaml \
  --included-files="backend/**"
```

**Frontend Trigger:**
```bash
gcloud builds triggers create github \
  --repo-name=qa_agent \
  --repo-owner=mirunasuresh23 \
  --branch-pattern="^feature_leyin2$" \
  --build-config=cloudbuild.yaml \
  --ignored-files="backend/**"
```

## Files Overview

### `deploy.config`
**Single source of truth** for all deployment parameters.
- Edit this file to change project, region, or service names
- Used by `deploy-all.sh` (automatic)
- Referenced by `cloudbuild.yaml` files (manual sync via script)

### `deploy-all.sh`
**Unified deployment script** with auto-detection.
- Automatically detects deployment type
- Auto-syncs YAML files from `deploy.config`
- Intelligent OAuth prompting
- Supports `--backend`, `--frontend`, `--force-oauth` flags

### `sync-config.sh`
**Standalone YAML sync utility**.
- Updates `cloudbuild.yaml` files from `deploy.config`
- Integrated into `deploy-all.sh` (runs automatically)
- Can be run standalone: `./sync-config.sh`

### `cloudbuild.yaml` (Frontend)
Cloud Build configuration for frontend service.
- Auto-synced from `deploy.config`

### `backend/cloudbuild.yaml` (Backend)
Cloud Build configuration for backend service.
- Auto-synced from `deploy.config`

## Environment Variables Persistence

**Important**: Once environment variables are set in Cloud Run, they persist across redeployments.

- ✅ OAuth credentials remain configured
- ✅ No need to reconfigure on feature updates
- ✅ Only update if credentials change or service URL changes

## Troubleshooting

### Error: "The required property [project] is not currently set"

**Problem**: When running `./deploy-all.sh`, you get an error about missing project property.

**Solution**:
```bash
# Set your gcloud project
gcloud config set project leyin-sandpit

# Then run deployment again
./deploy-all.sh
```

This happens when gcloud doesn't have a default project configured. Setting it once will persist for future commands.

### Check OAuth Configuration

```bash
gcloud run services describe data-qa-agent-frontend \
  --region us-central1 \
  --format="value(spec.template.spec.containers[0].env)"
```

### Check Service URL

```bash
gcloud run services describe data-qa-agent-frontend \
  --region us-central1 \
  --format="value(status.url)"
```

### View Deployment Logs

```bash
# Frontend logs
gcloud run logs tail data-qa-agent-frontend --region us-central1

# Backend logs
gcloud run logs tail data-qa-agent-backend --region us-central1
```

### Force OAuth Reconfiguration

```bash
./deploy-all.sh --force-oauth
```

## Best Practices

1. **Edit `deploy.config` only** - Single source of truth for configuration
2. **Let script auto-detect** - No need for manual flags
3. **Test locally first** - Run `npm run dev` and `uvicorn app.main:app --reload`
4. **Monitor logs** - Use `gcloud run logs tail` to check for errors
5. **Keep OAuth secure** - Never commit credentials to git
6. **Use CI/CD for production** - Set up Cloud Build triggers for automated deployments

## Summary

**Simple workflow:**
1. Edit `deploy.config` (if needed)
2. Run `./deploy-all.sh`
3. Script handles everything automatically!

**No manual configuration needed!** ✅

