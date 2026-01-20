# Cloud Build Deployment Guide

This guide explains how to use the generated Cloud Build configuration files to deploy your Backend and Frontend to Cloud Run.

## Prerequisites

1.  **Artifact Registry**: You must have an Artifact Registry repository named `agent-repo` created in your project.
    ```bash
    gcloud artifacts repositories create agent-repo \
      --repository-format=docker \
      --location=us-central1 \
      --description="Docker repository for QA Agent"
    ```

2.  **Permissions**: Ensure your Cloud Build Service Account has permissions to deploy to Cloud Run (see `ENTERPRISE_ACCESS_SETUP.md`).

## 1. Deploying the Backend

The backend config looks for the `Dockerfile` inside the `backend/` directory.

**Command to run:**
```bash
gcloud builds submit . \
  --config cloudbuild.backend.yaml \
  --substitutions=_SERVICE_NAME=qa-agent-backend,LOCATION=us-central1
```

## 2. Deploying the Frontend

The frontend config looks for the `Dockerfile` in the root directory.

**Command to run:**
```bash
# 1. Deploy Backend first to get the URL
gcloud builds submit . \
  --config cloudbuild.backend.yaml \
  --substitutions=_SERVICE_NAME=qa-agent-backend,LOCATION=us-central1

# 2. Get the Backend URL (replace with actual command interpretation or manual Step)
# Example: https://qa-agent-backend-xyz.run.app

# 3. Deploy Frontend with Backend URL
gcloud builds submit . \
  --config cloudbuild.frontend.yaml \
  --substitutions=_SERVICE_NAME=qa-agent-frontend,LOCATION=us-central1,_BACKEND_URL=https://your-backend-url.run.app
```

## 3. Customizing the Deployment

*   **Service Name**: Change the `_SERVICE_NAME` substitution to rename your Cloud Run service.
*   **Region**: Change `LOCATION=us-central1` if you are deploying to a different region.
*   **Service Accounts**: To attach the specific service accounts we identified earlier, uncomment the `--service-account` lines in the YAML files or add strict IAM bindings via Terraform.
