# Cloud Build Deployment Guide

This guide explains how to use the generated Cloud Build configuration files to deploy your Backend and Frontend to Cloud Run.

## Prerequisites

1.  **Artifact Registry**: You must have an Artifact Registry repository named `agent-repo` created in your project.
    ```bash
    gcloud artifacts repositories create agent-repo \
      --repository-format=docker \
      --location=australia-southeast2 \
      --project=crown-cdw-intelia-dev \
      --description="Docker repository for QA Agent"
    ```

2.  **Permissions**: Ensure your Cloud Build Service Account has permissions to deploy to Cloud Run (see `ENTERPRISE_ACCESS_SETUP.md`).

## 1. Deploying the Backend

The backend config looks for the `Dockerfile` inside the `backend/` directory.

**Command to run:**
```bash
gcloud builds submit . \
  --project=crown-cdw-intelia-dev \
  --config=cloudbuild.backend.yaml \
  --substitutions=_SERVICE_NAME=qa-agent-backend,LOCATION=australia-southeast2
```

## 2. Deploying the Frontend

The frontend config looks for the `Dockerfile` in the root directory.

**Command to run:**
```bash
# 1. Deploy Backend first to get the URL
gcloud builds submit . \
  --project=crown-cdw-intelia-dev \
  --config=cloudbuild.backend.yaml \
  --substitutions=_SERVICE_NAME=qa-agent-backend,LOCATION=australia-southeast2

# 2. Get the Backend URL from the output...

# 3. Deploy Frontend with Backend URL
gcloud builds submit . \
  --project=crown-cdw-intelia-dev \
  --config=cloudbuild.frontend.yaml \
  --substitutions=_SERVICE_NAME=qa-agent-frontend,LOCATION=australia-southeast2,_BACKEND_URL=https://qa-agent-backend-xxx.a.run.app
```

## 4. Deploying from GitHub

If you want to deploy directly from your GitHub repository (without cloning it locally), you can use the `--git-source-url` flag.

**Backend from GitHub:**
```bash
gcloud builds submit https://github.com/mirunasuresh23/qa_agent \
  --project=crown-cdw-intelia-dev \
  --config=cloudbuild.backend.yaml \
  --substitutions=_SERVICE_NAME=qa-agent-backend,LOCATION=australia-southeast2
```

**Note:** This requires the `cloudbuild.backend.yaml` file to be present in the root of your GitHub repository.

## 5. Setting up a Build Trigger

For an enterprise environment, we recommend setting up a **Cloud Build Trigger** so that your app redeploys automatically whenever you push code to GitHub.

1.  Go to [Cloud Build Triggers](https://console.cloud.google.com/cloud-build/triggers?project=crown-cdw-intelia-dev) in the Google Cloud Console.
2.  Click **Manage Repositories** and connect your GitHub repo `mirunasuresh23/qa_agent`.
### Automated Trigger via CLI

If your repository is already connected to Google Cloud, you can create the trigger using this command:

```bash
gcloud beta builds triggers create github \
  --project=crown-cdw-intelia-dev \
  --repo-owner=mirunasuresh23 \
  --repo-name=qa_agent \
  --branch-pattern=".*" \
  --build-config=cloudbuild.backend.yaml \
  --substitutions=_SERVICE_NAME=qa-agent-backend,LOCATION=australia-southeast2
```

## 6. Enterprise 1st Gen Trigger (Inline YAML)

For your enterprise setup on `crown-cdw-intelia-dev`, here are the specific settings for a **1st Gen** push trigger with **Inline YAML**.

### CLI Command to Create Trigger

```bash
gcloud alpha builds triggers create github \
  --project=crown-cdw-intelia-dev \
  --repo-owner=mirunasuresh23 \
  --repo-name=qa_agent \
  --branch-pattern="^main$" \
  --tags="qa-agent,gcp-cloud-build-deploy-cloud-run" \
  --service-account="projects/crown-cdw-intelia-dev/serviceAccounts/445471616138-compute@developer.gserviceaccount.com" \
  --substitutions="_SERVICE_NAME=qa-agent-backend,LOCATION=australia-southeast2" \
  --inline-config="cloudbuild.backend.yaml" 
```

### Manual Console Settings (UI)

If configuring via the [Google Cloud Console](https://console.cloud.google.com/cloud-build/triggers), use these exact values:

| Field | Value |
| :--- | :--- |
| **Name** | `qa-agent-backend-trigger` |
| **Tags** | `qa-agent`, `gcp-cloud-build-deploy-cloud-run` |
| **Event** | Push to a branch |
| **Source** | `mirunasuresh23/qa_agent` (1st Gen) |
| **Branch** | `^main$` |
| **Configuration** | Cloud Build configuration file -> **Inline** |
| **Service Account** | `445471616138-compute@developer.gserviceaccount.com` |
| **Substitutions** | `_SERVICE_NAME`: `qa-agent-backend`, `LOCATION`: `australia-southeast2` |

> [!TIP]
> When using **Inline YAML**, copy the contents of your [cloudbuild.backend.yaml](file:///c:/Users/MirunaSuresh/Documents/antigravity/Test1/cloudbuild.backend.yaml) and paste it directly into the Cloud Build UI editor.

