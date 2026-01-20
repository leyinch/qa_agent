# Enterprise Access & Service Account Setup

This document outlines the necessary Service Accounts, IAM Roles, and API enablements required to migrate the Data QA Agent to an enterprise Google Cloud project.

## 1. Required APIs

Ensure the following APIs are enabled in the enterprise project:

*   **Cloud Run API**: `run.googleapis.com`
*   **Cloud Workflows API**: `workflows.googleapis.com`
*   **BigQuery API**: `bigquery.googleapis.com`
*   **Cloud Storage API**: `storage.googleapis.com`
*   **Vertex AI API**: `aiplatform.googleapis.com` (for Gemini)
*   **Cloud Build API**: `cloudbuild.googleapis.com` (for CI/CD deployment)
*   **Artifact Registry API**: `artifactregistry.googleapis.com` (for container storage)
*   **Cloud Logging/Monitoring**: `logging.googleapis.com`, `monitoring.googleapis.com`

## 2. Service Accounts & IAM Roles

For a secure enterprise setup, we recommend creating dedicated service accounts for each component (Principle of Least Privilege).

### A. Backend Service Account
**Identity for:** The FastAPI Cloud Run Service.
**Recommended Name:** `sa-qa-backend@{project-id}.iam.gserviceaccount.com`

| Role | Purpose |
| :--- | :--- |
| `roles/bigquery.dataEditor` | Read/Write access to BigQuery datasets for test generation and inspection. |
| `roles/bigquery.jobUser` | Ability to run query jobs in BigQuery. |
| `roles/storage.objectViewer` | Read configuration files from Cloud Storage (if inputs are in GCS). |
| `roles/storage.objectCreator` | Write test results or logs to GCS (if applicable). |
| `roles/aiplatform.user` | Invoke Vertex AI (Gemini) models. |
| `roles/logging.logWriter` | Write application logs to Cloud Logging. |

### B. Workflow Service Account
**Identity for:** The Cloud Workflow orchestration.
**Recommended Name:** `sa-qa-workflow@{project-id}.iam.gserviceaccount.com`

| Role | Purpose |
| :--- | :--- |
| `roles/run.invoker` | Permission to invoke the Backend Cloud Run service directly. |
| `roles/logging.logWriter` | Write workflow logic executions and outcomes to logs. |

### C. Frontend Service Account (Optional but Recommended)
**Identity for:** The Next.js Cloud Run Service.
**Recommended Name:** `sa-qa-frontend@{project-id}.iam.gserviceaccount.com`

| Role | Purpose |
| :--- | :--- |
| `roles/run.invoker` | If the frontend makes server-side calls to the backend. |
| `roles/logging.logWriter` | Write application logs. |

## 3. Infrastructure & Deployment Access

**Deployment Pipeline / User Identity:**
The entity (User or CI/CD Pipeline) doing the deployment needs:

*   `roles/run.admin`: Create/Update Cloud Run services.
*   `roles/iam.serviceAccountUser`: Act as the Service Accounts listed above (to attach them to services).
*   `roles/storage.admin`: Manage GCS buckets (for tfstate or artifacts).
*   `roles/artifactregistry.admin`: Push Docker images.
*   `roles/workflows.editor`: Create/Update Workflows.

## 4. Security Note (Public Access)

Since the application-level login has been removed, the service as currently configured will be **publicly accessible** if deployed with `--allow-unauthenticated`.

**Enterprise Security Options:**
1.  **Internal Only:** Deploy with `--ingress=internal` to only allow access from within the VPC.
2.  **Identity-Aware Proxy (IAP):** Use IAP (requires Load Balancer) to handle authentication at the infrastructure level, independent of the app code.

## 5. Deployment Updates

When deploying to the new project, you must explicitly attach the service accounts.

**Example (Backend):**
```bash
gcloud run deploy data-qa-agent-backend \
  --project {ENTERPRISE_PROJECT_ID} \
  --service-account sa-qa-backend@{ENTERPRISE_PROJECT_ID}.iam.gserviceaccount.com \
  ...
```

**Example (Workflow):**
Update `workflow.yaml` or deployment command to specify the service account.
```bash
gcloud workflows deploy qa-test-flow \
  --service-account=sa-qa-workflow@{ENTERPRISE_PROJECT_ID}.iam.gserviceaccount.com \
  ...
```
