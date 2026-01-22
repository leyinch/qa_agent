# 🤖 Gemini Instruction Manual: Data QA Agent (Combined)

This document provides a comprehensive overview of the **Data QA Agent** project to help future AI assistants (like Gemini/Antigravity) understand the codebase, architecture, and operational workflows.

---

## 🏗️ Architecture Overview

The project is a **Data Quality Assurance Platform** that uses Generative AI (Vertex AI) to analyze BigQuery schemas and ER diagrams to generate and execute quality tests.

- **Frontend**: Next.js (TypeScript/React) - Handles UI, OAuth, and result visualization.
- **Backend**: FastAPI (Python) - Handles logic, BigQuery interactions, and Vertex AI integrations.
- **Infrastructure**: Google Cloud Platform (BigQuery, Vertex AI, Cloud Run, Artifact Registry).
- **Orchestration**: Docker & Docker Compose for local development.

---

## 📂 Key Project Structure

```text
/
├── backend/                # FastAPI Application
│   ├── app/                # Source code
│   │   ├── services/       # BigQuery, Vertex AI, Test Execution logic
│   │   └── main.py         # API Endpoints
│   ├── Dockerfile          # Backend container def (Exposes 8080)
│   └── service-account.json # (OPTIONAL) Local GCP credentials (IGNORED BY GIT)
├── src/                    # Next.js Frontend (TypeScript)
│   ├── app/                # Next.js App Router pages
│   └── components/         # UI Components (Dashboard, Sidebar, etc.)
├── docker-compose.yml       # Orchestrates local FE + BE
├── deploy-all.sh           # Master deployment script for Cloud Run
├── deploy.config           # Deployment configuration constants
├── .env.local              # Frontend environment variables (IGNORED BY GIT)
└── .gitignore              # PROTECTS: .env.local, service-account.json, .next, etc.
```

---

## 🚀 Local Development Workflow

### 1. Prerequisite Environment Variables
Create a `.env.local` in the root directory:
```bash
GOOGLE_CLIENT_ID=xxx.apps.googleusercontent.com
GOOGLE_CLIENT_SECRET=xxx
NEXTAUTH_SECRET=a-random-long-string
NEXTAUTH_URL=http://localhost:3000
GOOGLE_CLOUD_PROJECT=your-project-id
```

### 2. Google Cloud Authentication (Local Docker)
The backend needs permissions to query BigQuery. You have two choices:
- **Option A (Personal Login)**: Run `gcloud auth application-default login` on your host. Docker is configured to mount `${APPDATA}/gcloud` to share these.
- **Option B (Service Account)**: Place a JSON key at `backend/service-account.json`. The `docker-compose.yml` is configured to pick this up automatically.

### 3. Running the App
```bash
docker-compose up --build -d
```
- **Frontend**: [http://localhost:3000](http://localhost:3000)
- **Backend API**: [http://localhost:8000](http://localhost:8000)
- **API Docs (Swagger)**: [http://localhost:8000/docs](http://localhost:8000/docs)

---

## ☁️ Deployment (Cloud Run)

The project uses a unified deployment script that handles both services:

1.  **Configure**: Update `deploy.config` with your `PROJECT_ID`, `REGION`, and `SERVICE_NAMES`.
2.  **Deploy**:
    ```bash
    chmod +x deploy-all.sh
    ./deploy-all.sh
    ```
    - The script automatically builds containers, enables APIs, and links the Frontend to the Backend.
    - It uses `--quiet` flags to prevent blocking in automated environments.

---

## ⚠️ Critical Development Rules

1.  **Port Mapping**:
    - Both Dockerfiles internally use port **8080** (Cloud Run default).
    - `docker-compose.yml` maps them to **3000** (FE) and **8000** (BE) on the host.
2.  **Security**:
    - NEVER commit `service-account.json` or `.env.local`.
    - Always verify `.gitignore` before a new commit.
3.  **API Routing**:
    - The Frontend uses `next.config.js` rewrites to proxy `/api/:path*` to the Backend.
    - Locally, the destination is `http://127.0.0.1:8000` or `http://backend:8080` (inside network).
4.  **BigQuery Config**:
    - The app expects a dataset (default: `config`) containing configuration tables like `scd_validation_config`.
    - See `config_tables_setup.sql` for the required schema.

---

## 🛠️ Common Troubleshooting

| Issue | Cause | Fix |
| :--- | :--- | :--- |
| **500 Internal Error** | Missing GCP Credentials | Run `gcloud auth application-default login` or check `service-account.json`. |
| **Login Redirect Loop** | `NEXTAUTH_URL` mismatch | Match `NEXTAUTH_URL` to your current browser URL (localhost vs Cloud Run). |
| **Backend 404** | Port Mismatch | Ensure FE is calling port 8000 (host) or using the internal Docker name `backend:8080`. |
| **Build Failures** | Cache issues | Run `docker-compose build --no-cache`. |

---

*Generated for future AI collaborators.*
