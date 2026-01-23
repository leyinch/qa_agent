# Data QA Agent (Combined)

This is a powerful Data Quality Assurance Platform that uses Generative AI (Vertex AI) to analyze BigQuery schemas and ER diagrams to generate and execute quality tests.

## 🚀 Key Features

- **GCS to BigQuery Comparison**: Validate data integrity between files and tables.
- **Schema Validation**: Verify BigQuery schemas against ERD descriptions.
- **SCD Validation (Types 1 & 2)**: Comprehensive integrity checks for dimension tables (overlaps, gaps, current flags, etc.).
- **AI-Powered Test Suggestions**: Vertex AI suggests context-aware custom tests.
- **Advanced Results Dashboard**:
  - **Tabbed UI**: Batch results organized by mapping.
  - **Bad Data Preview**: View the actual problematic rows directly in the UI.
  - **SQL Transparency**: View the underlying BigQuery SQL for every test.
  - **Sample Data**: Inline visualization of validation failures.
- **Execution History**: Track all runs with status distributions and historical performance.

## Prerequisites

- Google Cloud Project
- BigQuery Dataset (default: `config` for metadata, `qa_results` for history)
- Vertex AI API enabled
- BigQuery API enabled

## Documentation Links
- [SCD Validation Guide](./SCD_VALIDATION_README.md) - Deep dive into dimension testing.
- [Config Tables Setup](./CONFIG_TABLES_README.md) - How to set up the BigQuery backend.
- [Deployment Guide](./DEPLOYMENT_GUIDE.md) - How to deploy to Cloud Run.
- [Logging Guide](./LOGGING_GUIDE.md) - How to debug and view logs.

## Local Development

### 1. Prerequisite Environment Variables
Create a `.env.local` in the root directory:
```bash
GOOGLE_CLIENT_ID=xxx.apps.googleusercontent.com
GOOGLE_CLIENT_SECRET=xxx
NEXTAUTH_SECRET=a-random-long-string
NEXTAUTH_URL=http://localhost:3000
GOOGLE_CLOUD_PROJECT=your-project-id
```

### 2. Running with Docker Compose (Recommended)
```bash
docker-compose up --build -d
```
- **Frontend**: [http://localhost:3000](http://localhost:3000)
- **Backend API**: [http://localhost:8000](http://localhost:8000)
- **API Docs (Swagger)**: [http://localhost:8000/docs](http://localhost:8000/docs)

### 3. Running Manually
#### Frontend (Next.js):
```bash
npm install
npm run dev
```

#### Backend (FastAPI):
```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

