# Python Backend Migration - Deployment Guide

## 🎯 Overview

The Data QA Agent now uses a hybrid architecture:
- **Frontend**: Next.js (TypeScript/React) on port 3000
- **Backend**: Python FastAPI on port 8000

## 📋 Prerequisites

- Python 3.11+
- Node.js 18+
- Google Cloud SDK
- Docker (optional, for containerized deployment)

---

## 🚀 Local Development

### Option 1: Manual Start

**Terminal 1 - Backend:**
```bash
cd backend
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate

pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

**Terminal 2 - Frontend:**
```bash
# In project root
npm install
npm run dev
```

### Option 2: Docker Compose

```bash
docker-compose up
```

### Access Points

- Frontend: http://localhost:3000
- Backend API: http://localhost:8000
- API Docs: http://localhost:8000/docs
- Health Check: http://localhost:8000/health

---

## 🔧 Configuration

### Backend (.env)

Create `backend/.env`:
```bash
GOOGLE_CLOUD_PROJECT=miruna-sandpit
```

### Frontend (.env.local)

Create `.env.local`:
```bash
NEXT_PUBLIC_BACKEND_URL=http://localhost:8000
GOOGLE_CLIENT_ID=your-client-id
GOOGLE_CLIENT_SECRET=your-client-secret
NEXTAUTH_SECRET=your-secret
NEXTAUTH_URL=http://localhost:3000
```

---

## ☁️ Cloud Run Deployment

### Deploy Backend

```bash
cd backend

gcloud run deploy data-qa-agent-backend \
  --source . \
  --platform managed \
  --region us-central1 \
  --project miruna-sandpit \
  --allow-unauthenticated \
  --set-env-vars GOOGLE_CLOUD_PROJECT=miruna-sandpit
```

**Note the backend URL** from the output (e.g., `https://data-qa-agent-backend-xxx.run.app`)

### Deploy Frontend

```bash
# In project root
gcloud run deploy data-qa-agent-frontend \
  --source . \
  --platform managed \
  --region us-central1 \
  --project miruna-sandpit \
  --allow-unauthenticated \
  --set-env-vars NEXT_PUBLIC_BACKEND_URL=https://data-qa-agent-backend-xxx.run.app,GOOGLE_CLIENT_ID=your-id,GOOGLE_CLIENT_SECRET=your-secret,NEXTAUTH_SECRET=your-secret,NEXTAUTH_URL=https://data-qa-agent-frontend-xxx.run.app
```

### Update OAuth Redirect URIs

Add the frontend URL to Google Cloud Console OAuth credentials:
- Authorized redirect URIs: `https://data-qa-agent-frontend-xxx.run.app/api/auth/callback/google`

---

## 🧪 Testing the Backend

### Health Check

```bash
curl http://localhost:8000/health
```

### List Predefined Tests

```bash
curl http://localhost:8000/api/predefined-tests
```

### Generate Tests (Single File)

```bash
curl -X POST http://localhost:8000/api/generate-tests \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": "miruna-sandpit",
    "comparison_mode": "gcs",
    "gcs_bucket": "your-bucket",
    "gcs_file_path": "path/to/file.csv",
    "target_dataset": "your_dataset",
    "target_table": "your_table"
  }'
```

### Generate Tests (Config Table)

```bash
curl -X POST http://localhost:8000/api/generate-tests \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": "miruna-sandpit",
    "comparison_mode": "gcs-config",
    "config_dataset": "config",
    "config_table": "data_load_config"
  }'
```

---

## 🐛 Troubleshooting

### Backend Issues

**Import errors:**
```bash
# Reinstall dependencies
pip install -r requirements.txt
```

**Google Cloud authentication:**
```bash
gcloud auth application-default login
```

**Port already in use:**
```bash
# Change port
uvicorn app.main:app --port 8001
```

### Frontend Issues

**Cannot connect to backend:**
- Check `NEXT_PUBLIC_BACKEND_URL` in `.env.local`
- Verify backend is running: `curl http://localhost:8000/health`

**CORS errors:**
- Backend CORS is configured for `http://localhost:3000`
- Check browser console for exact error

### Docker Issues

**Build failures:**
```bash
# Clean rebuild
docker-compose down
docker-compose build --no-cache
docker-compose up
```

---

## 📊 Monitoring

### Backend Logs (Local)

Backend logs appear in the terminal where uvicorn is running.

### Cloud Run Logs

```bash
# Backend logs
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=data-qa-agent-backend" --limit 50

# Frontend logs
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=data-qa-agent-frontend" --limit 50
```

---

## 🔄 Migration Checklist

- [x] Python backend created
- [x] Core services implemented
- [x] API endpoints created
- [x] Frontend updated to use backend URL
- [x] Docker configuration added
- [ ] Test locally with both services
- [ ] Deploy backend to Cloud Run
- [ ] Deploy frontend to Cloud Run
- [ ] Update OAuth credentials
- [ ] End-to-end testing

---

## 📚 API Documentation

Once the backend is running, visit:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

These provide interactive API documentation with request/response examples.
