# Python Backend for Data QA Agent

AI-powered data quality testing backend built with FastAPI.

## Features

- GCS file processing with wildcard support
- BigQuery query execution and metadata retrieval
- Predefined test suite (8 standard tests)
- AI-powered test suggestions using Vertex AI
- Config table support for batch processing
- RESTful API with automatic documentation

## Setup

### Prerequisites

- Python 3.11+
- Google Cloud SDK
- Access to BigQuery and GCS

### Installation

1. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment:
```bash
cp .env.example .env
# Edit .env with your configuration
```

4. Authenticate with Google Cloud:
```bash
gcloud auth application-default login
```

### Running Locally

```bash
uvicorn app.main:app --reload --port 8000
```

API will be available at: http://localhost:8000

API Documentation: http://localhost:8000/docs

## API Endpoints

### POST /api/generate-tests
Generate and execute data quality tests.

**Modes:**
- `gcs`: Single file comparison
- `gcs-config`: Config table batch processing
- `schema`: Schema validation (coming soon)

### GET /health
Health check endpoint.

### GET /api/predefined-tests
List all available predefined tests.

## Docker

Build:
```bash
docker build -t data-qa-agent-backend .
```

Run:
```bash
docker run -p 8000:8000 \
  -v ~/.config/gcloud:/root/.config/gcloud:ro \
  -e GOOGLE_CLOUD_PROJECT=your-project \
  data-qa-agent-backend
```

## Testing

```bash
pytest
```

## Project Structure

```
backend/
├── app/
│   ├── main.py              # FastAPI application
│   ├── config.py            # Configuration
│   ├── models/              # Pydantic models
│   ├── services/            # Business logic
│   │   ├── gcs_service.py
│   │   ├── bigquery_service.py
│   │   ├── vertex_ai_service.py
│   │   └── test_executor.py
│   └── tests/               # Test definitions
│       └── predefined_tests.py
├── requirements.txt
├── Dockerfile
└── README.md
```

## Deployment

Deploy to Cloud Run:
```bash
gcloud run deploy data-qa-agent-backend \
  --source . \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated
```
