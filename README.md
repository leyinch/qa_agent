# Data QA Agent

This is a Next.js application for testing data quality using AI. It uses Google Cloud Vertex AI to generate test cases from an ER diagram and BigQuery schema, and executes them against BigQuery.

## Prerequisites

- Google Cloud Project: `leyin-sandpit`
- BigQuery Dataset: `config`, `qa_results`, `crown_scd_mock`
- Vertex AI API enabled
- BigQuery API enabled

## 🚀 Quick Start (Setup)

To set up all required BigQuery datasets and tables in a single step:

1. Open [BigQuery Console](https://console.cloud.google.com/bigquery?project=leyin-sandpit).
2. Run the master setup script: [setup_scd_resources.sql](setup_scd_resources.sql).
3. Follow the [SCD Validation Guide](SCD_VALIDATION_README.md) for detailed test instructions.


## Local Development

Note: Node.js is required for local development.

1. Install dependencies:
   ```bash
   npm install
   ```

2. Run the development server:
   ```bash
   npm run dev
   ```

3. Open [http://localhost:3000](http://localhost:3000) with your browser.

## Deployment to Google Cloud Run

We use a unified deployment script for ease of use. All configurations are managed in `deploy.config`.

To deploy the entire application (Frontend + Backend):
```bash
./deploy-all.sh
```

To deploy only the backend:
```bash
./deploy-all.sh --backend
```

## Environment Variables

- `GOOGLE_CLIENT_ID`: Google OAuth Client ID
- `GOOGLE_CLIENT_SECRET`: Google OAuth Client Secret
- `NEXTAUTH_SECRET`: Random string for session encryption
- `NEXTAUTH_URL`: The canonical URL of your site
