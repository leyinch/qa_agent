# Data QA Agent

This is a Next.js application for testing data quality using AI. It uses Google Cloud Vertex AI to generate test cases from an ER diagram and BigQuery schema, and executes them against BigQuery.

## Prerequisites

- Google Cloud Project: `leyin-sandpit`
- BigQuery Dataset: `transform_config`, `qa_agent_metadata`, `crown_scd_mock`
- Vertex AI API enabled
- BigQuery API enabled

## 🚀 Quick Start (Setup)

To set up all required BigQuery datasets and tables in a single step:

1. Open [BigQuery Console](https://console.cloud.google.com/bigquery?project=leyin-sandpit).
2. Run the master setup script: [setup_scd_resources.sql](file:///c:/Users/LeyinChen/Documents/Client%20-%20Crown/Antigravity/qa_agent/setup_scd_resources.sql).
3. Follow the [SCD Validation Guide](file:///c:/Users/LeyinChen/Documents/Client%20-%20Crown/Antigravity/qa_agent/SCD_VALIDATION_README.md) for detailed test instructions.


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

1. Build the container image:
   ```bash
   gcloud builds submit --tag gcr.io/PROJECT_ID/test-case-generator
   ```

2. Deploy to Cloud Run:
   ```bash
   gcloud run deploy test-case-generator \
     --image gcr.io/PROJECT_ID/test-case-generator \
     --platform managed \
     --region us-central1 \
     --allow-unauthenticated \
     --set-env-vars GOOGLE_CLIENT_ID=your-client-id,GOOGLE_CLIENT_SECRET=your-client-secret,NEXTAUTH_SECRET=your-secret,NEXTAUTH_URL=https://your-service-url
   ```

## Environment Variables

- `GOOGLE_CLIENT_ID`: Google OAuth Client ID
- `GOOGLE_CLIENT_SECRET`: Google OAuth Client Secret
- `NEXTAUTH_SECRET`: Random string for session encryption
- `NEXTAUTH_URL`: The canonical URL of your site
