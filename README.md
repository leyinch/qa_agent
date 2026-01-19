# Data QA Agent

This is a Next.js application for testing data quality using AI. It uses Google Cloud Vertex AI to generate test cases from an ER diagram and BigQuery schema, and executes them against BigQuery.

## Prerequisites

- Google Cloud Project: `your-project-id`
- BigQuery Dataset: `config`, `qa_results`, `crown_scd_mock`
- Vertex AI API enabled
- BigQuery API enabled

## 🚀 Quick Start (Setup)

To set up all required BigQuery datasets and tables:

1. **Parameterize SQL**: Run `./parameterize-sql.sh` to generate your project-specific scripts.
2. **Run Scripts**: Execute `setup_scd_resources.generated.sql` and `config_tables_setup.generated.sql` in the [BigQuery Console](https://console.cloud.google.com/bigquery?project=your-project-id).
3. **Follow the Guides**: See the [Deployment Guide](DEPLOYMENT.md) and [SCD Validation Guide](SCD_VALIDATION_README.md) for more details.


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
