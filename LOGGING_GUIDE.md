# Viewing Logs for Test Case Generator

## Debugging in the UI

The Results page provides several tools to debug validation failures:
- **Error/Details Column**: Shows the specific error message for tests with `ERROR` status.
- **View Bad Data**: For `FAIL` status, click this button to see the actual rows in BigQuery that failed the validation.
- **Show SQL**: View the exact BigQuery SQL statement executed for each test. This can be copied directly into the BigQuery console for further analysis.
- **Sample Data Grid**: See the specific values that triggered the failure directly in the results table.

## Viewing Cloud Run Logs

For more detailed server-side logs, you can view the Cloud Run logs:

### Option 1: Google Cloud Console (Web UI)

1. Go to [Cloud Run Services](https://console.cloud.google.com/run?project=miruna-sandpit)
2. Click on **test-case-generator**
3. Click the **LOGS** tab
4. You'll see all requests and errors

### Option 2: Command Line

```bash
gcloud run services logs read test-case-generator --region us-central1 --project miruna-sandpit --limit 50
```

To follow logs in real-time:
```bash
gcloud run services logs tail test-case-generator --region us-central1 --project miruna-sandpit
```

### Option 3: Logs Explorer (Advanced)

For advanced filtering:
1. Go to [Logs Explorer](https://console.cloud.google.com/logs/query?project=miruna-sandpit)
2. Use this query:
```
resource.type="cloud_run_revision"
resource.labels.service_name="test-case-generator"
severity>=ERROR
```

## Common Error Types

- **BigQuery Errors**: Permission issues, invalid SQL, table not found
- **Vertex AI Errors**: Model access issues, quota exceeded
- **Authentication Errors**: Invalid OAuth credentials

## Debugging Tips

1. **Check the UI first**: Error messages are now displayed in the results table
2. **Check Cloud Run logs**: For detailed stack traces and server errors
3. **Verify permissions**: Ensure your service account has BigQuery and Vertex AI access
4. **Check quotas**: Vertex AI has rate limits that may cause errors
