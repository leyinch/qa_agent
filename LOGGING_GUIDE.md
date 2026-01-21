# Viewing Logs for Test Case Generator

## Error Messages in the UI

The updated Results page now shows error messages directly in the table under the "Error/Details" column. For tests with ERROR status, you'll see the specific error message that caused the test to fail.

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
