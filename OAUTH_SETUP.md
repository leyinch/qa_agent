# OAuth Setup Instructions

Your Test Case Generator app has been successfully deployed to:
**https://test-case-generator-750147355601.us-central1.run.app**

## Next Steps: Configure Google OAuth

### 1. Create OAuth Credentials

1. Go to [Google Cloud Console - APIs & Services - Credentials](https://console.cloud.google.com/apis/credentials?project=miruna-sandpit)
2. Click **"Create Credentials"** → **"OAuth client ID"**
3. If prompted, configure the OAuth consent screen:
   - User Type: **External**
   - App name: **Test Case Generator**
   - User support email: Your email
   - Developer contact: Your email
   - Click **Save and Continue** through the remaining steps

### 2. Configure OAuth Client

1. Application type: **Web application**
2. Name: **Test Case Generator**
3. Authorized redirect URIs - Add:
   ```
   https://test-case-generator-750147355601.us-central1.run.app/api/auth/callback/google
   ```
4. Click **Create**
5. **Copy the Client ID and Client Secret** (you'll need these next)

### 3. Update Cloud Run Environment Variables

Run this command, replacing `YOUR_CLIENT_ID` and `YOUR_CLIENT_SECRET` with the values from step 2:

```powershell
gcloud run services update test-case-generator `
  --region us-central1 `
  --project miruna-sandpit `
  --set-env-vars "GOOGLE_CLIENT_ID=YOUR_CLIENT_ID,GOOGLE_CLIENT_SECRET=YOUR_CLIENT_SECRET,NEXTAUTH_SECRET=your-random-secret-key-here,NEXTAUTH_URL=https://test-case-generator-750147355601.us-central1.run.app"
```

For `NEXTAUTH_SECRET`, you can generate a random string or use any secure random value.

### 4. Test the Application

Once the environment variables are updated, visit:
**https://test-case-generator-750147355601.us-central1.run.app**

You should be able to:
1. Sign in with Google
2. Enter your Project ID and Dataset
3. Provide an ERD description
4. Generate and run data quality tests
5. View results with charts and diagrams
