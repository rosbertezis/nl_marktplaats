# Marktplaats XML Generator - Deployment Guide

## Render Deployment Setup

### 1. Prerequisites
- GitHub repository: `rosbertezis/nl_marktplaats`
- Google Service Account credentials (`credentials.json`)
- Cloudinary account (optional)

### 2. Render Setup Steps

1. **Go to Render Dashboard**
   - Visit [https://render.com](https://render.com)
   - Sign up/Login with GitHub

2. **Create Web Service**
   - Click "New +" → "Web Service"
   - Connect GitHub repository: `rosbertezis/nl_marktplaats`
   - Render will auto-detect configuration from `render.yaml`

3. **Environment Variables**
   Set these in Render dashboard:
   ```
   SPREADSHEET_NAME=your_google_sheets_name
   WORKSHEET_NAME=your_worksheet_name
   CLOUDINARY_CLOUD_NAME=your_cloudinary_cloud_name
   CLOUDINARY_API_KEY=your_cloudinary_api_key
   CLOUDINARY_API_SECRET=your_cloudinary_api_secret
   ```

4. **Google Credentials**
   - Upload `credentials.json` file in Render environment
   - Or add as environment variable `GOOGLE_CREDENTIALS_JSON` with file content

### 3. Application Endpoints

Once deployed, your app will have these endpoints:
- `/` - Main page with service status
- `/generate-feed` - Generate and upload XML feed
- `/xml` - Download current XML feed

### 4. Configuration Files

- `APP.py` - Main application (updated version)
- `render.yaml` - Render deployment configuration
- `requirements.txt` - Python dependencies

### 5. Features

- ✅ Google Sheets integration
- ✅ XML feed generation
- ✅ Cloudinary upload (optional)
- ✅ Local XML file storage
- ✅ Error handling and logging
- ✅ XSD schema validation

### 6. Troubleshooting Deployment Issues

#### Common Issue: "ModuleNotFoundError: No module named 'app'"

**Solution 1: Use Procfile (Recommended)**
- The repository includes a `Procfile` with `web: gunicorn app:app`
- Render will automatically detect and use the Procfile

**Solution 2: Manual Configuration in Render Dashboard**
If `render.yaml` is not being used:
1. Go to your service settings in Render
2. Navigate to "Settings" tab
3. Set "Start Command" to: `gunicorn app:app`
4. Set "Build Command" to: `pip install -r requirements.txt`

**Solution 3: Verify File Structure**
Ensure your repository has:
- `app.py` (main application file)
- `requirements.txt` (dependencies)
- `Procfile` (deployment command)

### 7. Environment Variables Reference

| Variable | Required | Description |
|----------|----------|-------------|
| `SPREADSHEET_NAME` | Yes | Google Sheets spreadsheet name |
| `WORKSHEET_NAME` | Yes | Google Sheets worksheet name |
| `GOOGLE_CREDENTIALS_PATH` | Yes | Path to credentials.json |
| `CLOUDINARY_CLOUD_NAME` | No | Cloudinary cloud name |
| `CLOUDINARY_API_KEY` | No | Cloudinary API key |
| `CLOUDINARY_API_SECRET` | No | Cloudinary API secret |

### 8. Deployment Files

- `app.py` - Main application (Flask app with `app` variable)
- `APP.py` - Same as app.py (backup)
- `Procfile` - Render deployment command
- `render.yaml` - Render service configuration
- `requirements.txt` - Python dependencies
