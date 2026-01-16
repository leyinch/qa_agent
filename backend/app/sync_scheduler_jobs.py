"""
Utility script to synchronize BigQuery configurations with Cloud Scheduler.

Purpose:
- This script bridges the gap between BigQuery configuration tables and Cloud Scheduler jobs.
- While the UI automatically creates scheduler jobs when you add a config via the Dashboard, 
  manual SQL INSERTs into BigQuery do not automatically trigger the backend to create a schedule.
- Run this script to "catch up" and create/update Cloud Scheduler jobs for any configurations added manually via SQL.

When to run:
1. After manually inserting rows into `config.scd_validation_config` via BigQuery Console.
2. During initial deployment if Cloud Scheduler jobs were not created.
3. To repair or resync jobs if they were accidentally deleted from Cloud Scheduler.

Usage:
    python scripts/src/sync_scheduler_jobs.py
"""
import os
import sys
import asyncio
import json
from typing import List, Dict, Any

# Add backend to path so we can import services
sys.path.append(os.path.join(os.getcwd(), 'backend'))

try:
    from app.services.bigquery_service import bigquery_service
    from app.services.scheduler_service import scheduler_service
    from app.config import settings
except ImportError as e:
    print(f"Error importing backend services: {e}")
    print("Make sure you are running this script from the project root.")
    sys.exit(1)

def load_deploy_config():
    """Load config from deploy.config file."""
    config = {}
    try:
        with open('deploy.config', 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip()
    except Exception as e:
        print(f"Warning: Could not read deploy.config: {e}")
    return config

async def sync_all():
    deploy_config = load_deploy_config()
    project_id = deploy_config.get('PROJECT_ID', settings.google_cloud_project)
    
    print(f"Project: {project_id}")
    
    # Try to find Backend URL if not in settings
    if not settings.cloud_run_url:
        print("\nCLOUD_RUN_URL not set in environment.")
        print(f"Please enter the URL for your backend service:")
        print("Example: https://data-qa-agent-backend2-xxxxxxx-uc.a.run.app")
        url = input("> ").strip()
        if not url:
            print("Error: Backend URL is required to create scheduler jobs.")
            return
        settings.cloud_run_url = url
    else:
        print(f"Using CLOUD_RUN_URL: {settings.cloud_run_url}")

    print("\n--- Starting Full Scheduler Synchronization ---")
    try:
        summary = await scheduler_service.sync_all_from_config()
        
        print(f"\nSync Results:")
        print(f"  Total Configs Found: {summary.get('total', 0)}")
        print(f"  Synced/Updated Jobs: {summary.get('synced', 0)}")
        print(f"  Deleted Obsolete Jobs: {summary.get('deleted', 0)}")
        print(f"  Failed Ops: {summary.get('failed', 0)}")
        print(f"  Skipped (no cron): {summary.get('skipped', 0)}")
        
        if summary.get('details'):
            print("\nDetails:")
            for detail in summary['details']:
                print(f"  - {detail}")
                
    except Exception as e:
        print(f"Error during synchronization: {e}")

    print("\nSync complete!")

if __name__ == "__main__":
    asyncio.run(sync_all())
