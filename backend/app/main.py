"""Main FastAPI application for Data QA Agent backend."""
import logging
from typing import Union, Dict, Any
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.config import settings
from app.models import (
    GenerateTestsRequest,
    GenerateTestsResponse,
    ConfigTableResponse,
    HealthResponse,
    TestSummary,
    ConfigTableSummary,
    ConfigTableSummary,
    CustomTestRequest,
    ProjectSettings
)
from app.services.test_executor import test_executor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown events."""
    logger.info("Starting Data QA Agent Backend...")
    yield
    logger.info("Shutting down Data QA Agent Backend...")


# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    description="AI-powered data quality testing for BigQuery and GCS",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    return HealthResponse(status="healthy", version="1.0.0")


@app.post("/api/generate-tests", response_model=Union[ConfigTableResponse, GenerateTestsResponse, Dict[str, Any]])
async def generate_tests(request: GenerateTestsRequest):
    """
    Generate and execute data quality tests.
    
    Supports three modes:
    - schema: Validate BigQuery schema against ERD
    - gcs: Compare single GCS file to BigQuery table
    - gcs-config: Process multiple mappings from config table
    """
    try:
        logger.info(f"Received request: mode={request.comparison_mode}, project={request.project_id}")
        
        # Config table mode
        if request.comparison_mode == 'gcs-config':
            if not request.config_dataset or not request.config_table:
                raise HTTPException(
                    status_code=400,
                    detail="Missing required fields: config_dataset, config_table"
                )
            
            logger.info(f"Processing config table: {request.config_dataset}.{request.config_table}")
            result = await test_executor.process_config_table(
                project_id=request.project_id,
                config_dataset=request.config_dataset,
                config_table=request.config_table,
                filters=request.config_filters
            )
            
            # Log execution in background
            try:
                from app.services.bigquery_service import bigquery_service
                
                # Flatten results for granular logging
                rows_to_log = []
                import uuid
                exec_id = request.execution_id or str(uuid.uuid4())
                
                for mapping_result in result['results_by_mapping']:
                    # Handle errors on mapping level
                    if mapping_result.error:
                         rows_to_log.append({
                            "execution_id": exec_id,
                            "project_id": request.project_id,
                            "comparison_mode": "gcs-config",
                            "status": "ERROR",
                            "error_message": mapping_result.error,
                            "source": f"{request.config_dataset}.{request.config_table}",
                            "target": "Unknown",
                            "test_name": "Mapping Processing",
                            "category": "system",
                            "severity": "HIGH"
                        })
                         continue

                    # Log each test result
                    for test in mapping_result.predefined_results:
                        rows_to_log.append({
                            "execution_id": exec_id,
                            "project_id": request.project_id,
                            "comparison_mode": "gcs-config",
                            "mapping_id": mapping_result.mapping_id,
                            "test_id": test.test_id,
                            "test_name": test.test_name,
                            "category": test.category,
                            "status": test.status,
                            "severity": test.severity,
                            "description": test.description,
                            "error_message": test.error_message,
                            "source": mapping_result.mapping_info.source if mapping_result.mapping_info else None,
                            "target": mapping_result.mapping_info.target if mapping_result.mapping_info else None,
                            "rows_affected": test.rows_affected,
                            "sql_query": test.sql_query
                        })

                await bigquery_service.log_execution(
                    project_id=request.project_id,
                    execution_data=rows_to_log
                )
            except Exception as e:
                logger.error(f"Background logging failed: {e}")

            return ConfigTableResponse(
                execution_id=exec_id,
                summary=ConfigTableSummary(**result['summary']),
                results_by_mapping=result['results_by_mapping']
            )
        

        # GCS Single File
        elif request.comparison_mode == 'gcs':
            if not all([request.gcs_bucket, request.gcs_file_path, 
                       request.target_dataset, request.target_table]):
                raise HTTPException(
                    status_code=400,
                    detail="Missing required fields for GCS comparison"
                )
            
            logger.info(f"Processing single file: gs://{request.gcs_bucket}/{request.gcs_file_path}")
            mapping = {
                'mapping_id': 'single_file_comparison',
                'source_bucket': request.gcs_bucket,
                'source_file_path': request.gcs_file_path,
                'source_file_format': request.file_format,
                'target_dataset': request.target_dataset,
                'target_table': request.target_table,
                'enabled_test_ids': request.enabled_test_ids or ['row_count_match', 'no_nulls_required', 'no_duplicates_pk'],
                'auto_suggest': True
            }
            
            result = await test_executor.process_mapping(request.project_id, mapping)
            
            if result.error:
                logger.error(f"Mapping processing failed: {result.error}")
                raise HTTPException(status_code=400, detail=result.error)

            summary = TestSummary(
                total_tests=len(result.predefined_results),
                passed=len([t for t in result.predefined_results if t.status == 'PASS']),
                failed=len([t for t in result.predefined_results if t.status == 'FAIL']),
                errors=len([t for t in result.predefined_results if t.status == 'ERROR'])
            )
            
            # Log execution
            try:
                from app.services.bigquery_service import bigquery_service
                
                rows_to_log = []
                import uuid
                exec_id = request.execution_id or str(uuid.uuid4())
                
                for test in result.predefined_results:
                    rows_to_log.append({
                        "execution_id": exec_id,
                        "project_id": request.project_id,
                        "comparison_mode": "gcs",
                        "test_id": test.test_id,
                        "test_name": test.test_name,
                        "category": test.category,
                        "status": test.status,
                        "severity": test.severity,
                        "description": test.description,
                        "error_message": test.error_message,
                        "source": f"gs://{request.gcs_bucket}/{request.gcs_file_path}",
                        "target": f"{request.target_dataset}.{request.target_table}",
                        "rows_affected": test.rows_affected,
                        "sql_query": test.sql_query
                    })

                await bigquery_service.log_execution(
                    project_id=request.project_id,
                    execution_data=rows_to_log
                )
            except Exception as e:
                logger.error(f"Background logging failed: {e}")
                # Ensure exec_id is available even if logging fails (if it was generated)
                if 'exec_id' not in locals():
                     exec_id = request.execution_id or "unknown"
            
            return GenerateTestsResponse(
                execution_id=exec_id,
                summary=summary,
                results=result.predefined_results
            )
        
        # Schema validation mode
        elif request.comparison_mode == 'schema':
            try:
                logger.info(f"Schema validation for: {request.datasets}")
                result_data = await test_executor.process_schema_validation(
                    project_id=request.project_id,
                    datasets=request.datasets or [],
                    erd_description=request.erd_description or ""
                )
                
                # Log Schema Validation
                try:
                    from app.services.bigquery_service import bigquery_service
                    
                    rows_to_log = []
                    import uuid
                    exec_id = request.execution_id or str(uuid.uuid4())

                    for test in result_data.get('predefined_results', []):
                         rows_to_log.append({
                            "execution_id": exec_id,
                            "project_id": request.project_id,
                            "comparison_mode": "schema",
                            "test_name": test.test_name,
                            "category": test.category,
                            "status": test.status,
                            "severity": test.severity,
                            "description": test.description,
                            "source": "ERD",
                            "target": ",".join(request.datasets or []),
                            "sql_query": test.sql_query
                        })
                    
                    await bigquery_service.log_execution(
                        project_id=request.project_id,
                        execution_data=rows_to_log
                    )
                except Exception as log_err:
                    logger.error(f"Background logging failed: {log_err}")
                    if 'exec_id' not in locals():
                        exec_id = request.execution_id or "unknown"

                # Patch execution_id into the raw dict response for schema mode
                result_data['execution_id'] = exec_id
                return result_data
            except Exception as e:
                logger.error(f"Schema validation crash: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=str(e))
        
        else:
            raise HTTPException(status_code=400, detail=f"Invalid mode: {request.comparison_mode}")

    except HTTPException:
        raise
    except ValueError as ve:
        logger.error(f"VALUATION ERROR: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"UNEXPECTED ERROR: {e}", exc_info=True)
        # Return exception type and message for easier client-side debugging
        detail = f"{type(e).__name__}: {str(e)}"
        raise HTTPException(status_code=500, detail=detail)


@app.get("/api/history")
async def get_test_history(project_id: str = settings.google_cloud_project, limit: int = 50):
    """Get previous test runs from BigQuery."""
    try:
        from app.services.bigquery_service import bigquery_service
        return await bigquery_service.get_execution_history(project_id=project_id, limit=limit)
    except Exception as e:
        logger.error(f"History fetch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/predefined-tests")
async def list_predefined_tests():
    """List all available predefined tests."""
    from app.tests.predefined_tests import PREDEFINED_TESTS
    
    return {
        'tests': [
            {
                'id': test.id,
                'name': test.name,
                'category': test.category,
                'severity': test.severity,
                'description': test.description,
                'is_global': test.is_global
            }
            for test in PREDEFINED_TESTS.values()
        ]
    }


@app.post("/api/custom-tests")
async def save_custom_test(request: CustomTestRequest):
    """Save a custom test case."""
    try:
        from app.services.bigquery_service import bigquery_service
        success = await bigquery_service.save_custom_test(request.dict())
        if not success:
            raise HTTPException(status_code=500, detail="Persistence failure")
        return {"status": "success", "message": "Custom test saved"}
    except Exception as e:
        logger.error(f"Custom test save failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/api/settings")
async def get_settings(project_id: str):
    """Get project alert settings."""
    try:
        from app.services.bigquery_service import bigquery_service
        settings = await bigquery_service.get_project_settings(project_id)
        if not settings:
             # Return default structure
             return {
                 "project_id": project_id,
                 "alert_emails": [],
                 "slack_webhook_url": "",
                 "alert_on_failure": True
             }
        return settings
    except Exception as e:
        logger.error(f"Failed to get settings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/settings")
async def save_settings(settings: ProjectSettings):
    """Save project alert settings."""
    try:
        from app.services.bigquery_service import bigquery_service
        success = await bigquery_service.save_project_settings(settings.dict())
        if not success:
            raise HTTPException(status_code=500, detail="Persistence failure")
        return {"status": "success", "message": "Settings saved"}
    except Exception as e:
        logger.error(f"Failed to save settings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/notify")
async def notify_execution(payload: Dict[str, Any]):
    """
    Trigger alerts for an execution.
    Payload: { "execution_id": "...", "project_id": "..." }
    """
    try:
        execution_id = payload.get("execution_id")
        project_id = payload.get("project_id")
        
        if not execution_id or not project_id:
             raise HTTPException(status_code=400, detail="Missing execution_id or project_id")

        from app.services.bigquery_service import bigquery_service
        
        # 1. Get Settings
        settings = await bigquery_service.get_project_settings(project_id)
        if not settings or not settings.get('alert_on_failure', True):
            logger.info("Alerts disabled or no settings found.")
            return {"status": "skipped", "reason": "Alerts disabled"}

        # 2. Get Execution Stats
        summary = payload.get("summary")
        if summary:
            # Trusted payload from workflow
            total = summary.get('total_tests', 0)
            passed = summary.get('passed', 0)
            failed_count = summary.get('failed', 0)
            error_count = summary.get('errors', 0)
            stats = { "total": total, "passed": passed, "failed": failed_count, "errors": error_count }
        else:
             # Fallback to BigQuery (race condition prone)
            query = f"""
                SELECT 
                    COUNT(*) as total,
                    COUNTIF(status = 'PASS') as passed,
                    COUNTIF(status = 'FAIL') as failed,
                    COUNTIF(status = 'ERROR') as errors
                FROM `{project_id}.config.test_execution_history`
                WHERE execution_id = '{execution_id}'
            """
            stats_rows = await bigquery_service.execute_query(query)
            if not stats_rows:
                return {"status": "skipped", "reason": "No execution data found"}
                
            stats = stats_rows[0]
            failed_count = stats.get('failed', 0)
            error_count = stats.get('errors', 0)
        
        # 3. Check Condition
        if failed_count == 0 and error_count == 0:
             return {"status": "skipped", "reason": "No failures"}

        # 4. Construct Message
        message = (
            f"🚨 *Data Quality Alert* 🚨\n"
            f"Project: `{project_id}`\n"
            f"Execution ID: `{execution_id}`\n"
            f"Status: *FAILED*\n"
            f"Stats: {stats.get('total', 0)} Total | {stats.get('passed', 0)} Passed | {failed_count} Failed | {error_count} Errors\n"
        )
        
        # 5. Send Teams Alert
        teams_url = settings.get('teams_webhook_url')
        if teams_url:
            try:
                import json
                import urllib.request
                
                # Simple O365 Connector Card format
                teams_payload = {
                    "@type": "MessageCard",
                    "@context": "http://schema.org/extensions",
                    "themeColor": "d70000",
                    "summary": f"Data Quality Alert for {project_id}",
                    "sections": [{
                        "activityTitle": "🚨 Data Quality Alert",
                        "activitySubtitle": f"Project: {project_id}",
                        "facts": [
                            {"name": "Execution ID", "value": execution_id},
                            {"name": "Status", "value": "FAILED"},
                            {"name": "Total Tests", "value": str(stats.get('total', 0))},
                            {"name": "Passed", "value": str(stats.get('passed', 0))},
                            {"name": "Failed", "value": str(failed_count)},
                            {"name": "Errors", "value": str(error_count)}
                        ],
                        "markdown": True
                    }]
                }

                req = urllib.request.Request(
                    teams_url, 
                    data=json.dumps(teams_payload).encode('utf-8'),
                    headers={'Content-Type': 'application/json'}
                )
                urllib.request.urlopen(req)
                logger.info(f"Sent Teams alert to {teams_url}")
            except Exception as teams_err:
                logger.error(f"Failed to send Teams alert: {teams_err}")

        # 6. Log Email Alert (Simulation)
        emails = settings.get('alert_emails', [])
        if emails:
             logger.info(f"📧 [SIMULATION] Sending email to {emails}: \n{message}")

        return {"status": "sent", "recipient_count": 1 if teams_url else 0}

    except Exception as e:
        logger.error(f"Notification failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
