"""Main FastAPI application for Combined Data QA Agent backend."""
import logging
import traceback
import json
import uuid
from typing import Union, Dict, Any, List
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
    CustomTestRequest,
    ProjectSettings,
    AddSCDConfigRequest,
    TableMetadataResponse
)
from app.services.test_executor import test_executor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_short_id() -> str:
    """Generate a short 8-character execution ID."""
    return str(uuid.uuid4())[:8]



@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown events."""
    logger.info("Starting Data QA Agent Backend (Combined Version)...")
    yield
    logger.info("Shutting down Data QA Agent Backend...")


# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    description="AI-powered data quality testing for BigQuery and GCS (Combined Features)",
    version="1.1.0",
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
    return HealthResponse(status="healthy", version="1.1.0")


@app.post("/api/generate-tests", response_model=Union[ConfigTableResponse, GenerateTestsResponse, Dict[str, Any]])
async def generate_tests(request: GenerateTestsRequest):
    """
    Generate and execute data quality tests.
    """
    # Normalize project ID (Test1/Test3 Fix)
    if not request.project_id or not request.project_id.strip():
        request.project_id = settings.google_cloud_project

    try:
        logger.info(f"Received request: mode={request.comparison_mode}, project={request.project_id}")
        
        # 1. Config Table Mode (Test1 Logic + Granular Logging)
        if request.comparison_mode == 'gcs-config':
            if not request.config_dataset or not request.config_table:
                raise HTTPException(status_code=400, detail="Missing fields: config_dataset, config_table")
            
            logger.info(f"Processing config table: {request.config_dataset}.{request.config_table}")
            result = await test_executor.process_config_table(
                project_id=request.project_id,
                config_dataset=request.config_dataset,
                config_table=request.config_table,
                filters=request.config_filters
            )
            
            # Log execution (Granular - Test1 Style)
            try:
                from app.services.bigquery_service import bigquery_service
                rows_to_log: List[Dict[str, Any]] = []
                exec_id = request.execution_id or generate_short_id()
                
                for mapping_result in result['results_by_mapping']:
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

                logger.info(f"GCS Config: Prepared {len(rows_to_log)} rows for logging with execution_id={exec_id}")
                await bigquery_service.log_execution(
                    project_id=request.project_id,
                    execution_data=rows_to_log
                )
                logger.info(f"GCS Config: Successfully called log_execution for {len(rows_to_log)} rows")
            except Exception as e:
                logger.error(f"Background logging failed: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                exec_id = request.execution_id or "unknown"

            return ConfigTableResponse(
                execution_id=exec_id,
                summary=ConfigTableSummary(**result['summary']),
                results_by_mapping=result['results_by_mapping']
            )

        # 2. GCS Single File (Test1 Logic + Granular Logging)
        elif request.comparison_mode == 'gcs':
            if not all([request.gcs_bucket, request.gcs_file_path, request.target_dataset, request.target_table]):
                raise HTTPException(status_code=400, detail="Missing required fields for GCS comparison")
            
            mapping = {
                'mapping_id': 'single_file_comparison',
                'source_bucket': request.gcs_bucket,
                'source_file_path': request.gcs_file_path,
                'source_file_format': request.file_format,
                'target_dataset': request.target_dataset,
                'target_table': request.target_table,
                'enabled_test_ids': request.enabled_test_ids,
                'auto_suggest': True
            }
            
            result = await test_executor.process_mapping(request.project_id, mapping)
            
            if result.error:
                raise HTTPException(status_code=400, detail=result.error)

            summary = TestSummary(
                total_tests=len(result.predefined_results),
                passed=len([t for t in result.predefined_results if t.status == 'PASS']),
                failed=len([t for t in result.predefined_results if t.status == 'FAIL']),
                errors=len([t for t in result.predefined_results if t.status == 'ERROR'])
            )
            
            # Log execution (Granular)
            try:
                from app.services.bigquery_service import bigquery_service
                rows_to_log = []
                exec_id = request.execution_id or generate_short_id()
                
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
                exec_id = request.execution_id or "unknown"
            
            return GenerateTestsResponse(
                execution_id=exec_id,
                comparison_mode=request.comparison_mode,
                summary=summary,
                results=result.predefined_results
            )

        # 3. Schema Validation (Test1 Logic + Granular Logging)
        elif request.comparison_mode == 'schema':
            result_data = await test_executor.process_schema_validation(
                project_id=request.project_id,
                datasets=request.datasets or [],
                erd_description=request.erd_description or ""
            )
            
            # Log Schema Validation
            try:
                from app.services.bigquery_service import bigquery_service
                rows_to_log = []
                exec_id = request.execution_id or generate_short_id()

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
            except Exception as e:
                logger.error(f"Background logging failed: {e}")
                exec_id = request.execution_id or "unknown"

            result_data['execution_id'] = exec_id
            result_data['comparison_mode'] = request.comparison_mode
            return result_data

        # 4. SCD Config Mode (Test3 Feature)
        elif request.comparison_mode == 'scd-config':
            if not request.config_dataset or not request.config_table:
                raise HTTPException(status_code=400, detail="Missing required fields for scd-config mode")
            
            # Generate a single execution ID for the entire batch
            exec_id = request.execution_id or generate_short_id()
            logger.info(f"Starting Batch SCD validation from config: {request.config_dataset}.{request.config_table} with Execution ID: {exec_id}")
            
            result = await test_executor.process_scd_config_table(
                project_id=request.project_id,
                config_dataset=request.config_dataset,
                config_table=request.config_table
            )
            
            # Log execution (SCD Table-Level - One row per table)
            try:
                from app.services.bigquery_service import bigquery_service
                
                rows_to_log = []
                all_mappings = result.get('results_by_mapping', [])
                
                # Aggregate at table level - one row per mapping/table
                for mapping_result in all_mappings:
                    t_dataset = "unknown"
                    t_table = "unknown"
                    
                    m_info = mapping_result.mapping_info
                    if m_info and m_info.target:
                        parts = m_info.target.split('.')
                        if len(parts) >= 2:
                             t_table = parts[-1]
                             t_dataset = parts[-2]
                        else:
                            t_table = m_info.target

                    # Calculate aggregated counts for this table
                    total_tests = len(mapping_result.predefined_results)
                    passed_tests = sum(1 for test in mapping_result.predefined_results if test.status == 'PASS')
                    failed_tests = sum(1 for test in mapping_result.predefined_results if test.status == 'FAIL')
                    error_tests = sum(1 for test in mapping_result.predefined_results if test.status == 'ERROR')
                    
                    # Determine overall status for this table
                    if error_tests > 0:
                        overall_status = 'ERROR'
                    elif failed_tests > 0:
                        overall_status = 'FAIL'
                    else:
                        overall_status = 'PASS'
                    
                    # Create one row per table with aggregated data
                    rows_to_log.append({
                        "execution_id": exec_id,
                        "project_id": request.project_id,
                        "comparison_mode": "scd-config",
                        "mapping_id": mapping_result.mapping_id,
                        "target_dataset": t_dataset,
                        "target_table": t_table,
                        "status": overall_status,
                        "total_tests": total_tests,
                        "passed_tests": passed_tests,
                        "failed_tests": failed_tests,
                        "error_message": None,  # Populated if there's a table-level error
                        "test_results": [t.dict() for t in mapping_result.predefined_results],  # Store detailed results
                        "executed_by": "Manual Run",
                        "metadata": {
                            "source": f"Batch SCD: {t_table}",
                            "status": overall_status,
                            "summary": {
                                "total": total_tests,
                                "passed": passed_tests,
                                "failed": failed_tests,
                                "errors": error_tests
                            }
                        }
                    })

                logger.info(f"SCD Config: Prepared {len(rows_to_log)} table-level rows for logging from {len(all_mappings)} mappings")
                
                if rows_to_log:
                    await bigquery_service.log_scd_execution(
                        project_id=request.project_id,
                        execution_data=rows_to_log
                    )

            except Exception as e:
                logger.error(f"SCD Config logging failed: {e}", exc_info=True)
                # Keep the exec_id even if logging fails
            
            # Inject the common execution ID into the response structure
            result['execution_id'] = exec_id

            return ConfigTableResponse(
                execution_id=exec_id,
                comparison_mode=request.comparison_mode,
                summary=ConfigTableSummary(**result['summary']),
                results_by_mapping=result['results_by_mapping']
            )

        # 5. SCD Single Mode (Test3 Feature)
        elif request.comparison_mode == 'scd':
            if not request.target_dataset or not request.target_table:
                raise HTTPException(status_code=400, detail="target_dataset and target_table required for scd mode")
            
            mapping = {
                'target_dataset': request.target_dataset,
                'target_table': request.target_table,
                'scd_type': request.scd_type or 'scd2',
                'primary_keys': request.primary_keys or [],
                'surrogate_key': request.surrogate_key,
                'begin_date_column': request.begin_date_column,
                'end_date_column': request.end_date_column,
                'active_flag_column': request.active_flag_column,
                'enabled_test_ids': request.enabled_test_ids,
                'custom_tests': request.custom_tests
            }
            
            result = await test_executor.process_scd(request.project_id, mapping)
            
            # Log execution (SCD Granular - Test4 Feature)
            try:
                from app.services.bigquery_service import bigquery_service
                exec_id = request.execution_id or generate_short_id()
                
                rows_to_log = []
                for test in result.predefined_results:
                    rows_to_log.append({
                        "execution_id": exec_id,
                        "project_id": request.project_id,
                        "comparison_mode": "scd",
                        "mapping_id": result.mapping_id,  # Important for correlating back later
                        "target_dataset": request.target_dataset,
                        "target_table": request.target_table,
                        "test_id": test.test_id,
                        "test_name": test.test_name,
                        "category": test.category,
                        "status": test.status,
                        "severity": test.severity,
                        "description": test.description,
                        "error_message": test.error_message,
                        "rows_affected": test.rows_affected,
                        "sql_query": test.sql_query,
                        "executed_by": "Manual Run"
                    })

                await bigquery_service.log_scd_execution(
                    project_id=request.project_id,
                    execution_data=rows_to_log
                )
            except Exception as e:
                logger.error(f"SCD logging failed: {e}")
                exec_id = request.execution_id or "unknown"

            response_data = {
                'execution_id': exec_id,
                'comparison_mode': request.comparison_mode,
                'summary': table_summary,
                'results_by_mapping': [result.dict()]
            }
            return response_data
        
        else:
            raise HTTPException(status_code=400, detail=f"Invalid mode: {request.comparison_mode}")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"UNEXPECTED ERROR: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/history")
async def get_test_history(project_id: str = settings.google_cloud_project, limit: int = 50):
    """Get previous test runs from BigQuery."""
    # Handle empty string from frontend
    active_project = project_id if project_id and project_id.strip() else settings.google_cloud_project
    try:
        from app.services.bigquery_service import bigquery_service
        return await bigquery_service.get_execution_history(project_id=active_project, limit=limit)
    except Exception as e:
        logger.error(f"History fetch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/history/{execution_id}")
async def delete_history(
    execution_id: str,
    project_id: str = settings.google_cloud_project
):
    """Delete a specific test execution history."""
    # Handle empty string from frontend
    active_project = project_id if project_id and project_id.strip() else settings.google_cloud_project
    try:
        logger.info(f"Delete request for execution_id: {execution_id}, project_id: {active_project}")
        from app.services.bigquery_service import bigquery_service
        success = await bigquery_service.delete_execution_history(active_project, execution_id)
        logger.info(f"Delete result: {success}")
        if success:
            return {"status": "success"}
        raise HTTPException(status_code=500, detail="Failed to delete history")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete failed with exception: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/history")
async def delete_all_history(
    project_id: str = settings.google_cloud_project
):
    """Delete ALL test execution history."""
    try:
        # Handle empty string from frontend
        active_project = project_id if project_id and project_id.strip() else settings.google_cloud_project
        from app.services.bigquery_service import bigquery_service
        success = await bigquery_service.delete_all_execution_history(active_project)
        if success:
            return {"status": "success"}
        raise HTTPException(status_code=500, detail="Failed to clear history")
    except Exception as e:
        logger.error(f"Clear all failed: {e}")
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

# --- Test1 Features: Settings & Notification ---

@app.get("/api/settings")
async def get_settings(project_id: str):
    """Get project alert settings."""
    try:
        from app.services.bigquery_service import bigquery_service
        settings = await bigquery_service.get_project_settings(project_id)
        if not settings:
             return {
                 "project_id": project_id,
                 "alert_emails": [],
                 "teams_webhook_url": "",
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
    """Trigger alerts for an execution (Test1 Feature)."""
    try:
        execution_id = payload.get("execution_id")
        project_id = payload.get("project_id")
        
        if not execution_id or not project_id:
             raise HTTPException(status_code=400, detail="Missing execution_id or project_id")

        from app.services.bigquery_service import bigquery_service
        
        # 1. Get Settings
        settings = await bigquery_service.get_project_settings(project_id)
        if not settings or not settings.get('alert_on_failure', True):
            return {"status": "skipped", "reason": "Alerts disabled"}

        # 2. Get Execution Stats
        summary = payload.get("summary")
        if summary:
            # Trusted payload
            failed_count = summary.get('failed', 0)
            error_count = summary.get('errors', 0)
            stats = summary
        else:
             # Fallback: Check Granular Table
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

        # 4. Send Teams Alert
        teams_url = settings.get('teams_webhook_url')
        if teams_url:
            try:
                import urllib.request
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
                            {"name": "Total", "value": str(stats.get('total', 0) or stats.get('total_tests', 0))},
                            {"name": "Failed", "value": str(failed_count)}
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

        return {"status": "sent", "recipient_count": 1 if teams_url else 0}

    except Exception as e:
        logger.error(f"Notification failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- Test3 Features: SCD Config & Metadata ---

@app.get("/api/table-metadata", response_model=TableMetadataResponse)
async def get_table_metadata(
    project_id: str = settings.google_cloud_project,
    dataset_id: str = ...,
    table_id: str = ...
):
    """Get metadata for a specific BigQuery table."""
    # Handle empty string from frontend
    active_project = project_id if project_id and project_id.strip() else settings.google_cloud_project
    try:
        from app.services.bigquery_service import bigquery_service
        metadata = await bigquery_service.get_table_metadata(active_project, dataset_id, table_id)
        columns = [field['name'] for field in metadata.get('schema', {}).get('fields', [])]
        return TableMetadataResponse(
            full_table_name=metadata['full_table_name'],
            columns=columns,
            schema_info=metadata
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/scd-config/{project_id}/{config_dataset}/{config_table}/{target_dataset}/{target_table}")
async def get_scd_config_by_table(
    project_id: str,
    config_dataset: str,
    config_table: str,
    target_dataset: str,
    target_table: str
):
    """Fetch an existing SCD config by target dataset and table."""
    try:
        from app.services.bigquery_service import bigquery_service
        config = await bigquery_service.get_scd_config_by_table(
            project_id=project_id,
            config_dataset=config_dataset,
            config_table=config_table,
            target_dataset=target_dataset,
            target_table=target_table
        )
        if not config:
            raise HTTPException(status_code=404, detail="Configuration not found")
        return config
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/scd-config")
async def add_scd_config(request: AddSCDConfigRequest):
    """Add a new SCD validation configuration to the config table."""
    try:
        config_data = {
            "config_id": request.config_id,
            "target_dataset": request.target_dataset,
            "target_table": request.target_table,
            "scd_type": request.scd_type,
            "primary_keys": request.primary_keys,
            "surrogate_key": request.surrogate_key,
            "begin_date_column": request.begin_date_column,
            "end_date_column": request.end_date_column,
            "active_flag_column": request.active_flag_column,
            "description": request.description,
            "custom_tests": request.custom_tests
        }
        
        from app.services.bigquery_service import bigquery_service
        success = await bigquery_service.insert_scd_config(
            project_id=request.project_id,
            config_dataset=request.config_dataset,
            config_table=request.config_table,
            config_data=config_data
        )
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to insert SCD configuration")
        
        return {
            "success": True,
            "message": "SCD configuration added successfully",
            "config_id": request.config_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
