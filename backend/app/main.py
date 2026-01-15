"""Main FastAPI application for Data QA Agent backend."""
import logging
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
    AddSCDConfigRequest,
    TableMetadataResponse,
    SaveHistoryRequest,
    ScheduledTestRunRequest
)
# Services will be imported lazily within endpoints to improve startup time


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
    # Scheduler sync removed from startup to prevent blocking
    # Use POST /api/sync-scheduler to manually sync after deployment
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
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    logger.info("Health check probe received")
    return HealthResponse(status="healthy", version="1.0.0")


@app.post("/api/sync-scheduler")
async def sync_scheduler():
    """Trigger a full synchronization of Cloud Scheduler jobs with BigQuery config."""
    try:
        from app.services.scheduler_service import scheduler_service
        if not settings.cloud_run_url:
            raise HTTPException(status_code=400, detail="CLOUD_RUN_URL environment variable is not set")
            
        summary = await scheduler_service.sync_all_from_config()
        return {
            "status": "success",
            "message": "Cloud Scheduler synchronization completed",
            "summary": summary
        }
    except Exception as e:
        logger.error(f"Error syncing scheduler: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/generate-tests")
async def generate_tests(request: GenerateTestsRequest):
    """
    Generate and execute data quality tests.
    
    Supports three modes:
    - schema: Validate BigQuery schema against ERD
    - gcs: Compare single GCS file to BigQuery table
    - gcs-config: Process multiple mappings from config table
    - scd: Validate Slowly Changing Dimension (Type 1 or Type 2)
    """
    from app.services.test_executor import test_executor
    from app.services.history_service import TestHistoryService
    history_service = TestHistoryService()

    try:
        logger.info(f"Received test generation request: mode={request.comparison_mode}")
        
        # Config table mode
        if request.comparison_mode == 'gcs-config':
            if not request.config_dataset or not request.config_table:
                raise HTTPException(
                    status_code=400,
                    detail="Missing required fields: config_dataset, config_table"
                )
            
            result = await test_executor.process_config_table(
                project_id=request.project_id,
                config_dataset=request.config_dataset,
                config_table=request.config_table
            )
            
            try:
                summary_data = result['summary']
                # Convert results objects to dicts for JSON serialization
                results_by_mapping_dicts = [r.dict() for r in result['results_by_mapping']]

                history_service.save_test_results(
                    project_id=request.project_id,
                    comparison_mode="gcs_config_table",
                    test_results=results_by_mapping_dicts,
                    target_dataset=request.config_dataset,
                    target_table=request.config_table,
                    metadata={
                        "summary": summary_data,
                        "source": f"{request.config_dataset}.{request.config_table}",
                        "status": "AT_RISK" if summary_data['failed'] > 0 else "PASS"
                    }
                )
            except Exception as e:
                logger.error(f"Failed to log config execution: {e}")

            return ConfigTableResponse(
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
            
            # Create mapping configuration
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
            
            # Calculate summary
            summary = TestSummary(
                total_tests=len(result.predefined_results),
                passed=len([t for t in result.predefined_results if t.status == 'PASS']),
                failed=len([t for t in result.predefined_results if t.status == 'FAIL']),
                errors=len([t for t in result.predefined_results if t.status == 'ERROR'])
            )
            
            # Prepare response data
            response_data = {
                'summary': summary,
                'mapping_info': result.mapping_info,
                'predefined_results': result.predefined_results,
                'ai_suggestions': result.ai_suggestions
            }

            # Log execution
            try:
                # Note: error_message in history is for system/engine errors, 
                # while data validation failures are tracked in summary stats.
                history_service.save_test_results(
                    project_id=request.project_id,
                    comparison_mode="gcs_single_file",
                    test_results=[r.dict() for r in result.predefined_results],
                    target_dataset=request.target_dataset,
                    target_table=request.target_table,
                    executed_by="Manual Run",
                    metadata={
                        "summary": summary.dict(),
                        "mapping_info": result.mapping_info.dict() if result.mapping_info else None,
                        "ai_suggestions": [s.dict() for s in result.ai_suggestions],
                        "source": f"gs://{request.gcs_bucket}/{request.gcs_file_path}",
                        "status": "FAIL" if summary.failed > 0 or summary.errors > 0 else "PASS"
                    }
                )
            except Exception as e:
                logger.error(f"Failed to log execution: {e}")

            
            return response_data
        
        # Schema validation mode
        elif request.comparison_mode == 'schema':
            try:
                result_data = await test_executor.process_schema_validation(
                    project_id=request.project_id,
                    datasets=request.datasets or [],
                    erd_description=request.erd_description or ""
                )
                
                # Log Schema Validation
                try:
                    summary = result_data.get('summary', {})
                    issues = result_data.get('summary', {}).get('total_issues', 0)
                    
                    history_service.save_test_results(
                        project_id=request.project_id,
                        comparison_mode="schema_validation",
                        test_results=result_data, # Schema validation returns a dict
                        target_dataset=",".join(request.datasets or []),
                        executed_by="Manual Run",
                        metadata={
                            "summary": summary,
                            "source": "ERD Description",
                            "status": "AT_RISK" if issues > 0 else "PASS"
                        }
                    )
                except Exception as log_err:
                    logger.error(f"Failed to log schema execution: {log_err}")

                return result_data
            except Exception as e:
                logger.error(f"Error in schema validation: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))
        
        # SCD Config Table mode
        elif request.comparison_mode == 'scd-config':
            if not request.config_dataset or not request.config_table:
                raise HTTPException(
                    status_code=400,
                    detail="Missing required fields: config_dataset, config_table for scd-config mode"
                )
            
            result = await test_executor.process_scd_config_table(
                project_id=request.project_id,
                config_dataset=request.config_dataset,
                config_table=request.config_table
            )
            
            try:
                summary_data = result['summary']
                # Convert results objects to dicts for JSON serialization
                results_by_mapping_dicts = [r.dict() for r in result['results_by_mapping']]

                history_service.save_test_results(
                    project_id=request.project_id,
                    comparison_mode="scd_config_table",
                    test_results=results_by_mapping_dicts,
                    target_dataset=request.config_dataset,
                    target_table=request.config_table,
                    metadata={
                        "summary": summary_data,
                        "source": f"{request.config_dataset}.{request.config_table}",
                        "status": "AT_RISK" if summary_data['failed'] > 0 else "PASS"
                    }
                )
            except Exception as e:
                logger.error(f"Failed to log scd config execution: {e}")

            return ConfigTableResponse(
                summary=ConfigTableSummary(**result['summary']),
                results_by_mapping=result['results_by_mapping']
            )
        
        elif request.comparison_mode == 'scd':
            if not request.target_dataset or not request.target_table:
                raise HTTPException(status_code=400, detail="target_dataset and target_table are required for scd mode")
            
            mapping = {
                'target_dataset': request.target_dataset,
                'target_table': request.target_table,
                'scd_type': request.scd_type or 'scd2',
                'primary_keys': request.primary_keys or [],
                'surrogate_key': request.surrogate_key,
                'begin_date_column': request.begin_date_column,
                'end_date_column': request.end_date_column,
                'active_flag_column': request.active_flag_column,
                'enabled_test_ids': request.enabled_test_ids
            }
            
            try:
                result = await test_executor.process_scd(request.project_id, mapping)
                
                # Log execution
                try:
                    history_service.save_test_results(
                        project_id=request.project_id,
                        comparison_mode="scd",
                        test_results=[r.dict() for r in result.predefined_results],
                        target_dataset=request.target_dataset,
                        target_table=request.target_table,
                        executed_by="Manual Run",
                        metadata={
                            "summary": {
                                "total_tests": len(result.predefined_results),
                                "passed": len([t for t in result.predefined_results if t.status == 'PASS']),
                                "failed": len([t for t in result.predefined_results if t.status == 'FAIL']),
                                "errors": len([t for t in result.predefined_results if t.status == 'ERROR'])
                            },
                            "source": f"SCD: {request.target_table}",
                            "status": "FAIL" if any(r.status in ['FAIL', 'ERROR'] for r in result.predefined_results) else "PASS"
                        }
                    )
                except Exception as log_err:
                    logger.error(f"Failed to log scd execution: {log_err}")
                
                return {
                    'summary': {
                        'total_tests': len(result.predefined_results),
                        'passed': len([t for t in result.predefined_results if t.status == 'PASS']),
                        'failed': len([t for t in result.predefined_results if t.status == 'FAIL']),
                        'errors': len([t for t in result.predefined_results if t.status == 'ERROR'])
                    },
                    'results_by_mapping': [result.dict()],
                    'cron_schedule': result.cron_schedule
                }
            except Exception as e:
                logger.error(f"Error in scd validation: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid comparison_mode: {request.comparison_mode}"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating tests: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/api/save-test-history")
async def save_test_history(request: SaveHistoryRequest):
    """Save test execution results to BigQuery history."""
    from app.services.history_service import TestHistoryService
    history_service = TestHistoryService()

    try:
        execution_id = history_service.save_test_results(
            project_id=request.project_id,
            comparison_mode=request.comparison_mode,
            test_results=request.test_results,
            target_dataset=request.target_dataset,
            target_table=request.target_table,
            mapping_id=request.mapping_id,
            metadata=request.metadata,
            cron_schedule=request.cron_schedule
        )

        return {"status": "success", "execution_id": execution_id}
    except Exception as e:
        logger.error(f"Error saving test history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/history")
async def get_test_history(project_id: str = settings.google_cloud_project, limit: int = 50):
    """Get previous test runs (summary level) from BigQuery."""
    try:
        from app.services.history_service import TestHistoryService
        history_service = TestHistoryService()
        return history_service.get_test_history(project_id=project_id, limit=limit)

    except Exception as e:
        logger.error(f"Error fetching execution history: {e}")
        return []


@app.delete("/api/history")
async def clear_test_history(project_id: str):
    """Clear all test execution history for a project."""
    try:
        from app.services.history_service import TestHistoryService
        history_service = TestHistoryService()
        history_service.clear_history(project_id)
        return {"status": "success", "message": f"History cleared for {project_id}"}
    except Exception as e:
        logger.error(f"Error clearing history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/history-details")
async def get_history_details(
    execution_id: str,
    project_id: str = settings.google_cloud_project
):
    """Get detailed test results for a specific execution."""
    try:
        from app.services.history_service import TestHistoryService
        history_service = TestHistoryService()
        
        # Query detailed results from the new history service
        results = history_service.get_test_history(
            project_id=project_id,
            execution_id=execution_id
        )

        return results
    except Exception as e:
        logger.error(f"Error fetching detailed history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/table-history")
async def get_table_history(
    target_table: str,
    project_id: str = settings.google_cloud_project,
    limit: int = 20
):
    """Get history for a specific table across all executions."""
    try:
        from app.services.history_service import TestHistoryService
        history_service = TestHistoryService()

        return history_service.get_test_history(
            project_id=project_id,
            target_table=target_table,
            limit=limit
        )

    except Exception as e:
        logger.error(f"Error fetching table history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/scd-config")
async def add_scd_config(request: AddSCDConfigRequest):
    """Add a new SCD validation configuration to the config table."""
    try:
        # Prepare config data
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
            "custom_tests": request.custom_tests,
            "cron_schedule": request.cron_schedule
        }
        
        # Insert into config table
        from app.services.bigquery_service import bigquery_service
        success = await bigquery_service.insert_scd_config(
            project_id=request.project_id,
            config_dataset=request.config_dataset,
            config_table=request.config_table,
            config_data=config_data
        )
        
        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to insert SCD configuration into config table"
            )
        
        # Upsert scheduler job if cron_schedule is provided
        if request.cron_schedule:
            from app.services.scheduler_service import scheduler_service
            await scheduler_service.upsert_job(
                config_id=request.config_id,
                cron_schedule=request.cron_schedule,
                target_dataset=request.target_dataset,
                target_table=request.target_table,
                config_dataset=request.config_dataset,
                config_table=request.config_table
            )
        
        return {
            "success": True,
            "message": "SCD configuration added successfully",
            "config_id": request.config_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding SCD config: {str(e)}", exc_info=True)
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
            raise HTTPException(status_code=500, detail="Failed to save custom test")
        return {"status": "success", "message": "Custom test saved"}
    except Exception as e:
        logger.error(f"Error saving custom test: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/table-metadata", response_model=TableMetadataResponse)
async def get_table_metadata(
    project_id: str = settings.google_cloud_project,
    dataset_id: str = ...,
    table_id: str = ...
):
    """Get metadata for a specific BigQuery table."""
    try:
        from app.services.bigquery_service import bigquery_service
        metadata = await bigquery_service.get_table_metadata(project_id, dataset_id, table_id)
        
        # Extract just column names for easier frontend usage
        columns = [field['name'] for field in metadata.get('schema', {}).get('fields', [])]
        
        return TableMetadataResponse(
            full_table_name=metadata['full_table_name'],
            columns=columns,
            schema_info=metadata
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error fetching table metadata: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/run-scheduled-tests")
async def run_scheduled_tests(request: ScheduledTestRunRequest):
    """Endpoint triggered by Cloud Scheduler to run tests for a single table."""
    try:
        from app.services.test_executor import TestExecutor
        from app.services.bigquery_service import bigquery_service
        from app.services.history_service import TestHistoryService
        
        executor = TestExecutor()
        history_service = TestHistoryService()
        
        # Fetch full config details from BigQuery
        configs = await bigquery_service.read_scd_config_table(
            request.project_id, 
            request.config_dataset, 
            request.config_table
        )

        
        table_config = next((c for c in configs if c['config_id'] == request.config_id), None)
        if not table_config:
            logger.error(f"Config {request.config_id} not found in {request.config_dataset}.{request.config_table}")
            raise HTTPException(status_code=404, detail=f"Config {request.config_id} not found")
            
        logger.info(f"Running scheduled tests for {request.config_id}")
        mapping_result = await executor.process_scd(request.project_id, table_config)
        results = [r.dict() if hasattr(r, 'dict') else r for r in mapping_result.predefined_results]
        
        # Save to history (Policy: scheduled runs ALWAYS write to history)
        summary = {
            "total": len(results),
            "passed": len([r for r in results if r.get("status") == "PASS"]),
            "failed": len([r for r in results if r.get("status") == "FAIL"]),
            "errors": len([r for r in results if r.get("status") == "ERROR"])
        }
        
        # Determine execution source based on time
        # If running close to scheduled time (09:00 Melbourne), it's Scheduled. Otherwise Manual.
        import pytz
        from datetime import datetime
        
        executed_by = "Manual Run"
        try:
            tz = pytz.timezone("Australia/Melbourne")
            now = datetime.now(tz)
            
            # Check if within 5 minutes of 09:00 AM
            if now.hour == 9 and now.minute <= 5:
                 executed_by = "Scheduled Run"
        except Exception:
            # Fallback if timezone conversion fails
            executed_by = "Scheduled Run"

        history_service.save_test_results(
            project_id=request.project_id,
            comparison_mode="scd",
            test_results=results,
            target_dataset=request.target_dataset,
            target_table=request.target_table,
            mapping_id=request.config_id,
            cron_schedule=request.cron_schedule,
            executed_by=executed_by,
            metadata={
                "summary": summary,
                "source": f"Scheduled SCD: {request.target_table}",
                "status": "FAIL" if summary["failed"] > 0 or summary["errors"] > 0 else "PASS"
            }
        )
        
        return {
            "success": True,
            "message": f"Scheduled tests for {request.config_id} completed and saved to history",
            "results_summary": {
                "total": len(results),
                "passed": len([t for t in results if t.get('status') == 'PASS']),
                "failed": len([t for t in results if t.get('status') == 'FAIL'])
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error running scheduled tests: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
