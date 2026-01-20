"""Main FastAPI application for Data QA Agent backend."""
import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import traceback

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
    SaveHistoryRequest
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
            
            # History logging disabled for non-SCD tests per user request
            # if needed in future, enable it or write to a different table
            pass

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
            # History logging disabled for non-SCD tests per user request
            try:
                # Log execution - disabled
                pass 
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
                # History logging disabled for non-SCD tests per user request
                pass

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
            
            logger.info(f"🔍 Starting Batch SCD validation from config: {request.config_dataset}.{request.config_table}")
            result = await test_executor.process_scd_config_table(
                project_id=request.project_id,
                config_dataset=request.config_dataset,
                config_table=request.config_table
            )
            logger.info(f"📋 Batch SCD validation completed. Found {len(result['results_by_mapping'])} table mappings.")
            
            try:
                summary_data = result['summary']
                # Convert results objects to dicts for JSON serialization
                results_by_mapping_dicts = [r.dict() for r in result['results_by_mapping']]

                # Log each individual table result instead of the config table itself
                logger.info(f"💾 Attempting to save {len(result['results_by_mapping'])} table results to history...")
                for mapping_result in result['results_by_mapping']:
                    try:
                        # Calculate summary for this specific table
                        table_results = mapping_result.predefined_results
                        table_summary = {
                            "total": len(table_results),
                            "passed": len([t for t in table_results if t.status == 'PASS']),
                            "failed": len([t for t in table_results if t.status == 'FAIL']),
                            "errors": len([t for t in table_results if t.status == 'ERROR'])
                        }
                        
                        # Extract target info from mapping info
                        target_ds = mapping_result.mapping_info.target.split('.')[0] if mapping_result.mapping_info and '.' in mapping_result.mapping_info.target else request.config_dataset
                        target_tbl = mapping_result.mapping_info.target.split('.')[1] if mapping_result.mapping_info and '.' in mapping_result.mapping_info.target else mapping_result.mapping_id

                        history_service.save_test_results(
                            project_id=request.project_id,
                            comparison_mode="scd",  # Log as standard SCD run
                            test_results=[r.dict() for r in table_results],
                            target_dataset=target_ds,
                            target_table=target_tbl,
                            mapping_id=mapping_result.mapping_id,
                            executed_by="Batch Run",
                            metadata={
                                "summary": table_summary,
                                "source": f"Batch SCD: {target_tbl}",
                                "status": "FAIL" if table_summary['failed'] > 0 or table_summary['errors'] > 0 else "PASS"
                            }
                        )
                    except Exception as inner_e:
                        logger.error(f"❌ Failed to log individual result for {mapping_result.mapping_id}: {inner_e}")
                logger.info(f"✅ Batch SCD history saved successfully.")
            except Exception as e:
                logger.error(f"💥 Failed to log scd config execution: {e}", exc_info=True)

            logger.info(f"🚀 Returning Batch SCD results to frontend.")
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
                'enabled_test_ids': request.enabled_test_ids,
                'custom_tests': request.custom_tests
            }
            
            try:
                logger.info(f"🔍 Starting SCD validation for {request.target_table} in {request.target_dataset}")
                result = await test_executor.process_scd(request.project_id, mapping)
                logger.info(f"📋 SCD validation completed. Found {len(result.predefined_results)} tests.")
                
                # Log execution
                try:
                    logger.info(f"💾 Attempting to save SCD results to history...")
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
                    logger.info(f"✅ SCD history saved successfully.")
                except Exception as log_err:
                    logger.error(f"❌ Failed to log scd execution: {log_err}", exc_info=True)
                
                response_data = {
                    'summary': {
                        'total_tests': len(result.predefined_results),
                        'passed': len([t for t in result.predefined_results if t.status == 'PASS']),
                        'failed': len([t for t in result.predefined_results if t.status == 'FAIL']),
                        'errors': len([t for t in result.predefined_results if t.status == 'ERROR'])
                    },
                    'results_by_mapping': [result.dict()]
                }
                logger.info(f"🚀 Returning SCD results to frontend.")
                return response_data
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
        error_details = traceback.format_exc()
        logger.error(f"Error generating tests: {str(e)}\n{error_details}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"{str(e)}\n\nTraceback:\n{error_details}")



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
            metadata=request.metadata
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
        logger.error(f"Error fetching execution history: {e}", exc_info=True)
        # Don't return empty list on real error, let frontend show error
        raise HTTPException(status_code=500, detail=str(e))


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


@app.get("/api/scd-config/{project_id}/{config_dataset}/{config_table}/{target_dataset}/{target_table}")
async def get_scd_config_by_table(
    project_id: str,
    config_dataset: str,
    config_table: str,
    target_dataset: str,
    target_table: str
):
    """Fetch an existing SCD config by target dataset and table for auto-fill."""
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
        logger.error(f"Error fetching SCD config: {str(e)}", exc_info=True)
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
            "custom_tests": request.custom_tests
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
            logger.error(f"BigQuery insert_scd_config returned False for config_id: {request.config_id}")
            raise HTTPException(
                status_code=500,
                detail="Failed to insert SCD configuration. Check backend logs for details."
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





if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
