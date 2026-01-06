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
    AddSCDConfigRequest
)
from app.services.test_executor import test_executor
from app.services.bigquery_service import bigquery_service

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

                await bigquery_service.log_execution(
                    project_id=request.project_id,
                    execution_data={
                        "comparison_mode": "gcs_config_table",
                        "source": f"{request.config_dataset}.{request.config_table}",
                        "target": "Multiple Targets",
                        "status": "AT_RISK" if summary_data['failed'] > 0 else "PASS",
                        "total_tests": summary_data['total_tests'],
                        "passed_tests": summary_data['passed'],
                        "failed_tests": summary_data['failed'],
                        "details": {
                            "summary": summary_data,
                            "results_by_mapping": results_by_mapping_dicts
                        }
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
                await bigquery_service.log_execution(
                    project_id=request.project_id,
                    execution_data={
                        "comparison_mode": "gcs_single_file",
                        "source": f"gs://{request.gcs_bucket}/{request.gcs_file_path}",
                        "target": f"{request.target_dataset}.{request.target_table}",
                        "status": "FAIL" if summary.failed > 0 or summary.errors > 0 else "PASS",
                        "total_tests": summary.total_tests,
                        "passed_tests": summary.passed,
                        "failed_tests": summary.failed,
                        "details": {
                            "summary": summary.dict(),
                            "mapping_info": result.mapping_info.dict() if result.mapping_info else None,
                            "predefined_results": [r.dict() for r in result.predefined_results],
                            "ai_suggestions": [s.dict() for s in result.ai_suggestions]
                        }
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
                    
                    await bigquery_service.log_execution(
                        project_id=request.project_id,
                        execution_data={
                            "comparison_mode": "schema_validation",
                            "source": "ERD Description",
                            "target": ",".join(request.datasets or []),
                            "status": "AT_RISK" if issues > 0 else "PASS",
                            "total_tests": summary.get('total_tables', 0),
                            "passed_tests": summary.get('total_tables', 0) - (1 if issues > 0 else 0),
                            "failed_tests": issues,
                            "details": result_data
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

                await bigquery_service.log_execution(
                    project_id=request.project_id,
                    execution_data={
                        "comparison_mode": "scd_config_table",
                        "source": f"{request.config_dataset}.{request.config_table}",
                        "target": "Multiple SCD Tables",
                        "status": "AT_RISK" if summary_data['failed'] > 0 else "PASS",
                        "total_tests": summary_data['total_tests'],
                        "passed_tests": summary_data['passed'],
                        "failed_tests": summary_data['failed'],
                        "details": {
                            "summary": summary_data,
                            "results_by_mapping": results_by_mapping_dicts
                        }
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
                    await bigquery_service.log_execution(
                        project_id=request.project_id,
                        execution_data={
                            "comparison_mode": "scd",
                            "source": f"SCD: {request.target_table}",
                            "target": f"{request.target_dataset}.{request.target_table}",
                            "status": "FAIL" if any(r.status in ['FAIL', 'ERROR'] for r in result.predefined_results) else "PASS",
                            "total_tests": len(result.predefined_results),
                            "passed_tests": len([t for t in result.predefined_results if t.status == 'PASS']),
                            "failed_tests": len([t for t in result.predefined_results if t.status == 'FAIL']),
                            "details": {
                                "summary": {
                                    "total_tests": len(result.predefined_results),
                                    "passed": len([t for t in result.predefined_results if t.status == 'PASS']),
                                    "failed": len([t for t in result.predefined_results if t.status == 'FAIL']),
                                    "errors": len([t for t in result.predefined_results if t.status == 'ERROR'])
                                },
                                "predefined_results": [r.dict() for r in result.predefined_results]
                            }
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
                    'results_by_mapping': [result]
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



@app.get("/api/history")
async def get_test_history(project_id: str = settings.google_cloud_project, limit: int = 50):
    """Get previous test runs from BigQuery."""
    try:
        from app.services.bigquery_service import bigquery_service
        return await bigquery_service.get_execution_history(project_id=project_id, limit=limit)
    except Exception as e:
        logger.error(f"Error fetching history: {e}")
        return []


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
            "description": request.description
        }
        
        # Insert into config table
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
