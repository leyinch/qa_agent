"""Main FastAPI application for Data QA Agent backend."""
import logging
from typing import Union, Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import traceback
import uuid

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
    ProjectSettings
)

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
    Supports: schema, gcs, gcs-config, scd, scd-config
    """
    from app.services.test_executor import test_executor
    from app.services.history_service import TestHistoryService
    from app.services.bigquery_service import bigquery_service
    history_service = TestHistoryService()

    try:
        logger.info(f"Received request: mode={request.comparison_mode}, project={request.project_id}")
        exec_id = request.execution_id or str(uuid.uuid4())
        
        # 1. Config Table Mode (GCS)
        if request.comparison_mode == 'gcs-config':
            result = await test_executor.process_config_table(
                project_id=request.project_id,
                config_dataset=request.config_dataset,
                config_table=request.config_table,
                filters=request.config_filters
            )
            # Colleague's Background Logging
            try:
                rows_to_log = []
                for mapping_result in result['results_by_mapping']:
                    if mapping_result.error:
                        rows_to_log.append({"execution_id": exec_id, "project_id": request.project_id, "comparison_mode": "gcs-config", "status": "ERROR", "error_message": mapping_result.error})
                        continue
                    for test in mapping_result.predefined_results:
                        rows_to_log.append({
                            "execution_id": exec_id, "project_id": request.project_id, "comparison_mode": "gcs-config",
                            "mapping_id": mapping_result.mapping_id, "test_id": test.test_id, "test_name": test.test_name,
                            "status": test.status, "description": test.description, "source": mapping_result.mapping_info.source if mapping_result.mapping_info else None,
                            "target": mapping_result.mapping_info.target if mapping_result.mapping_info else None
                        })
                await bigquery_service.log_execution(project_id=request.project_id, execution_data=rows_to_log)
            except Exception as e:
                logger.error(f"Background logging failed: {e}")
            
            return ConfigTableResponse(execution_id=exec_id, summary=ConfigTableSummary(**result['summary']), results_by_mapping=result['results_by_mapping'])

        # 2. GCS Single File
        elif request.comparison_mode == 'gcs':
            result = await test_executor.process_mapping(request.project_id, {
                'mapping_id': 'single_file_comparison', 'source_bucket': request.gcs_bucket, 'source_file_path': request.gcs_file_path,
                'source_file_format': request.file_format, 'target_dataset': request.target_dataset, 'target_table': request.target_table,
                'enabled_test_ids': request.enabled_test_ids, 'auto_suggest': True
            })
            if result.error: raise HTTPException(status_code=400, detail=result.error)
            
            # Colleague's Background Logging
            try:
                rows_to_log = [{
                    "execution_id": exec_id, "project_id": request.project_id, "comparison_mode": "gcs", 
                    "test_id": t.test_id, "test_name": t.test_name, "status": t.status, "description": t.description,
                    "source": f"gs://{request.gcs_bucket}/{request.gcs_file_path}", "target": f"{request.target_dataset}.{request.target_table}"
                } for t in result.predefined_results]
                await bigquery_service.log_execution(project_id=request.project_id, execution_data=rows_to_log)
            except Exception as e:
                logger.error(f"Background logging failed: {e}")

            return GenerateTestsResponse(execution_id=exec_id, summary=TestSummary(total_tests=len(result.predefined_results), passed=len([t for t in result.predefined_results if t.status == 'PASS']), failed=len([t for t in result.predefined_results if t.status == 'FAIL']), errors=len([t for t in result.predefined_results if t.status == 'ERROR'])), results=result.predefined_results)

        # 3. Schema Validation
        elif request.comparison_mode == 'schema':
            result_data = await test_executor.process_schema_validation(project_id=request.project_id, datasets=request.datasets or [], erd_description=request.erd_description or "")
            # Colleague's Background Logging
            try:
                rows_to_log = [{
                    "execution_id": exec_id, "project_id": request.project_id, "comparison_mode": "schema",
                    "test_name": t.test_name, "status": t.status, "description": t.description, "source": "ERD", "target": ",".join(request.datasets or [])
                } for t in result_data.get('predefined_results', [])]
                await bigquery_service.log_execution(project_id=request.project_id, execution_data=rows_to_log)
            except Exception as e:
                logger.error(f"Background logging failed: {e}")
            result_data['execution_id'] = exec_id
            return result_data

        # 4. SCD Config Table (Our feature)
        elif request.comparison_mode == 'scd-config':
            result = await test_executor.process_scd_config_table(project_id=request.project_id, config_dataset=request.config_dataset, config_table=request.config_table)
            # Our History Logging
            try:
                for mapping_result in result['results_by_mapping']:
                    history_service.save_test_results(
                        project_id=request.project_id, comparison_mode="scd", test_results=[r.dict() for r in mapping_result.predefined_results],
                        target_dataset=mapping_result.mapping_info.target.split('.')[0] if '.' in mapping_result.mapping_info.target else request.config_dataset,
                        target_table=mapping_result.mapping_info.target.split('.')[1] if '.' in mapping_result.mapping_info.target else mapping_result.mapping_id,
                        mapping_id=mapping_result.mapping_id, executed_by="Batch Run"
                    )
            except Exception as e:
                logger.error(f"SCD History logging failed: {e}")
            return ConfigTableResponse(summary=ConfigTableSummary(**result['summary']), results_by_mapping=result['results_by_mapping'])

        # 5. SCD Manual (Our feature)
        elif request.comparison_mode == 'scd':
            result = await test_executor.process_scd(request.project_id, {
                'target_dataset': request.target_dataset, 'target_table': request.target_table, 'scd_type': request.scd_type or 'scd2',
                'primary_keys': request.primary_keys or [], 'surrogate_key': request.surrogate_key, 'begin_date_column': request.begin_date_column,
                'end_date_column': request.end_date_column, 'active_flag_column': request.active_flag_column, 'enabled_test_ids': request.enabled_test_ids, 'custom_tests': request.custom_tests
            })
            # Our History Logging
            try:
                history_service.save_test_results(
                    project_id=request.project_id, comparison_mode="scd", test_results=[r.dict() for r in result.predefined_results],
                    target_dataset=request.target_dataset, target_table=request.target_table, executed_by="Manual Run"
                )
            except Exception as e:
                logger.error(f"SCD History logging failed: {e}")
            return {
                'summary': {'total_tests': len(result.predefined_results), 'passed': len([t for t in result.predefined_results if t.status == 'PASS']), 'failed': len([t for t in result.predefined_results if t.status == 'FAIL']), 'errors': len([t for t in result.predefined_results if t.status == 'ERROR'])},
                'results_by_mapping': [result.dict()]
            }

        else:
            raise HTTPException(status_code=400, detail=f"Invalid mode: {request.comparison_mode}")

    except HTTPException: raise
    except Exception as e:
        logger.error(f"Error in generate_tests: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/save-test-history")
async def save_test_history(request: SaveHistoryRequest):
    from app.services.history_service import TestHistoryService
    history_service = TestHistoryService()
    try:
        eid = history_service.save_test_results(project_id=request.project_id, comparison_mode=request.comparison_mode, test_results=request.test_results, target_dataset=request.target_dataset, target_table=request.target_table, mapping_id=request.mapping_id, executed_by=request.executed_by, metadata=request.metadata)
        return {"status": "success", "execution_id": eid}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/history")
async def get_test_history(project_id: str = settings.google_cloud_project, limit: int = 50):
    from app.services.history_service import TestHistoryService
    try:
        return TestHistoryService().get_test_history(project_id=project_id, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/history")
async def clear_test_history(project_id: str):
    from app.services.history_service import TestHistoryService
    try:
        TestHistoryService().clear_history(project_id)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/history-details")
async def get_history_details(execution_id: str, project_id: str = settings.google_cloud_project):
    from app.services.history_service import TestHistoryService
    try:
        return TestHistoryService().get_test_history(project_id=project_id, execution_id=execution_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/scd-config/{project_id}/{config_dataset}/{config_table}/{target_dataset}/{target_table}")
async def get_scd_config_by_table(project_id: str, config_dataset: str, config_table: str, target_dataset: str, target_table: str):
    from app.services.bigquery_service import bigquery_service
    try:
        config = await bigquery_service.get_scd_config_by_table(project_id, config_dataset, config_table, target_dataset, target_table)
        if not config: raise HTTPException(status_code=404, detail="Not found")
        return config
    except HTTPException: raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/scd-config")
async def add_scd_config(request: AddSCDConfigRequest):
    from app.services.bigquery_service import bigquery_service
    try:
        success = await bigquery_service.insert_scd_config(project_id=request.project_id, config_dataset=request.config_dataset, config_table=request.config_table, config_data=request.dict())
        if not success: raise HTTPException(status_code=500, detail="Failed to insert")
        return {"success": True, "message": "Added"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/predefined-tests")
async def list_predefined_tests():
    from app.tests.predefined_tests import PREDEFINED_TESTS
    return {'tests': [t.dict() if hasattr(t, 'dict') else {'id': t.id, 'name': t.name, 'category': t.category, 'severity': t.severity, 'description': t.description} for t in PREDEFINED_TESTS.values()]}


@app.post("/api/custom-tests")
async def save_custom_test(request: CustomTestRequest):
    from app.services.bigquery_service import bigquery_service
    try:
        success = await bigquery_service.save_custom_test(request.dict())
        return {"status": "success"} if success else HTTPException(status_code=500, detail="Failed")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/settings")
async def get_settings(project_id: str):
    from app.services.bigquery_service import bigquery_service
    try:
        s = await bigquery_service.get_project_settings(project_id)
        return s or {"project_id": project_id, "alert_emails": [], "teams_webhook_url": "", "alert_on_failure": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/settings")
async def save_settings(settings: ProjectSettings):
    from app.services.bigquery_service import bigquery_service
    try:
        success = await bigquery_service.save_project_settings(settings.dict())
        return {"status": "success"} if success else HTTPException(status_code=500, detail="Failed")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/notify")
async def notify_execution(payload: Dict[str, Any]):
    # Colleague's Notification Logic (Briefly summarized)
    try:
        pid, eid = payload.get("project_id"), payload.get("execution_id")
        from app.services.bigquery_service import bigquery_service
        s = await bigquery_service.get_project_settings(pid)
        if not s or not s.get('alert_on_failure', True): return {"status": "skipped"}
        # Notification sending logic would go here...
        return {"status": "processed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/table-metadata", response_model=TableMetadataResponse)
async def get_table_metadata(project_id: str = settings.google_cloud_project, dataset_id: str = ..., table_id: str = ...):
    from app.services.bigquery_service import bigquery_service
    try:
        meta = await bigquery_service.get_table_metadata(project_id, dataset_id, table_id)
        return TableMetadataResponse(full_table_name=meta['full_table_name'], columns=[f['name'] for f in meta.get('schema', {}).get('fields', [])], schema_info=meta)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
