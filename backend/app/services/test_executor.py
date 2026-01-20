"""Test executor service for orchestrating test execution."""
import logging
from typing import Dict, List, Any, Optional
import json

from app.services.gcs_service import gcs_service
from app.services.bigquery_service import bigquery_service
from app.services.vertex_ai_service import vertex_ai_service
from app.tests.predefined_tests import get_enabled_tests
from app.models import TestResult, MappingInfo, AISuggestion, MappingResult

logger = logging.getLogger(__name__)


class TestExecutor:
    """Service for executing tests on data mappings."""
    
    async def process_mapping(
        self,
        project_id: str,
        mapping: Dict[str, Any]
    ) -> MappingResult:
        """
        Process a single mapping with predefined tests and AI suggestions.
        
        Args:
            project_id: Google Cloud project ID
            mapping: Mapping configuration dictionary
            
        Returns:
            MappingResult with test results and suggestions
        """
        mapping_id = mapping.get('mapping_id', 'unknown')
        
        try:
            # Extract mapping configuration
            target_dataset = mapping['target_dataset']
            target_table = mapping['target_table']
            full_target_name = f"{project_id}.{target_dataset}.{target_table}"
            
            # Determine Source Type
            source_project = mapping.get('source_project')
            source_dataset = mapping.get('source_dataset')
            source_table = mapping.get('source_table')
            
            is_bq_source = bool(source_dataset and source_table)
            
            file_row_count = 0
            source_description = ""
            
            if is_bq_source:
                # BigQuery Source Logic
                src_proj = source_project or project_id
                full_source_name = f"{src_proj}.{source_dataset}.{source_table}"
                source_description = full_source_name
                logger.info(f"Processing BQ-to-BQ: {full_source_name} -> {full_target_name}")
                
                # Get Source Info (BQ)
                file_row_count = await bigquery_service.get_row_count(full_source_name)
                
            else:
                # GCS Source Logic (Default)
                source_bucket = mapping.get('source_bucket')
                source_file_path = mapping.get('source_file_path')
                
                if not source_bucket or not source_file_path:
                     raise ValueError(f"Mapping {mapping_id} missing source info (GCS or BQ)")

                # Resolve wildcard pattern if present
                matching_files = await gcs_service.resolve_pattern(source_bucket, source_file_path)
                actual_file_path = matching_files[0]
                logger.info(f"Resolved {source_file_path} to {actual_file_path}. Found {len(matching_files)} matching files.")
                
                source_description = f"gs://{source_bucket}/{actual_file_path}"
                
                # Get GCS file info
                file_row_count = await gcs_service.count_csv_rows(source_bucket, actual_file_path)

            
            # Get Target BigQuery table info
            bq_row_count = await bigquery_service.get_row_count(full_target_name)
            table_metadata = await bigquery_service.get_table_metadata(project_id, target_dataset, target_table)
            
            # Prepare test configuration
            test_config = {
                'full_table_name': full_target_name,
                'primary_key_columns': mapping.get('primary_key_columns', []) or [
                    col['name'] for col in table_metadata['schema']['fields'] 
                    if col['name'].lower() in ['id', 'key', 'uuid', 'guid', f"{target_table}_id"]
                ],
                'required_columns': mapping.get('required_columns', []) or [
                    col['name'] for col in table_metadata['schema']['fields']
                    if col['mode'] == 'REQUIRED'
                ],
                'date_columns': mapping.get('date_columns', []),
                'numeric_range_checks': json.loads(mapping.get('numeric_range_checks', '{}')) if isinstance(mapping.get('numeric_range_checks'), str) else mapping.get('numeric_range_checks', {}),
                'date_range_checks': json.loads(mapping.get('date_range_checks', '{}')) if isinstance(mapping.get('date_range_checks'), str) else mapping.get('date_range_checks', {}),
                'foreign_key_checks': json.loads(mapping.get('foreign_key_checks', '{}')) if isinstance(mapping.get('foreign_key_checks'), str) else mapping.get('foreign_key_checks', {}),
                'pattern_checks': json.loads(mapping.get('pattern_checks', '{}')) if isinstance(mapping.get('pattern_checks'), str) else mapping.get('pattern_checks', {}),
                'outlier_columns': mapping.get('outlier_columns', []) or [
                    col['name'] for col in table_metadata['schema']['fields']
                    if col['type'] in ['INTEGER', 'FLOAT', 'NUMERIC', 'BIGNUMERIC']
                ]
            }
            
            # Get enabled tests
            enabled_test_ids = mapping.get('enabled_test_ids', [])
            if not enabled_test_ids and not mapping.get('enabled_test_ids'):
                 # If explicit list is empty/None, default to these for Single File mode inference
                 # However, get_enabled_tests(None) returns Globals.
                 pass
            enabled_tests = get_enabled_tests(enabled_test_ids)
            
            # Execute predefined tests
            predefined_results = []
            
            # Row count test (always run first)
            predefined_results.append(TestResult(
                test_id='row_count_match',
                test_name='Row Count Match',
                category='completeness',
                description=f"Source ({'BQ' if is_bq_source else 'GCS'}): {file_row_count} rows, Target: {bq_row_count} rows",
                status='PASS' if file_row_count == bq_row_count else 'FAIL',
                severity='HIGH',
                sql_query='',
                rows_affected=abs(file_row_count - bq_row_count),
                error_message=f"Row count mismatch: {abs(file_row_count - bq_row_count)} rows difference" if file_row_count != bq_row_count else None
            ))
            
            # Run other enabled tests
            for test in enabled_tests:
                if test.id == 'row_count_match':
                    continue  # Already done
                
                sql = test.generate_sql(test_config)
                if not sql:
                    continue  # Skip if no SQL (test not applicable)
                
                try:
                    rows = await bigquery_service.execute_query(sql)
                    row_count = len(rows)
                    
                    predefined_results.append(TestResult(
                        test_id=test.id,
                        test_name=test.name,
                        category=test.category,
                        description=test.description,
                        status='PASS' if row_count == 0 else 'FAIL',
                        severity=test.severity,
                        sql_query=sql,
                        rows_affected=row_count,
                        error_message=None
                    ))
                except Exception as e:
                    predefined_results.append(TestResult(
                        test_id=test.id,
                        test_name=test.name,
                        category=test.category,
                        description=test.description,
                        status='ERROR',
                        severity=test.severity,
                        sql_query=sql,
                        rows_affected=0,
                        error_message=str(e)
                    ))
            
            # Execute custom tests
            try:
                active_custom_tests = await bigquery_service.get_active_custom_tests(
                    project_id, target_dataset, target_table
                )
                
                for custom_test in active_custom_tests:
                    try:
                        sql = custom_test['sql_query']
                        rows = await bigquery_service.execute_query(sql)
                        row_count = len(rows)
                        
                        status = 'PASS' if row_count == 0 else 'FAIL'
                        
                        predefined_results.append(TestResult(
                            test_id=f"custom_{custom_test.get('test_name', 'unknown')}",
                            test_name=f"[Custom] {custom_test.get('test_name', 'Custom Test')}",
                            category=custom_test.get('test_category', 'custom'),
                            description=custom_test.get('description', ''),
                            status=status,
                            severity=custom_test.get('severity', 'HIGH'),
                            sql_query=sql,
                            rows_affected=row_count
                        ))
                    except Exception as e:
                        predefined_results.append(TestResult(
                            test_id=f"custom_error_{custom_test.get('test_name', 'unknown')}",
                            test_name=f"[Custom] {custom_test.get('test_name', 'Custom Test')}",
                            category='custom',
                            description=f"Error executing custom test: {str(e)}",
                            status='ERROR',
                            severity='HIGH',
                            sql_query=custom_test.get('sql_query', ''),
                            rows_affected=0,
                            error_message=str(e)
                        ))
            except Exception as e:
                logger.error(f"Failed to fetch/run custom tests: {e}")

            # Generate AI suggestions if enabled
            ai_suggestions = []
            if mapping.get('auto_suggest', True):
                try:
                    source_sample_data = []
                    if is_bq_source:
                         source_sample_data = await bigquery_service.get_sample_data(full_source_name, 5)
                    else:
                         source_sample_data = await gcs_service.sample_csv_data(source_bucket, actual_file_path, 5)
                         
                    bq_sample = await bigquery_service.get_sample_data(full_target_name, 5)
                    
                    existing_test_names = [test.name for test in enabled_tests]
                    
                    suggestions = await vertex_ai_service.generate_test_suggestions(
                        mapping_id=mapping_id,
                        source_info=source_description,
                        target_table=full_target_name,
                        bq_schema=table_metadata['schema'],
                        gcs_sample=source_sample_data, # Re-using param name for generic sample
                        bq_sample=bq_sample,
                        existing_tests=existing_test_names
                    )
                    
                    ai_suggestions = [
                        AISuggestion(**suggestion)
                        for suggestion in suggestions
                    ]
                except Exception as e:
                    logger.error(f"Failed to generate AI suggestions for {mapping_id}: {str(e)}")
            
            return MappingResult(
                mapping_id=mapping_id,
                mapping_info=MappingInfo(
                    source=source_description,
                    target=full_target_name,
                    file_row_count=file_row_count,
                    table_row_count=bq_row_count
                ),
                predefined_results=predefined_results,
                ai_suggestions=ai_suggestions
            )
            
        except Exception as e:
            logger.error(f"Error processing mapping {mapping_id}: {str(e)}")
            return MappingResult(
                mapping_id=mapping_id,
                predefined_results=[],
                ai_suggestions=[],
                error=str(e)
            )
    
    async def process_config_table(
        self,
        project_id: str,
        config_dataset: str,
        config_table: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Process all mappings from a config table.
        
        Args:
            project_id: Google Cloud project ID
            config_dataset: Config table dataset
            config_table: Config table name
            filters: Optional dictionary to filter config table records
            
        Returns:
            Dictionary with summary and results by mapping
        """
        try:
            # Read config table
            mappings = await bigquery_service.read_config_table(
                project_id, config_dataset, config_table, filters
            )
            
            if not mappings:
                raise ValueError("No active mappings found in config table")
            
            # Process each mapping
            results = []
            for mapping in mappings:
                result = await self.process_mapping(project_id, mapping)
                results.append(result)
            
            # Calculate summary
            total_tests = sum(len(r.predefined_results) for r in results)
            passed = sum(
                len([t for t in r.predefined_results if t.status == 'PASS'])
                for r in results
            )
            failed = sum(
                len([t for t in r.predefined_results if t.status == 'FAIL'])
                for r in results
            )
            errors = sum(
                len([t for t in r.predefined_results if t.status == 'ERROR'])
                for r in results
            )
            total_suggestions = sum(len(r.ai_suggestions) for r in results)
            
            return {
                'summary': {
                    'total_mappings': len(results),
                    'total_tests': total_tests,
                    'passed': passed,
                    'failed': failed,
                    'errors': errors,
                    'total_suggestions': total_suggestions
                },
                'results_by_mapping': results
            }
            
        except Exception as e:
            logger.error(f"Error processing config table: {str(e)}")
            raise


    async def process_schema_validation(
        self,
        project_id: str,
        datasets: List[str],
        erd_description: str
    ) -> Dict[str, Any]:
        """
        Process schema validation request.
        
        Args:
            project_id: Google Cloud project ID
            datasets: List of BigQuery datasets to scan
            erd_description: ERD description text
            
        Returns:
            Validation results summary and details
        """
        all_schemas = {}
        
        # 1. Gather all schemas
        for dataset_id in datasets:
            try:
                table_ids = await bigquery_service.get_tables_in_dataset(project_id, dataset_id)
                for table_id in table_ids:
                    try:
                        full_name = f"{project_id}.{dataset_id}.{table_id}"
                        metadata = await bigquery_service.get_table_metadata(project_id, dataset_id, table_id)
                        all_schemas[full_name] = metadata['schema']
                    except Exception as e:
                        logger.warning(f"Skipping table {table_id}: {str(e)}")
            except Exception as e:
                logger.error(f"Error listing tables for {dataset_id}: {str(e)}")
        
        if not all_schemas:
            logger.warning("No schemas found to validate.")
            return {
                'summary': {'total_tables': 0, 'issues': 0},
                'predefined_results': [],
                'ai_suggestions': []
            }

        # 2. Verify with AI
        ai_findings = await vertex_ai_service.validate_schema(erd_description, all_schemas)
        
        # 3. Convert to TestResult objects
        results = []
        for finding in ai_findings:
            results.append(TestResult(
                test_name=finding.get('test_name', 'Schema Check'),
                category=finding.get('test_category', 'schema_validation'),
                status=finding.get('status', 'INFO'),
                severity=finding.get('severity', 'MEDIUM'),
                description=finding.get('reasoning', ''),
                sql_query=finding.get('sql_query', ''),
                rows_affected=0
            ))
            
        return {
            'summary': {
                'total_tables': len(all_schemas),
                'total_issues': len(results)
            },
            'predefined_results': results,
            'ai_suggestions': []
        }


# Singleton instance
test_executor = TestExecutor()
