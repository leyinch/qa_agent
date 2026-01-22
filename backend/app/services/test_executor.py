"""Test executor service for orchestrating test execution."""
import logging
from typing import Dict, List, Any, Optional
import json
import asyncio

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
        Process a single mapping with predefined tests and AI suggestions (Test1 Logic).
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
            
            # --- PARALLEL TEST EXECUTION START ---
            async def run_predefined_test(test):
                if test.id == 'row_count_match':
                    return None
                
                sql = test.generate_sql(test_config)
                if not sql:
                    return None
                
                try:
                    rows = await bigquery_service.execute_query(sql)
                    row_count = len(rows)
                    
                    # Convert rows to serializable dicts
                    sample_data = rows[:10] if row_count > 0 else None
                    
                    return TestResult(
                        test_id=test.id,
                        test_name=test.name,
                        category=test.category,
                        description=test.description,
                        status='PASS' if row_count == 0 else 'FAIL',
                        severity=test.severity,
                        sql_query=sql,
                        rows_affected=row_count,
                        sample_data=sample_data,
                        error_message=None
                    )
                except Exception as e:
                    return TestResult(
                        test_id=test.id,
                        test_name=test.name,
                        category=test.category,
                        description=test.description,
                        status='ERROR',
                        severity=test.severity,
                        sql_query=sql or "",
                        rows_affected=0,
                        error_message=str(e)
                    )

            async def run_custom_test(ct):
                try:
                    sql = ct['sql_query']
                    rows = await bigquery_service.execute_query(sql)
                    row_count = len(rows)
                    
                    status = 'PASS' if row_count == 0 else 'FAIL'
                    
                    return TestResult(
                        test_id=f"custom_{ct.get('test_name', 'unknown')}",
                        test_name=f"[Custom] {ct.get('test_name', 'Custom Test')}",
                        category=ct.get('test_category', 'custom'),
                        description=ct.get('description', ''),
                        status=status,
                        severity=ct.get('severity', 'HIGH'),
                        sql_query=sql,
                        rows_affected=row_count,
                        sample_data=rows[:10] if row_count > 0 else None
                    )
                except Exception as e:
                    return TestResult(
                        test_id=f"custom_error_{ct.get('test_name', 'unknown')}",
                        test_name=f"[Custom] {ct.get('test_name', 'Custom Test')}",
                        category='custom',
                        description=f"Error executing custom test: {str(e)}",
                        status='ERROR',
                        severity='HIGH',
                        sql_query=ct.get('sql_query', ''),
                        rows_affected=0,
                        error_message=str(e)
                    )

            # Row count test (already done, but keeping structure)
            predefined_results = [predefined_results[0]] # Keep row_count_match

            # Fetch active custom tests
            try:
                active_custom_tests = await bigquery_service.get_active_custom_tests(
                    project_id, target_dataset, target_table
                )
            except Exception as e:
                logger.error(f"Failed to fetch custom tests: {e}")
                active_custom_tests = []

            # Prepare all tasks
            test_tasks = [run_predefined_test(t) for t in enabled_tests]
            custom_tasks = [run_custom_test(ct) for ct in active_custom_tests]

            # Execute all
            all_raw_results = await asyncio.gather(*test_tasks, *custom_tasks)
            predefined_results.extend([r for r in all_raw_results if r is not None])
            # --- PARALLEL TEST EXECUTION END ---

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
                        gcs_sample=source_sample_data, 
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

    async def process_scd(
        self,
        project_id: str,
        mapping: Dict[str, Any]
    ) -> MappingResult:
        """
        Process SCD validation for a table (Test3 Feature).
        """
        mapping_id = mapping.get('mapping_id', f"{mapping.get('target_table', 'unknown')}_scd")
        
        try:
            target_dataset = mapping['target_dataset']
            target_table = mapping['target_table']
            scd_type = mapping.get('scd_type', 'scd2')
            
            logger.info(f"Processing SCD mapping: {mapping_id} type={scd_type} project={project_id}")
            
            full_table_name = f"{project_id}.{target_dataset}.{target_table}"
            table_metadata = await bigquery_service.get_table_metadata(project_id, target_dataset, target_table)
            
            # Prepare test configuration
            test_config = {
                'full_table_name': full_table_name,
                'primary_keys': mapping.get('primary_keys', []),
                'surrogate_key': mapping.get('surrogate_key'),
                'begin_date_column': mapping.get('begin_date_column', 'DWBeginEffDateTime'),
                'end_date_column': mapping.get('end_date_column', 'DWEndEffDateTime'),
                'active_flag_column': mapping.get('active_flag_column', 'DWCurrentRowFlag')
            }
            
            # Auto-selection of tests if none provided
            enabled_test_ids = mapping.get('enabled_test_ids', [])
            if not enabled_test_ids:
                # 1. Table exists smoke test (Always run)
                enabled_test_ids.append('table_exists')
                
                # 2 & 3. Basic structural tests
                if test_config['surrogate_key']:
                    enabled_test_ids.extend(['surrogate_key_null', 'surrogate_key_unique'])
                
                # 4 & 5. SCD specific tests
                if scd_type == 'scd1':
                    enabled_test_ids.extend(['scd1_primary_key_null', 'scd1_primary_key_unique'])
                elif scd_type == 'scd2':
                    enabled_test_ids.extend([
                        'surrogate_key_null', 'surrogate_key_unique',
                        'scd2_primary_key_null',
                        'scd2_begin_date_null', 'scd2_end_date_null', 'scd2_flag_null',
                        'scd2_one_current_row', 'scd2_current_date_check', 
                        'scd2_invalid_flag_combination', 'scd2_date_order',
                        'scd2_unique_begin_date', 'scd2_unique_end_date',
                        'scd2_continuity', 'scd2_no_record_after_current'
                    ])
            
            enabled_tests = get_enabled_tests(enabled_test_ids)
            
            # --- PARALLEL TEST EXECUTION START ---
            async def run_predefined_test(test):
                sql = test.generate_sql(test_config)
                if not sql:
                    return None
                
                try:
                    rows = await bigquery_service.execute_query(sql)
                    row_count = len(rows)
                    
                    is_smoke = test.category == 'smoke'
                    status = 'PASS' if (row_count > 0 if is_smoke else row_count == 0) else 'FAIL'
                    
                    return TestResult(
                        test_id=test.id,
                        test_name=test.name,
                        category=test.category,
                        description=test.description,
                        status=status,
                        severity=test.severity,
                        sql_query=sql,
                        rows_affected=0 if status == 'PASS' else row_count,
                        sample_data=rows[:10] if status == 'FAIL' and not is_smoke else None,
                        error_message=None
                    )
                except Exception as e:
                    return TestResult(
                        test_id=test.id,
                        test_name=test.name,
                        category=test.category,
                        description=test.description,
                        status='ERROR',
                        severity=test.severity,
                        sql_query=sql or "",
                        rows_affected=0,
                        error_message=str(e)
                    )

            # Define run_custom_test inside to capture variables
            async def run_custom_test(inner_ct):
                name = inner_ct.get('name') or inner_ct.get('test_name', 'Unnamed Business Rule')
                raw_sql = inner_ct.get('sql') or inner_ct.get('sql_query')
                if not raw_sql: return None
                
                sql = raw_sql.replace('{{target}}', f"`{full_table_name}`")
                try:
                    rows = await bigquery_service.execute_query(sql)
                    row_count = len(rows)
                    return TestResult(
                        test_id=f"custom_{name.lower().replace(' ', '_')}",
                        test_name=name,
                        category='business_rule',
                        description=inner_ct.get('description', f"Custom business rule: {name}"),
                        status='PASS' if row_count == 0 else 'FAIL',
                        severity=inner_ct.get('severity', 'HIGH'),
                        sql_query=sql,
                        rows_affected=row_count,
                        sample_data=rows[:10] if row_count > 0 else None,
                        error_message=None
                    )
                except Exception as e:
                    return TestResult(
                        test_id=f"custom_{name.lower().replace(' ', '_')}",
                        test_name=name,
                        category='business_rule',
                        description=f"Error executing custom rule: {name}",
                        status='ERROR',
                        severity=inner_ct.get('severity', 'HIGH'),
                        sql_query=sql or "",
                        rows_affected=0,
                        error_message=str(e)
                    )

            # Prepare tasks
            custom_tests = mapping.get('custom_tests', [])
            if isinstance(custom_tests, str):
                try:
                    custom_tests = json.loads(custom_tests)
                except:
                    custom_tests = []
            
            test_tasks = [run_predefined_test(t) for t in enabled_tests]
            custom_tasks = [run_custom_test(ct) for ct in (custom_tests or [])]
            
            # Combine and execute all in parallel
            all_raw_results = await asyncio.gather(*test_tasks, *custom_tasks)
            predefined_results = [r for r in all_raw_results if r is not None]
            # --- PARALLEL TEST EXECUTION END ---
            
            # Get row count for info
            try:
                bq_row_count = await bigquery_service.get_row_count(full_table_name)
            except:
                bq_row_count = 0

            return MappingResult(
                mapping_id=mapping_id,
                mapping_info=MappingInfo(
                    source="SCD Validation",
                    target=f"{target_dataset}.{target_table}",
                    file_row_count=0,
                    table_row_count=bq_row_count
                ),
                predefined_results=predefined_results,
                ai_suggestions=[]
            )
            
        except Exception as e:
            logger.error(f"Error in process_scd for {mapping_id}: {str(e)}")
            return MappingResult(
                mapping_id=mapping_id,
                predefined_results=[],
                ai_suggestions=[],
                error=str(e)
            )

    async def process_scd_config_table(
        self,
        project_id: str,
        config_dataset: str,
        config_table: str
    ) -> Dict[str, Any]:
        """
        Process all SCD validations from a config table (Test3 Feature).
        """
        try:
            # Read SCD config table
            scd_configs = await bigquery_service.read_scd_config_table(
                project_id, config_dataset, config_table
            )
            
            if not scd_configs:
                raise ValueError("No SCD configurations found in config table")
            
            # Convert configs to mapping format for process_scd
            mappings = []
            for config in scd_configs:
                mapping = {
                    'mapping_id': config.get('config_id', f"{config['target_table']}_scd"),
                    'target_dataset': config['target_dataset'],
                    'target_table': config['target_table'],
                    'scd_type': config.get('scd_type', 'scd2'),
                    'primary_keys': config.get('primary_keys', []),
                    'surrogate_key': config.get('surrogate_key'),
                    'begin_date_column': config.get('begin_date_column'),
                    'end_date_column': config.get('end_date_column'),
                    'active_flag_column': config.get('active_flag_column'),
                    'custom_tests': config.get('custom_tests')
                }
                mappings.append(mapping)
            
            # Process SCD validations in parallel
            tasks = [self.process_scd(project_id, mapping) for mapping in mappings]
            logger.info(f"Gathering results for {len(tasks)} SCD mappings...")
            results = await asyncio.gather(*tasks)
            logger.info(f"Successfully gathered {len(results)} results")
            
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
            
            result_payload = {
                'summary': {
                    'total_mappings': len(results),
                    'total_tests': total_tests,
                    'passed': passed,
                    'failed': failed,
                    'errors': errors,
                    'total_suggestions': 0
                },
                'results_by_mapping': results
            }
            logger.info(f"Returning SCD config results: {result_payload['summary']}")
            return result_payload
            
        except Exception as e:
            logger.error(f"Error processing SCD config table: {str(e)}")
            raise

    
    async def process_config_table(
        self,
        project_id: str,
        config_dataset: str,
        config_table: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Process all mappings from a config table (Test1 Feature with Test3 Parallels).
        """
        try:
            # Read config table
            mappings = await bigquery_service.read_config_table(
                project_id, config_dataset, config_table, filters
            )
            
            if not mappings:
                raise ValueError("No active mappings found in config table")
            
            # Process each mapping (Parallelized)
            tasks = [self.process_mapping(project_id, mapping) for mapping in mappings]
            results = await asyncio.gather(*tasks)
            
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
