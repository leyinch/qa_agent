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
        Process a single mapping with predefined tests and AI suggestions.
        """
        mapping_id = mapping.get('mapping_id', 'unknown')
        
        try:
            target_dataset = mapping['target_dataset']
            target_table = mapping['target_table']
            full_target_name = f"{project_id}.{target_dataset}.{target_table}"
            
            source_project = mapping.get('source_project')
            source_dataset = mapping.get('source_dataset')
            source_table = mapping.get('source_table')
            is_bq_source = bool(source_dataset and source_table)
            
            file_row_count = 0
            source_description = ""
            
            if is_bq_source:
                src_proj = source_project or project_id
                full_source_name = f"{src_proj}.{source_dataset}.{source_table}"
                source_description = full_source_name
                file_row_count = await bigquery_service.get_row_count(full_source_name)
            else:
                source_bucket = mapping.get('source_bucket')
                source_file_path = mapping.get('source_file_path')
                if not source_bucket or not source_file_path:
                     raise ValueError(f"Mapping {mapping_id} missing source info")
                matching_files = await gcs_service.resolve_pattern(source_bucket, source_file_path)
                actual_file_path = matching_files[0]
                source_description = f"gs://{source_bucket}/{actual_file_path}"
                file_row_count = await gcs_service.count_csv_rows(source_bucket, actual_file_path)

            bq_row_count = await bigquery_service.get_row_count(full_target_name)
            table_metadata = await bigquery_service.get_table_metadata(project_id, target_dataset, target_table)
            
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
            
            enabled_test_ids = mapping.get('enabled_test_ids', [])
            enabled_tests = get_enabled_tests(enabled_test_ids)
            
            predefined_results = []
            predefined_results.append(TestResult(
                test_id='row_count_match',
                test_name='Row Count Match',
                category='completeness',
                description=f"Source: {file_row_count} rows, Target: {bq_row_count} rows",
                status='PASS' if file_row_count == bq_row_count else 'FAIL',
                severity='HIGH',
                sql_query='',
                rows_affected=abs(file_row_count - bq_row_count),
                error_message=f"Row count mismatch" if file_row_count != bq_row_count else None
            ))
            
            for test in enabled_tests:
                if test.id == 'row_count_match': continue
                sql = test.generate_sql(test_config)
                if not sql: continue
                try:
                    rows = await bigquery_service.execute_query(sql)
                    row_count = len(rows)
                    predefined_results.append(TestResult(
                        test_id=test.id, test_name=test.name, category=test.category,
                        description=test.description, status='PASS' if row_count == 0 else 'FAIL',
                        severity=test.severity, sql_query=sql, rows_affected=row_count
                    ))
                except Exception as e:
                    predefined_results.append(TestResult(
                        test_id=test.id, test_name=test.name, category=test.category,
                        description=test.description, status='ERROR', severity=test.severity,
                        sql_query=sql, rows_affected=0, error_message=str(e)
                    ))
            
            # Custom tests from BigQuery (Colleague change)
            try:
                active_custom_tests = await bigquery_service.get_active_custom_tests(
                    project_id, target_dataset, target_table
                )
                for custom_test in active_custom_tests:
                    try:
                        sql = custom_test['sql_query']
                        rows = await bigquery_service.execute_query(sql)
                        row_count = len(rows)
                        predefined_results.append(TestResult(
                            test_id=f"custom_{custom_test.get('test_name', 'unknown')}",
                            test_name=f"[Custom] {custom_test.get('test_name', 'Custom Test')}",
                            category=custom_test.get('test_category', 'custom'),
                            description=custom_test.get('description', ''),
                            status='PASS' if row_count == 0 else 'FAIL',
                            severity=custom_test.get('severity', 'HIGH'),
                            sql_query=sql, rows_affected=row_count
                        ))
                    except Exception as e:
                        predefined_results.append(TestResult(
                            test_id=f"custom_err_{custom_test.get('test_name', 'unknown')}",
                            status='ERROR', error_message=str(e)
                        ))
            except Exception as e:
                logger.error(f"Failed to run custom tests: {e}")

            # AI suggestions
            ai_suggestions = []
            if mapping.get('auto_suggest', True):
                try:
                    source_sample = await bigquery_service.get_sample_data(full_source_name, 5) if is_bq_source else await gcs_service.sample_csv_data(source_bucket, actual_file_path, 5)
                    bq_sample = await bigquery_service.get_sample_data(full_target_name, 5)
                    suggestions = await vertex_ai_service.generate_test_suggestions(
                        mapping_id=mapping_id, source_info=source_description,
                        target_table=full_target_name, bq_schema=table_metadata['schema'],
                        gcs_sample=source_sample, bq_sample=bq_sample,
                        existing_tests=[t.name for t in enabled_tests]
                    )
                    ai_suggestions = [AISuggestion(**s) for s in suggestions]
                except Exception as e:
                    logger.error(f"AI suggestions failed: {str(e)}")
            
            return MappingResult(
                mapping_id=mapping_id,
                mapping_info=MappingInfo(source=source_description, target=full_target_name, file_row_count=file_row_count, table_row_count=bq_row_count),
                predefined_results=predefined_results, ai_suggestions=ai_suggestions
            )
        except Exception as e:
            logger.error(f"Error processing mapping {mapping_id}: {str(e)}")
            return MappingResult(mapping_id=mapping_id, predefined_results=[], ai_suggestions=[], error=str(e))

    async def process_scd(self, project_id: str, mapping: Dict[str, Any]) -> MappingResult:
        """Process SCD validation (Our feature)."""
        mapping_id = mapping.get('mapping_id', f"{mapping.get('target_table', 'unknown')}_scd")
        try:
            target_dataset = mapping['target_dataset']
            target_table = mapping['target_table']
            scd_type = mapping.get('scd_type', 'scd2')
            full_table_name = f"{project_id}.{target_dataset}.{target_table}"
            
            test_config = {
                'full_table_name': full_table_name,
                'primary_keys': mapping.get('primary_keys', []),
                'surrogate_key': mapping.get('surrogate_key'),
                'begin_date_column': mapping.get('begin_date_column', 'DWBeginEffDateTime'),
                'end_date_column': mapping.get('end_date_column', 'DWEndEffDateTime'),
                'active_flag_column': mapping.get('active_flag_column', 'DWCurrentRowFlag')
            }
            
            enabled_test_ids = mapping.get('enabled_test_ids', [])
            if not enabled_test_ids:
                enabled_test_ids.append('table_exists')
                if test_config['surrogate_key']:
                    enabled_test_ids.extend(['surrogate_key_null', 'surrogate_key_unique'])
                if scd_type == 'scd1':
                    enabled_test_ids.extend(['scd1_primary_key_null', 'scd1_primary_key_unique'])
                elif scd_type == 'scd2':
                    enabled_test_ids.extend([
                        'scd2_primary_key_null', 'scd2_begin_date_null', 'scd2_end_date_null', 'scd2_flag_null',
                        'scd2_one_current_row', 'scd2_current_date_check', 'scd2_invalid_flag_combination',
                        'scd2_date_order', 'scd2_unique_begin_date', 'scd2_unique_end_date',
                        'scd2_continuity', 'scd2_no_record_after_current'
                    ])
            
            enabled_tests = get_enabled_tests(enabled_test_ids)
            predefined_results = []
            for test in enabled_tests:
                sql = test.generate_sql(test_config)
                if not sql: continue
                try:
                    rows = await bigquery_service.execute_query(sql)
                    row_count = len(rows)
                    predefined_results.append(TestResult(
                        test_id=test.id, test_name=test.name, category=test.category,
                        description=test.description, status=('PASS' if row_count > 0 else 'FAIL') if test.category == 'smoke' else ('PASS' if row_count == 0 else 'FAIL'),
                        severity=test.severity, sql_query=sql, rows_affected=row_count,
                        sample_data=rows[:10] if row_count > 0 else None
                    ))
                except Exception as e:
                    predefined_results.append(TestResult(test_id=test.id, status='ERROR', error_message=str(e)))
            
            return MappingResult(
                mapping_id=mapping_id,
                mapping_info=MappingInfo(source="SCD Validation", target=f"{target_dataset}.{target_table}", file_row_count=0, table_row_count=0),
                predefined_results=predefined_results, ai_suggestions=[]
            )
        except Exception as e:
            return MappingResult(mapping_id=mapping_id, predefined_results=[], ai_suggestions=[], error=str(e))

    async def process_scd_config_table(self, project_id: str, config_dataset: str, config_table: str) -> Dict[str, Any]:
        """Process multiple SCD validations (Our feature)."""
        configs = await bigquery_service.read_scd_config_table(project_id, config_dataset, config_table)
        tasks = [self.process_scd(project_id, {
            'mapping_id': c.get('config_id', f"{c['target_table']}_scd"),
            'target_dataset': c['target_dataset'], 'target_table': c['target_table'],
            'scd_type': c.get('scd_type', 'scd2'), 'primary_keys': c.get('primary_keys', []),
            'surrogate_key': c.get('surrogate_key'), 'begin_date_column': c.get('begin_date_column'),
            'end_date_column': c.get('end_date_column'), 'active_flag_column': c.get('active_flag_column'),
            'custom_tests': c.get('custom_tests')
        }) for c in configs]
        results = await asyncio.gather(*tasks)
        return {
            'summary': {'total_mappings': len(results), 'passed': sum(len([t for t in r.predefined_results if t.status == 'PASS']) for r in results)},
            'results_by_mapping': results
        }

    async def process_config_table(self, project_id: str, config_dataset: str, config_table: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Process mappings from config table (Colleague change)."""
        mappings = await bigquery_service.read_config_table(project_id, config_dataset, config_table, filters)
        results = [await self.process_mapping(project_id, m) for m in mappings]
        return {
            'summary': {'total_mappings': len(results), 'total_tests': sum(len(r.predefined_results) for r in results)},
            'results_by_mapping': results
        }

    async def process_schema_validation(self, project_id: str, datasets: List[str], erd_description: str) -> Dict[str, Any]:
        """AI Schema Validation (Colleague change)."""
        all_schemas = {}
        for ds in datasets:
            tables = await bigquery_service.get_tables_in_dataset(project_id, ds)
            for t in tables:
                meta = await bigquery_service.get_table_metadata(project_id, ds, t)
                all_schemas[f"{project_id}.{ds}.{t}"] = meta['schema']
        findings = await vertex_ai_service.validate_schema(erd_description, all_schemas)
        results = [TestResult(test_name=f.get('test_name', 'Schema Check'), category=f.get('test_category', 'schema_validation'), status=f.get('status', 'INFO'), severity=f.get('severity', 'MEDIUM'), description=f.get('reasoning', ''), sql_query=f.get('sql_query', ''), rows_affected=0) for f in findings]
        return {'summary': {'total_tables': len(all_schemas), 'total_issues': len(results)}, 'predefined_results': results, 'ai_suggestions': []}

test_executor = TestExecutor()
