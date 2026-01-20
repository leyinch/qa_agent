"""Predefined test templates for data quality validation."""
from typing import Dict, List, Callable, Optional


class TestTemplate:
    """Template for a predefined test."""
    
    def __init__(
        self,
        test_id: str,
        name: str,
        category: str,
        severity: str,
        description: str,
        is_global: bool,
        generate_sql: Callable[[Dict], Optional[str]]
    ):
        self.id = test_id
        self.name = name
        self.category = category
        self.severity = severity
        self.description = description
        self.is_global = is_global
        self.generate_sql = generate_sql


# Predefined test templates
PREDEFINED_TESTS = {
    'row_count_match': TestTemplate(
        test_id='row_count_match',
        name='Row Count Match',
        category='completeness',
        severity='HIGH',
        description='Verify source and target row counts match',
        is_global=True,
        generate_sql=lambda config: None  # Handled programmatically
    ),
    
    'no_nulls_required': TestTemplate(
        test_id='no_nulls_required',
        name='No NULLs in Required Fields',
        category='completeness',
        severity='HIGH',
        description='Check required columns have no NULL values',
        is_global=True,
        generate_sql=lambda config: (
            f"""
            SELECT * FROM `{config['full_table_name']}`
            WHERE {' OR '.join(f"{col} IS NULL" for col in config.get('required_columns', []))}
            LIMIT 100
            """ if config.get('required_columns') else None
        )
    ),
    
    'no_duplicates_pk': TestTemplate(
        test_id='no_duplicates_pk',
        name='No Duplicate Primary Keys',
        category='integrity',
        severity='HIGH',
        description='Ensure primary key uniqueness',
        is_global=True,
        generate_sql=lambda config: (
            f"""
            SELECT {', '.join(config['primary_key_columns'])}, COUNT(*) as duplicate_count
            FROM `{config['full_table_name']}`
            GROUP BY {', '.join(config['primary_key_columns'])}
            HAVING COUNT(*) > 1
            """ if config.get('primary_key_columns') else None
        )
    ),
    
    'referential_integrity': TestTemplate(
        test_id='referential_integrity',
        name='Referential Integrity',
        category='integrity',
        severity='HIGH',
        description='Validate foreign key relationships',
        is_global=False,
        generate_sql=lambda config: (
            ' UNION ALL '.join([
                f"""
                SELECT 
                    '{fk_col}' as fk_column, 
                    {fk_col} as invalid_value,
                    COUNT(*) as occurrence_count
                FROM `{config['full_table_name']}` t
                WHERE {fk_col} IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM `{ref['table']}` r
                    WHERE r.{ref['column']} = t.{fk_col}
                )
                GROUP BY {fk_col}
                """
                for fk_col, ref in config.get('foreign_key_checks', {}).items()
            ]) if config.get('foreign_key_checks') else None
        )
    ),
    
    'numeric_range': TestTemplate(
        test_id='numeric_range',
        name='Numeric Range Validation',
        category='quality',
        severity='MEDIUM',
        description='Check numeric values are within expected ranges',
        is_global=False,
        generate_sql=lambda config: (
            f"""
            SELECT * FROM `{config['full_table_name']}`
            WHERE {' OR '.join(
                f"({col} < {range_val['min']} OR {col} > {range_val['max']})"
                for col, range_val in config.get('numeric_range_checks', {}).items()
            )}
            LIMIT 100
            """ if config.get('numeric_range_checks') else None
        )
    ),
    
    'date_range': TestTemplate(
        test_id='date_range',
        name='Date Range Validation',
        category='quality',
        severity='MEDIUM',
        description='Validate dates are within expected range',
        is_global=False,
        generate_sql=lambda config: (
            f"""
            SELECT * FROM `{config['full_table_name']}`
            WHERE {' OR '.join(
                f"({col} < '{range_val['min_date']}' OR {col} > '{range_val['max_date']}')"
                for col, range_val in config.get('date_range_checks', {}).items()
            )}
            LIMIT 100
            """ if config.get('date_range_checks') else None
        )
    ),
    
    'pattern_validation': TestTemplate(
        test_id='pattern_validation',
        name='Pattern Validation',
        category='quality',
        severity='MEDIUM',
        description='Check string patterns (email, phone, etc.)',
        is_global=False,
        generate_sql=lambda config: (
            f"""
            SELECT * FROM `{config['full_table_name']}`
            WHERE {' OR '.join(
                f"NOT REGEXP_CONTAINS(CAST({col} AS STRING), r'{pattern}')"
                for col, pattern in config.get('pattern_checks', {}).items()
            )}
            LIMIT 100
            """ if config.get('pattern_checks') else None
        )
    ),
    
    'outlier_detection': TestTemplate(
        test_id='outlier_detection',
        name='Statistical Outlier Detection',
        category='statistical',
        severity='LOW',
        description='Detect statistical outliers using standard deviation',
        is_global=False,
        generate_sql=lambda config: (
            f"""
            WITH stats AS (
                SELECT 
                    AVG({config['outlier_columns'][0]}) as mean,
                    STDDEV({config['outlier_columns'][0]}) as stddev
                FROM `{config['full_table_name']}`
                WHERE {config['outlier_columns'][0]} IS NOT NULL
            )
            SELECT t.* 
            FROM `{config['full_table_name']}` t, stats
            WHERE ABS(t.{config['outlier_columns'][0]} - stats.mean) > 3 * stats.stddev
            LIMIT 100
            """ if config.get('outlier_columns') else None
        )
    )
}


def get_enabled_tests(enabled_test_ids: Optional[List[str]] = None) -> List[TestTemplate]:
    """
    Get enabled test templates.
    
    Args:
        enabled_test_ids: List of test IDs to enable. If None, returns all global tests.
        
    Returns:
        List of enabled test templates
    """
    if not enabled_test_ids:
        # Return all global tests by default
        return [test for test in PREDEFINED_TESTS.values() if test.is_global]
    
    return [
        PREDEFINED_TESTS[test_id]
        for test_id in enabled_test_ids
        if test_id in PREDEFINED_TESTS
    ]
