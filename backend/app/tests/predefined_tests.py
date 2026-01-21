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
    'table_exists': TestTemplate(
        test_id='table_exists',
        name='Table exists (smoke)',
        category='smoke',
        severity='HIGH',
        description='Check if the target table exists and has rows',
        is_global=True,
        generate_sql=lambda config: f"SELECT * FROM `{config['full_table_name']}` LIMIT 1"
    ),

    'surrogate_key_null': TestTemplate(
        test_id='surrogate_key_null',
        name='Surrogate key NOT NULL',
        category='completeness',
        severity='HIGH',
        description='Ensure surrogate key column has no NULLs',
        is_global=False,
        generate_sql=lambda config: (
            f"SELECT * FROM `{config['full_table_name']}` WHERE {config['surrogate_key']} IS NULL LIMIT 100"
            if config.get('surrogate_key') else None
        )
    ),

    'surrogate_key_unique': TestTemplate(
        test_id='surrogate_key_unique',
        name='Surrogate key uniqueness',
        category='integrity',
        severity='HIGH',
        description='Ensure surrogate key column has no duplicates',
        is_global=False,
        generate_sql=lambda config: (
            f"SELECT {config['surrogate_key']}, COUNT(*) FROM `{config['full_table_name']}` GROUP BY 1 HAVING COUNT(*) > 1 LIMIT 100"
            if config.get('surrogate_key') else None
        )
    ),

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

    # --- SCD Tests ---
    'scd_primary_key_null': TestTemplate(
        test_id='scd_primary_key_null',
        name='Primary Key NOT NULL',
        category='completeness',
        severity='HIGH',
        description='Check composite primary key for NULL values',
        is_global=False,
        generate_sql=lambda config: (
            f"""
            SELECT * FROM `{config['full_table_name']}`
            WHERE {' OR '.join([f"{col} IS NULL" for col in config['primary_keys']])}
            LIMIT 100
            """ if config.get('primary_keys') else None
        )
    ),

    'scd_primary_key_unique': TestTemplate(
        test_id='scd_primary_key_unique',
        name='Primary Key uniqueness',
        category='integrity',
        severity='HIGH',
        description='Check for duplicate primary keys (for SCD1) or non-unique keys in current state',
        is_global=False,
        generate_sql=lambda config: (
            f"""
            SELECT {', '.join(config['primary_keys'])}, COUNT(*) as duplicate_count
            FROM `{config['full_table_name']}`
            GROUP BY {', '.join(config['primary_keys'])}
            HAVING COUNT(*) > 1
            LIMIT 100
            """ if config.get('primary_keys') else None
        )
    ),

    'scd2_invalid_flag_combination': TestTemplate(
        test_id='scd2_invalid_flag_combination',
        name='Invalid active flag/date combination',
        category='integrity',
        severity='HIGH',
        description='Ensure flag matches end date logic',
        is_global=False,
        generate_sql=lambda config: (
            f"""
            SELECT * FROM `{config['full_table_name']}`
            WHERE (SAFE_CAST({config['active_flag_column']} AS STRING) IN ('true', 'TRUE', 'Y', '1') AND CAST({config['end_date_column']} AS STRING) NOT LIKE '2099-12-31%')
            OR (SAFE_CAST({config['active_flag_column']} AS STRING) NOT IN ('true', 'TRUE', 'Y', '1') AND CAST({config['end_date_column']} AS STRING) LIKE '2099-12-31%')
            LIMIT 100
            """
        )
    ),

    'scd2_no_record_after_current': TestTemplate(
        test_id='scd2_no_record_after_current',
        name='No records after current record',
        category='validity',
        severity='HIGH',
        description='Ensure no history exists with begin date after current record end date',
        is_global=False,
        generate_sql=lambda config: (
            f"""
            SELECT * FROM `{config['full_table_name']}`
            WHERE {config['begin_date_column']} > (SELECT MAX({config['begin_date_column']}) FROM `{config['full_table_name']}` WHERE SAFE_CAST({config['active_flag_column']} AS STRING) IN ('true', 'TRUE', 'Y', '1'))
            AND SAFE_CAST({config['active_flag_column']} AS STRING) NOT IN ('true', 'TRUE', 'Y', '1')
            LIMIT 100
            """
        )
    ),

    'scd2_begin_date_null': TestTemplate(
        test_id='scd2_begin_date_null',
        name='Begin effective datetime NOT NULL',
        category='completeness',
        severity='HIGH',
        description='Check SCD2 begin date for NULL values',
        is_global=False,
        generate_sql=lambda config: f"SELECT * FROM `{config['full_table_name']}` WHERE {config['begin_date_column']} IS NULL LIMIT 100"
    ),

    'scd2_end_date_null': TestTemplate(
        test_id='scd2_end_date_null',
        name='End effective datetime NOT NULL',
        category='completeness',
        severity='HIGH',
        description='Check SCD2 end date for NULL values',
        is_global=False,
        generate_sql=lambda config: f"SELECT * FROM `{config['full_table_name']}` WHERE {config['end_date_column']} IS NULL LIMIT 100"
    ),

    'scd2_flag_null': TestTemplate(
        test_id='scd2_flag_null',
        name='Current row flag NOT NULL',
        category='completeness',
        severity='HIGH',
        description='Check SCD2 active flag for NULL values',
        is_global=False,
        generate_sql=lambda config: f"SELECT * FROM `{config['full_table_name']}` WHERE {config['active_flag_column']} IS NULL LIMIT 100"
    ),

    'scd2_one_current_row': TestTemplate(
        test_id='scd2_one_current_row',
        name='One current row per Primary Key',
        category='integrity',
        severity='HIGH',
        description='Ensure exactly one active record per primary key',
        is_global=False,
        generate_sql=lambda config: (
            f"""
            SELECT {' , '.join(config['primary_keys'])}, COUNTIF(SAFE_CAST({config['active_flag_column']} AS STRING) IN ('true', 'TRUE', 'Y', '1')) as active_count
            FROM `{config['full_table_name']}`
            GROUP BY {' , '.join(config['primary_keys'])}
            HAVING active_count <> 1
            LIMIT 100
            """
        )
    ),

    'scd2_current_date_check': TestTemplate(
        test_id='scd2_current_date_check',
        name='Current rows end on 2099-12-31',
        category='validity',
        severity='HIGH',
        description='Ensure active rows have the high-watermark end date',
        is_global=False,
        generate_sql=lambda config: (
            f"""
            SELECT * FROM `{config['full_table_name']}`
            WHERE SAFE_CAST({config['active_flag_column']} AS STRING) IN ('true', 'TRUE', 'Y', '1')
            AND CAST({config['end_date_column']} AS STRING) NOT LIKE '2099-12-31%'
            LIMIT 100
            """
        )
    ),

    'scd2_date_order': TestTemplate(
        test_id='scd2_date_order',
        name='Begin < End datetime',
        category='validity',
        severity='HIGH',
        description='Ensure BeginEffDateTime < EndEffDateTime',
        is_global=False,
        generate_sql=lambda config: f"SELECT * FROM `{config['full_table_name']}` WHERE {config['begin_date_column']} >= {config['end_date_column']} LIMIT 100"
    ),

    'scd2_unique_begin_date': TestTemplate(
        test_id='scd2_unique_begin_date',
        name='Unique begin datetime per Primary Key',
        category='integrity',
        severity='HIGH',
        description='Ensure no primary key has multiple records starting at the same time',
        is_global=False,
        generate_sql=lambda config: (
            f"""
            SELECT {' , '.join(config['primary_keys'])}, {config['begin_date_column']}, COUNT(*) as duplicate_count
            FROM `{config['full_table_name']}`
            GROUP BY {' , '.join(config['primary_keys'])}, {config['begin_date_column']}
            HAVING COUNT(*) > 1
            LIMIT 100
            """
        )
    ),

    'scd2_unique_end_date': TestTemplate(
        test_id='scd2_unique_end_date',
        name='Unique end datetime per Primary Key',
        category='integrity',
        severity='HIGH',
        description='Ensure no primary key has multiple records ending at the same time',
        is_global=False,
        generate_sql=lambda config: (
            f"""
            SELECT {' , '.join(config['primary_keys'])}, {config['end_date_column']}, COUNT(*) as duplicate_count
            FROM `{config['full_table_name']}`
            GROUP BY {' , '.join(config['primary_keys'])}, {config['end_date_column']}
            HAVING COUNT(*) > 1
            LIMIT 100
            """
        )
    ),

    'scd2_continuity': TestTemplate(
        test_id='scd2_continuity',
        name='Continuous history (no gaps)',
        category='validity',
        severity='HIGH',
        description='Check for gaps or overlaps in SCD history',
        is_global=False,
        generate_sql=lambda config: (
            f"""
            WITH ordered_history AS (
                SELECT 
                    {', '.join(config['primary_keys'])},
                    {config['begin_date_column']},
                    {config['end_date_column']},
                    LEAD({config['begin_date_column']}) OVER (PARTITION BY {', '.join(config['primary_keys'])} ORDER BY {config['begin_date_column']}) as next_begin
                FROM `{config['full_table_name']}`
            )
            SELECT * FROM ordered_history
            WHERE next_begin IS NOT NULL 
            AND {config['end_date_column']} <> next_begin
            LIMIT 100
            """
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
    """
    if not enabled_test_ids:
        return [test for test in PREDEFINED_TESTS.values() if test.is_global]
    
    return [
        PREDEFINED_TESTS[test_id]
        for test_id in enabled_test_ids
        if test_id in PREDEFINED_TESTS
    ]
