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
    
    'table_exists': TestTemplate(
        test_id='table_exists',
        name='Table exists (smoke)',
        category='smoke',
        severity='HIGH',
        description='Verify the target table exists and is accessible',
        is_global=False,
        generate_sql=lambda config: f"SELECT TRUE FROM `{config['full_table_name']}` LIMIT 1"
    ),

    # --- SCD1 Tests ---
    'scd1_primary_key_null': TestTemplate(
        test_id='scd1_primary_key_null',
        name='Primary Key NOT NULL',
        category='completeness',
        severity='HIGH',
        description='Check composite primary key for NULL values',
        is_global=False,
        generate_sql=lambda config: (
            f"""
            SELECT * FROM `{config['full_table_name']}`
            WHERE ({' || '.join([f"SAFE_CAST({col} AS STRING)" for col in config['primary_keys']])}) IS NULL
            LIMIT 100
            """ if config.get('primary_keys') else None
        )
    ),

    'scd1_primary_key_unique': TestTemplate(
        test_id='scd1_primary_key_unique',
        name='Primary Key uniqueness',
        category='integrity',
        severity='HIGH',
        description='Ensure composite primary key uniqueness',
        is_global=False,
        generate_sql=lambda config: (
            f"""
            SELECT 
                ({' || '.join([f"IFNULL(SAFE_CAST({col} AS STRING), '')" for col in config['primary_keys']])}) as primary_key,
                COUNT(*) as duplicate_count
            FROM `{config['full_table_name']}`
            GROUP BY 1
            HAVING COUNT(*) > 1
            LIMIT 100
            """ if config.get('primary_keys') else None
        )
    ),

    # --- SCD2 Tests (11 + 4 Structural) ---
    'scd2_primary_key_null': TestTemplate(
        test_id='scd2_primary_key_null',
        name='Primary Key NOT NULL',
        category='completeness',
        severity='HIGH',
        description='Check SCD2 primary key for NULL values',
        is_global=False,
        generate_sql=lambda config: (
            f"""
            SELECT * FROM `{config['full_table_name']}`
            WHERE ({' || '.join([f"SAFE_CAST({col} AS STRING)" for col in config['primary_keys']])}) IS NULL
            LIMIT 100
            """ if config.get('primary_keys') else None
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

    'scd2_invalid_flag_combination': TestTemplate(
        test_id='scd2_invalid_flag_combination',
        name='No invalid current-row combinations',
        category='validity',
        severity='HIGH',
        description='Ensure active flag is only set for high-watermark end dates',
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
            SELECT {' , '.join(config['primary_keys'])}, {config['begin_date_column']}, COUNT(*)
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
            SELECT {' , '.join(config['primary_keys'])}, {config['end_date_column']}, COUNT(*)
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
                    *,
                    LEAD({config['begin_date_column']}) OVER (PARTITION BY {' , '.join(config['primary_keys'])} ORDER BY {config['begin_date_column']}) as next_begin
                FROM `{config['full_table_name']}`
            )
            SELECT * FROM ordered_history
            WHERE next_begin IS NOT NULL 
            AND DATE_ADD({config['end_date_column']}, INTERVAL 1 SECOND) <> next_begin
            LIMIT 100
            """
        )
    ),

    'scd2_no_record_after_current': TestTemplate(
        test_id='scd2_no_record_after_current',
        name='No record after current row',
        category='validity',
        severity='HIGH',
        description='Ensure no record exists after the high-watermark current row',
        is_global=False,
        generate_sql=lambda config: (
            f"""
            WITH history AS (
                SELECT 
                    *,
                    LEAD({config['begin_date_column']}) OVER (PARTITION BY {' , '.join(config['primary_keys'])} ORDER BY {config['begin_date_column']}) as next_begin
                FROM `{config['full_table_name']}`
            )
            SELECT * FROM history
            WHERE SAFE_CAST({config['active_flag_column']} AS STRING) IN ('true', 'TRUE', 'Y', '1')
            AND next_begin IS NOT NULL
            LIMIT 100
            """
        )
    ),

    # --- Structural Tests ---
    'surrogate_key_null': TestTemplate(
        test_id='surrogate_key_null',
        name='Surrogate key NOT NULL',
        category='completeness',
        severity='HIGH',
        description='Check surrogate key for NULL values',
        is_global=False,
        generate_sql=lambda config: f"SELECT * FROM `{config['full_table_name']}` WHERE {config['surrogate_key']} IS NULL LIMIT 100" if config.get('surrogate_key') else None
    ),

    'surrogate_key_unique': TestTemplate(
        test_id='surrogate_key_unique',
        name='Surrogate key uniqueness',
        category='integrity',
        severity='HIGH',
        description='Ensure surrogate key uniqueness',
        is_global=False,
        generate_sql=lambda config: (
            f"""
            SELECT {config['surrogate_key']}, COUNT(*) as duplicate_count
            FROM `{config['full_table_name']}`
            GROUP BY {config['surrogate_key']}
            HAVING COUNT(*) > 1
            LIMIT 100
            """ if config.get('surrogate_key') else None
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
