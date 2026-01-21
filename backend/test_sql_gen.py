from typing import Dict, List, Callable, Optional

class TestTemplate:
    def __init__(self, test_id, name, category, severity, description, is_global, generate_sql):
        self.id = test_id
        self.name = name
        self.category = category
        self.severity = severity
        self.description = description
        self.is_global = is_global
        self.generate_sql = generate_sql

PREDEFINED_TESTS = {
    'scd_primary_key_null': TestTemplate(
        test_id='scd_primary_key_null',
        name='Primary Key NOT NULL',
        category='completeness',
        severity='HIGH',
        description='Check composite primary key for NULL values',
        is_global=False,
        generate_sql=lambda config: (
            f"SELECT * FROM `{config['full_table_name']}` WHERE {' OR '.join([f'{col} IS NULL' for col in config['primary_keys']])} LIMIT 100"
            if config.get('primary_keys') else None
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
            f"SELECT * FROM `{config['full_table_name']}` "
            f"WHERE (SAFE_CAST({config['active_flag_column']} AS STRING) IN ('true', 'TRUE', 'Y', '1') AND CAST({config['end_date_column']} AS STRING) NOT LIKE '2099-12-31%') "
            f"OR (SAFE_CAST({config['active_flag_column']} AS STRING) NOT IN ('true', 'TRUE', 'Y', '1') AND CAST({config['end_date_column']} AS STRING) LIKE '2099-12-31%') LIMIT 100"
        )
    )
}

def test_sql_generation():
    config = {
        'full_table_name': 'project.dataset.table',
        'primary_keys': ['ID', 'NAME'],
        'active_flag_column': 'IS_CURRENT',
        'end_date_column': 'END_DT'
    }
    
    sql_null = PREDEFINED_TESTS['scd_primary_key_null'].generate_sql(config)
    print(f"SQL Null: {sql_null}")
    
    sql_flag = PREDEFINED_TESTS['scd2_invalid_flag_combination'].generate_sql(config)
    print(f"SQL Flag: {sql_flag}")

    assert "ID IS NULL OR NAME IS NULL" in sql_null
    assert "IS_CURRENT" in sql_flag
    assert "END_DT" in sql_flag
    print("SQL generation logic is CORRECT!")

if __name__ == "__main__":
    test_sql_generation()
