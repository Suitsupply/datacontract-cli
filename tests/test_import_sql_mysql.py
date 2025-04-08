import yaml

from datacontract.data_contract import DataContract

sql_file_path = "fixtures/mysql/import/ddl.sql"


def test_import_sql_mysql():
    result = DataContract().import_from_source("sql", sql_file_path, dialect="mysql")

    expected = """
dataContractSpecification: 1.1.0
id: my-data-contract-id
info:
  title: My Data Contract
  version: 0.0.1
servers:
  mysql:
    type: mysql
models:
  my_table:
    type: table
    fields:
      field_primary_key:
        type: int
        primaryKey: true
        description: Primary key
        config:
          mysqlType: INT
      field_not_null:
        type: int
        required: true
        description: Not null
        config:
          mysqlType: INT
      field_char:
        type: string
        description: Fixed-length string
        maxLength: 10
        config:
          mysqlType: CHAR(10)
      field_varchar:
        type: string
        description: Variable-length string
        maxLength: 100
        config:
          mysqlType: VARCHAR(100)
      field_text:
        type: string
        description: Large variable-length string
        config:
          mysqlType: TEXT
      field_tinyint:
        type: int
        description: Integer (0-255)
        config:
          mysqlType: TINYINT
      field_smallint:
        type: int
        description: Integer (-32,768 to 32,767)
        config:
          mysqlType: SMALLINT
      field_int:
        type: int
        description: Integer (-2.1B to 2.1B)
        config:
          mysqlType: INT
      field_bigint:
        type: long
        description: Large integer (-9 quintillion to 9 quintillion)
        config:
          mysqlType: BIGINT
      field_decimal:
        type: decimal
        description: Fixed precision decimal
        precision: 10
        scale: 2
        config:
          mysqlType: DECIMAL(10, 2)
      field_float:
        type: float
        description: Approximate floating-point
        config:
          mysqlType: FLOAT
      field_real:
        type: float
        description: Smaller floating point (equivalent to FLOAT in MySQL)
        config:
          mysqlType: FLOAT
      field_date:
        type: date
        description: Date only (YYYY-MM-DD)
        config:
          mysqlType: DATE
      field_time:
        type: string
        description: Time only (HH:MM:SS)
        config:
          mysqlType: TIME
      field_datetime:
        type: timestamp_ntz
        description: Standard datetime
        config:
          mysqlType: DATETIME
      field_timestamp:
        type: timestamp_ntz
        description: Timestamp (auto-updating)
        config:
          mysqlType: DATETIME          
    """

    assert yaml.safe_load(result.to_yaml()) == yaml.safe_load(expected)
    # Disable linters so we don't get "missing description" warnings
    assert DataContract(data_contract_str=expected).lint(enabled_linters=set()).has_passed()
