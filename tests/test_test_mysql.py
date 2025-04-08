import pytest
from testcontainers.mysql import MySqlContainer

from datacontract.data_contract import DataContract
from datacontract.model.exceptions import DataContractException
from datacontract.model.run import ResultEnum

# logging.basicConfig(level=logging.DEBUG, force=True)

mysql = MySqlContainer("mysql:8.0")


@pytest.fixture(scope="function", autouse=True)
def mysql_container(request):
    mysql.start()

    def remove_container():
        mysql.stop()

    request.addfinalizer(remove_container)


def test_test_mysql(mysql_container, monkeypatch):
    monkeypatch.setenv("DATACONTRACT_MYSQL_USERNAME", mysql.username)
    monkeypatch.setenv("DATACONTRACT_MYSQL_PASSWORD", mysql.password)
    _init_sql_mysql("fixtures/mysql/data/data.sql")

    datacontract_file = "fixtures/mysql/datacontract.yaml"
    data_contract_str = _setup_datacontract_mysql(datacontract_file)
    data_contract = DataContract(data_contract_str=data_contract_str)

    run = data_contract.test()

    print(run)
    assert run.result == "passed"
    assert all(check.result == ResultEnum.passed for check in run.checks)


def test_test_mysql_case_sensitive_table_name(mysql_container, monkeypatch):
    monkeypatch.setenv("DATACONTRACT_MYSQL_USERNAME", mysql.username)
    monkeypatch.setenv("DATACONTRACT_MYSQL_PASSWORD", mysql.password)
    _init_sql_mysql("fixtures/mysql/data/data_case_sensitive.sql")

    datacontract_file = "fixtures/mysql/datacontract_case_sensitive.yaml"
    data_contract_str = _setup_datacontract_mysql(datacontract_file)
    data_contract = DataContract(data_contract_str=data_contract_str)

    run = data_contract.test()

    print(run)
    assert run.result == "passed"
    assert all(check.result == ResultEnum.passed for check in run.checks)


def _setup_datacontract_mysql(file):
    with open(file) as data_contract_file:
        data_contract_str = data_contract_file.read()
    port = mysql.get_exposed_port(3306)
    data_contract_str = data_contract_str.replace("3306", port)
    return data_contract_str


def _init_sql_mysql(file_path):
    try:
        import pymysql
    except ImportError as e:
        raise DataContractException(
            type="schema",
            result="failed",
            name="pymysql missing",
            reason="Install pymysql to use MySQL for testing",
            engine="datacontract",
            original_exception=e,
        )

    connection = pymysql.connect(
        db=mysql.dbname or "test",  # PyMySQL uses 'db' instead of 'database'
        user=mysql.username,
        password=mysql.password,
        host=mysql.get_container_host_ip(),
        port=int(mysql.get_exposed_port(3306)),
    )
    cursor = connection.cursor()
    with open(file_path, "r") as sql_file:
        sql_commands = sql_file.read()
        for command in sql_commands.split(";"):
            if command.strip():
                cursor.execute(command)
    connection.commit()
    cursor.close()
    connection.close()
