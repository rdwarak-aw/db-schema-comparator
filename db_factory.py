from db_adapters.sqlserver_adapter import SQLServerAdapter
from db_adapters.mysql_adapter import MySQLAdapter
from db_adapters.postgresql_adapter import PostgreSQLAdapter


def get_db_adapter(db_type: str, config, logger):
    db_type = db_type.lower()
    if db_type == "sqlserver":
        return SQLServerAdapter(config, logger)
    elif db_type == "mysql":
        return MySQLAdapter(config, logger)
    elif db_type == "postgresql":
        return PostgreSQLAdapter(config, logger)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")