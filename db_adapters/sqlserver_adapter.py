import pyodbc
from db_adapters.base_db_adapter import BaseDBAdapter

class SQLServerAdapter(BaseDBAdapter):
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.conn = None

    def connect(self, dbconstr):
        try:
            auth_type = dbconstr.get("auth_type", "sql").lower()
            server = dbconstr["server"]
            database = dbconstr["database"]
            timeout = dbconstr.get("timeout", 30)

            if auth_type == "windows":
                conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};"
                f"DATABASE={database};Trusted_Connection=yes;Timeout={timeout};"
                )
            else:
                username = dbconstr["username"]
                password = dbconstr["password"]
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};"
                    f"DATABASE={database};UID={username};PWD={password};Timeout={timeout};"
                )

            self.conn = pyodbc.connect(conn_str, timeout=dbconstr.get("timeout", 30))
            self.logger.info(f"Connected to SQL Server {server}-{database}")
            return self.conn
        except Exception as e:
            self.logger.exception(f"Database connection error {server}-{database}: {str(e)}")
            raise

    def extract_metadata(self) -> dict:
        cursor = self.conn.cursor()
        metadata = {}
        schemas = self.config["schemas_to_compare"]
        object_types = self.config["compare_objects"]

        if object_types.get("tables"):
            metadata["tables"] = self.extract_tables(cursor, schemas)
        if object_types.get("views"):
            metadata["views"] = self.extract_views(cursor, schemas)
        if object_types.get("stored_procedures"):
            metadata["stored_procedures"] = self.extract_routines(cursor, schemas, "P")
        if object_types.get("functions"):
            metadata["functions"] = self.extract_routines(cursor, schemas,"FN")
        if object_types.get("constraints"):
            metadata["constraints"] = self.extract_constraints(cursor, schemas)
        if object_types.get("indexes"):
            metadata["indexes"] = self.extract_indexes(cursor, schemas)
        if object_types.get("triggers"):
            metadata["triggers"] = self.extract_triggers(cursor, schemas)

        # ... continue for other types

        return metadata

    def extract_tables(self, cursor, schemas: list[str]) -> dict:
        tables = {}
        for schema in schemas:
            query = f'''
            SELECT t.name AS table_name, c.name AS column_name, c.column_id, ty.name AS data_type, c.max_length
            FROM sys.tables t
            JOIN sys.columns c ON t.object_id = c.object_id
            JOIN sys.types ty ON c.user_type_id = ty.user_type_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ?
            ORDER BY t.name, c.column_id'''
          
            cursor.execute(query, schema)
            for row in cursor.fetchall():
                tbl = f"{schema}.{row.table_name}"
                if tbl not in tables:
                    tables[tbl] = []
                tables[tbl].append({
                    "column": row.column_name,
                    "data_type": row.data_type,
                    "max_length": row.max_length
                })
        self.logger.info(f"Extracted tables for schemas: {schemas}")
        return tables

    def extract_views(self, cursor, schemas: list[str]) -> dict:
        views = {}
        for schema in schemas:
            query = '''
            SELECT s.name AS schema_name, v.name AS view_name, m.definition
            FROM sys.views v
            JOIN sys.sql_modules m ON v.object_id = m.object_id
            JOIN sys.schemas s ON v.schema_id = s.schema_id
            WHERE s.name = ?'''

            cursor.execute(query, schema)
            for row in cursor.fetchall():
                views[f"{row.schema_name}.{row.view_name}"] = row.definition
        self.logger.info(f"Extracted views for schemas: {schemas}")
        return views

    def extract_constraints(self, cursor, schemas: list[str]) -> dict:
        constraints = {
            "primary_keys": {},
            "foreign_keys": {},
            "unique_constraints": {}
        }

        for schema in schemas:
            # Primary Keys and Unique Constraints
            query_keys = '''
            SELECT 
                s.name AS schema_name,
                t.name AS table_name,
                kc.name AS constraint_name,
                kc.type AS constraint_type,
                c.name AS column_name
            FROM sys.key_constraints kc
            JOIN sys.tables t ON kc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.index_columns ic ON kc.unique_index_id = ic.index_id AND kc.parent_object_id = ic.object_id
            JOIN sys.columns c ON ic.column_id = c.column_id AND c.object_id = t.object_id
            WHERE s.name = ?
            '''

            cursor.execute(query_keys, schema)
            for row in cursor.fetchall():
                full_table = f"{schema}.{row.table_name}"
                entry = {
                    "constraint_name": row.constraint_name,
                    "column": row.column_name
                }
                if row.constraint_type == "PK":
                    constraints["primary_keys"].setdefault(full_table, []).append(entry)
                elif row.constraint_type == "UQ":
                    constraints["unique_constraints"].setdefault(full_table, []).append(entry)

            # Foreign Keys
            query_fk = '''
            SELECT 
                s.name AS schema_name,
                t.name AS table_name,
                fk.name AS fk_name,
                c.name AS column_name
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.columns c ON fkc.parent_column_id = c.column_id AND c.object_id = t.object_id
            WHERE s.name = ?
            '''

            cursor.execute(query_fk, schema)
            for row in cursor.fetchall():
                full_table = f"{schema}.{row.table_name}"
                entry = {
                    "constraint_name": row.fk_name,
                    "column": row.column_name
                }
                constraints["foreign_keys"].setdefault(full_table, []).append(entry)

        self.logger.info(f"Extracted constraints for schemas: {schemas}")
        return constraints

    def extract_indexes(self, cursor, schemas: list[str]) -> dict:
        indexes = {}
        for schema in schemas:
            query = '''
            SELECT 
                s.name AS schema_name,
                t.name AS table_name,
                i.name AS index_name,
                i.type_desc AS index_type,
                c.name AS column_name,
                ic.is_included_column
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.column_id = c.column_id AND ic.object_id = c.object_id
            JOIN sys.tables t ON i.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ? AND i.is_primary_key = 0 AND i.is_unique_constraint = 0
            ORDER BY t.name, i.name, ic.key_ordinal
            '''
            cursor.execute(query, schema)
            for row in cursor.fetchall():
                full_table = f"{schema}.{row.table_name}"
                key = f"{full_table}.{row.index_name}"
                entry = {
                    "column": row.column_name,
                    "index_type": row.index_type,
                    "included": bool(row.is_included_column)
                }
                indexes.setdefault(key, []).append(entry)

        self.logger.info(f"Extracted indexes for schemas: {schemas}")
        return indexes

    def extract_routines(self, cursor, schemas: list[str], routine_type) -> dict:
        routines = {}
        type_clause = "AND o.type = ?"
        for schema in schemas:
            query = f'''
            SELECT s.name AS schema_name, o.name AS routine_name, m.definition
            FROM sys.objects o
            JOIN sys.sql_modules m ON o.object_id = m.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = ? {type_clause}'''

            cursor.execute(query, schema, routine_type)
            for row in cursor.fetchall():
                routines[f"{row.schema_name}.{row.routine_name}"] = row.definition
        self.logger.info(f"Extracted routines for schemas: {schemas}")
        return routines

    def extract_triggers(self, cursor, schemas: list[str]) -> dict:
        triggers = {}
        for schema in schemas:
            query = '''
            SELECT 
                s.name AS schema_name,
                t.name AS table_name,
                tr.name AS trigger_name,
                m.definition,
                tr.is_disabled
            FROM sys.triggers tr
            JOIN sys.tables t ON tr.parent_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.sql_modules m ON tr.object_id = m.object_id
            WHERE s.name = ?
            '''
            cursor.execute(query, schema)
            for row in cursor.fetchall():
                key = f"{schema}.{row.table_name}.{row.trigger_name}"
                triggers[key] = {
                    "definition": row.definition,
                    "disabled": bool(row.is_disabled)
                }

        self.logger.info(f"Extracted triggers for schemas: {schemas}")
        return triggers

    def close(self):
        if self.conn:
            self.conn.close()
            self.logger.info("SQL Server connection closed.")

