import mysql.connector
from db_adapters.base_db_adapter import BaseDBAdapter

class MySQLAdapter(BaseDBAdapter):
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.conn = None

    def connect(self, dbconstr):
        try:
            self.conn = mysql.connector.connect(
                host=dbconstr["server"],
                database=dbconstr["database"],
                user=dbconstr["username"],
                password=dbconstr["password"],
                port=dbconstr.get("port", 3306)
            )
            self.logger.info("Connected to MySQL {host}-{database}")
            return self.conn
        except Exception as e:
            self.logger.exception(f"MySQL connection error: {str(e)}")
            raise

    def extract_metadata(self) -> dict:
        cursor = self.conn.cursor(dictionary=True)
        metadata = {}
        schemas = self.config["schemas_to_compare"]
        types = self.config["compare_objects"]

        if types.get("tables"):
            metadata["tables"] = self.extract_tables(cursor, schemas)
        if types.get("views"):
            metadata["views"] = self.extract_views(cursor, schemas)
        if types.get("stored_procedures") or types.get("functions"):
            metadata["routines"] = self.extract_routines(cursor, schemas)
        if types.get("constraints"):
            metadata["constraints"] = self.extract_constraints(cursor, schemas)
        if types.get("indexes"):
            metadata["indexes"] = self.extract_indexes(cursor, schemas)
        if types.get("triggers"):
            metadata["triggers"] = self.extract_triggers(cursor, schemas)

        return metadata

    def extract_tables(self, cursor, schemas):
        result = {}
        for schema in schemas:
            cursor.execute("""
                SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s
                ORDER BY TABLE_NAME, ORDINAL_POSITION
            """, (schema,))
            for row in cursor.fetchall():
                tbl = f"{schema}.{row['TABLE_NAME']}"
                result.setdefault(tbl, []).append({
                    "column": row["COLUMN_NAME"],
                    "data_type": row["DATA_TYPE"],
                    "nullable": row["IS_NULLABLE"],
                    "max_length": row["CHARACTER_MAXIMUM_LENGTH"]
                })
        self.logger.info(f"Extracted tables from MySQL: {schemas}")
        return result

    def extract_views(self, cursor, schemas):
        views = {}
        for schema in schemas:
            cursor.execute("""
                SELECT TABLE_NAME, VIEW_DEFINITION
                FROM INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_SCHEMA = %s
            """, (schema,))
            for row in cursor.fetchall():
                views[f"{schema}.{row['TABLE_NAME']}"] = row["VIEW_DEFINITION"]
        self.logger.info("Extracted views from MySQL.")
        return views

    def extract_routines(self, cursor, schemas):
        routines = {}
        for schema in schemas:
            cursor.execute("""
                SELECT ROUTINE_NAME, ROUTINE_TYPE, ROUTINE_DEFINITION
                FROM INFORMATION_SCHEMA.ROUTINES
                WHERE ROUTINE_SCHEMA = %s
            """, (schema,))
            for row in cursor.fetchall():
                routines[f"{schema}.{row['ROUTINE_NAME']}"] = {
                    "type": row["ROUTINE_TYPE"],
                    "definition": row["ROUTINE_DEFINITION"]
                }
        self.logger.info("Extracted routines from MySQL.")
        return routines

    def extract_constraints(self, cursor, schemas):
        constraints = {"primary_keys": {}, "foreign_keys": {}, "unique_constraints": {}}
        for schema in schemas:
            cursor.execute("""
                SELECT TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                WHERE CONSTRAINT_TYPE IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE') AND TABLE_SCHEMA = %s
            """, (schema,))
            for row in cursor.fetchall():
                full_table = f"{schema}.{row['TABLE_NAME']}"
                kind = row["CONSTRAINT_TYPE"].lower().replace(" ", "_")
                constraints.setdefault(kind, {}).setdefault(full_table, []).append(row["CONSTRAINT_NAME"])
        self.logger.info("Extracted constraints from MySQL.")
        return constraints

    def extract_indexes(self, cursor, schemas):
        indexes = {}
        for schema in schemas:
            cursor.execute("""
                SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, NON_UNIQUE
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = %s
            """, (schema,))
            for row in cursor.fetchall():
                key = f"{schema}.{row['TABLE_NAME']}.{row['INDEX_NAME']}"
                indexes.setdefault(key, []).append({
                    "column": row["COLUMN_NAME"],
                    "non_unique": bool(row["NON_UNIQUE"])
                })
        self.logger.info("Extracted indexes from MySQL.")
        return indexes

    def extract_triggers(self, cursor, schemas):
        triggers = {}
        for schema in schemas:
            cursor.execute("""
                SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_STATEMENT
                FROM INFORMATION_SCHEMA.TRIGGERS
                WHERE TRIGGER_SCHEMA = %s
            """, (schema,))
            for row in cursor.fetchall():
                key = f"{schema}.{row['EVENT_OBJECT_TABLE']}.{row['TRIGGER_NAME']}"
                triggers[key] = {
                    "definition": row["ACTION_STATEMENT"]
                }
        self.logger.info("Extracted triggers from MySQL.")
        return triggers

    def close(self):
        if self.conn:
            self.conn.close()
            self.logger.info("MySQL connection closed.")
