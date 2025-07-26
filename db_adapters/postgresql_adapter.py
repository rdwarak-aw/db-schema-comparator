import psycopg2
from db_adapters.base_db_adapter import BaseDBAdapter

class PostgreSQLAdapter(BaseDBAdapter):
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.conn = None

    def connect(self, dbconstr):
        try:
            self.conn = psycopg2.connect(
                host=dbconstr["server"],
                dbname=dbconstr["database"],
                user=dbconstr["username"],
                password=dbconstr["password"],
                port=dbconstr.get("port", 5432)
            )
            self.logger.info("Connected to PostgreSQL {host}-{dbname}")
            return self.conn
        except Exception as e:
            self.logger.exception(f"PostgreSQL connection error: {str(e)}")
            raise

    def extract_metadata(self) -> dict:
        cursor = self.conn.cursor()
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
                SELECT table_name, column_name, data_type, is_nullable, character_maximum_length
                FROM information_schema.columns
                WHERE table_schema = %s
                ORDER BY table_name, ordinal_position
            """, (schema,))
            for row in cursor.fetchall():
                tbl = f"{schema}.{row[0]}"
                result.setdefault(tbl, []).append({
                    "column": row[1],
                    "data_type": row[2],
                    "nullable": row[3],
                    "max_length": row[4]
                })
        self.logger.info("Extracted tables from PostgreSQL.")
        return result

    def extract_views(self, cursor, schemas):
        views = {}
        for schema in schemas:
            cursor.execute("""
                SELECT table_name, view_definition
                FROM information_schema.views
                WHERE table_schema = %s
            """, (schema,))
            for row in cursor.fetchall():
                views[f"{schema}.{row[0]}"] = row[1]
        self.logger.info("Extracted views from PostgreSQL.")
        return views

    def extract_routines(self, cursor, schemas):
        routines = {}
        for schema in schemas:
            cursor.execute("""
                SELECT routine_name, routine_type, routine_definition
                FROM information_schema.routines
                WHERE specific_schema = %s
            """, (schema,))
            for row in cursor.fetchall():
                routines[f"{schema}.{row[0]}"] = {
                    "type": row[1],
                    "definition": row[2]
                }
        self.logger.info("Extracted routines from PostgreSQL.")
        return routines

    def extract_constraints(self, cursor, schemas):
        constraints = {"primary_keys": {}, "foreign_keys": {}, "unique_constraints": {}}
        for schema in schemas:
            cursor.execute("""
                SELECT conname, contype, conrelid::regclass::text, pg_get_constraintdef(oid)
                FROM pg_constraint
                WHERE connamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s)
            """, (schema,))
            for row in cursor.fetchall():
                full_table = row[2]
                con_type = row[1]
                record = {"name": row[0], "definition": row[3]}
                if con_type == 'p':
                    constraints["primary_keys"].setdefault(full_table, []).append(record)
                elif con_type == 'u':
                    constraints["unique_constraints"].setdefault(full_table, []).append(record)
                elif con_type == 'f':
                    constraints["foreign_keys"].setdefault(full_table, []).append(record)
        self.logger.info("Extracted constraints from PostgreSQL.")
        return constraints

    def extract_indexes(self, cursor, schemas):
        indexes = {}
        for schema in schemas:
            cursor.execute("""
                SELECT tab.relname as table_name, idx.relname as index_name, a.attname as column_name
                FROM pg_class tab
                JOIN pg_index i ON tab.oid = i.indrelid
                JOIN pg_class idx ON idx.oid = i.indexrelid
                JOIN pg_attribute a ON a.attrelid = tab.oid AND a.attnum = ANY(i.indkey)
                JOIN pg_namespace ns ON ns.oid = tab.relnamespace
                WHERE ns.nspname = %s
            """, (schema,))
            for row in cursor.fetchall():
                key = f"{schema}.{row[0]}.{row[1]}"
                indexes.setdefault(key, []).append(row[2])
        self.logger.info("Extracted indexes from PostgreSQL.")
        return indexes

    def extract_triggers(self, cursor, schemas):
        triggers = {}
        for schema in schemas:
            cursor.execute("""
                SELECT event_object_table, trigger_name, action_statement
                FROM information_schema.triggers
                WHERE trigger_schema = %s
            """, (schema,))
            for row in cursor.fetchall():
                key = f"{schema}.{row[0]}.{row[1]}"
                triggers[key] = {"definition": row[2]}
        self.logger.info("Extracted triggers from PostgreSQL.")
        return triggers

    def close(self):
        if self.conn:
            self.conn.close()
            self.logger.info("PostgreSQL connection closed.")
