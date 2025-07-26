from abc import ABC, abstractmethod

class BaseDBAdapter(ABC):
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    @abstractmethod
    def connect(self, dbconstr):
        pass

    @abstractmethod
    def extract_metadata(self) -> dict:
    
        # DRY: Shared logic in one place  
        """
        cursor = self.conn.cursor()
        metadata = {}
        schemas = self.config["schemas_to_compare"]
        object_types = self.config["compare_objects"]

        if object_types.get("tables"):
            metadata["tables"] = {}
            for schema in schemas:
                metadata["tables"].update(self.get_tables(cursor, schema))

        if object_types.get("views"):
            metadata["views"] = {}
            for schema in schemas:
                metadata["views"].update(self.get_views(cursor, schema))

        if object_types.get("stored_procedures"):
            metadata["stored_procedures"] = {}
            for schema in schemas:
                metadata["stored_procedures"].update(self.get_routines(cursor, schema, 'P'))

        if object_types.get("functions"):
            metadata["functions"] = {}
            for schema in schemas:
                metadata["functions"].update(self.get_routines(cursor, schema, 'FN'))

        self.logger.info(f"Extracted metadata for schemas: {schemas}")
        return metadata
        """
        pass

    @abstractmethod
    def extract_tables(self, cursor, schema: str) -> dict:
        pass

    @abstractmethod
    def extract_views(self, cursor, schema: str) -> dict:
        pass

    @abstractmethod
    def extract_routines(self, cursor, schema: str, routine_type: str) -> dict:
        pass
    
    @abstractmethod
    def close(self):
        pass
