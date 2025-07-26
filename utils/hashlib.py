import hashlib

def hash_definition(definition: str) -> str:
    return hashlib.sha256(definition.encode("utf-8")).hexdigest()

