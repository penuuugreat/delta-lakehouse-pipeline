from deltalake import DeltaTable


def open_table(path: str):
    """Return the DeltaTable or None if it does not exist yet."""
    return DeltaTable(path) if DeltaTable.is_deltatable(path) else None
