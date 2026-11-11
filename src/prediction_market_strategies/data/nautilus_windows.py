"""Pinned v2 wheel path boundary: Python uses extended paths, Rust plain paths."""
from contextlib import contextmanager
import threading

_lock = threading.RLock()


def native_path(path):
    value = str(path)
    if value.startswith("\\\\?\\UNC\\"):
        return "\\\\" + value[8:]
    if value.startswith("\\\\?\\"):
        return value[4:]
    return value


@contextmanager
def catalog_paths():
    """Scoped compatibility shim; never patches the installed engine on disk."""
    import nautilus_trader.persistence as persistence
    with _lock:
        original = persistence.ParquetDataCatalog
        def catalog(path, *args, **kwargs):
            return original(native_path(path), *args, **kwargs)
        persistence.ParquetDataCatalog = catalog
        try:
            yield
        finally:
            persistence.ParquetDataCatalog = original
