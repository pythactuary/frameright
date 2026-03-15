"""Backend registry: detection and lazy loading of backend adapters."""

from __future__ import annotations

from typing import Any, Dict, Type

from .base import BackendAdapter

# Lazy-loaded singletons (one adapter instance per backend)
_BACKENDS: Dict[str, BackendAdapter] = {}

# Type string → backend module import path
_BACKEND_REGISTRY: Dict[str, str] = {
    "pandas": "proteusframe.backends.pandas_backend",
    "polars": "proteusframe.backends.polars_backend",
    "narwhals": "proteusframe.backends.narwhals_backend",
}


def _load_backend(name: str) -> BackendAdapter:
    """Import and instantiate a backend adapter by name."""
    if name in _BACKENDS:
        return _BACKENDS[name]

    module_path = _BACKEND_REGISTRY.get(name)
    if module_path is None:
        raise ValueError(
            f"Unknown backend '{name}'. Available: {sorted(_BACKEND_REGISTRY)}"
        )

    import importlib

    mod = importlib.import_module(module_path)

    # Convention: each backend module exposes a class named <Name>Backend
    cls_name = name.capitalize() + "Backend"
    adapter_cls: Type[BackendAdapter] = getattr(mod, cls_name)
    instance = adapter_cls()
    _BACKENDS[name] = instance
    return instance


def detect_backend(data: Any) -> BackendAdapter:
    """Auto-detect which backend adapter to use for *data*.

    Checks the type of *data* and returns the appropriate adapter.
    Uses string-based type checks to avoid importing optional libraries.

    Args:
        data: A DataFrame-like object (pd.DataFrame, pl.DataFrame, nw.DataFrame, etc.)

    Returns:
        The matching ``BackendAdapter`` instance.

    Raises:
        TypeError: If no known backend supports the given data type.
    """
    type_name = type(data).__module__ + "." + type(data).__qualname__

    # Narwhals (check first as it wraps other types)
    if type_name.startswith("narwhals."):
        return _load_backend("narwhals")

    # Pandas
    if type_name.startswith("pandas."):
        return _load_backend("pandas")

    # Polars
    if type_name.startswith("polars."):
        return _load_backend("polars")

    # cuDF (future)
    if type_name.startswith("cudf."):
        # When implemented, register "cudf" in _BACKEND_REGISTRY
        raise TypeError("cuDF backend is not yet implemented. Contributions welcome!")

    raise TypeError(
        f"No ProteusFrame backend for type '{type(data).__name__}'. "
        f"Supported: pandas.DataFrame, polars.DataFrame, polars.LazyFrame, narwhals.DataFrame."
    )


def get_backend(name: str) -> BackendAdapter:
    """Get a backend adapter by name.

    Args:
        name: One of 'pandas', 'polars', 'narwhals', etc.

    Returns:
        The matching ``BackendAdapter`` instance.

    Raises:
        ValueError: If the backend name is unknown.
    """
    return _load_backend(name)


def register_backend(name: str, module_path: str) -> None:
    """Register a custom backend adapter.

    This allows third-party libraries (e.g. cuDF) to register their
    backends without modifying ProteusFrame's source.

    Args:
        name: Short name for the backend (e.g. 'cudf').
        module_path: Dotted module path containing the adapter class
                     (e.g. 'mylib.backends.cudf_backend').
    """
    _BACKEND_REGISTRY[name] = module_path
    # Clear cached instance if it exists (allows re-registration)
    _BACKENDS.pop(name, None)
