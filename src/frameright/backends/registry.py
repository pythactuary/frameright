"""Backend registry: lazy loading of backend adapters."""

from __future__ import annotations

from typing import Dict, Type

from .base import BackendAdapter

# Lazy-loaded singletons (one adapter instance per backend)
_BACKENDS: Dict[str, BackendAdapter] = {}

# Type string → backend module import path
_BACKEND_REGISTRY: Dict[str, str] = {
    "pandas": "frameright.backends.pandas_backend",
    "polars": "frameright.backends.polars_eager_backend",
    "polars_lazy": "frameright.backends.polars_lazy_backend",
    "narwhals": "frameright.backends.narwhals_backend",
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

    # Map backend names to class names
    class_name_map = {
        "pandas": "PandasBackend",
        "polars": "PolarsEagerBackend",
        "polars_lazy": "PolarsLazyBackend",
        "narwhals": "NarwhalsBackend",
    }

    cls_name = class_name_map.get(name, name.capitalize() + "Backend")
    adapter_cls: Type[BackendAdapter] = getattr(mod, cls_name)
    instance = adapter_cls()
    _BACKENDS[name] = instance
    return instance


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
    backends without modifying Schema's source.

    Args:
        name: Short name for the backend (e.g. 'cudf').
        module_path: Dotted module path containing the adapter class
                     (e.g. 'mylib.backends.cudf_backend').
    """
    _BACKEND_REGISTRY[name] = module_path
    # Clear cached instance if it exists (allows re-registration)
    _BACKENDS.pop(name, None)
