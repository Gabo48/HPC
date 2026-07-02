from __future__ import annotations

import importlib
import os
from collections.abc import Callable


_BACKENDS = {
    "sequential": "processor.v1_sequential",
    "numpy": "processor.v2_numpy",
    "cython": "processor.v3_cython",
    "ray": "processor.v4_ray",
}


def get_preprocessor() -> Callable[[list[bytes]], list[dict]]:
    backend = os.getenv("HPC_BACKEND", "ray").lower()
    module_name = _BACKENDS.get(backend)
    if module_name is None:
        valid = ", ".join(sorted(_BACKENDS))
        raise ValueError(f"HPC_BACKEND invalido: {backend!r}. Valores validos: {valid}")

    module = importlib.import_module(module_name)
    return module.preprocess_chunks


def preprocess_chunks(chunks: list[bytes]) -> list[dict]:
    return get_preprocessor()(chunks)
