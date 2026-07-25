from __future__ import annotations
import hashlib
import os
from typing import Any, Optional

def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def redact(value: Any) -> Optional[str]:
    if value is None:
        return None
    return None  # safer default: drop value entirely

def hash_sha256(value: Any, salt: str) -> Optional[str]:
    if value is None:
        return None
    return _sha256_hex(f"{salt}{value}")

def salted_sha256_lookup(value: Any, salt: str) -> Optional[str]:
    if value is None:
        return None
    return _sha256_hex(f"{salt}{value}")

def get_env_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v
