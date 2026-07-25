from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict
import yaml

@dataclass(frozen=True)
class FieldRule:
    name: str
    classification: str
    strategy: Dict[str, Any]

@dataclass(frozen=True)
class EntityRule:
    name: str
    fields: Dict[str, FieldRule]

@dataclass(frozen=True)
class PiiPolicy:
    version: int
    domain: str
    entities: Dict[str, EntityRule]

def load_pii_policy(path: str) -> PiiPolicy:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    entities: Dict[str, EntityRule] = {}
    for entity_name, entity_def in raw["entities"].items():
        fields: Dict[str, FieldRule] = {}
        for field_name, field_def in entity_def["fields"].items():
            fields[field_name] = FieldRule(
                name=field_name,
                classification=field_def["classification"],
                strategy=field_def["strategy"],
            )
        entities[entity_name] = EntityRule(name=entity_name, fields=fields)

    return PiiPolicy(
        version=raw["version"],
        domain=raw["domain"],
        entities=entities,
    )
