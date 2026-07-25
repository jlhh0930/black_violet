from dataclasses import dataclass
from typing import Protocol

class NonReversibleStrategy(Protocol):
    def apply(self, value: str) -> str: ...

class ReIdentifiableTokenStrategy(Protocol):
    def tokenize(self, value: str) -> str: ...
    # reverse intentionally NOT in analytics path

@dataclass
class PiiFieldRule:
    name: str
    classification: str
    mode: str  # "non_reversible_mask" | "re_identifiable_token"
