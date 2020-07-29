from dataclasses import dataclass
from typing import List


@dataclass
class Column:
    name: str
    types: List[str]

    def cast_required(self) -> bool:
        return len(self.types) > 1

    @property
    def resolved_type(self) -> str:
        if len(self.types) == 1:
            return self.types[0]

        if self.types == ["DATETIME", "TIMESTAMP"]:
            return "TIMESTAMP"

        return "STRING"
