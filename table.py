from dataclasses import dataclass
from typing import List

from column import Column


@dataclass
class Table:
    name: str
    columns: List[Column]

    def has_column(self, column: Column) -> bool:
        for c in self.columns:
            if c.name == column.name:
                return True

        return False
