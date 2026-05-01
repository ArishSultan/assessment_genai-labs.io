import yaml

from typing import Any, Iterable, Dict
from config import SETTINGS

from pathlib import Path


class SchemaCache:
    def __init__(self, yaml_path: str | Path, table_name: str) -> None:
        self.yaml_path = Path(yaml_path)
        self.table_name = table_name
        self.table_data: dict[str, Any] = {}

        self._load_schema()

    def _load_schema(self) -> None:
        if not self.yaml_path.exists():
            raise FileNotFoundError(f"Schema file not found at: {self.yaml_path}")

        with self.yaml_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        self.table_data = data.get(self.table_name, {})
        if not self.table_data:
            raise ValueError(f"Table '{self.table_name}' not found in the YAML schema.")

    @property
    def schema(self) -> Dict[str, Iterable[str]]:
        return {self.table_name: self.columns.keys()}

    @property
    def columns(self) -> dict[str, dict[str, Any]]:
        return self.table_data.get("Columns", {})

    def condensed_text(self) -> str:
        description = self.table_data.get("Description", "")

        header = f"Table: {self.table_name}"
        if description:
            header += f" — {description}"

        lines = [header, "Columns:"]

        for col_name, col_props in self.columns.items():
            col_type = col_props.pop("type", "TEXT")

            col_desc = None
            match SETTINGS.schema_description_level:
                case 'full':
                    col_props.pop("tiny-description", "")
                    col_desc = col_props.pop("description", "")
                case 'standard':
                    col_props.pop("description", "")
                    col_desc = col_props.pop("tiny-description", "")

            line = f"  - {col_name} ({col_type})"
            if col_desc:
                line += f" — {col_desc}"

            if col_props:
                extras = [f"{k}: {v}" for k, v in col_props.items()]
                line += f"\n      {' | '.join(extras)}"

            lines.append(line)

        return "\n".join(lines)
