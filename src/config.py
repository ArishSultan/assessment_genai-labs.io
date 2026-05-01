from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DescriptionLevel = Literal["minimal", "standard", "full"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
        env_file=".env",
        case_sensitive=False,
        env_file_encoding="utf-8",
    )

    model: str = Field(default="openai/gpt-5-nano", alias="OPENROUTER_MODEL")
    table: str = Field(default="gaming", alias="DB_TABLE")
    db_path: Path = Field(default=Path("data/gaming.db"), alias="DB_PATH")
    max_sql_tokens: int = Field(default=512, alias="MAX_SQL_TOKENS")
    max_answer_tokens: int = Field(default=512, alias="MAX_ANSWER_TOKENS")
    answer_row_preview: int = Field(default=30, alias="ANSWER_ROW_PREVIEW")
    answer_max_str_len: int = Field(default=120, alias="ANSWER_MAX_STR_LEN")
    answer_max_avg_col_len: int = Field(default=80, alias="ANSWER_MAX_AVG_COL_LEN")

    schema_columns_raw: str = Field(default="", alias="SCHEMA_COLUMNS")
    schema_description_level: DescriptionLevel = Field(
        default="full", alias="SCHEMA_DESCRIPTION_LEVEL"
    )
    schema_descriptions_path: Path | None = Field(
        default=None, alias="SCHEMA_DESCRIPTIONS_PATH"
    )

    @field_validator("schema_description_level", mode="before")
    @classmethod
    def _norm_level(cls, v: object) -> object:
        if isinstance(v, str):
            return v.strip().lower()
        return v

    @property
    def schema_columns(self) -> list[str] | None:
        raw = (self.schema_columns_raw or "").strip()
        if not raw:
            return None
        parts = [c.strip() for c in raw.split(",") if c.strip()]
        return parts or None


SETTINGS = Settings()
