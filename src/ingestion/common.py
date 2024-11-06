import re
from datetime import datetime, timezone

from polars import DataFrame

from src.common import App


class IngestionApp(App):
    def transform(self, data: DataFrame) -> DataFrame:
        data = self._rename_columns(data)
        data = self._add_ingestion_date_column(data)

        return data

    def _add_ingestion_date_column(self, data: DataFrame) -> DataFrame:
        data = data.with_columns(ingestion_date=datetime.now(timezone.utc).date())

        return data

    def _rename_columns(self, data: DataFrame) -> DataFrame:
        columns = {
            column: self._to_snake_case(column).lower() for column in data.columns
        }
        data = data.rename(columns)

        return data

    def _to_snake_case(self, column_name: str) -> str:
        if self._is_camelcase(column_name):
            return "".join(
                ["_" + char.lower() if char.isupper() else char for char in column_name]
            ).lstrip("_")

        return column_name.lower()

    def _is_camelcase(self, column_name: str) -> bool:
        pattern = r"^[a-z]+([A-Z][a-z]+)*$"
        match = re.match(pattern, column_name)
        return bool(match)
