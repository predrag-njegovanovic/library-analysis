from abc import abstractmethod
from datetime import datetime

import polars as pl
from polars import LazyFrame

from src.common import App


class TransformationApp(App):
    @abstractmethod
    def transform(self, data: LazyFrame) -> LazyFrame:
        pass

    @abstractmethod
    def set_schema(self, data: LazyFrame) -> LazyFrame:
        pass

    def filter(
        self, data: LazyFrame, start_date: datetime, end_date: datetime, column: str
    ) -> LazyFrame:
        data = data.filter((pl.col(column) >= start_date) & (pl.col(column) < end_date))

        return data

    def clean_and_titlecase(self, data: LazyFrame, column_name: str) -> LazyFrame:
        return data.with_columns(
            pl.col(column_name)
            .str.strip_chars()
            .str.split(" ")
            .list.eval(pl.element().str.strip_chars())
            .list.eval(pl.element().filter(pl.element() != ""))
            .list.eval(pl.element().str.to_lowercase())
            .list.eval(pl.element().str.to_titlecase())
            .list.join(" ")
            .alias(column_name)
        )

    def titlecase(self, data: LazyFrame, column_name: str) -> LazyFrame:
        return data.with_columns(
            pl.col(column_name)
            .str.to_lowercase()
            .str.strip_chars()
            .str.to_titlecase()
            .alias(column_name)
        )

    def uppercase(self, data: LazyFrame, column_name: str) -> LazyFrame:
        return data.with_columns(
            pl.col(column_name).str.strip_chars().str.to_uppercase().alias(column_name)
        )

    def clean_dates(self, data: LazyFrame, column_name: str) -> LazyFrame:
        return data.with_columns(
            pl.col(column_name)
            .str.strip_chars()
            .str.replace_all("[^0-9 -]", "")
            .str.to_date(format="%Y-%m-%d", strict=False)
            .alias(column_name)
        )
