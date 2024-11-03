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
