from abc import abstractmethod

import polars as pl
from polars import Expr, LazyFrame

from src.common import App


class AggregationApp(App):
    @abstractmethod
    def aggregate(self, data: LazyFrame) -> LazyFrame:
        pass

    @abstractmethod
    def set_schema(self, data: LazyFrame) -> LazyFrame:
        pass

    def fill_nulls(
        self, data: LazyFrame, column_name: str, pl_expr: Expr, alias: str
    ) -> LazyFrame:
        return data.with_columns(pl.col(column_name).fill_null(pl_expr).alias(alias))
