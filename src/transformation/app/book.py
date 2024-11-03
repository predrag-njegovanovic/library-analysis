# type: ignore

import logging
from datetime import datetime

import polars as pl
from polars import LazyFrame

from src.transformation.common import TransformationApp

logger = logging.getLogger(__name__)


class TransformationBookApp(TransformationApp):
    def run(self) -> None:
        if self.arguments:
            start_date = self.arguments.get("start_date")
            end_date = self.arguments.get("end_date")

        if start_date and end_date:
            start_date = datetime.strftime(start_date, "%Y-%m-%d")
            end_date = datetime.strftime(end_date, "%Y-%m-%d")

        logger.info("Reading book data from the bronze layer.")
        data = self.read(self.config.input)

        if start_date and end_date:
            data = self.filter(data, start_date, end_date, "ingestion_date")

        logger.info("Applying transformations.")
        data = self.transform(data)
        data = self.set_schema(data)

        logger.info(f"Writing book data to path: '{self.config.output.path}'")

        data_to_write = data.collect()
        self.write(data_to_write, self.config.output)

    def transform(self, data: LazyFrame) -> LazyFrame:
        # drop duplicates
        data = data.unique(pl.col("id"))

        data = self.clean_and_titlecase(data, "title")
        data = self._extract_from_list(data, "authors")
        data = self.clean_and_titlecase(data, "publisher")
        data = self._add_published_year(data)
        data = self._extract_from_list(data, "categories")
        data = self._clean_prices(data)
        data = self._clean_pages(data)

        data = data.drop(pl.col("ingestion_date"))

        return data

    def set_schema(self, data: LazyFrame) -> LazyFrame:
        book_schema = {
            "id": pl.String,
            "title": pl.String,
            "authors": pl.List(pl.String),
            "publisher": pl.String,
            "published_date": pl.String,
            "published_year": pl.Int32,
            "categories": pl.List(pl.String),
            "price": pl.Float32,
            "pages": pl.Int32,
        }

        data = data.cast(dtypes=book_schema)

        return data

    def _extract_from_list(self, data: LazyFrame, column_name: str) -> LazyFrame:
        return data.with_columns(
            pl.col(column_name)
            .str.extract("'([^,]*)'")
            .cast(pl.List(pl.String))
            .alias(column_name)
        )

    def _add_published_year(self, data: LazyFrame) -> LazyFrame:
        return data.with_columns(
            pl.col("published_date")
            .str.strip_chars()
            .str.replace_all("[^0-9 -]", "")
            .str.to_date(format="%Y", strict=False)
            .dt.year()
            .alias("published_year")
        )

    def _clean_prices(self, data: LazyFrame) -> LazyFrame:
        return data.with_columns(
            pl.col("price")
            .str.strip_chars()
            .str.replace_all("[^0-9 .]", "")
            .cast(pl.Float32)
            .round(2)
            .alias("price")
        )

    def _clean_pages(self, data: LazyFrame) -> LazyFrame:
        return data.with_columns(
            pl.col("pages")
            .str.strip_chars()
            .str.replace_all("[^0-9]", "")
            .cast(pl.Int32)
            .alias("pages")
        )
