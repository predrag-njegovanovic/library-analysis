# type: ignore

import logging
from datetime import datetime

import polars as pl
from polars import LazyFrame

from src.transformation.common import TransformationApp

logger = logging.getLogger(__name__)


class TransformationLibraryApp(TransformationApp):
    def run(self) -> None:
        if self.arguments:
            start_date = self.arguments.get("start_date")
            end_date = self.arguments.get("end_date")

        if start_date and end_date:
            start_date = datetime.strftime(start_date, "%Y-%m-%d")
            end_date = datetime.strftime(end_date, "%Y-%m-%d")

        logger.info("Reading library data from the bronze layer.")
        data = self.read(self.config.input)

        if start_date and end_date:
            data = self.filter(data, start_date, end_date, "ingestion_date")

        logger.info("Applying transformations.")
        data = self.transform(data)
        data = self.set_schema(data)

        logger.info(f"Writing library data to path: '{self.config.output.path}'")

        data_to_write = data.collect()
        self.write(data_to_write, self.config.output)

    def transform(self, data: LazyFrame) -> LazyFrame:
        # drop duplicates
        data = data.unique(pl.col("id"))

        # clean data
        data = self.clean_and_titlecase(data, "name")
        data = self.clean_and_titlecase(data, "street_address")
        data = self.titlecase(data, "city")
        data = self.uppercase(data, "region")
        data = self._clean_postal_code(data)

        # drop ingestion column
        data = data.drop(pl.col("ingestion_date"))

        return data

    def set_schema(self, data: LazyFrame) -> LazyFrame:
        library_schema = {
            "id": pl.String,
            "name": pl.String,
            "street_address": pl.String,
            "city": pl.String,
            "region": pl.String,
            "postal_code": pl.String,
        }

        data = data.cast(dtypes=library_schema)

        return data

    def _clean_postal_code(self, data: LazyFrame) -> LazyFrame:
        return data.with_columns(
            pl.col("postal_code").str.strip_chars().str.replace_all("[^A-Za-z0-9]", "")
        )
