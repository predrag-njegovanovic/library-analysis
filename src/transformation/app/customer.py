# type: ignore

import logging
from datetime import datetime

import polars as pl
from polars import LazyFrame

from src.transformation.common import TransformationApp

logger = logging.getLogger(__name__)


class TransformationCustomerApp(TransformationApp):
    def run(self) -> None:
        if self.arguments:
            start_date = self.arguments.get("start_date")
            end_date = self.arguments.get("end_date")

        if start_date and end_date:
            start_date = datetime.strftime(start_date, "%Y-%m-%d")
            end_date = datetime.strftime(end_date, "%Y-%m-%d")

        logger.info("Reading customer data from the bronze layer.")
        data = self.read(self.config.input)

        if start_date and end_date:
            data = self.filter(data, start_date, end_date, "ingestion_date")

        logger.info("Applying transformations.")
        data = self.transform(data)
        data = self.set_schema(data)

        logger.info(f"Writing customer data to path: '{self.config.output.path}'")

        data_to_write = data.collect()
        self.write(data_to_write, self.config.output)

    def transform(self, data: LazyFrame) -> LazyFrame:
        # drop duplicates
        data = data.unique(pl.col("id"))

        # clean data
        data = self.clean_and_titlecase(data, "name")
        data = self.clean_and_titlecase(data, "street_address")
        data = self.titlecase(data, "city")
        data = self.uppercase(data, "state")
        data = self._clean_zipcode(data)
        data = self.clean_dates(data, "birth_date")
        data = self._clean_gender(data)
        data = self.clean_and_titlecase(data, "education")
        data = self.clean_and_titlecase(data, "occupation")

        # drop ingestion column
        data = data.drop(pl.col("ingestion_date"))

        return data

    def set_schema(self, data: LazyFrame) -> LazyFrame:
        customer_schema = {
            "id": pl.String,
            "name": pl.String,
            "street_address": pl.String,
            "city": pl.String,
            "zipcode": pl.Int8,
            "birth_date": pl.Date,
            "gender": pl.String,
            "education": pl.String,
            "occupation": pl.String,
        }

        data = data.cast(dtypes=customer_schema)

        return data

    def _clean_zipcode(self, data: LazyFrame) -> LazyFrame:
        return data.with_columns(
            pl.col("zipcode")
            .str.strip_chars()
            .str.replace_all("[^0-9 .]", "")
            .cast(pl.Float32)
            .cast(pl.Int32)
            .alias("zipcode")
        )

    def _clean_gender(self, data: LazyFrame) -> LazyFrame:
        return data.with_columns(
            pl.col("gender").str.strip_chars().str.to_lowercase().alias("gender")
        )
