# type: ignore

import logging
from datetime import datetime

import polars as pl
from polars import LazyFrame

from src.transformation.common import TransformationApp

logger = logging.getLogger(__name__)


class TransformationCheckoutApp(TransformationApp):
    def run(self) -> None:
        if self.arguments:
            start_date = self.arguments.get("start_date")
            end_date = self.arguments.get("end_date")

        if start_date and end_date:
            start_date = datetime.strftime(start_date, "%Y-%m-%d")
            end_date = datetime.strftime(end_date, "%Y-%m-%d")

        logger.info("Reading checkouts data from the bronze layer.")
        data = self.read(self.config.input)

        if start_date and end_date:
            data = self.filter(data, start_date, end_date, "ingestion_date")

        logger.info("Applying transformations.")
        data = self.transform(data)
        data = self.set_schema(data)

        logger.info(f"Writing checkouts data to path: '{self.config.output.path}'")

        data_to_write = data.collect()
        self.write(data_to_write, self.config.output)

    def transform(self, data: LazyFrame) -> LazyFrame:
        # drop duplicates
        data = data.unique(
            subset=[pl.col("id"), pl.col("patron_id"), pl.col("library_id")]
        )

        # clean date values
        data = self.clean_dates(data, "date_checkout")
        data = self.clean_dates(data, "date_returned")

        data = data.drop(pl.col("ingestion_date"))

        return data

    def set_schema(self, data: LazyFrame) -> LazyFrame:
        checkout_schema = {
            "id": pl.String,
            "patron_id": pl.String,
            "library_id": pl.String,
            "date_checkout": pl.Date,
            "date_returned": pl.Date,
        }

        data = data.cast(dtypes=checkout_schema)

        return data
