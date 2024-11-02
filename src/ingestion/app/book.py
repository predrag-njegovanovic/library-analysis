# type: ignore

import logging

import polars as pl
from polars import Schema

from src.ingestion.common import IngestionApp

logger = logging.getLogger(__name__)

book_schema = Schema(
    {
        "id": pl.String,
        "authors": pl.String,
        "publisher": pl.List(pl.String),
        "published_date": pl.Date,
        "categories": pl.List(pl.String),
        "price": pl.Float32,
        "pages": pl.Int8,
    }
)


class IngestionBookApp(IngestionApp):
    def run(self) -> None:
        try:
            data = self.read(self.config.input)
            transformed_data = self.transform(data)

            logger.info(f"Saving data on path: '{self.config.output.path}'")
            self.write(transformed_data, self.config.output)
        except Exception as error:
            logger.error(error)
            logger.warning("Shutting down job.")
