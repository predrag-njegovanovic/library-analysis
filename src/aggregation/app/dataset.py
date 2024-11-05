# type: ignore

import logging
from datetime import datetime, timezone

import polars as pl
from polars import LazyFrame

from src.aggregation.common import AggregationApp

logger = logging.getLogger(__name__)

RETURN_LIMIT = 28


class DatasetApp(AggregationApp):
    def run(self) -> None:
        logger.info("Reading data from silver layer.")

        books_df = self.read(self.config.input.book)
        customers_df = self.read(self.config.input.customer)
        checkouts_df = self.read(self.config.input.checkout)

        logger.info("All tables loaded.")

        customers_df = self._add_age_category(customers_df)

        # preparing data for aggregation
        books_df = self.fill_nulls(books_df, "price", pl.col("price").median(), "price")
        customers_df = self.fill_nulls(
            customers_df, "age", pl.col("age").median(), "age"
        )

        # only applicable to checkouts table
        checkouts_df = self._add_label(checkouts_df)

        # join data
        customer_checkout_df = customers_df.rename({"id": "patron_id"}).join(
            checkouts_df, on="patron_id"
        )
        library_df = customer_checkout_df.join(books_df, on="id", how="left")

        dataset = self.aggregate(library_df)
        dataset = self.set_schema(dataset)

        logger.info(f"Writing dataset to path: '{self.config.output.path}'")

        dataset_to_write = dataset.collect()
        self.write(dataset_to_write, self.config.output)

    def aggregate(self, data: LazyFrame) -> LazyFrame:
        data = self._rename_dataset(data)

        data = data.drop_nulls()

        # Filter out invalid labels
        data = data.filter(pl.col("label") != -1)

        # change to categorical to physical representation
        data = self._transform_categorical_columns(data, "gender", "gender")
        data = self._transform_categorical_columns(data, "occupation", "occupation")
        data = self._transform_categorical_columns(data, "education", "education")
        data = self._transform_categorical_columns(data, "age_category", "age_category")

        # standardize price column around mean
        data = self._standardize_data(data, "price", "price_standardized")

        # standardize pages column around mean
        data = self._standardize_data(data, "pages", "pages_standardized")

        return data

    def set_schema(self, data: LazyFrame) -> LazyFrame:
        dataset_schema = {
            "customer_id": pl.String,
            "book_id": pl.String,
            "name": pl.String,
            "gender": pl.UInt32,
            "education": pl.UInt32,
            "occupation": pl.UInt32,
            "age_category": pl.UInt32,
            "price_standardized": pl.Float32,
            "pages_standardized": pl.Float32,
            "label": pl.UInt32,
        }

        data = data.cast(dtypes=dataset_schema)

        return data

    # private methods

    def _add_age_category(self, data: LazyFrame) -> LazyFrame:
        data = data.with_columns(
            (
                (
                    datetime.now(timezone.utc).date() - pl.col("birth_date")
                ).dt.total_days()
                / 365
            )
            .floor()
            .cast(pl.Int32)
            .alias("age")
        )

        data = data.with_columns(
            pl.when((pl.col("age") >= 3) & (pl.col("age") <= 19))
            .then(pl.lit("Young"))
            .when((pl.col("age") >= 20) & (pl.col("age") <= 39))
            .then(pl.lit("Adult"))
            .when((pl.col("age") >= 40) & (pl.col("age") <= 59))
            .then(pl.lit("Middle-Age"))
            .when((pl.col("age") >= 60) & (pl.col("age") <= 99))
            .then(pl.lit("Old"))
            .otherwise(pl.lit("Undefined"))
            .alias("age_category")
        )

        return data

    def _add_label(self, data: LazyFrame) -> LazyFrame:
        data = data.with_columns(
            pl.col("date_returned")
            .sub(pl.col("date_checkout"))
            .dt.total_days()
            .cast(pl.Int32)
            .alias("days_holding_book")
        )

        data = data.with_columns(
            pl.when(
                (pl.col("days_holding_book") <= RETURN_LIMIT)
                & (pl.col("days_holding_book") >= 0)
            )
            .then(pl.lit(1))
            .when(pl.col("days_holding_book") > RETURN_LIMIT)
            .then(pl.lit(0))
            .when(pl.col("date_returned").is_null())
            .then(pl.lit(0))
            .when(pl.col("days_holding_book") < 0)
            .then(pl.lit(-1))
            .otherwise(pl.lit(-1))
            .alias("book_returned")
        )
        return data

    def _rename_dataset(self, data: LazyFrame) -> LazyFrame:
        data = data.select(
            [
                "patron_id",
                "id",
                "name",
                "gender",
                "education",
                "occupation",
                "age_category",
                "price",
                "pages",
                "book_returned",
            ]
        ).rename(
            {"patron_id": "customer_id", "id": "book_id", "book_returned": "label"}
        )

        return data

    def _transform_categorical_columns(
        self, data: LazyFrame, column_name: str, alias: str
    ) -> LazyFrame:
        data = data.with_columns(
            pl.col(column_name).cast(pl.Categorical).to_physical().alias(alias)
        )

        return data

    def _standardize_data(
        self, data: LazyFrame, column_name: str, alias: str
    ) -> LazyFrame:
        data = data.with_columns(
            (
                (pl.col(column_name) - pl.col(column_name).mean())
                / pl.col(column_name).std()
            ).alias(alias)
        )

        return data
