from pathlib import Path

import polars as pl
import pytest
from polars import testing

from src.common import Config, CsvReader, ParquetWriter
from src.ingestion.common import IngestionApp


class TestIngestionApp:
    @pytest.fixture(scope="class")
    def ingestion_app(
        self,
        config: Config,
        csv_reader: CsvReader,
        parquet_writer: ParquetWriter,
    ) -> IngestionApp:

        IngestionApp.__abstractmethods__ = set()
        return IngestionApp(config, csv_reader, parquet_writer)

    # _rename_columns is private method and should be tested through transform
    # but _add_ingestion_date has a function that changes function internal state
    # _add_ingestion_date should be removed or changed to remove this behaviour
    def test_rename_columns(self, ingestion_app: IngestionApp) -> None:
        original_data = pl.DataFrame(
            {
                "ID": "test",
                "publishedDate": "2022-11-11",
                "PrIceS": 599,
                "authorS": ["Test Author"],
                "snake_case_column": "example",
            }
        )

        expected_data = pl.DataFrame(
            {
                "id": "test",
                "published_date": "2022-11-11",
                "prices": 599,
                "authors": ["Test Author"],
                "snake_case_column": "example",
            }
        )

        original_data = ingestion_app._rename_columns(original_data)

        testing.assert_frame_equal(
            original_data,
            expected_data,
            check_column_order=False,
            check_row_order=False,
        )
