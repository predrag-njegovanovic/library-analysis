# pylint: disable=redefined-outer-name

from pathlib import Path

import polars as pl
import pytest
from polars import DataFrame, testing

from src.common import Config, ParquetReader, ParquetWriter
from src.transformation.common import TransformationApp


@pytest.fixture(scope="module")
def fixture_path() -> str:
    root_path = Path(__file__).absolute().parent
    return str(root_path / "fixtures" / "data.json")


@pytest.fixture(scope="module")
def data(fixture_path: str) -> DataFrame:
    return pl.read_json(fixture_path)


class TestTransformationApp:
    @pytest.fixture(scope="class")
    def transformation_app(
        self,
        config: Config,
        parquet_reader: ParquetReader,
        parquet_writer: ParquetWriter,
    ) -> TransformationApp:

        TransformationApp.__abstractmethods__ = set()
        return TransformationApp(config, parquet_reader, parquet_writer)

    def test_clean_and_titlecase(
        self, data: DataFrame, transformation_app: TransformationApp
    ) -> None:
        expected_data = pl.DataFrame(
            [
                {
                    "id": "test1",
                    "title": "Example Title",
                    "author": "['test']",
                    "publisher": "Univ of california press",
                    "published_date": " 20180609",
                    "city": "portland",
                },
                {
                    "id": "test2",
                    "title": "Test",
                    "author": "['author1']",
                    "publisher": "Lulu",
                    "published_date": "2022-02-01",
                    "city": "San francisco",
                },
                {
                    "id": "test3",
                    "title": "Example",
                    "author": "['author']",
                    "publisher": "john willey & sons",
                    "published_date": "%2018 05 05",
                    "city": "new york",
                },
            ]
        )

        data = transformation_app.clean_and_titlecase(data, "title")

        testing.assert_frame_equal(
            data, expected_data, check_column_order=False, check_row_order=False
        )

    def test_titlecase(
        self, data: DataFrame, transformation_app: TransformationApp
    ) -> None:
        expected_data = pl.DataFrame(
            [
                {
                    "id": "test1",
                    "title": " example Title  ",
                    "author": "['test']",
                    "publisher": "Univ Of California Press",
                    "published_date": " 20180609",
                    "city": "portland",
                },
                {
                    "id": "test2",
                    "title": "Test  ",
                    "author": "['author1']",
                    "publisher": "Lulu",
                    "published_date": "2022-02-01",
                    "city": "San francisco",
                },
                {
                    "id": "test3",
                    "title": "example",
                    "author": "['author']",
                    "publisher": "John Willey & Sons",
                    "published_date": "%2018 05 05",
                    "city": "new york",
                },
            ]
        )

        data = transformation_app.titlecase(data, "publisher")

        testing.assert_frame_equal(
            data, expected_data, check_column_order=False, check_row_order=False
        )

    # By writing this test I detected that clean_dates
    # doesn't work as expected as it returns `None` as dates
    def test_clean_dates(
        self, data: DataFrame, transformation_app: TransformationApp
    ) -> None:
        expected_data = pl.DataFrame(
            [
                {
                    "id": "test1",
                    "title": " example Title  ",
                    "author": "['test']",
                    "publisher": "Univ of california press",
                    "published_date": None,
                    "city": "portland",
                },
                {
                    "id": "test2",
                    "title": "Test  ",
                    "author": "['author1']",
                    "publisher": "Lulu",
                    "published_date": "2022-02-01",
                    "city": "San francisco",
                },
                {
                    "id": "test3",
                    "title": "example",
                    "author": "['author']",
                    "publisher": "john willey & sons",
                    "published_date": None,
                    "city": "new york",
                },
            ]
        )
        expected_data = expected_data.cast({"published_date": pl.Date})

        data = transformation_app.clean_dates(data, "published_date")

        testing.assert_frame_equal(
            data, expected_data, check_column_order=False, check_row_order=False
        )

    def test_extract_from_list(
        self, data: DataFrame, transformation_app: TransformationApp
    ) -> None:
        expected_data = pl.DataFrame(
            [
                {
                    "id": "test1",
                    "title": " example Title  ",
                    "author": ["test"],
                    "publisher": "Univ of california press",
                    "published_date": " 20180609",
                    "city": "portland",
                },
                {
                    "id": "test2",
                    "title": "Test  ",
                    "author": ["author1"],
                    "publisher": "Lulu",
                    "published_date": "2022-02-01",
                    "city": "San francisco",
                },
                {
                    "id": "test3",
                    "title": "example",
                    "author": ["author"],
                    "publisher": "john willey & sons",
                    "published_date": "%2018 05 05",
                    "city": "new york",
                },
            ]
        )

        data = transformation_app.extract_from_list(data, "author")

        testing.assert_frame_equal(data, expected_data)
