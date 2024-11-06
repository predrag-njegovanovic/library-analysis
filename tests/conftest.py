# pylint: disable=redefined-outer-name

from __future__ import annotations

from pathlib import Path

import pytest

from src.command.utils import set_root_data_dir
from src.common import Config, CsvReader, ParquetReader, ParquetWriter


@pytest.fixture(scope="session")
def config() -> Config:
    # config requirement
    set_root_data_dir()
    config_path = Path(__package__).absolute().parent / "src/config/settings.toml"
    config = Config.load(str(config_path))

    return config


@pytest.fixture(scope="session")
def csv_reader() -> CsvReader:
    return CsvReader()


@pytest.fixture(scope="session")
def parquet_reader() -> ParquetReader:
    return ParquetReader()


@pytest.fixture(scope="session")
def parquet_writer() -> ParquetWriter:
    return ParquetWriter()
