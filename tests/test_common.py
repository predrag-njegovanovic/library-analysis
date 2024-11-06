# pylint: disable=redefined-outer-name

import pytest

from src.common import (
    Config,
    CsvReader,
    ParquetWriter,
    get_class,
    init_reader,
    init_writer,
    load_config_module,
)
from src.exception import MissingClassImplementation, MissingConfigModuleImplementation


@pytest.fixture(scope="module")
def valid_class_name() -> str:
    return "CsvReader"


@pytest.fixture(scope="module")
def non_valid_class_name() -> str:
    return "TestReader"


@pytest.fixture(scope="module")
def valid_config_section() -> str:
    return "ingestion.book"


@pytest.fixture(scope="module")
def non_valid_config_section() -> str:
    return "test.example"


def test_load_config_module(config: Config, valid_config_section: str) -> None:
    actual = load_config_module(config, valid_config_section)
    expected = config.ingestion.book

    assert actual == expected


def test_non_valid_load_config_module(
    config: Config, non_valid_config_section: str
) -> None:
    with pytest.raises(MissingConfigModuleImplementation) as config_error:
        load_config_module(config, non_valid_config_section)
        assert (
            str(config_error.value)
            == "Missing config module implementation 'test.example'"
        )


def test_get_class(valid_class_name: str) -> None:
    actual = get_class(valid_class_name)
    expected = CsvReader()

    assert actual.__class__.__name__ == expected.__class__.__name__


def test_non_valid_get_class(non_valid_class_name: str) -> None:
    with pytest.raises(MissingClassImplementation) as missing_class_error:
        get_class(non_valid_class_name)

        assert (
            str(missing_class_error.value)
            == "Missing class implementation 'TestReader'"
        )


def test_init_reader(config: Config) -> None:
    config_module = load_config_module(config, "ingestion.book")
    reader = init_reader(config_module)

    assert isinstance(reader, CsvReader)


def test_init_writer(config: Config) -> None:
    config_module = load_config_module(config, "ingestion.book")
    writer = init_writer(config_module)

    assert isinstance(writer, ParquetWriter)
