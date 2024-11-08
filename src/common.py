from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from importlib import import_module
from typing import Any

import polars as pl
from dynaconf import Dynaconf
from polars import DataFrame, LazyFrame

from src.exception import MissingClassImplementation, MissingConfigModuleImplementation

logger = logging.getLogger(__name__)


class Config(Dynaconf):
    @classmethod
    def load(cls, config_path: str) -> Dynaconf:
        return Dynaconf(settings_files=[config_path])


def load_config_module(config: Config, config_section: str) -> Config:
    if section := config.get(config_section):
        return section

    raise MissingConfigModuleImplementation(config_section)


def get_class(class_name: str) -> Any:
    try:
        return getattr(import_module(__name__), class_name)()
    except AttributeError as error:
        logger.error(error)
        raise MissingClassImplementation(class_name) from error


def init_reader(config_module: Config | None) -> Reader | None:
    try:
        reader_name = config_module.reader if config_module else None
        return get_class(reader_name)
    except MissingClassImplementation:
        return None


def init_writer(config_module: Config | None) -> Writer | None:
    try:
        writer_name = config_module.writer if config_module else None
        return get_class(writer_name)
    except MissingClassImplementation:
        return None


class Reader(ABC):
    """Abstract reader class. Subclasses implement read method."""

    @abstractmethod
    def read(self, config: Config, *args, **kwargs) -> LazyFrame:
        pass


class Writer(ABC):
    """Abstract writer class. Subclasses implement write method."""

    @abstractmethod
    def write(self, data: DataFrame, config: Config, *args, **kwargs) -> None:
        pass


class App(ABC):
    """Definition of an application that is deployed and run as a job.
    Defines configuration, reader and a writer.
    """

    def __init__(
        self,
        config: Config,
        reader: Reader,
        writer: Writer,
        arguments: dict | None = None,
    ) -> None:
        self.config = config
        self.reader = reader
        self.writer = writer
        self.arguments = arguments

    @abstractmethod
    def run(self) -> None:
        pass

    def read(self, config: Config, *args, **kwargs) -> LazyFrame:
        return self.reader.read(config, *args, **kwargs)

    def write(self, data: DataFrame, config: Config, *args, **kwargs) -> None:
        self.writer.write(data, config, *args, **kwargs)


# Readers
class CsvReader(Reader):
    def read(self, config: Config, *args, **kwargs) -> LazyFrame:
        return pl.scan_csv(source=config.path, has_header=True)


class ParquetReader(Reader):
    def read(self, config: Config, *args, **kwargs) -> LazyFrame:
        schema = kwargs.get("schema")
        if schema:
            return pl.scan_parquet(source=config.path, schema=schema)

        return pl.scan_parquet(source=config.path)


# Writers


class ParquetWriter(Writer):
    def write(self, data: DataFrame, config: Config, *args, **kwargs) -> None:
        if len(config.partition_by):
            partition_by = config.partition_by
        else:
            partition_by = None

        data.write_parquet(
            file=config.path, compression="snappy", partition_by=partition_by
        )
