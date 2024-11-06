import logging
import pickle
import sys

import click
import polars as pl

from src.command.utils import (
    DEFAULT_CONFIG_PATH,
    load_config_module,
    set_root_data_dir,
    set_root_model_dir,
)
from src.common import Config, init_reader

logger = logging.getLogger(__name__)

COLUMNS = (
    "gender",
    "education",
    "occupation",
    "age_category",
    "price_standardized",
    "pages_standardized",
)


@click.command()
@click.option(
    "--config-path",
    "-p",
    required=True,
    type=click.types.Path(),
    default=DEFAULT_CONFIG_PATH,
    help="Path to configuration file.",
)
@click.option(
    "--customer-id",
    "-c",
    required=True,
    type=str,
    help="Predict if customer with this id will return book.",
)
@click.option(
    "--book-id",
    "-b",
    required=True,
    type=str,
    help="Predict if this book will be returned on time.",
)
def predict(config_path: str, customer_id: str, book_id: str) -> None:
    """This is a mock of the predict function which should return prediction of a late return.
    We don't have feature store implemented
    so we don't have a fast way of retrieving customer features and books features separately.
    Everything will be fetched from gold layer and dataset table.

    Args:
        config_path (str): Path to configuration.
        customer_id (str): Customer to get predicted.
        book_id (str): Book to get predicted.
    """

    set_root_data_dir()
    set_root_model_dir()

    config = Config.load(str(config_path))
    config_module = load_config_module(config, "aggregation")

    reader = init_reader(config_module)
    if not reader:
        raise ValueError("Can't load reader.")

    dataset = reader.read(config_module.dataset.output)

    result = dataset.filter(
        (pl.col("customer_id") == customer_id) & (pl.col("book_id") == book_id)
    ).collect()

    feature_vector = result.select(COLUMNS).to_pandas()

    if len(feature_vector) == 0:
        logger.error("Combination of customer id and book id doesn't exist.")
        # gracefully shutdown
        sys.exit(-1)

    with open(config.model.path, "rb") as model_file:
        model = pickle.load(model_file)

    prediction = model.predict(feature_vector)

    logger.info(f"Prediction value: {prediction}.")

    if prediction:
        logger.info(
            f"Customer: {customer_id} and book: {book_id} will be returned on time."
        )
    else:
        logger.info(
            f"Customer: {customer_id} and book: {book_id} won't be returned on time."
        )
