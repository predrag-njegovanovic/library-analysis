from __future__ import annotations

import logging
from pathlib import Path

import click

from src.command.utils import DEFAULT_CONFIG_PATH, load_app, set_root_data_dir
from src.common import Config

logger = logging.getLogger(__name__)


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
    "--start-date",
    "-sd",
    required=False,
    type=str,
    help="Job reads data starting from this date. Format: YYYY-MM-DD.",
)
@click.option(
    "--end-date",
    "-ed",
    required=False,
    type=str,
    help="Job reads data to this date. Format: YYYY-MM-DD.",
)
def process(
    config_path: Path, start_date: str | None = None, end_date: str | None = None
) -> None:
    logger.info("Starting data processing..")
    logger.info("Setting data dir environment variable.")
    set_root_data_dir()

    config = Config.load(str(config_path))

    transform_customer_app = load_app(
        config,
        "transformation.customer",
        "src.transformation.TransformationCustomerApp",
        {"start_date": start_date, "end_date": end_date},
    )

    transform_checkout_app = load_app(
        config,
        "transformation.checkout",
        "src.transformation.TransformationCheckoutApp",
        {"start_date": start_date, "end_date": end_date},
    )

    transform_library_app = load_app(
        config,
        "transformation.library",
        "src.transformation.TransformationLibraryApp",
        {"start_date": start_date, "end_date": end_date},
    )

    logger.info("Running Customer data processing")
    transform_customer_app.run()

    logger.info("Running Checkout data processing")
    transform_checkout_app.run()

    logger.info("Running Library data processing...")
    transform_library_app.run()

    logger.info("Finished data processing.")
