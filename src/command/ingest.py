import logging
from pathlib import Path

import click

from src.command.utils import DEFAULT_CONFIG_PATH, load_app
from src.common import Config

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--config-path",
    "-p",
    required=True,
    type=click.Path,
    default=DEFAULT_CONFIG_PATH,
    help="Path to configuration file.",
)
def ingest(config_path: Path) -> None:
    config = Config.load(str(config_path))

    ingest_book_app = load_app(config, "ingest.books", "ingest.IngestBooksApp")
    ingest_customer_app = load_app(config, "ingest.customer", "ingest.CustomerApp")
    ingest_checkout_app = load_app(config, "ingest.checkout", "ingest.CheckoutApp")
    ingest_library_app = load_app(config, "ingest.library", "ingest.LibraryApp")

    logger.info("Running Book data ingestion...")
    ingest_book_app.run()

    logger.info("Running Customer data ingestion...")
    ingest_customer_app.run()

    logger.info("Running Checkout data ingestion...")
    ingest_checkout_app.run()

    logger.info("Running Library data ingestion...")
    ingest_library_app.run()

    logger.info("Finished ingestion process.")
