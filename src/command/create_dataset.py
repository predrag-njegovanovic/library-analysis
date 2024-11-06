import logging
import sys
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
def create_dataset(config_path: Path):
    """Command for creating dataset by applying cleaning techniques and aggregations.
    Saves data in the ready-to-use layer.

    Args:
        config_path (Path): Path to configuration file.
    """

    logger.info("Creating dataset in the gold layer...")

    logger.info("Setting data dir environment variable.")
    set_root_data_dir()

    config = Config.load(str(config_path))

    dataset_app = load_app(config, "aggregation.dataset", "src.aggregation.DatasetApp")

    try:
        dataset_app.run()
    except Exception as error:
        logger.error(error)
        sys.exit(-1)

    logger.info("Finished process of creating dataset.")
