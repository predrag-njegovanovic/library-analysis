import logging

from src.ingestion.common import IngestionApp

logger = logging.getLogger(__name__)


class IngestionLibraryApp(IngestionApp):
    def run(self) -> None:
        try:
            data = self.read(self.config.input)
            transformed_data = self.transform(data)

            transformed_data = transformed_data.collect()

            logger.info(f"Saving data on path: '{self.config.output.path}'")
            self.write(transformed_data, self.config.output)
        except Exception as error:
            logger.error(error)
            logger.warning("Shutting down job.")
