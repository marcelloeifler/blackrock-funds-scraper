import asyncio
import logging

import src.etl.transform as transform
from src.config.constants import RequestConfig
from src.etl.extract import Extract
from src.utils import helpers

log = logging.getLogger(__name__)


class FundsList:
    def __init__(self):
        self.extract = Extract()

    async def run(self) -> None:
        log.info("[FundsList] Fetching data from the API")

        await self.process_codigos()
        await self.extract.close()

    async def process_codigos(self) -> None:
        pass


async def main():
    log.info("[FundsList] Initializing BlackRock Funds List Scraper")

    scraper = FundsList()
    await scraper.run()

    log.info("[FundsList] BlackRock Funds List Scraper finished")


if __name__ == "__main__":
    asyncio.run(main())
