import asyncio
import logging

import pandas as pd

import src.etl.transform as transform
from src.config.constants import RequestConfig
from src.etl.extract import Extract
from src.utils import helpers

log = logging.getLogger(__name__)


class FundsList:
    def __init__(self):
        self.extract = Extract()

    async def run(self) -> None:
        df_funds = await self.get_df_funds()
        await self.process_funds(df_funds=df_funds)
        await self.extract.close()

    async def get_df_funds(self) -> pd.DataFrame:
        log.info("[FundsList] Fetching data from the API")
        response = await self.extract.request_get(
            url=RequestConfig.URL_US_FUNDS, headers=RequestConfig.BASIC_HEADERS
        )
        response_data = response.json()
        df_funds = transform.parse_funds(response_data=response_data)

        return df_funds

    @staticmethod
    async def process_funds(df_funds) -> None:
        log.info("[FundsList] Processing data into Supabase")


async def main():
    log.info("[FundsList] Initializing BlackRock Funds List Scraper")

    scraper = FundsList()
    await scraper.run()

    log.info("[FundsList] BlackRock Funds List Scraper finished")


if __name__ == "__main__":
    asyncio.run(main())
