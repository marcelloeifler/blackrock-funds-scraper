import logging
from datetime import date, datetime
from typing import Optional, Tuple, Union

import pandas as pd

from src.config import constants
from src.utils import helpers

log = logging.getLogger(__name__)


def parse_funds(response_data: dict) -> pd.DataFrame:
    df_list = []

    for fund_code, fund_data in response_data.items():
        df_row = {
            "fund_code": fund_code,
            "local_exchange_ticker": fund_data["localExchangeTicker"],
            "fund_name": fund_data["fundName"],
            "investor_class_name": fund_data["investorClassName"],
            "asset_class": fund_data["aladdinAssetClass"],
            "country": fund_data["aladdinCountry"],
            "region": fund_data["aladdinRegion"],
            "esg_classification": fund_data["aladdinEsgClassification"],
            "market_type": fund_data["aladdinMarketType"],
            "sub_asset_class": fund_data["aladdinSubAssetClass"],
            "inception_date": fund_data["inceptionDate"]["r"],
            "investment_style": fund_data["investmentStyle"],
            "product_page_url": fund_data["productPageUrl"],
        }
        df_list.append(df_row)

    return pd.DataFrame(df_list)
