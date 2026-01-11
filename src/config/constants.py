from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar, Optional

from src.utils import helpers


@dataclass(frozen=True)
class AsyncConfig:
    SEMAPHORE: int = 3
    BATCH_SIZE: int = 1000
    PROGRESS_STEP_PERCENT: int = 2


@dataclass(frozen=True)
class RequestConfig:
    URL_US_FUNDS: str = (
        "https://www.blackrock.com/us/individual/product-screener/product-screener-v3.1.jsn?dcrPath=/templatedata/config/product-screener-v3/data/en/one/v4/product-screener-backend-config&userType=individual&siteEntryPassthrough=true"
    )

    BASIC_HEADERS: ClassVar[dict] = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0"
    }
