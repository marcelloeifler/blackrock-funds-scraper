import asyncio
import logging
import math
from dataclasses import dataclass
from typing import Optional, Protocol

import pandas as pd

log = logging.getLogger(__name__)


class RowHandler(Protocol):
    async def __call__(self, row: pd.Series) -> None: ...


@dataclass
class ExecStats:
    total: int = 0
    processed: int = 0
    errors: int = 0
    last_logged_pct: int = -1


class AsyncDFProcessor:
    def __init__(
        self,
        *,
        batch_size: int = 1000,
        concurrency: int = 3,
        progress_step_percent: int = 2,
        log_erros: bool = True,
    ):
        self.batch_size = batch_size
        self.concurrency = concurrency
        self.progress_step_percent = progress_step_percent
        self.log_erros = log_erros

    async def _process_row_with_semaphore(
        self,
        row: pd.Series,
        semaphore: asyncio.Semaphore,
        handler: RowHandler,
        stats: ExecStats,
    ):
        async with semaphore:
            try:
                await handler(row)

            except Exception:
                if self.log_erros:
                    log.exception(
                        "[AsyncDFProcessor] Falha ao processar linha (index=%s)",
                        row.name,
                    )
                stats.errors += 1

            finally:
                stats.processed += 1
                pct = int((stats.processed * 100) / stats.total) if stats.total else 100
                step = self.progress_step_percent

                if pct % step == 0 and pct != stats.last_logged_pct:
                    stats.last_logged_pct = pct
                    log.info(
                        "[AsyncDFProcessor] Progresso: %d%% (%d/%d)",
                        pct,
                        stats.processed,
                        stats.total,
                    )

    async def process_dataframe(
        self,
        df: pd.DataFrame,
        row_handler: RowHandler,
        *,
        return_exceptions: bool = True,
    ) -> ExecStats:
        """
        Processa um DataFrame em lotes, aplicando um handler assíncrono em cada linha.
        - row_handler: async (row: pd.Series, total: int) -> None
        """
        if df is None or df.empty:
            raise ValueError("DataFrame vazio ou None")

        stats = ExecStats(total=len(df))
        semaphore = asyncio.Semaphore(self.concurrency)

        num_batches = math.ceil(stats.total / self.batch_size)
        for batch_index in range(num_batches):
            start = batch_index * self.batch_size
            end = min(start + self.batch_size, stats.total)
            batch_df = df.iloc[start:end]

            tasks = [
                self._process_row_with_semaphore(row, semaphore, row_handler, stats)
                for _, row in batch_df.iterrows()
            ]
            await asyncio.gather(*tasks, return_exceptions=return_exceptions)

        return stats
