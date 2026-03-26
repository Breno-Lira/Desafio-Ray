from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from time import sleep

import pandas as pd
import requests


PTAX_URL = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaDia(moeda='{currency}',dataCotacao='{date}')"


@dataclass(frozen=True)
class BcbClientConfig:
    timeout_seconds: int = 20
    retry_attempts: int = 3


class BcbClient:
    def __init__(self, config: BcbClientConfig) -> None:
        self.config = config

    def fetch_rates(self, currencies: list[str], start_date: date, end_date: date) -> pd.DataFrame:
        if end_date < start_date:
            raise ValueError("BCB end date cannot be earlier than start date")

        records: list[dict] = []
        current = start_date

        while current <= end_date:
            for currency in currencies:
                result = self._fetch_rate(currency=currency, quote_date=current)
                if result:
                    records.append(result)
            current += timedelta(days=1)

        return pd.DataFrame(records)

    def _fetch_rate(self, currency: str, quote_date: date) -> dict | None:
        formatted_date = quote_date.strftime("%m-%d-%Y")
        url = PTAX_URL.format(currency=currency, date=formatted_date)

        params = {
            "$top": 1,
            "$format": "json",
        }

        for attempt in range(1, self.config.retry_attempts + 1):
            try:
                response = requests.get(url, params=params, timeout=self.config.timeout_seconds)
                response.raise_for_status()
                payload = response.json().get("value", [])
                if not payload:
                    return None

                item = payload[0]
                return {
                    "moeda": currency,
                    "data": item.get("dataHoraCotacao", "")[:10],
                    "cotacao_compra": item.get("cotacaoCompra"),
                    "cotacao_venda": item.get("cotacaoVenda"),
                    "data_hora_cotacao": item.get("dataHoraCotacao"),
                }
            except requests.RequestException:
                if attempt == self.config.retry_attempts:
                    return None
                sleep(1)

        return None
