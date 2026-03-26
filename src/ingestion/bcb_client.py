from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from time import sleep
from typing import Iterable

import pandas as pd
import requests


PTAX_URL = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaDia(moeda='{currency}',dataCotacao='{date}')"
PTAX_PERIOD_URL = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinal)"


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

        quote_dates: list[date] = []
        current = start_date
        while current <= end_date:
            quote_dates.append(current)
            current += timedelta(days=1)

        return self.fetch_rates_for_dates(currencies=currencies, quote_dates=quote_dates)

    def fetch_rates_for_dates(self, currencies: list[str], quote_dates: Iterable[date]) -> pd.DataFrame:
        records: list[dict] = []
        unique_dates = sorted(set(quote_dates))
        date_ranges = _group_contiguous_dates(unique_dates)

        for range_start, range_end in date_ranges:
            for currency in currencies:
                period_records = self._fetch_rates_period(
                    currency=currency,
                    start_date=range_start,
                    end_date=range_end,
                )
                records.extend(period_records)

        return pd.DataFrame(records)

    def _fetch_rates_period(self, currency: str, start_date: date, end_date: date) -> list[dict]:
        params = {
            "@moeda": f"'{currency}'",
            "@dataInicial": f"'{start_date.strftime('%m-%d-%Y')}'",
            "@dataFinal": f"'{end_date.strftime('%m-%d-%Y')}'",
            "$top": 100000,
            "$format": "json",
        }

        for attempt in range(1, self.config.retry_attempts + 1):
            try:
                response = requests.get(PTAX_PERIOD_URL, params=params, timeout=self.config.timeout_seconds)
                response.raise_for_status()
                payload = response.json().get("value", [])
                if not payload:
                    return []

                latest_by_day: dict[str, dict] = {}
                for item in payload:
                    data_hora = item.get("dataHoraCotacao")
                    if not data_hora:
                        continue
                    day = data_hora[:10]
                    existing = latest_by_day.get(day)
                    if existing is None or str(data_hora) > str(existing["data_hora_cotacao"]):
                        latest_by_day[day] = {
                            "moeda": currency,
                            "data": day,
                            "cotacao_compra": item.get("cotacaoCompra"),
                            "cotacao_venda": item.get("cotacaoVenda"),
                            "data_hora_cotacao": data_hora,
                        }

                records: list[dict] = []
                for day in sorted(latest_by_day.keys()):
                    entry = latest_by_day[day]
                    records.append(
                        {
                            "moeda": entry["moeda"],
                            "data": entry["data"],
                            "cotacao_compra": entry["cotacao_compra"],
                            "cotacao_venda": entry["cotacao_venda"],
                        }
                    )

                return records
            except requests.RequestException:
                if attempt == self.config.retry_attempts:
                    return []
                sleep(1)

        return []

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
                }
            except requests.RequestException:
                if attempt == self.config.retry_attempts:
                    return None
                sleep(1)

        return None


def _group_contiguous_dates(sorted_dates: list[date]) -> list[tuple[date, date]]:
    if not sorted_dates:
        return []

    ranges: list[tuple[date, date]] = []
    start = sorted_dates[0]
    end = sorted_dates[0]

    for current in sorted_dates[1:]:
        if current == end + timedelta(days=1):
            end = current
            continue

        ranges.append((start, end))
        start = current
        end = current

    ranges.append((start, end))
    return ranges
