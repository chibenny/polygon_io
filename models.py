import datetime
from typing import Any, Optional

from sqlmodel import Field, Relationship, SQLModel


class Ticker(SQLModel, table=True):
    __tablename__ = "tickers"
    symbol: str = Field(primary_key=True)
    candles: list["AggregateCandle"] | None = Relationship(back_populates="ticker")

    def model_post_init(self, __context: Any) -> None:
        self.symbol = self.symbol.upper()


class AggregateCandle(SQLModel, table=True):
    __tablename__ = "aggregate_candles"
    id: Optional[int] = Field(default=None, primary_key=True)
    ticker_id: Optional[str] = Field(foreign_key="tickers.symbol")
    ticker: Optional[Ticker] = Relationship(back_populates="candles")
    open_price: float
    close_price: float
    high_price: float
    low_price: float
    volume: int
    vwap: Optional[float] = Field(nullable=True)
    time: int = Field(index=True)
    time_iso: Optional[str] = Field(nullable=True)
    timespan: str = Field(default="day")
    notes: Optional[str] = Field(max_length=255 * 2, nullable=True)

    def model_post_init(self, __context: Any) -> None:
        self.time_iso = datetime.datetime.fromtimestamp(self.time / 1000.0).isoformat()


def map_results_for_bar(bar: dict[str, Any]) -> dict[str, Any]:
    return {
        "open_price": bar["o"],
        "close_price": bar["c"],
        "high_price": bar["h"],
        "low_price": bar["l"],
        "volume": bar["v"],
        "vwap": bar["vw"],
        "time": bar["t"],
    }
