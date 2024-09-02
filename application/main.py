from contextlib import asynccontextmanager
from functools import lru_cache

import httpx
from fastapi import Depends, FastAPI
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import create_engine
from sqlmodel import Session, SQLModel, select
from typing_extensions import Annotated

from .models import AggregateCandle, Ticker, map_results_for_bar


class Settings(BaseSettings):
    api_key: str
    transport: str = "httpx"
    base_url: str = "https://api.polygon.io"
    multiplier: int = 1
    timespan: str = "day"

    model_config = SettingsConfigDict(env_file=".env")


@lru_cache
def get_settings():
    return Settings()


sqlite_url: str = "sqlite:///./polygon_io.db"
connect_args = {"check_same_thread": False}
engine = create_engine(sqlite_url, echo=True, connect_args=connect_args)


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting")
    create_db_and_tables()
    yield
    print("Stopping")


app = FastAPI(lifespan=lifespan)


def _call_polygon_api(url: str, headers: dict = None, transport: object = httpx):
    return transport.get(url, headers=headers)


@app.get("/bars/{ticker}/{start}/{end}")
def get_aggregate_bars(
    ticker: str,
    start: str,
    end: str,
    settings: Annotated[Settings, Depends(get_settings)],
):
    """Connect to the polygon API and get the stock data
    /v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}
    """
    url = (
        f"{settings.base_url}"
        f"/v2/aggs/ticker/{ticker}"
        f"/range/{settings.multiplier}"
        f"/{settings.timespan}/{start}/{end}"
    )
    result = _call_polygon_api(
        url, headers={"Authorization": f"Bearer {settings.api_key}"}
    )

    # Store results to the database
    with Session(engine) as session:
        statement = select(Ticker).where(Ticker.symbol == ticker)
        ticker_ = session.exec(statement).first()

        if not ticker_:
            ticker_ = Ticker(symbol=ticker)
            session.add(ticker_)
            session.commit()

        # make sure we don't duplicate the data
        times = [bar["t"] for bar in result.json()["results"]]
        statement = select(AggregateCandle).where(
            AggregateCandle.ticker_id == ticker_.symbol
            and AggregateCandle.time in times
        )
        existing = session.exec(statement).all()

        for bar in result.json()["results"]:
            if bar["t"] in [candle.time for candle in existing]:
                continue

            data = map_results_for_bar(bar)
            data["timespan"] = settings.timespan
            data["ticker_id"] = ticker_.symbol
            session.add(AggregateCandle(**data))
        session.commit()

    return result.json()
