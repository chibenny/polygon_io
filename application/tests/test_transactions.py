"""Learn about database transactions in SQLAlcheModel and FasAPI."""

import datetime
import json
import random
import uuid
from unittest import mock

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlmodel import Session, select

from ..main import app
from ..models import AggregateCandle

client = TestClient(app)


def set_up():
    pass


def tear_down():
    pass


def to_float(low: int, high: int, precision: int = 2) -> float:
    return float(f"{random.uniform(low, high):.{precision}f}")


def make_response(days: int = 0) -> dict:
    high = to_float(1, 100)
    low = to_float(1, high)

    def make_result(days) -> dict:
        time_ = datetime.datetime.now() + datetime.timedelta(days=days)
        return {
            "o": to_float(low, high),
            "c": to_float(low, high),
            "h": high,
            "l": low,
            "v": random.randint(10**5, 10**6),
            "vw": to_float(low, high, 4),
            "t": int(time_.timestamp() * 1000),  # Unix timestamp in milliseconds
        }

    data = {
        "ticker": "".join(
            [chr(random.randint(65, 90)) for _ in range(random.randint(3, 4))]
        ),
        "queryCount": 30,
        "resultsCount": 30,
        "adjusted": True,
        "results": [],
        "status": "OK",
        "request_id": uuid.uuid4().hex,
        "count": 30,
    }

    results = []
    for i in range(data["count"]):
        results.append(make_result(i))

    data["results"] = results

    return data


def test_make_response():
    for i in range(10):
        response = make_response(days=i)
        assert "ticker" in response
        assert len(response["ticker"]) in [3, 4]
        assert all([c.isupper() for c in response["ticker"]])
        assert response["results"][0]["t"] < response["results"][-1]["t"]


class MockResponse:
    def __init__(self, json_data: dict, status_code: int):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return json.loads(self.json_data)


def test_get_candles():
    """Do we get a candles loaded from \"results\" and
    do we inhibit duplicates?
    """
    response_data = make_response()
    with mock.patch("polygon_io.main._call_polygon_api") as mock_call:
        mock_call.return_value = MockResponse(
            json_data=json.dumps(response_data), status_code=200
        )
        ticker = response_data["ticker"]

        # Start and end dates are arbitrary
        # They're not relevant to the test
        start = "2021-01-01"
        end = "2021-01-02"
        response = client.get(f"/bars/{ticker}/{start}/{end}")
        assert response.status_code == 200

        # Check the database
        sqlite_url: str = "sqlite:///./polygon_io.db"
        connect_args = {"check_same_thread": False}
        engine = create_engine(sqlite_url, echo=True, connect_args=connect_args)
        with Session(engine) as session:
            stmt = select(AggregateCandle).where(AggregateCandle.ticker_id == ticker)
            candles = session.exec(stmt).all()
            assert len(candles) == response_data["count"]
