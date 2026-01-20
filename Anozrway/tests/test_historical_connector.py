import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from anozrway_modules import AnozrwayModule
from anozrway_modules.historical_connector import (
    AnozrwayHistoricalConfiguration,
    AnozrwayHistoricalConnector,
)


def configured_connector(symphony_storage):
    AnozrwayHistoricalConnector.__annotations__["configuration"] = AnozrwayHistoricalConfiguration

    module = AnozrwayModule()
    module.configuration = {
        "anozrway_client_id": "client-id",
        "anozrway_client_secret": "client-secret",
    }

    module_config = AnozrwayHistoricalConfiguration(
        intake_key="key",
        domains="example.com",
        lookback_days=1,
        window_seconds=3600,
        chunk_size=2,
        frequency=60,
    )
    module.load_config = MagicMock(return_value=module_config)

    connector = AnozrwayHistoricalConnector(module=module, data_path=symphony_storage)
    connector.configuration = module_config
    connector.log = MagicMock()
    connector.log_exception = MagicMock()
    return connector


@pytest.fixture
def connector(symphony_storage):
    return configured_connector(symphony_storage)


def test_parse_domains(connector):
    connector.configuration.domains = " example.com , test.com, ,"
    assert connector._parse_domains() == ["example.com", "test.com"]


def test_last_checkpoint_default(connector):
    before = datetime.now(timezone.utc) - timedelta(days=1, seconds=5)
    result = connector.last_checkpoint()
    after = datetime.now(timezone.utc)

    assert before <= result <= after


def test_last_checkpoint_from_context(connector):
    with connector.context_store as c:
        c["last_checkpoint"] = "2024-01-01T00:00:00Z"

    result = connector.last_checkpoint()
    assert result == datetime(2024, 1, 1, tzinfo=timezone.utc)


def test_save_checkpoint_sets_context(monkeypatch, connector):
    gauge = MagicMock()
    monkeypatch.setattr("anozrway_modules.historical_connector.checkpoint_age", gauge)

    checkpoint = datetime(2024, 1, 1, tzinfo=timezone.utc)
    connector.save_checkpoint(checkpoint)

    with connector.context_store as c:
        assert c["last_checkpoint"] == "2024-01-01T00:00:00Z"
        assert "last_successful_run" in c

    gauge.set.assert_called_once()


def test_cleanup_event_cache(connector):
    old = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat().replace("+00:00", "Z")
    fresh = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    with connector.event_cache_store as s:
        s["old"] = old
        s["fresh"] = fresh
        s["invalid"] = "not-a-date"

    connector._cleanup_event_cache()

    with connector.event_cache_store as s:
        assert "old" not in s
        assert "invalid" not in s
        assert "fresh" in s


def test_is_new_event_and_duplicate(monkeypatch, connector):
    duplicated = MagicMock()
    monkeypatch.setattr("anozrway_modules.historical_connector.events_duplicated", duplicated)

    event = {
        "email": "user@example.com",
        "source": {"name": "leak", "type": "db", "detection_date": "2024-01-01T00:00:00Z"},
    }
    key = connector._compute_dedup_key("example.com", event)
    assert connector._is_new_event(key) is True
    assert connector._is_new_event(key) is False
    duplicated.inc.assert_called_once()


def test_extract_event_ts_priority(connector):
    event = {
        "source": {
            "detection_date": "2024-01-02T00:00:00Z",
            "date": "2024-01-01T00:00:00Z",
        }
    }
    ts = connector._extract_event_ts(event)
    assert ts == datetime(2024, 1, 2, tzinfo=timezone.utc)


def test_fetch_events_no_domains(connector):
    connector.configuration.domains = ""
    client = MagicMock()

    async def collect():
        return [batch async for batch in connector.fetch_events(client)]

    results = asyncio.run(collect())
    assert results == []
    connector.log.assert_called_with(message="No domains configured. Nothing to collect.", level="warning")


def test_fetch_events_checkpoint_up_to_date(connector):
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    with connector.context_store as c:
        c["last_checkpoint"] = now

    client = MagicMock()
    async def collect():
        return [batch async for batch in connector.fetch_events(client)]

    results = asyncio.run(collect())
    assert results == []
    connector.log.assert_called_with(message="Checkpoint is up-to-date. No new data to collect.", level="info")


def test_fetch_events_batches_and_checkpoint(connector):
    start = datetime.now(timezone.utc) - timedelta(minutes=10)
    with connector.context_store as c:
        c["last_checkpoint"] = start.isoformat().replace("+00:00", "Z")

    connector.configuration.domains = "example.com, test.com"
    connector.configuration.chunk_size = 1

    records = [
        {
            "email": "a@example.com",
            "source": {"name": "leak", "type": "db", "detection_date": "2024-01-01T00:00:00Z"},
        },
        {
            "email": "b@example.com",
            "source": {"name": "leak", "type": "db", "detection_date": "2024-01-01T00:00:01Z"},
        },
    ]
    client = MagicMock()
    client.search_domain_v1 = AsyncMock(side_effect=[records, []])

    async def collect():
        return [batch async for batch in connector.fetch_events(client)]

    results = asyncio.run(collect())

    assert results == [
        [
            {
                "email": "a@example.com",
                "source": {"name": "leak", "type": "db", "detection_date": "2024-01-01T00:00:00Z"},
                "_searched_domain": "example.com",
                "_context": "demo",
            }
        ],
        [
            {
                "email": "b@example.com",
                "source": {"name": "leak", "type": "db", "detection_date": "2024-01-01T00:00:01Z"},
                "_searched_domain": "example.com",
                "_context": "demo",
            }
        ],
    ]
    assert client.search_domain_v1.call_count == 2

    with connector.context_store as c:
        assert "last_checkpoint" in c


def test_fetch_events_continues_on_error(connector):
    start = datetime.now(timezone.utc) - timedelta(minutes=10)
    with connector.context_store as c:
        c["last_checkpoint"] = start.isoformat().replace("+00:00", "Z")

    connector.configuration.domains = "fail.com, ok.com"
    connector.configuration.chunk_size = 10

    client = MagicMock()
    client.search_domain_v1 = AsyncMock(
        side_effect=[
            RuntimeError("boom"),
            [
                {
                    "email": "ok@example.com",
                    "source": {"name": "leak", "type": "db", "detection_date": "2024-01-01T00:00:00Z"},
                }
            ],
        ]
    )

    async def collect():
        return [batch async for batch in connector.fetch_events(client)]

    results = asyncio.run(collect())

    assert results == [
        [
            {
                "email": "ok@example.com",
                "source": {"name": "leak", "type": "db", "detection_date": "2024-01-01T00:00:00Z"},
                "_searched_domain": "ok.com",
                "_context": "demo",
            }
        ]
    ]
    connector.log_exception.assert_called()


def test_fetch_events_empty_records_no_checkpoint(connector):
    start = datetime.now(timezone.utc) - timedelta(minutes=10)
    with connector.context_store as c:
        c["last_checkpoint"] = start.isoformat().replace("+00:00", "Z")

    connector.configuration.domains = "example.com"
    client = MagicMock()
    client.search_domain_v1 = AsyncMock(return_value=[])
    connector.save_checkpoint = MagicMock()

    async def collect():
        return [batch async for batch in connector.fetch_events(client)]

    results = asyncio.run(collect())

    assert results == []
    connector.save_checkpoint.assert_not_called()


def test_fetch_events_invalid_added_date(connector):
    start = datetime.now(timezone.utc) - timedelta(minutes=10)
    with connector.context_store as c:
        c["last_checkpoint"] = start.isoformat().replace("+00:00", "Z")

    connector.configuration.domains = "example.com"
    connector.configuration.chunk_size = 10

    client = MagicMock()
    client.search_domain_v1 = AsyncMock(
        return_value=[
            {
                "email": "bad@example.com",
                "source": {"name": "leak", "type": "db", "detection_date": "not-a-date"},
            },
        ]
    )

    async def collect():
        return [batch async for batch in connector.fetch_events(client)]

    results = asyncio.run(collect())

    assert results == [
        [
            {
                "email": "bad@example.com",
                "source": {"name": "leak", "type": "db", "detection_date": "not-a-date"},
                "_searched_domain": "example.com",
                "_context": "demo",
            }
        ]
    ]


def test_fetch_events_same_added_date_no_max_update(connector):
    start = datetime.now(timezone.utc) - timedelta(minutes=10)
    with connector.context_store as c:
        c["last_checkpoint"] = start.isoformat().replace("+00:00", "Z")

    connector.configuration.domains = "example.com"
    connector.configuration.chunk_size = 10

    records = [
        {
            "email": "a@example.com",
            "source": {"name": "leak", "type": "db", "detection_date": "2024-01-01T00:00:00Z"},
        },
        {
            "email": "b@example.com",
            "source": {"name": "leak", "type": "db", "detection_date": "2024-01-01T00:00:00Z"},
        },
    ]
    client = MagicMock()
    client.search_domain_v1 = AsyncMock(return_value=records)

    async def collect():
        return [batch async for batch in connector.fetch_events(client)]

    results = asyncio.run(collect())

    assert results == [
        [
            {
                "email": "a@example.com",
                "source": {"name": "leak", "type": "db", "detection_date": "2024-01-01T00:00:00Z"},
                "_searched_domain": "example.com",
                "_context": "demo",
            },
            {
                "email": "b@example.com",
                "source": {"name": "leak", "type": "db", "detection_date": "2024-01-01T00:00:00Z"},
                "_searched_domain": "example.com",
                "_context": "demo",
            },
        ]
    ]


def test_next_batch_uses_client(monkeypatch, connector):
    async def fake_fetch_events(_client):
        yield [{"leak_name": "l1", "added_date": "2024-01-01T00:00:00Z"}]

    dummy_client = MagicMock()
    dummy_client.__aenter__ = AsyncMock(return_value=dummy_client)
    dummy_client.__aexit__ = AsyncMock(return_value=None)
    monkeypatch.setattr("anozrway_modules.historical_connector.AnozrwayClient", lambda *args, **kwargs: dummy_client)
    monkeypatch.setattr(connector, "fetch_events", fake_fetch_events)

    async def collect():
        return [batch async for batch in connector.next_batch()]

    results = asyncio.run(collect())
    assert results == [[{"leak_name": "l1", "added_date": "2024-01-01T00:00:00Z"}]]


def test_async_run_pushes_batches(monkeypatch, connector):
    async def fake_next_batch():
        connector._stop_event.set()
        yield [{"leak_name": "l1", "added_date": "2024-01-01T00:00:00Z"}]

    monkeypatch.setattr(connector, "next_batch", fake_next_batch)
    monkeypatch.setattr("anozrway_modules.historical_connector.sleep", AsyncMock())
    connector.push_data_to_intakes = AsyncMock()
    connector._stop_event.clear()

    asyncio.run(connector._async_run())

    connector.push_data_to_intakes.assert_called_once()


def test_async_run_handles_error(monkeypatch, connector):
    async def bad_next_batch():
        raise RuntimeError("boom")
        yield []  # pragma: no cover

    async def stop_sleep(_):
        connector._stop_event.set()
        return None

    monkeypatch.setattr(connector, "next_batch", bad_next_batch)
    monkeypatch.setattr("anozrway_modules.historical_connector.sleep", stop_sleep)
    connector._stop_event.clear()

    asyncio.run(connector._async_run())

    connector.log_exception.assert_called()


def test_async_run_auth_error(monkeypatch, connector):
    from anozrway_modules.client.errors import AnozrwayAuthError

    async def bad_next_batch():
        raise AnozrwayAuthError("nope")
        yield []  # pragma: no cover

    connector._stop_event.clear()
    monkeypatch.setattr(connector, "next_batch", bad_next_batch)

    with pytest.raises(AnozrwayAuthError):
        asyncio.run(connector._async_run())
