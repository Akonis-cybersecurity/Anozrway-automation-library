from __future__ import annotations

import asyncio
import json
import signal
from asyncio import sleep
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional

from sekoia_automation.connector import DefaultConnectorConfiguration
from sekoia_automation.aio.connector import AsyncConnector
from sekoia_automation.storage import PersistentJSON

from anozrway_modules.client.http_client import AnozrwayClient, AnozrwayCredentials


class AnozrwayDomainSearchConfiguration(DefaultConnectorConfiguration):
    frequency: int = 600
    chunk_size: int = 500
    context: str = "demo"
    domains: List[str] = []
    lookback_hours_on_first_run: int = 24
    output_file: str = "anozrway_results.jsonl"


class AnozrwayDomainSearchConnector(AsyncConnector):
    # must match connector_*.json docker_parameters
    name = "anozrway_domain_search"
    configuration: AnozrwayDomainSearchConfiguration

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.context_store = PersistentJSON("context.json", self._data_path)
        self.event_cache_store = PersistentJSON("event_cache.json", self._data_path)
        self.event_cache_ttl = timedelta(hours=48)

        self.log(
            message=(
                "AnozrwayDomainSearchConnector initialized - "
                f"Data path: {self._data_path}, Frequency: {self.configuration.frequency}s, "
                f"Chunk size: {self.configuration.chunk_size}, Domains: {len(self.configuration.domains)}"
            ),
            level="info",
        )

    def _cleanup_event_cache(self) -> None:
        cutoff = datetime.now(timezone.utc) - self.event_cache_ttl
        with self.event_cache_store as s:
            keys = list(s.keys())
            for k in keys:
                try:
                    ts = datetime.fromisoformat(str(s[k]).replace("Z", "+00:00")).astimezone(timezone.utc)
                    if ts < cutoff:
                        del s[k]
                except Exception:
                    del s[k]

    def _event_key(self, domain: str, item: Dict[str, Any]) -> str:
        src = item.get("source") or {}
        raw = {
            "domain": domain,
            "email": item.get("email"),
            "username": item.get("username"),
            "hash": item.get("hash"),
            "public_url": item.get("public_url"),
            "source_name": src.get("name"),
            "source_type": src.get("type"),
            "source_date": src.get("date"),
            "detection_date": src.get("detection_date"),
        }
        return sha256(str(raw).encode("utf-8")).hexdigest()

    def _is_new_event(self, key: str) -> bool:
        with self.event_cache_store as s:
            if key in s:
                return False
            s[key] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            return True

    def last_event_date(self) -> datetime:
        default_start = datetime.now(timezone.utc) - timedelta(hours=int(self.configuration.lookback_hours_on_first_run))
        with self.context_store as c:
            ts = c.get("last_collection_end_time")
            if ts:
                return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
        return default_start

    def save_checkpoint(self, last_event_date: datetime) -> None:
        checkpoint_time = last_event_date.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        with self.context_store as c:
            c["last_collection_end_time"] = checkpoint_time
            c["last_successful_run"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    async def fetch_events(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        self._cleanup_event_cache()

        from_time = self.last_event_date()
        to_time = datetime.now(timezone.utc) - timedelta(minutes=2)
        self.save_checkpoint(to_time)

        start_date = from_time.isoformat().replace("+00:00", "Z")
        end_date = to_time.isoformat().replace("+00:00", "Z")

        cfg = self.module.configuration
        creds = AnozrwayCredentials(
            client_id=cfg["anozrway_client_id"],
            client_secret=cfg["anozrway_client_secret"],
            token_url=cfg.get("anozrway_token_url", "https://auth.anozrway.com/oauth2/token"),
            base_url=cfg.get("anozrway_base_url", "https://balise.anozrway.com"),
        )

        batch: List[Dict[str, Any]] = []

        async with AnozrwayClient(creds) as client:
            for domain in self.configuration.domains:
                self.log(message=f"Querying Anozrway domain={domain} [{start_date} -> {end_date}]", level="info")

                try:
                    results = await client.domain_search(
                        context=self.configuration.context,
                        domain=domain,
                        start_date=start_date,
                        end_date=end_date,
                    )
                except Exception as e:
                    self.log_exception(e, message=f"Failed domain_search for domain={domain}")
                    continue

                for item in results or []:
                    key = self._event_key(domain, item)
                    if not self._is_new_event(key):
                        continue

                    event = {
                        "vendor": "anozrway",
                        "type": "domain_search_result",
                        "domain": domain,
                        "collected_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                        "result": item,
                    }

                    batch.append(event)

                    if len(batch) >= self.configuration.chunk_size:
                        chunk = batch[: self.configuration.chunk_size]
                        batch = batch[self.configuration.chunk_size :]
                        yield chunk

                # respect documented rate limit: 1 req/s
                await asyncio.sleep(1)

        if batch:
            yield batch

    async def next_batch(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        async for batch in self.fetch_events():
            yield batch

    def _write_batch_to_file(self, batch: List[Dict[str, Any]]) -> None:
        out_path = Path(self._data_path) / self.configuration.output_file
        out_path.parent.mkdir(parents=True, exist_ok=True)

        with out_path.open("a", encoding="utf-8") as f:
            for evt in batch:
                f.write(json.dumps(evt, ensure_ascii=False) + "\n")

        self.log(message=f"Wrote {len(batch)} events to {out_path}", level="info")

    def run(self):  # pragma: no cover
        loop = asyncio.get_event_loop()

        def handle_stop_signal():
            loop.create_task(self.shutdown())

        loop.add_signal_handler(signal.SIGTERM, handle_stop_signal)
        loop.add_signal_handler(signal.SIGINT, handle_stop_signal)

        try:
            loop.run_until_complete(self._async_run())
        finally:
            loop.close()

    async def _async_run(self):
        while self.running:
            try:
                total = 0
                async for batch in self.next_batch():
                    total += len(batch)
                    self._write_batch_to_file(batch)

                self.log(message=f"Iteration done - total new events: {total}", level="info")
                await sleep(self.configuration.frequency)

            except Exception as e:
                self.log_exception(e, message="Error in connector loop - retry in 60s")
                await sleep(60)
