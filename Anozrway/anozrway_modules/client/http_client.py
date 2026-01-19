from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import aiohttp


@dataclass
class AnozrwayCredentials:
    client_id: str
    client_secret: str
    token_url: str
    base_url: str


class AnozrwayClient:
    def __init__(self, credentials: AnozrwayCredentials, timeout_seconds: int = 30):
        self.creds = credentials
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._session: Optional[aiohttp.ClientSession] = None
        self._token: Optional[str] = None

    async def __aenter__(self) -> "AnozrwayClient":
        self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session:
            await self._session.close()

    async def _get_token(self) -> str:
        if not self._session:
            raise RuntimeError("ClientSession not initialized")

        async with self._session.post(
            self.creds.token_url,
            data={
                "client_id": self.creds.client_id,
                "client_secret": self.creds.client_secret,
                "grant_type": "client_credentials",
            },
        ) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"OAuth2 token request failed: {resp.status} {text}")

            data = await resp.json()
            token = data.get("access_token")
            if not token:
                raise RuntimeError(f"OAuth2 response missing access_token: {data}")
            return token

    async def _auth_header(self) -> Dict[str, str]:
        if not self._token:
            self._token = await self._get_token()
        return {"authorization": f"Bearer {self._token}"}

    async def domain_search(
        self,
        context: str,
        domain: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        POST {base_url}/v1/domain/searches
        Returns: list of results (leaks)
        """
        if not self._session:
            raise RuntimeError("ClientSession not initialized")

        url = self.creds.base_url.rstrip("/") + "/v1/domain/searches"
        payload: Dict[str, Any] = {"context": context, "domain": domain}
        if start_date:
            payload["start_date"] = start_date
        if end_date:
            payload["end_date"] = end_date

        headers = {"Content-Type": "application/json"}
        headers.update(await self._auth_header())

        async with self._session.post(url, json=payload, headers=headers) as resp:
            text = await resp.text()
            if resp.status == 401:
                # token might be expired/invalid -> refresh once
                self._token = None
                headers.update(await self._auth_header())
                async with self._session.post(url, json=payload, headers=headers) as resp2:
                    text2 = await resp2.text()
                    if resp2.status >= 400:
                        raise RuntimeError(f"Anozrway domain_search failed: {resp2.status} {text2}")
                    data2 = await resp2.json()
                    return data2.get("results", [])

            if resp.status >= 400:
                raise RuntimeError(f"Anozrway domain_search failed: {resp.status} {text}")

            data = await resp.json()
            return data.get("results", [])
