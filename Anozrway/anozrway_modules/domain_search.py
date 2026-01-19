from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import requests
from sekoia_automation.action import Action


@dataclass
class OAuth2ClientCredentials:
    token_url: str
    client_id: str
    client_secret: str
    timeout: int = 30

    def get_token(self) -> str:
        r = requests.post(
            self.token_url,
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
            timeout=self.timeout,
        )
        r.raise_for_status()
        data = r.json()
        token = data.get("access_token")
        if not token:
            raise RuntimeError(f"OAuth2 response missing access_token: {data}")
        return token


class DomainSearch(Action):
    """
    POST /v1/domain/searches
    """

    def run(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
        cfg = self.module.configuration

        client_id = cfg["anozrway_client_id"]
        client_secret = cfg["anozrway_client_secret"]
        token_url = cfg.get("anozrway_token_url", "https://auth.anozrway.com/oauth2/token")
        base_url = cfg.get("anozrway_base_url", "https://balise.anozrway.com").rstrip("/")
        timeout = int(cfg.get("timeout_seconds", 30))

        token = OAuth2ClientCredentials(
            token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
            timeout=timeout,
        ).get_token()

        domain = arguments.get("domain")
        if not domain:
            raise ValueError("Missing required argument: domain")

        payload: Dict[str, Any] = {
            "context": arguments.get("context") or "demo",
            "domain": domain,
        }
        if arguments.get("start_date"):
            payload["start_date"] = arguments["start_date"]
        if arguments.get("end_date"):
            payload["end_date"] = arguments["end_date"]

        url = f"{base_url}/v1/domain/searches"
        r = requests.post(
            url,
            headers={
                "Content-Type": "application/json",
                "authorization": f"Bearer {token}",
            },
            json=payload,
            timeout=timeout,
        )
        r.raise_for_status()
        data = r.json()

        results: List[Dict[str, Any]] = data.get("results") or []
        return {"count": len(results), "results": results}
