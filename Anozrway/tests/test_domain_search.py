from unittest.mock import MagicMock

import pytest
import requests_mock

from anozrway_modules import AnozrwayModule
from anozrway_modules.domain_search import DomainSearch, OAuth2ClientCredentials


def configured_action(symphony_storage):
    module = AnozrwayModule()
    action = DomainSearch(module=module, data_path=symphony_storage)
    action.module.configuration = {
        "anozrway_client_id": "client-id",
        "anozrway_client_secret": "client-secret",
        "anozrway_token_url": "https://auth.anozrway.test/oauth2/token",
        "anozrway_base_url": "https://balise.anozrway.test",
        "timeout_seconds": 5,
    }
    return action


def test_oauth2_get_token():
    with requests_mock.Mocker() as mock:
        mock.register_uri(
            "POST",
            "https://auth.anozrway.test/oauth2/token",
            json={"access_token": "token-123"},
        )

        token = OAuth2ClientCredentials(
            token_url="https://auth.anozrway.test/oauth2/token",
            client_id="client-id",
            client_secret="client-secret",
            timeout=5,
        ).get_token()

        assert token == "token-123"


def test_oauth2_get_token_missing_access_token():
    with requests_mock.Mocker() as mock:
        mock.register_uri(
            "POST",
            "https://auth.anozrway.test/oauth2/token",
            json={},
        )

        creds = OAuth2ClientCredentials(
            token_url="https://auth.anozrway.test/oauth2/token",
            client_id="client-id",
            client_secret="client-secret",
            timeout=5,
        )

        with pytest.raises(RuntimeError):
            creds.get_token()


def test_domain_search_action(symphony_storage):
    action = configured_action(symphony_storage)
    with requests_mock.Mocker() as mock:
        mock.register_uri(
            "POST",
            "https://auth.anozrway.test/oauth2/token",
            json={
                "access_token": "token-123",
                "token_type": "bearer",
                "expires_in": 1799,
            },
        )
        mock.register_uri(
            "POST",
            "https://balise.anozrway.test/v1/domain/searches",
            json={"results": [{"id": 1}, {"id": 2}]},
        )

        result = action.run(
            {
                "context": "ctx",
                "domain": "example.com",
                "start_date": "2024-01-01T00:00:00Z",
                "end_date": "2024-01-02T00:00:00Z",
            }
        )

        assert result == {"count": 2, "results": [{"id": 1}, {"id": 2}]}
        assert mock.call_count == 2
        assert mock.request_history[1].json() == {
            "context": "ctx",
            "domain": "example.com",
            "start_date": "2024-01-01T00:00:00Z",
            "end_date": "2024-01-02T00:00:00Z",
        }


def test_domain_search_missing_domain(symphony_storage):
    action = configured_action(symphony_storage)
    action.error = MagicMock()

    with requests_mock.Mocker() as mock:
        mock.register_uri(
            "POST",
            "https://auth.anozrway.test/oauth2/token",
            json={
                "access_token": "token-123",
                "token_type": "bearer",
                "expires_in": 1799,
            },
        )

        result = action.run({"context": "ctx"})

    assert result == {"count": 0, "results": []}
    action.error.assert_called_once()
