"""Tests for HiveMindHTTPClient outbound BUS serialization."""

import json
from threading import Event
from unittest.mock import MagicMock, patch

from ovos_bus_client.message import Message

from hivemind_bus_client.http_client import HiveMindHTTPClient


def _bare_http_client():
    client = object.__new__(HiveMindHTTPClient)
    identity = MagicMock()
    identity.name = "http-test-client"
    identity.access_key = "test-key"
    identity.site_id = "test-site"
    client.identity = identity
    client._host = "http://localhost"
    client._port = 5678
    client.connected = Event()
    client.connected.set()
    client.session_id = "test-session"
    client.protocol = MagicMock(binarize=False)
    client.compress = False
    client.binarize = False
    client.crypto_key = None
    return client


def test_modern_bus_topic_uses_one_legacy_http_message():
    client = _bare_http_client()

    with patch("hivemind_bus_client.http_client.requests.post") as post:
        client.emit(
            Message(
                "ovos.utterance.handle",
                {"utterances": ["Quelle heure est-il?"], "lang": "fr-fr"},
            )
        )

    post.assert_called_once()
    wire = json.loads(post.call_args.kwargs["data"]["message"])
    assert wire["payload"]["type"] == "recognizer_loop:utterance"
    assert wire["payload"]["data"]["utterances"] == [
        "Quelle heure est-il?",
    ]
