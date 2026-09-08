import json
import time
import uuid

import click
from ovos_bus_client import Message
from ovos_utils.log import LOG
from ovos_utils.fakebus import FakeBus
from hivemind_bus_client.hive_map import HiveMapper
from hivemind_bus_client.client import HiveMessageBusClient
from hivemind_bus_client.message import HiveMessage, HiveMessageType
from hivemind_bus_client.identity import NodeIdentity
LOG.set_level("ERROR")


@click.group()
def hmclient_cmds():
    pass


@hmclient_cmds.command(help="persist node identity / credentials", name="set-identity")
@click.option("--key", help="HiveMind access key", type=str, default="")
@click.option("--password", help="HiveMind password", type=str, default="")
@click.option("--host", help="default host for hivemind-core", type=str, default="")
@click.option("--port", help="default port for hivemind-core", type=int, required=False)
@click.option("--siteid", help="location identifier for message.context", type=str, default="")
def identity_set(key: str, password: str, host: str, port: int, siteid: str):
    if not key and not password and not siteid:
        raise ValueError("please set at least one of key/password/siteid/host")
    identity = NodeIdentity()
    identity.password = password or identity.password
    identity.access_key = key or identity.access_key
    identity.site_id = siteid or identity.site_id
    identity.default_port = port or identity.default_port or 5678
    host = host or identity.default_master
    if not host.startswith("ws://") and not host.startswith("wss://"):
        host = "ws://" + host
    identity.default_master = host
    if not identity.public_key:
        identity.create_keys()
        print("PUBKEY:", identity.public_key)
    identity.save()
    print(f"identity saved: {identity.IDENTITY_FILE.path}")


@hmclient_cmds.command(help="simple cli interface to inject utterances and print speech", name="terminal")
@click.option("--key", help="HiveMind access key (default read from identity file)", type=str, default="")
@click.option("--password", help="HiveMind password (default read from identity file)", type=str, default="")
@click.option("--host", help="HiveMind host (default read from identity file)", type=str, default="")
@click.option("--port", help="HiveMind port number (default: 5678)", type=int, required=False)
@click.option("--siteid", help="location identifier for message.context  (default read from identity file)", type=str, default="")
def terminal(key: str, password: str, host: str, port: int, siteid: str):
    identity = NodeIdentity()
    password = password or identity.password
    key = key or identity.access_key
    host = host or identity.default_master
    siteid = siteid or identity.site_id or "unknown"
    port = port or identity.default_port or 5678

    if not host.startswith("ws://") and not host.startswith("wss://"):
        host = "ws://" + host

    if not key or not password or not host:
        raise RuntimeError("NodeIdentity not set, please pass key/password/host or "
                           "call 'hivemind-client set-identity'")

    node = HiveMessageBusClient(key, host=host, port=port, password=password)
    node.connect(FakeBus(), site_id=siteid)

    # node.connected_event.wait()
    print("== connected to HiveMind")

    def handle_speak(message: Message):
        utt = message.data["utterance"]
        print("> ", utt)

    node.on_mycroft("speak", handle_speak)

    while True:
        try:
            utt = input("Utterance:")
            node.emit_mycroft(
                Message("recognizer_loop:utterance",
                        {"utterances": [utt]},
                        {"destination": "skills"})
            )
        except KeyboardInterrupt:
            break
        except Exception:
            LOG.exception("error")
            break

    node.close()


@hmclient_cmds.command(help="send a single mycroft message",
                       name="send-mycroft")
@click.option("--key", help="HiveMind access key (default read from identity file)", type=str, default="")
@click.option("--password", help="HiveMind password (default read from identity file)", type=str, default="")
@click.option("--host", help="HiveMind host (default read from identity file)", type=str, default="")
@click.option("--port", help="HiveMind port number (default: 5678)", type=int, required=False)
@click.option("--siteid", help="location identifier for message.context  (default read from identity file)", type=str, default="")
@click.option("--msg", help="ovos message type to inject", type=str)
@click.option("--payload", help="ovos message.data json", type=str)
def send_mycroft(key: str, password: str, host: str, port: int, siteid: str, msg: str, payload: str):
    node = HiveMessageBusClient(key, host=host, port=port, password=password)
    node.connect(FakeBus(), site_id=siteid)

    node.connected_event.wait()
    print("== connected to HiveMind")

    node.emit_mycroft(Message(msg, json.loads(payload)))

    node.close()


@hmclient_cmds.command(help="escalate a single mycroft message",
                       name="escalate")
@click.option("--key", help="HiveMind access key (default read from identity file)", type=str, default="")
@click.option("--password", help="HiveMind password (default read from identity file)", type=str, default="")
@click.option("--host", help="HiveMind host (default read from identity file)", type=str, default="")
@click.option("--port", help="HiveMind port number (default: 5678)", type=int, required=False)
@click.option("--siteid", help="location identifier for message.context  (default read from identity file)", type=str, default="")
@click.option("--msg", help="ovos message type to inject", type=str)
@click.option("--payload", help="ovos message.data json", type=str)
def escalate(key: str, password: str, host: str, port: int, siteid: str, msg: str, payload: str):
    identity = NodeIdentity()
    password = password or identity.password
    key = key or identity.access_key
    host = host or identity.default_master
    siteid = siteid or identity.site_id or "unknown"
    port = port or identity.default_port or 5678

    if not host.startswith("ws://") and not host.startswith("wss://"):
        host = "ws://" + host

    if not key or not password or not host:
        raise RuntimeError("NodeIdentity not set, please pass key/password/host or "
                           "call 'hivemind-client set-identity'")

    node = HiveMessageBusClient(key, host=host, port=port, password=password)
    node.connect(FakeBus(), site_id=siteid)

    node.connected_event.wait()
    print("== connected to HiveMind")

    hm = HiveMessage(HiveMessageType.ESCALATE,
                     Message(msg, json.loads(payload)))
    node.emit(hm)

    node.close()


@hmclient_cmds.command(help="propagate a single mycroft message",
                       name="propagate")
@click.option("--key", help="HiveMind access key (default read from identity file)", type=str, default="")
@click.option("--password", help="HiveMind password (default read from identity file)", type=str, default="")
@click.option("--host", help="HiveMind host (default read from identity file)", type=str, default="")
@click.option("--port", help="HiveMind port number (default: 5678)", type=int, required=False)
@click.option("--siteid", help="location identifier for message.context  (default read from identity file)", type=str, default="")
@click.option("--msg", help="ovos message type to inject", type=str)
@click.option("--payload", help="ovos message.data json", type=str)
def propagate(key: str, password: str, host: str, port: int, siteid: str, msg: str, payload: str):
    identity = NodeIdentity()
    password = password or identity.password
    key = key or identity.access_key
    host = host or identity.default_master
    siteid = siteid or identity.site_id or "unknown"
    port = port or identity.default_port or 5678

    if not host.startswith("ws://") and not host.startswith("wss://"):
        host = "ws://" + host

    if not key or not password or not host:
        raise RuntimeError("NodeIdentity not set, please pass key/password/host or "
                           "call 'hivemind-client set-identity'")

    node = HiveMessageBusClient(key, host=host, port=port, password=password)
    node.connect(FakeBus(), site_id=siteid)

    node.connected_event.wait()
    print("== connected to HiveMind")

    hm = HiveMessage(HiveMessageType.PROPAGATE,
                     Message(msg, json.loads(payload)))
    node.emit(hm)

    node.close()


@hmclient_cmds.command(help="test if Identity file can connect to HiveMind",
                       name="test-identity")
def test_identity():
    node = HiveMessageBusClient()
    node.connect(FakeBus())

    node.connected_event.wait()
    print("== Identity successfully connected to HiveMind!")

    node.close()


@hmclient_cmds.command(help="recreate the private RSA key for inter-node communication", name="reset-pgp")
def reset_keys():
    identity = NodeIdentity()
    identity.create_keys()
    print("PUBKEY:", identity.public_key)
    identity.save()
    print(f"identity saved: {identity.IDENTITY_FILE.path}")


@hmclient_cmds.command(
    help="Send a PING and display the reachable hive map.",
    name="ping",
)
@click.option("--key", help="HiveMind access key (default read from identity file)", type=str, default="")
@click.option("--password", help="HiveMind password (default read from identity file)", type=str, default="")
@click.option("--host", help="HiveMind host (default read from identity file)", type=str, default="")
@click.option("--port", help="HiveMind port number (default: 5678)", type=int, required=False)
@click.option("--siteid", help="location identifier (default read from identity file)", type=str, default="")
@click.option("--timeout", help="seconds to collect responses (default: 5.0)", type=float, default=5.0)
@click.option("--json", "output_json", is_flag=True, default=False, help="output raw JSON topology")
def ping(key: str, password: str, host: str, port: int, siteid: str,
         timeout: float, output_json: bool):
    """Send a PING flood and collect responsive PINGs to map the hive topology."""
    identity = NodeIdentity()
    password = password or identity.password
    key = key or identity.access_key
    host = host or identity.default_master
    siteid = siteid or identity.site_id or "unknown"
    port = port or identity.default_port or 5678

    if not key or not password or not host:
        raise RuntimeError("NodeIdentity not set, please pass key/password/host or "
                           "call 'hivemind-client set-identity'")

    if not host.startswith("ws://") and not host.startswith("wss://"):
        host = "ws://" + host

    mapper = HiveMapper()
    flood_id = str(uuid.uuid4())
    my_peer = f"{identity.name or 'hivemind-client'}::{flood_id[:8]}"

    node = HiveMessageBusClient(key, host=host, port=port, password=password)
    node.connect(FakeBus(), site_id=siteid)
    if not node.connected_event.wait(timeout=10):
        print("[ERROR] Failed to connect to HiveMind within 10 seconds")
        return
    print(f"== connected to HiveMind, sending PING (timeout={timeout}s)")

    mapper.start_ping(flood_id)

    def on_ping_response(outer: HiveMessage):
        """Handle a PROPAGATE message whose inner payload is a responsive PING."""
        inner = outer.payload
        if not isinstance(inner, HiveMessage):
            return
        if inner.msg_type != HiveMessageType.PING:
            return
        ping_data = inner.payload
        if not isinstance(ping_data, dict):
            return
        if ping_data.get("flood_id") != flood_id:
            return
        # Transfer route from outer PROPAGATE to inner PING so HiveMapper can read it
        inner.replace_route(outer.route)
        if mapper.on_ping(inner, received_at=time.time()):
            peer = ping_data.get("peer", "?")
            site = ping_data.get("site_id", "")
            site_str = f"  site={site}" if site else ""
            print(f"  PING from {peer}{site_str}")

    node.on(HiveMessageType.PROPAGATE, on_ping_response)

    ping_payload = {
        "flood_id": flood_id,
        "timestamp": time.time(),
        "peer": my_peer,
        "site_id": siteid,
    }
    ping_inner = HiveMessage(HiveMessageType.PING, ping_payload)
    ping_outer = HiveMessage(HiveMessageType.PROPAGATE, payload=ping_inner)
    node.emit(ping_outer)

    time.sleep(timeout)
    node.close()

    if not mapper.nodes:
        print("[No responses received within the timeout window]")
        return

    if output_json:
        print(mapper.to_json())
    else:
        print("\n== Hive Map ==")
        print(mapper.to_ascii(root_peer=my_peer))


if __name__ == "__main__":
    hmclient_cmds()
