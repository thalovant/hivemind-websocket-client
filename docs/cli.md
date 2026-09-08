# CLI Reference

The `hivemind-client` command provides utilities for managing identity and sending test messages.

```
hivemind-client [COMMAND] [OPTIONS]
```

---

## `set-identity`

Persist node credentials to the identity file (`~/.config/hivemind/_identity.json`).

```bash
hivemind-client set-identity [OPTIONS]
```

| Option | Description |
|---|---|
| `--key TEXT` | HiveMind access key |
| `--password TEXT` | HiveMind password |
| `--siteid TEXT` | Location identifier injected into `message.context` |

**Example:**

```bash
hivemind-client set-identity \
  --key "42caf3d2405075fb9e7a4e1ff44e4c4f" \
  --password "5ae486f7f1c26bd4645bd052e4af3ea3" \
  --siteid "living-room"
```

---

## `terminal`

Interactive text terminal — type utterances, see spoken responses.

```bash
hivemind-client terminal [OPTIONS]
```

| Option | Description |
|---|---|
| `--key TEXT` | HiveMind access key (overrides identity file) |
| `--host TEXT` | HiveMind host URL |
| `--port INTEGER` | HiveMind port |

**Example:**

```bash
hivemind-client terminal --host ws://192.168.1.10 --port 5678
> what time is it
It is 3:45 PM.
```

---

## `send-mycroft`

Send a single OVOS/Mycroft bus message to the hub.

```bash
hivemind-client send-mycroft [OPTIONS]
```

| Option | Description |
|---|---|
| `--key TEXT` | HiveMind access key |
| `--host TEXT` | HiveMind host |
| `--port INTEGER` | HiveMind port |
| `--msg TEXT` | OVOS message type to inject |
| `--payload TEXT` | OVOS message data as a JSON string |

**Example:**

```bash
hivemind-client send-mycroft \
  --msg "recognizer_loop:utterance" \
  --payload '{"utterances": ["turn off the lights"]}'
```

---

## `escalate`

Send a single OVOS message wrapped in a `HiveMessageType.ESCALATE` envelope. The message is forwarded upstream through the hub hierarchy.

```bash
hivemind-client escalate [OPTIONS]
```

| Option | Description |
|---|---|
| `--key TEXT` | HiveMind access key |
| `--host TEXT` | HiveMind host |
| `--port INTEGER` | HiveMind port |
| `--msg TEXT` | OVOS message type |
| `--payload TEXT` | OVOS message data as JSON |

---

## `propagate`

Send a single OVOS message wrapped in a `HiveMessageType.PROPAGATE` envelope. The message is forwarded to all peers and upstream hubs.

```bash
hivemind-client propagate [OPTIONS]
```

| Option | Description |
|---|---|
| `--key TEXT` | HiveMind access key |
| `--host TEXT` | HiveMind host |
| `--port INTEGER` | HiveMind port |
| `--msg TEXT` | OVOS message type |
| `--payload TEXT` | OVOS message data as JSON |

