# Audit — hivemind-websocket-client

## Known Issues

### CASCADE aggregator uses wall-clock timer
`CascadeAggregator` (`protocol.py:21`) uses `threading.Timer` which is subject to GIL and scheduling jitter. For production use with sub-second timeouts, consider an event-loop-based approach.

### PING payloads do not yet include public keys
`_handle_ping` (`protocol.py:397`) sends responsive PINGs without `public_key` in the payload (marked TODO). Until this is implemented, `HiveMapper.mark_trusted_nodes` cannot match discovered peers to trusted keys automatically — trust must be established out-of-band.

### Protocol coverage is 46%
`protocol.py` has 46% test coverage. Handlers without unit tests: `handle_handshake`, `handle_bus`, `handle_broadcast`, `handle_intercom`, `handle_propagate` (trust-gated BUS path), `bind`.
