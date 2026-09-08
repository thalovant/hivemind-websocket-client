# Changelog

## [0.10.2a2](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.10.2a2) (2026-07-06)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.10.2a1...0.10.2a2)

**Closed issues:**

- Allow poorman-handshake 2 in the websocket client [\#138](https://github.com/JarbasHiveMind/hivemind-websocket-client/issues/138)

**Merged pull requests:**

- Allow poorman-handshake 2 [\#139](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/139) ([goldyfruit](https://github.com/goldyfruit))

## [0.10.2a1](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.10.2a1) (2026-07-04)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.10.1a1...0.10.2a1)

**Merged pull requests:**

- fix: async client negotiates v3 Noise + awaits handshake emit [\#136](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/136) ([JarbasAl](https://github.com/JarbasAl))

## [0.10.1a1](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.10.1a1) (2026-07-04)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.10.0a1...0.10.1a1)

**Merged pull requests:**

- fix: keep hivemind-core git ref out of package metadata \(unblock PyPI publish\) [\#134](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/134) ([JarbasAl](https://github.com/JarbasAl))

## [0.10.0a1](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.10.0a1) (2026-07-04)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.9.2a1...0.10.0a1)

**Merged pull requests:**

- feat: protocol v3 Noise handshake client \(XXpsk2/KKpsk0\) with v2 fallback [\#131](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/131) ([JarbasAl](https://github.com/JarbasAl))

## [0.9.2a1](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.9.2a1) (2026-06-06)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.9.1a1...0.9.2a1)

**Merged pull requests:**

- fix\(deps\): require ovos-bus-client\>=2.0.0a3 \(drops bundled hivemind protocol\) [\#128](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/128) ([JarbasAl](https://github.com/JarbasAl))

## [0.9.1a1](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.9.1a1) (2026-06-05)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.9.0a4...0.9.1a1)

**Merged pull requests:**

- fix: QUERY/CASCADE response companion [\#124](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/124) ([JarbasAl](https://github.com/JarbasAl))

## [0.9.0a4](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.9.0a4) (2026-06-05)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.9.0a3...0.9.0a4)

**Merged pull requests:**

- docs: zero-to-hero README + library guide [\#125](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/125) ([JarbasAl](https://github.com/JarbasAl))

## [0.9.0a3](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.9.0a3) (2026-06-05)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.9.0a2...0.9.0a3)

**Merged pull requests:**

- ci: fix broken coverage job \(async tests need pytest-asyncio\) [\#122](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/122) ([JarbasAl](https://github.com/JarbasAl))

## [0.9.0a2](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.9.0a2) (2026-06-05)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.9.0a1...0.9.0a2)

**Merged pull requests:**

- build: bump stale OVOS dep floors to the modern major [\#120](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/120) ([JarbasAl](https://github.com/JarbasAl))

## [0.9.0a1](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.9.0a1) (2026-05-18)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.8.0a1...0.9.0a1)

**Merged pull requests:**

- feat: AsyncFakeHiveMessageBus alongside the async client [\#117](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/117) ([JarbasAl](https://github.com/JarbasAl))

## [0.8.0a1](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.8.0a1) (2026-05-18)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.7.0a2...0.8.0a1)

**Fixed bugs:**

- assert message.msg\_type == HiveMessageType.PING [\#108](https://github.com/JarbasHiveMind/hivemind-websocket-client/issues/108)
- AttributeError: 'dict' object has no attribute 'msg\_type' [\#104](https://github.com/JarbasHiveMind/hivemind-websocket-client/issues/104)

**Merged pull requests:**

- feat: AsyncHiveMessageBusClient \(asyncio-native client\) [\#115](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/115) ([JarbasAl](https://github.com/JarbasAl))

## [0.7.0a2](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.7.0a2) (2026-05-07)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.7.0a1...0.7.0a2)

## [0.7.0a1](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.7.0a1) (2026-05-07)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.6.1a1...0.7.0a1)

**Merged pull requests:**

- feat\(tests\): hivescope e2e suite + CI [\#112](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/112) ([JarbasAl](https://github.com/JarbasAl))

## [0.6.1a1](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.6.1a1) (2026-05-07)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.6.0a1...0.6.1a1)

**Merged pull requests:**

- fix: message typing [\#107](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/107) ([JarbasAl](https://github.com/JarbasAl))

## [0.6.0a1](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.6.0a1) (2026-03-23)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.5.0a6...0.6.0a1)

**Merged pull requests:**

- feat: implement QUERY and CASCADE message handlers \(satellite side\) [\#101](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/101) ([JarbasAl](https://github.com/JarbasAl))

## [0.5.0a6](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.5.0a6) (2026-03-23)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.5.0a5...0.5.0a6)

**Merged pull requests:**

- chore: remove dead code [\#99](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/99) ([JarbasAl](https://github.com/JarbasAl))

## [0.5.0a5](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.5.0a5) (2026-03-23)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.5.0a4...0.5.0a5)

**Merged pull requests:**

- chore: Docs and tests [\#97](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/97) ([JarbasAl](https://github.com/JarbasAl))

## [0.5.0a4](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.5.0a4) (2026-03-23)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.5.0a3...0.5.0a4)

## [0.5.0a3](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.5.0a3) (2026-03-23)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.5.0a2...0.5.0a3)

## [0.5.0a2](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.5.0a2) (2026-03-23)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.5.0a1...0.5.0a2)

## [0.5.0a1](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.5.0a1) (2026-03-23)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.4.6a3...0.5.0a1)

**Merged pull requests:**

- feat: add flood-based PING network discovery and hive topology mapping [\#90](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/90) ([JarbasAl](https://github.com/JarbasAl))

## [0.4.6a3](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.4.6a3) (2026-03-23)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.4.6a2...0.4.6a3)

**Merged pull requests:**

- chore: typing, Docs, tests [\#91](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/91) ([JarbasAl](https://github.com/JarbasAl))

## [0.4.6a2](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.4.6a2) (2026-03-21)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.4.6a1...0.4.6a2)

**Merged pull requests:**

- Fix: z85 [\#87](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/87) ([JarbasAl](https://github.com/JarbasAl))

## [0.4.6a1](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.4.6a1) (2026-01-23)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.4.5a3...0.4.6a1)

**Merged pull requests:**

- fix: add warning if websocket connection isnt open [\#83](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/83) ([JarbasAl](https://github.com/JarbasAl))

## [0.4.5a3](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.4.5a3) (2026-01-23)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.4.5a2...0.4.5a3)

**Merged pull requests:**

- chore\(deps\): update actions/checkout action to v6 [\#79](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/79) ([renovate[bot]](https://github.com/apps/renovate))

## [0.4.5a2](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.4.5a2) (2025-12-19)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.4.5a1...0.4.5a2)

**Merged pull requests:**

- chore\(deps\): update actions/setup-python action to v6 [\#81](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/81) ([renovate[bot]](https://github.com/apps/renovate))

## [0.4.5a1](https://github.com/JarbasHiveMind/hivemind-websocket-client/tree/0.4.5a1) (2025-12-18)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-websocket-client/compare/0.4.4...0.4.5a1)

**Merged pull requests:**

- chore: Configure Renovate [\#76](https://github.com/JarbasHiveMind/hivemind-websocket-client/pull/76) ([renovate[bot]](https://github.com/apps/renovate))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
