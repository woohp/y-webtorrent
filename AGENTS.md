# y-webtorrent agent guide

y-webtorrent is a browser-oriented Yjs provider. WebTorrent-compatible WebSocket trackers are used only for peer discovery and WebRTC offer/answer signaling; Yjs sync and awareness travel peer-to-peer over WebRTC data channels. This repo is the client only, not a tracker server.

Favor simple, maintainable solutions. Validate at the public options boundary and on untrusted tracker or peer input; do not add defensive handling between internal modules. The provider routes every event through `emitSafely`, so internal callbacks cannot throw. Do not complicate rare local races unless a concrete failure justifies it.

## Boundaries

- `WebtorrentProvider` (`src/y-webtorrent.ts`) owns Yjs/awareness wiring, peer lifecycle, outbound offer records, and `maxConns` capacity.
- `TrackerConnection` (`src/tracker.ts`) owns one tracker socket session: announces, reconnect backoff, and which published offers are still outstanding. It is internal and has a single caller.
- `PeerRegistry` (`src/peer.ts`) is bookkeeping only; it never destroys transports.
- WebRTC transport comes from `woohp/webrtc-data-peer` on browser-native `RTCPeerConnection`. Frame-size and buffered-byte limits live there; fragmentation and outbound queueing are intentionally unsupported. Avoid Node WebRTC shims unless explicitly adding Node support.
- Keep `README.md` to usage, options, and semantics callers rely on.

## Protocol and lifecycle invariants

- `info_hash` and `peer_id` are raw 20-character binary strings, not hex.
- ICE is non-trickle: offers and answers carry complete SDP, so initial connection takes a few seconds.
- Simultaneous offers resolve by peer-ID ordering while still accepting one-way inbound offers. Canceled offer records must never be announced after late SDP completion.
- Async callbacks check `connectionGeneration`; the bump in `disconnect` turns late transport and tracker callbacks into no-ops.
- `synced` requires Yjs Sync Step 2 from a currently connected peer. Protocol-send failure destroys that peer so a later tracker exchange can repair sync; directed messages (`sendToPeer`) are best-effort and never destroy peers.
- Remote awareness expires by timeout rather than on transport close: forwarded awareness frames have no authoritative owner.
- Room names select swarms; they are not secrets. Room passwords and the removed `password` option are intentionally unsupported.
- Debug events are gated by `debug: true`; keep default usage quiet and debug payloads lazy.

## Validation

- `npm run format`, then `npm run check` (lint, typecheck, build, offline tests).
- `npm run test:e2e` syncs two Playwright tabs through an in-process tracker. Run it when touching signaling, the transport, or `examples/browser-smoke/`.
- Offline provider tests drive internals through the fake transports in `test/provider.test.js` (`installSignalingPeers`, `connectProviders`); reuse them instead of adding new fakes.
- Manual smoke: `npm run dev`, open two tabs against a tracker at `ws://localhost:4000/`.
