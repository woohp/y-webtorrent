# y-webtorrent agent notes

## Project

Browser-oriented Yjs provider that uses WebTorrent-compatible WebSocket trackers for peer discovery/signaling and WebRTC data channels for Yjs sync.

## Key files

- `src/y-webtorrent.ts` — `WebtorrentProvider`, Yjs sync/awareness wiring.
- WebRTC transport is provided by `woohp/webrtc-data-peer`; provider-specific identity and signaling stay here.
- `src/tracker.ts` — minimal WebTorrent tracker WebSocket announce/signaling client.
- `src/crypto.ts` — raw 20-byte `info_hash` and `peer_id` helpers.
- `examples/browser-smoke/` — manual two-tab browser smoke test.
- `vite.config.js` — Vite config for the browser smoke test.

## Validation

- Formatting/linting:
    - `npm run format`
    - `npm run lint`
    - `npm run typecheck`
    - `npm run build`
    - `npm run check`
- Syntax/checks when useful:
    - `node --check examples/browser-smoke/main.js`
- Test script:
    - `npm test` builds and runs the offline provider/transport tests.
- Browser smoke:
    - `npm run dev`
    - open the printed localhost URL in two tabs
    - default tracker is `ws://localhost:4000/`
    - type in one tab's textarea and verify the other tab syncs

## Implementation notes

- This repo implements only the client/provider side, not a tracker server.
- Trackers are used only for discovery and WebRTC offer/answer signaling; Yjs updates go peer-to-peer over WebRTC.
- `info_hash` and `peer_id` must be raw 20-character binary strings, not hex.
- The provider uses browser-native `RTCPeerConnection`/`RTCDataChannel` via `webrtc-data-peer`; avoid Node WebRTC shims unless explicitly adding Node support.
- The provider currently uses non-trickle ICE, so initial connection can take a few seconds while complete offers/answers are gathered.
- Public debug internals are gated by `debug: true`; keep default usage quiet.
- `synced` requires Yjs Sync Step 2 from a currently connected peer. Protocol-send failure destroys that peer so a later tracker exchange can repair synchronization.
- Single-frame and buffered-byte safeguards come from `webrtc-data-peer`; fragmentation and outbound queueing are intentionally unsupported.
- Tracker offers may be simultaneous. Resolve duplicate attempts by peer-ID ordering while still accepting one-way inbound offers; canceled offer records must never be announced after late SDP completion.
