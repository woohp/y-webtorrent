# y-webtorrent agent notes

## Project

Browser-oriented Yjs provider that uses WebTorrent-compatible WebSocket trackers for peer discovery/signaling and WebRTC data channels for Yjs sync.

## Key files

- `src/y-webtorrent.js` — `WebtorrentProvider`, Yjs sync/awareness wiring, direct `simple-peer` use.
- `src/tracker.js` — minimal WebTorrent tracker WebSocket announce/signaling client.
- `src/crypto.js` — raw 20-byte `info_hash` and `peer_id` helpers.
- `examples/browser-smoke/` — manual two-tab browser smoke test.
- `vite.config.js` — Vite shims needed by `simple-peer`/browser smoke test.

## Validation

- Syntax/checks:
  - `node --check src/y-webtorrent.js`
  - `node --check src/tracker.js`
  - `node --check src/crypto.js`
  - `node --check examples/browser-smoke/main.js`
- Test script:
  - `npm test` currently runs Node's test runner, but there are no automated tests yet.
- Browser smoke:
  - `npm run dev`
  - open the printed localhost URL in two tabs
  - default tracker is `ws://localhost:4000/`
  - type in one tab's textarea and verify the other tab syncs

## Implementation notes

- This repo implements only the client/provider side, not a tracker server.
- Trackers are used only for discovery and WebRTC offer/answer signaling; Yjs updates go peer-to-peer over WebRTC.
- `info_hash` and `peer_id` must be raw 20-character binary strings, not hex.
- `simple-peer` is a runtime dependency.
- The provider imports `simple-peer/simplepeer.min.js` to avoid Vite/browser issues with Node stream polyfills.
- The provider currently uses non-trickle ICE (`trickle: false`), so initial connection can take a few seconds while complete offers/answers are gathered.
- Public debug internals are gated by `debug: true`; keep default usage quiet.
