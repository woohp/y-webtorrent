# y-webtorrent

Yjs provider that uses WebTorrent-compatible WebSocket trackers for peer discovery/signaling, then syncs over WebRTC data channels.

```js
import * as Y from "yjs";
import { WebtorrentProvider } from "y-webtorrent";

const doc = new Y.Doc();
const provider = new WebtorrentProvider("room-name", doc, {
  trackers: ["wss://tracker.openwebtorrent.com"],
});
```

Trackers only exchange WebRTC offers/answers. Yjs document updates are sent peer-to-peer over WebRTC and are not stored by the tracker.

## Options

- `trackers`: WebSocket tracker URLs.
- `password`: optional shared secret included in the room hash derivation.
- `awareness`: optional `y-protocols/awareness` instance.
- `maxConns`: maximum WebRTC peers, default `20`.
- `numwant`: offers to create per tracker announce, default `3`.
- `offerTimeout`: max milliseconds to wait for local WebRTC offers before announcing, default `5000`.
- `peerOpts`: extra options passed to `simple-peer`.
- `debug`: emit verbose `debug` events, default `false`.
- `WebSocket`: injectable WebSocket constructor, mostly for tests/non-browser runtimes.

## Browser smoke test

With a tracker running at `ws://localhost:4000/`:

```sh
npm run dev
```

Open the printed localhost URL in two tabs, click Connect in both, then type in the shared textarea. Use `?room=some-room&trackers=wss://tracker.webtorrent.dev` to override defaults.

If using Vite in your own app, `simple-peer` needs Node compatibility shims for packages like `readable-stream`; see `vite.config.js` in this repo.

## Notes

This package is browser-oriented. Node support requires supplying compatible WebSocket/WebRTC implementations through options.

Initial connections may take a few seconds because the provider currently uses non-trickle ICE (`simple-peer` with `trickle: false`) so tracker messages contain complete offers/answers.
