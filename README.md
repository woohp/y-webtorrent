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

## Directed messages

Applications can send transient binary data to one connected peer without adding it to the shared Yjs document:

```js
const delivered = provider.sendToPeer(peerId, new TextEncoder().encode("hello"));

provider.on("direct-message", (peerId, data) => {
    console.log(peerId, new TextDecoder().decode(data));
});
```

`sendToPeer` returns `false` when the peer is missing or its data channel is not open. Directed messages are not persisted, retried, or forwarded to other peers.

## Options

- `trackers`: WebSocket tracker URLs.
- `password`: optional shared secret included in the room hash derivation.
- `awareness`: optional `y-protocols/awareness` instance.
- `maxConns`: maximum WebRTC peers, default `20`.
- `numwant`: offers to create per tracker announce, default `3`.
- `offerTimeout`: max milliseconds to wait for local WebRTC offers before announcing, default `5000`.
- `rtcConfig`: optional `RTCPeerConnection` configuration.
- `channelName`: WebRTC data channel name, default `y-webtorrent`.
- `debug`: emit verbose `debug` events, default `false`.
- `WebSocket`: injectable WebSocket constructor, mostly for tests/non-browser runtimes.

## Build

```sh
npm run build
npm run check
```

The package is authored in TypeScript and built with TypeScript native preview/`tsgo` to `dist/`, including `.d.ts` declarations.

## Browser smoke test

With a tracker running at `ws://localhost:4000/`:

```sh
npm run dev
```

Open the printed localhost URL in two tabs, click Connect in both, then type in the shared textarea. Use `?room=some-room&trackers=wss://tracker.webtorrent.dev` to override defaults.

The provider uses browser-native `RTCPeerConnection`/`RTCDataChannel`; no Node WebRTC shim is bundled.

## Notes

This package is browser-oriented. Node support requires supplying compatible WebSocket/WebRTC implementations through options.

Initial connections may take a few seconds because the provider currently uses non-trickle ICE so tracker messages contain complete offers/answers.
