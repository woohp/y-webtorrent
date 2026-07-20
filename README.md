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

Tracker signaling is unauthenticated. Public or malicious trackers can observe signaling, substitute WebRTC endpoints, impersonate peers, and potentially gain access to document traffic through those substituted connections. Room names select swarms; they are not secrets or access-control credentials.

## Directed messages

Applications can send transient binary data to one connected peer without adding it to the shared Yjs document:

```js
const delivered = provider.sendToPeer(peerId, new TextEncoder().encode("hello"));

provider.on("direct-message", (peerId, data) => {
    console.log(peerId, new TextDecoder().decode(data));
});
```

`sendToPeer` returns `false` when the peer is missing or disconnected, the frame or buffering limit is exceeded, or the browser rejects the send. Transport failures may also be reported through `peer-error`. Directed messages are best-effort: failures do not destroy the peer or trigger reconnection, and messages are not persisted, retried, or forwarded. Yjs protocol-send failures remain destructive so a later tracker exchange can repair synchronization.

## Options

- `trackers`: WebSocket tracker URLs.
- `awareness`: optional `y-protocols/awareness` instance.
- `maxConns`: maximum WebRTC peers, default `20`.
- `numwant`: requested offers per tracker announce, default `3` capped by `maxConns` (therefore `0` when `maxConns` is `0`). Outstanding offers across all trackers share the provider-wide `maxConns` capacity, so an announce may create fewer offers—or none—while another tracker owns pending offers.
- `offerTimeout`: max milliseconds to wait for ICE gathering before using the current local description, default `5000`.
- `offerCollectionTimeout`: outer deadline for all native offer-creation work, default `offerTimeout + 5000`.
- `signalTimeout`: max milliseconds for a signaled peer to open, default `15000`.
- `trackerConnectTimeout`: max milliseconds for a tracker WebSocket to open, default `10000`.
- `announceResponseTimeout`: max milliseconds for a tracker to answer an announce before reconnecting, default `15000`; `0` requests an immediate timeout.
- `fallbackMaxMessageSize`: single-frame limit when SCTP does not report one, default `256` KiB.
- `maxBufferedAmount`: data-channel buffering ceiling, default `4` MiB.
- `rtcConfig`: optional `RTCPeerConnection` configuration. By default, connections use Google and Cloudflare public STUN servers for NAT traversal. Pass `{ iceServers: [] }` to disable them or provide your own STUN/TURN servers.
- `channelName`: WebRTC data channel name, default `y-webtorrent`.
- `peerId`: advanced identity injection for tests; must be a raw 20-character binary string. Normal callers should omit it.
- `debug`: emit verbose `debug` events, default `false`.
- `WebSocket`: injectable WebSocket constructor, mostly for tests/non-browser runtimes.
- `RTCPeerConnection`: injectable WebRTC constructor for tests/non-browser runtimes.

`provider.ready` resolves after room hashing and all configured tracker construction attempts have completed. It does not mean a tracker socket is open, a WebRTC peer is connected, or Yjs synchronization has completed; use `status`, `peers`, and `synced` for those states.

Tracker lifecycle is emitted through `status` events (`connected`, `reconnecting`, or `disconnected`). Valid swarm statistics are emitted separately through `announce` events.

Signaling timeouts and unexpected peer loss request a debounced recovery announce, limited to at most one attempt per tracker every five seconds plus up to one second of jitter.

Counts must be finite, non-negative integers. Timeouts must be between `0` and `2147483647` milliseconds. Send limits must be non-negative numbers and may be `Infinity`. Tracker-requested announce intervals use a 30-second minimum and up to 10% positive jitter; valid responses without a schedulable interval use a conservative 120-second fallback.

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

Initial connections may take a few seconds because the provider uses non-trickle ICE so tracker messages contain complete offers/answers. `provider.synced` becomes true only after Yjs Sync Step 2 completes with at least one currently connected peer.

Remote awareness states expire according to the Awareness timeout after an abrupt peer loss. They are not removed immediately because forwarded awareness frames do not identify an authoritative owning peer; removing every client ID observed through a closing peer could erase users still reachable through another peer.

WebRTC messages use single data-channel frames. Fragmentation, large-document synchronization beyond the negotiated or fallback frame limit, sustained burst traffic, and outbound queueing are intentionally unsupported.
