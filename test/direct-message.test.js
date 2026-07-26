import assert from "node:assert/strict";
import test from "node:test";
import * as Y from "yjs";

class FakeDataChannel {
    binaryType = "blob";
    readyState = "open";
    bufferedAmount = 0;
    listeners = new Map();

    addEventListener(event, listener) {
        this.listeners.set(event, listener);
    }

    send() {}

    close() {}

    receive(data) {
        this.listeners.get("message")?.({ data });
    }
}

class FakePeerConnection {
    connectionState = "connected";
    iceConnectionState = "connected";
    iceGatheringState = "complete";
    localDescription = null;
    channel = new FakeDataChannel();

    addEventListener() {}

    createDataChannel() {
        return this.channel;
    }

    close() {}
}

globalThis.RTCPeerConnection = FakePeerConnection;

const { WebtorrentProvider, defaultRtcConfig } = await import("../dist/index.js");

const validPeerId = (peerId) => peerId.padEnd(20, "\0").slice(0, 20);

function createProvider(peerId, opts = {}) {
    return new WebtorrentProvider("test-room", new Y.Doc(), {
        peerId: validPeerId(peerId),
        trackers: [],
        ...opts,
    });
}

// Registers a fake transport the way `createPeer` does, already promoted to open.
function registerOpenPeer(provider, peerId, peer) {
    provider.registry.add(peer, peerId, Date.now());
    provider.registry.promote(peer);
    return peer;
}

test("validates injected peer IDs", () => {
    for (const peerId of [
        "",
        "short",
        "x".repeat(21),
        `x${String.fromCharCode(256)}${"x".repeat(18)}`,
        42,
    ]) {
        assert.throws(
            () =>
                new WebtorrentProvider("test-room", new Y.Doc(), {
                    trackers: [],
                    peerId,
                }),
            /peerId/,
        );
    }
    const peerId = String.fromCharCode(...Array.from({ length: 20 }, (_, index) => index));
    const provider = new WebtorrentProvider("test-room", new Y.Doc(), {
        trackers: [],
        peerId,
    });
    assert.equal(provider.peerId, peerId);
    provider.destroy();
});

test("validates provider numeric options at construction", () => {
    for (const option of ["maxConns", "numwant"]) {
        for (const value of [-1, 0.5, Number.NaN, Number.POSITIVE_INFINITY, "1"]) {
            assert.throws(
                () => createProvider(`invalid-${option}`, { [option]: value }),
                new RegExp(option),
            );
        }
    }
    for (const option of [
        "offerTimeout",
        "offerCollectionTimeout",
        "signalTimeout",
        "trackerConnectTimeout",
        "announceResponseTimeout",
    ]) {
        for (const value of [-1, Number.NaN, Number.POSITIVE_INFINITY, 2_147_483_648, "1"]) {
            assert.throws(
                () => createProvider(`invalid-${option}`, { [option]: value }),
                new RegExp(option),
            );
        }
    }
    for (const option of ["fallbackMaxMessageSize", "maxBufferedAmount"]) {
        for (const value of [-1, Number.NaN, "1"]) {
            assert.throws(
                () => createProvider(`invalid-${option}`, { [option]: value }),
                new RegExp(option),
            );
        }
    }
    assert.throws(
        () => createProvider("computed-timeout-overflow", { offerTimeout: 2_147_483_647 }),
        /offerCollectionTimeout/,
    );

    const zeroCapacity = createProvider("zero-capacity", { maxConns: 0 });
    assert.equal(zeroCapacity.numwant, 0);
    zeroCapacity.destroy();

    const provider = createProvider("numeric-boundaries", {
        maxConns: 0,
        numwant: 0,
        offerTimeout: 2_147_483_647,
        offerCollectionTimeout: 2_147_483_647,
        signalTimeout: 0,
        trackerConnectTimeout: 0,
        announceResponseTimeout: 0,
        fallbackMaxMessageSize: Number.POSITIVE_INFINITY,
        maxBufferedAmount: Number.POSITIVE_INFINITY,
    });
    assert.equal(provider.announceResponseTimeout, 0);
    provider.destroy();
});
test("uses isolated public STUN defaults and allows rtcConfig overrides", () => {
    const provider = createProvider("default-rtc");
    const otherDefault = createProvider("other-default-rtc");
    assert.notEqual(provider.rtcConfig, defaultRtcConfig);
    assert.notEqual(provider.rtcConfig.iceServers, defaultRtcConfig.iceServers);
    assert.deepEqual(provider.rtcConfig.iceServers, [
        { urls: "stun:stun.l.google.com:19302" },
        { urls: "stun:stun.cloudflare.com:3478" },
    ]);
    provider.rtcConfig.iceServers.push({ urls: "stun:instance-only.test" });
    assert.equal(otherDefault.rtcConfig.iceServers.length, 2);
    assert.equal(defaultRtcConfig.iceServers.length, 2);

    const rtcConfig = { iceServers: [] };
    const overridden = new WebtorrentProvider("test-room", new Y.Doc(), {
        peerId: validPeerId("overridden-rtc"),
        trackers: [],
        rtcConfig,
    });
    assert.equal(overridden.rtcConfig, rtcConfig);

    provider.destroy();
    otherDefault.destroy();
    overridden.destroy();
});

test("deeply isolates nested default ICE server values", () => {
    const originalIceServers = defaultRtcConfig.iceServers;
    let first;
    let second;
    try {
        defaultRtcConfig.iceServers = [
            { urls: ["stun:first.test", "stun:second.test"], username: "default-user" },
        ];
        first = createProvider("nested-default-first");
        second = createProvider("nested-default-second");

        first.rtcConfig.iceServers[0].username = "instance-user";
        first.rtcConfig.iceServers[0].urls.push("stun:instance-only.test");

        assert.equal(second.rtcConfig.iceServers[0].username, "default-user");
        assert.deepEqual(second.rtcConfig.iceServers[0].urls, [
            "stun:first.test",
            "stun:second.test",
        ]);
        assert.equal(defaultRtcConfig.iceServers[0].username, "default-user");
        assert.deepEqual(defaultRtcConfig.iceServers[0].urls, [
            "stun:first.test",
            "stun:second.test",
        ]);
    } finally {
        first?.destroy();
        second?.destroy();
        defaultRtcConfig.iceServers = originalIceServers;
    }
});

test("debug output recognizes an injected WebRTC constructor", () => {
    const globalRtcPeerConnection = globalThis.RTCPeerConnection;
    globalThis.RTCPeerConnection = undefined;
    try {
        const provider = new WebtorrentProvider("test-room", new Y.Doc(), {
            peerId: validPeerId("injected-rtc-debug"),
            trackers: [],
            RTCPeerConnection: FakePeerConnection,
            debug: true,
        });
        const events = [];
        provider.on("debug", (event) => events.push(event));
        const peer = provider.createPeer("remote", true);

        assert.equal(
            events.find((event) => event.type === "peer-created").hasRtcPeerConnection,
            true,
        );
        peer.destroy();
        provider.destroy();
    } finally {
        globalThis.RTCPeerConnection = globalRtcPeerConnection;
    }
});

test("normalizes transport debug event types", () => {
    const provider = new WebtorrentProvider("test-room", new Y.Doc(), {
        peerId: validPeerId("debug-events"),
        trackers: [],
        debug: true,
    });
    const events = [];
    provider.on("debug", (event) => events.push(event));
    const peer = provider.createPeer("remote", true);

    peer.onDebug({ type: 42, detail: "preserved" });

    assert.deepEqual(events.at(-1), { type: "42", detail: "preserved" });
    peer.destroy();
    provider.destroy();
});

test("sendToPeer sends a directed binary message to only the selected peer", async () => {
    const sender = createProvider("sender");
    await sender.ready;
    const sent = [];
    registerOpenPeer(sender, "recipient", {
        connected: true,
        send: (data) => {
            sent.push(data);
            return true;
        },
        destroy: () => {},
    });
    registerOpenPeer(sender, "other", {
        connected: true,
        send: () => assert.fail("message was sent to an unintended peer"),
        destroy: () => {},
    });

    assert.equal(sender.sendToPeer("recipient", new Uint8Array([1, 2, 3])), true);
    assert.equal(sent.length, 1);

    const recipient = createProvider("recipient");
    await recipient.ready;
    // `createPeer` already registered this one; it only needs promoting.
    const peer = recipient.createPeer("sender", true);
    recipient.registry.promote(peer);
    const received = new Promise((resolve) => {
        recipient.on("direct-message", (peerId, payload) => resolve({ peerId, payload }));
    });
    peer.channel.receive(sent[0].buffer);

    assert.deepEqual(await received, {
        peerId: "sender",
        payload: new Uint8Array([1, 2, 3]),
    });

    sender.destroy();
    recipient.destroy();
});

test("sendToPeer returns false for missing and disconnected peers", async () => {
    const provider = createProvider("sender");
    await provider.ready;
    registerOpenPeer(provider, "disconnected", {
        connected: false,
        send: () => {},
        destroy: () => {},
    });

    assert.equal(provider.sendToPeer("missing", new Uint8Array()), false);
    assert.equal(provider.sendToPeer("disconnected", new Uint8Array()), false);

    provider.destroy();
});
