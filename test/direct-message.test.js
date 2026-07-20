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

function createProvider(peerId) {
    return new WebtorrentProvider("test-room", new Y.Doc(), { peerId, trackers: [] });
}

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
        peerId: "overridden-rtc",
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

test("normalizes transport debug event types", () => {
    const provider = new WebtorrentProvider("test-room", new Y.Doc(), {
        peerId: "debug-events",
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
    sender.peers.set("recipient", {
        connected: true,
        send: (data) => {
            sent.push(data);
            return true;
        },
        destroy: () => {},
    });
    sender.peers.set("other", {
        connected: true,
        send: () => assert.fail("message was sent to an unintended peer"),
        destroy: () => {},
    });

    assert.equal(sender.sendToPeer("recipient", new Uint8Array([1, 2, 3])), true);
    assert.equal(sent.length, 1);

    const recipient = createProvider("recipient");
    await recipient.ready;
    const peer = recipient.createPeer("sender", true);
    recipient.peers.set("sender", peer);
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
    provider.peers.set("disconnected", {
        connected: false,
        send: () => {},
        destroy: () => {},
    });

    assert.equal(provider.sendToPeer("missing", new Uint8Array()), false);
    assert.equal(provider.sendToPeer("disconnected", new Uint8Array()), false);

    provider.destroy();
});
