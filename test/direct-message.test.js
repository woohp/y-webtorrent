import assert from "node:assert/strict";
import test from "node:test";
import * as Y from "yjs";

class FakeDataChannel {
    binaryType = "blob";
    readyState = "open";
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

const { WebtorrentProvider } = await import("../dist/index.js");

function createProvider(peerId) {
    return new WebtorrentProvider("test-room", new Y.Doc(), { peerId, trackers: [] });
}

test("sendToPeer sends a directed binary message to only the selected peer", async () => {
    const sender = createProvider("sender");
    await sender.ready;
    const sent = [];
    sender.peers.set("recipient", {
        connected: true,
        send: (data) => sent.push(data),
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
