import assert from "node:assert/strict";
import test from "node:test";
import * as Y from "yjs";
import { Awareness } from "y-protocols/awareness";
import * as syncProtocol from "y-protocols/sync";
import * as encoding from "lib0/encoding";

class FakeDataChannel {
    readyState = "connecting";
    bufferedAmount = 0;
    addEventListener() {}
    close() {
        this.readyState = "closed";
    }
}

class FakePeerConnection {
    connectionState = "new";
    iceConnectionState = "new";
    iceGatheringState = "complete";
    localDescription = null;
    channel = new FakeDataChannel();
    addEventListener() {}
    removeEventListener() {}
    createDataChannel() {
        return this.channel;
    }
    async createOffer() {
        return { type: "offer", sdp: "partial-offer" };
    }
    async setLocalDescription(description) {
        this.localDescription = { ...description, toJSON: () => ({ ...description }) };
    }
    close() {}
}

globalThis.RTCPeerConnection = FakePeerConnection;

const { WebtorrentProvider } = await import("../dist/index.js");

const createProvider = (peerId, opts = {}) =>
    new WebtorrentProvider("provider-tests", new Y.Doc(), {
        peerId,
        trackers: [],
        ...opts,
    });

const copyBuffer = (message) => message.slice().buffer;

const installSignalingPeers = (provider, createOffer) => {
    const created = [];
    provider.createPeer = (remotePeerId, initiator, generation = provider.connectionGeneration) => {
        const id = `${provider.peerId}-${created.length}`;
        const localDescription = { type: "offer", sdp: id };
        let destroyed = false;
        const peer = {
            initiator,
            connected: false,
            pc: { localDescription: null },
            createOffer: async () => {
                if (createOffer) await createOffer(peer);
                peer.pc.localDescription = {
                    ...localDescription,
                    toJSON: () => localDescription,
                };
                return localDescription;
            },
            acceptOffer: async (offer) => ({ type: "answer", sdp: offer.sdp }),
            acceptAnswer: async () => {},
            send: () => true,
            destroy: () => {
                if (destroyed) return;
                destroyed = true;
                peer.connected = false;
                provider.removePeer(peer, generation);
            },
            get destroyed() {
                return destroyed;
            },
        };
        provider.peerIds.set(peer, remotePeerId);
        created.push(peer);
        return peer;
    };
    return created;
};

const createFakeTracker = () => ({
    answers: [],
    isOpen: () => true,
    sendAnswer(peerId, offerId, answer) {
        this.answers.push({ peerId, offerId, answer });
        return true;
    },
});

const connectProviders = (first, second) => {
    let firstDestroyed = false;
    let secondDestroyed = false;
    const firstPeer = {
        connected: true,
        send: (message) => {
            second.onPeerMessage(secondPeer, copyBuffer(message));
            return true;
        },
        destroy: () => {
            if (firstDestroyed) return;
            firstDestroyed = true;
            first.removePeer(firstPeer);
        },
    };
    const secondPeer = {
        connected: true,
        send: (message) => {
            first.onPeerMessage(firstPeer, copyBuffer(message));
            return true;
        },
        destroy: () => {
            if (secondDestroyed) return;
            secondDestroyed = true;
            second.removePeer(secondPeer);
        },
    };
    first.peerIds.set(firstPeer, second.peerId);
    second.peerIds.set(secondPeer, first.peerId);
    first.peers.set(second.peerId, firstPeer);
    second.peers.set(first.peerId, secondPeer);
    first.onPeerOpen(firstPeer, true);
    second.onPeerOpen(secondPeer, false);
    return { firstPeer, secondPeer };
};

test("syncs documents and waits for Sync Step 2 before setting synced", async () => {
    const first = createProvider("first");
    const second = createProvider("second");
    await Promise.all([first.ready, second.ready]);
    first.doc.getText("shared").insert(0, "from first");
    second.doc.getText("shared").insert(0, "from second ");

    const unopenedPeer = {
        connected: true,
        send: () => true,
        destroy: () => {},
    };
    first.peerIds.set(unopenedPeer, "unopened");
    first.onPeerOpen(unopenedPeer, true);
    assert.equal(first.synced, false);
    first.removePeer(unopenedPeer);

    const { firstPeer, secondPeer } = connectProviders(first, second);

    const convergedText = first.doc.getText("shared").toString();
    assert.equal(second.doc.getText("shared").toString(), convergedText);
    assert.match(convergedText, /from first/);
    assert.match(convergedText, /from second/);
    assert.equal(first.synced, true);
    assert.equal(second.synced, true);

    firstPeer.destroy();
    assert.equal(first.synced, false);
    secondPeer.destroy();
    first.destroy();
    second.destroy();
});

test("protocol send failure and malformed frames destroy the affected peer", async () => {
    const provider = createProvider("protocol-errors");
    await provider.ready;
    let sendFailureDestroyed = 0;
    const sendFailurePeer = {
        connected: true,
        send: () => false,
        destroy: () => {
            sendFailureDestroyed++;
        },
    };
    provider.peerIds.set(sendFailurePeer, "send-failure");

    provider.onPeerOpen(sendFailurePeer, true);
    assert.equal(sendFailureDestroyed, 1);

    let malformedDestroyed = 0;
    const malformedPeer = {
        connected: true,
        send: () => true,
        destroy: () => {
            malformedDestroyed++;
        },
    };
    provider.peerIds.set(malformedPeer, "malformed");
    provider.peers.set("malformed", malformedPeer);
    provider.onPeerMessage(malformedPeer, new Uint8Array([255]).buffer);
    assert.equal(malformedDestroyed, 1);
    provider.destroy();
});

test("disconnect removes and restores owned awareness", async () => {
    const provider = createProvider("owned-awareness");
    await provider.ready;
    const localState = { user: "local" };
    provider.awareness.setLocalState(localState);
    const sent = [];
    provider.peers.set("presence-peer", {
        connected: true,
        send: (message) => {
            sent.push(message);
            return true;
        },
        destroy: () => {},
    });

    provider.disconnect();

    assert.equal(sent.length, 1);
    assert.deepEqual(provider.awareness.getLocalState(), localState);
    provider.destroy();
});
for (const order of ["lower-first", "higher-first"]) {
    test(`simultaneous offers converge with maxConns 1 (${order})`, async () => {
        const lower = createProvider("a".repeat(20), { maxConns: 1, numwant: 1 });
        const higher = createProvider("b".repeat(20), { maxConns: 1, numwant: 1 });
        await Promise.all([lower.ready, higher.ready]);
        installSignalingPeers(lower);
        installSignalingPeers(higher);
        const lowerTracker = createFakeTracker();
        const higherTracker = createFakeTracker();
        const [lowerOffers, higherOffers] = await Promise.all([
            lower.createOffers(),
            higher.createOffers(),
        ]);
        const deliverLowerOffer = () =>
            higher.receiveOffer(
                lower.peerId,
                lowerOffers[0].offer_id,
                lowerOffers[0].offer,
                higherTracker,
            );
        const deliverHigherOffer = () =>
            lower.receiveOffer(
                higher.peerId,
                higherOffers[0].offer_id,
                higherOffers[0].offer,
                lowerTracker,
            );

        if (order === "lower-first") {
            await deliverLowerOffer();
            await deliverHigherOffer();
        } else {
            await deliverHigherOffer();
            await deliverLowerOffer();
        }
        assert.equal(higherTracker.answers.length, 1);
        assert.equal(lowerTracker.answers.length, 1);
        const answerForCanceledOffer = lowerTracker.answers[0];
        await higher.receiveAnswer(
            lower.peerId,
            answerForCanceledOffer.offerId,
            answerForCanceledOffer.answer,
        );
        const routedAnswer = higherTracker.answers[0];
        await lower.receiveAnswer(higher.peerId, routedAnswer.offerId, routedAnswer.answer);

        const lowerPeer = lower.pendingPeerIds.get(higher.peerId);
        const higherPeer = higher.pendingPeerIds.get(lower.peerId);
        assert.equal(lowerPeer.initiator, true);
        assert.equal(higherPeer.initiator, false);
        lowerPeer.connected = true;
        higherPeer.connected = true;
        lowerPeer.send = (message) => {
            higher.onPeerMessage(higherPeer, copyBuffer(message));
            return true;
        };
        higherPeer.send = (message) => {
            lower.onPeerMessage(lowerPeer, copyBuffer(message));
            return true;
        };
        lower.doc.getText("shared").insert(0, "lower");
        lower.peers.set(higher.peerId, lowerPeer);
        higher.peers.set(lower.peerId, higherPeer);
        lower.onPeerOpen(lowerPeer, true);
        higher.onPeerOpen(higherPeer, false);

        assert.equal(higher.doc.getText("shared").toString(), "lower");
        assert.equal(lower.synced, true);
        assert.equal(higher.synced, true);
        lower.destroy();
        higher.destroy();
    });
}

for (const [localId, remoteId, keepsOutbound] of [
    ["a".repeat(20), "b".repeat(20), true],
    ["b".repeat(20), "a".repeat(20), false],
]) {
    test(`answer-before-offer glare keeps the ${keepsOutbound ? "outbound" : "inbound"} role`, async () => {
        const provider = createProvider(localId, { maxConns: 1, numwant: 1 });
        await provider.ready;
        installSignalingPeers(provider);
        const tracker = createFakeTracker();
        const offers = await provider.createOffers();
        await provider.receiveAnswer(remoteId, offers[0].offer_id, {
            type: "answer",
            sdp: "answer-first",
        });
        const outbound = provider.pendingPeerIds.get(remoteId);

        await provider.receiveOffer(
            remoteId,
            "later-offer",
            { type: "offer", sdp: "later-offer" },
            tracker,
        );

        const survivor = provider.pendingPeerIds.get(remoteId);
        assert.equal(survivor.initiator, keepsOutbound);
        assert.equal(outbound.destroyed, !keepsOutbound);
        assert.equal(tracker.answers.length, keepsOutbound ? 0 : 1);
        provider.destroy();
    });
}
test("lower peer replaces a pending inbound duplicate with its outbound peer", async () => {
    const lower = createProvider("a".repeat(20), { maxConns: 2, numwant: 1 });
    await lower.ready;
    const peers = installSignalingPeers(lower);
    const tracker = createFakeTracker();
    await lower.receiveOffer(
        "b".repeat(20),
        "remote-offer",
        { type: "offer", sdp: "remote" },
        tracker,
    );
    const inbound = lower.pendingPeerIds.get("b".repeat(20));
    assert.equal(inbound.initiator, false);
    const offers = await lower.createOffers();

    await lower.receiveAnswer("b".repeat(20), offers[0].offer_id, { type: "answer", sdp: "local" });

    assert.equal(inbound.destroyed, true);
    assert.equal(lower.pendingPeerIds.get("b".repeat(20)), peers.at(-1));
    assert.equal(peers.at(-1).initiator, true);
    lower.destroy();
});

test("canceled outbound peer cannot regain a timeout after acceptAnswer resolves", async () => {
    const higher = createProvider("b".repeat(20), { maxConns: 1, numwant: 1 });
    await higher.ready;
    installSignalingPeers(higher);
    const tracker = createFakeTracker();
    const offers = await higher.createOffers();
    const outbound = higher.pendingOffers.get(offers[0].offer_id).peer;
    let resolveAnswer;
    outbound.acceptAnswer = () => new Promise((resolve) => (resolveAnswer = resolve));
    const receivingAnswer = higher.receiveAnswer("a".repeat(20), offers[0].offer_id, {
        type: "answer",
        sdp: "deferred",
    });
    await Promise.resolve();

    await higher.receiveOffer(
        "a".repeat(20),
        "competing-offer",
        { type: "offer", sdp: "competing" },
        tracker,
    );
    const inbound = higher.pendingPeerIds.get("a".repeat(20));
    assert.equal(inbound.initiator, false);
    assert.equal(outbound.destroyed, true);
    resolveAnswer();
    await receivingAnswer;

    assert.equal(higher.pendingTimers.has(outbound), false);
    assert.equal(higher.pendingPeerIds.get("a".repeat(20)), inbound);
    higher.destroy();
});
test("canceled unresolved offers cannot later be announced", async () => {
    const higher = createProvider("b".repeat(20), { maxConns: 1, numwant: 1 });
    await higher.ready;
    let resolveOffer;
    installSignalingPeers(higher, () => new Promise((resolve) => (resolveOffer = resolve)));
    const tracker = createFakeTracker();
    const creatingOffers = higher.createOffers();
    await Promise.resolve();

    await higher.receiveOffer(
        "a".repeat(20),
        "incoming",
        { type: "offer", sdp: "incoming" },
        tracker,
    );
    resolveOffer();
    const offers = await creatingOffers;

    assert.deepEqual(offers, []);
    assert.equal(higher.pendingOffers.size, 0);
    assert.equal(higher.pendingPeers.size, 1);
    higher.destroy();
});

test("unrelated outbound answers cannot exceed connected capacity", async () => {
    const provider = createProvider("capacity-owner", { maxConns: 1, numwant: 1 });
    await provider.ready;
    installSignalingPeers(provider);
    const offers = await provider.createOffers();
    const outbound = provider.pendingOffers.get(offers[0].offer_id).peer;
    const activePeer = { connected: true, initiator: false, send: () => true, destroy: () => {} };
    provider.peerIds.set(activePeer, "already-connected");
    provider.peers.set("already-connected", activePeer);

    await provider.receiveAnswer("different-peer", offers[0].offer_id, {
        type: "answer",
        sdp: "different-peer",
    });

    assert.equal(outbound.destroyed, true);
    assert.deepEqual(Array.from(provider.peers.keys()), ["already-connected"]);

    const latePeer = {
        connected: true,
        initiator: false,
        send: () => true,
        destroyed: false,
        destroy() {
            this.destroyed = true;
        },
    };
    provider.peerIds.set(latePeer, "late-unrelated");
    provider.pendingPeers.add(latePeer);
    provider.pendingPeerIds.set("late-unrelated", latePeer);
    provider.onPeerOpen(latePeer, false);
    assert.equal(latePeer.destroyed, true);
    assert.equal(provider.peers.size, 1);
    provider.destroy();
});
test("glare cancellation removes only one of multiple outstanding offers", async () => {
    const higher = createProvider("b".repeat(20), { maxConns: 2, numwant: 1 });
    await higher.ready;
    installSignalingPeers(higher);
    const firstTracker = createFakeTracker();
    await higher.createOffers();
    await higher.createOffers();
    assert.equal(higher.pendingOffers.size, 2);

    await higher.receiveOffer(
        "a".repeat(20),
        "incoming",
        { type: "offer", sdp: "incoming" },
        firstTracker,
    );

    assert.equal(higher.pendingOffers.size, 1);
    assert.equal(higher.pendingPeers.size, 2);
    assert.equal(higher.pendingPeerIds.has("a".repeat(20)), true);
    higher.destroy();
});

test("disconnect and destroy leave caller-owned awareness usable", async () => {
    const doc = new Y.Doc();
    const awareness = new Awareness(doc);
    awareness.setLocalState({ user: "external" });
    const provider = new WebtorrentProvider("external-awareness", doc, {
        peerId: "external-awareness",
        trackers: [],
        awareness,
    });
    await provider.ready;

    provider.disconnect();
    assert.deepEqual(awareness.getLocalState(), { user: "external" });
    provider.destroy();
    assert.deepEqual(awareness.getLocalState(), { user: "external" });
    awareness.setLocalState({ user: "still usable" });
    assert.deepEqual(awareness.getLocalState(), { user: "still usable" });
    awareness.destroy();
});

test("connect is reusable while destroy is final", async () => {
    const provider = createProvider("lifecycle");
    assert.equal(provider.connect(), provider.connect());
    await provider.ready;
    provider.disconnect();
    assert.equal(provider.shouldConnect, false);

    await provider.connect();
    assert.equal(provider.shouldConnect, true);
    provider.destroy();
    await provider.connect();
    assert.equal(provider.shouldConnect, false);
});

test("passes send safeguards to the WebRTC transport", async () => {
    const provider = createProvider("transport-options", {
        fallbackMaxMessageSize: 123,
        maxBufferedAmount: 456,
    });
    await provider.ready;
    const peer = provider.createPeer("remote", true);

    assert.equal(peer.fallbackMaxMessageSize, 123);
    assert.equal(peer.maxBufferedAmount, 456);
    peer.destroy();
    provider.destroy();
});
test("offer collection timeout cancels ICE without peer errors", async (context) => {
    class GatheringPeerConnection extends FakePeerConnection {
        iceGatheringState = "gathering";
    }
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const provider = createProvider("collection-timeout", {
        maxConns: 1,
        numwant: 1,
        offerTimeout: 200,
        offerCollectionTimeout: 100,
        RTCPeerConnection: GatheringPeerConnection,
    });
    await provider.ready;
    const errors = [];
    provider.on("peer-error", (error) => errors.push(error));
    const creatingOffers = provider.createOffers();
    await Promise.resolve();
    await Promise.resolve();

    context.mock.timers.tick(100);
    const offers = await creatingOffers;
    await Promise.resolve();

    assert.deepEqual(offers, []);
    assert.deepEqual(errors, []);
    assert.equal(provider.pendingPeers.size, 0);
    provider.destroy();
});

test("ICE fallback offer beats the separate collection deadline", async (context) => {
    class GatheringPeerConnection extends FakePeerConnection {
        iceGatheringState = "gathering";
    }
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const provider = createProvider("ice-fallback", {
        maxConns: 1,
        numwant: 1,
        offerTimeout: 100,
        offerCollectionTimeout: 200,
        RTCPeerConnection: GatheringPeerConnection,
    });
    await provider.ready;
    const creatingOffers = provider.createOffers();
    await Promise.resolve();
    await Promise.resolve();

    context.mock.timers.tick(100);
    const offers = await creatingOffers;

    assert.equal(offers.length, 1);
    assert.deepEqual(offers[0].offer, { type: "offer", sdp: "partial-offer" });
    provider.destroy();
});
test("late Sync Step 2 from a removed peer cannot restore synced", async () => {
    const provider = createProvider("stale-sync");
    await provider.ready;
    const peer = { connected: true, send: () => true, destroy: () => {} };
    provider.peerIds.set(peer, "removed");
    provider.peers.set("removed", peer);
    provider.removePeer(peer);
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, 0);
    syncProtocol.writeSyncStep2(encoder, provider.doc);

    provider.onPeerMessage(peer, encoding.toUint8Array(encoder).buffer);

    assert.equal(provider.synced, false);
    assert.equal(provider.syncedPeers.size, 0);
    provider.destroy();
});

test("disconnect prevents late room derivation from creating trackers", async () => {
    let sockets = 0;
    class CountingWebSocket {
        static OPEN = 1;
        constructor() {
            sockets++;
        }
    }
    const provider = new WebtorrentProvider("late-connect", new Y.Doc(), {
        peerId: "late-connect",
        trackers: ["wss://tracker.test"],
        WebSocket: CountingWebSocket,
    });

    provider.disconnect();
    await provider.ready;

    assert.equal(sockets, 0);
    assert.equal(provider.trackerConnections.length, 0);
    provider.destroy();
});

test("pending signaled peers expire", async (context) => {
    const provider = createProvider("signal-timeout", { signalTimeout: 100 });
    await provider.ready;
    context.mock.timers.enable({ apis: ["setTimeout"] });
    let destroyed = 0;
    const peer = {
        acceptAnswer: async () => {},
        destroy: () => {
            destroyed++;
            provider.removePeer(peer);
        },
    };
    provider.pendingPeers.add(peer);
    provider.pendingOffers.set("offer", { peer, tracker: {} });

    await provider.receiveAnswer("remote", "offer", { type: "answer", sdp: "answer" });
    context.mock.timers.tick(100);

    assert.equal(destroyed, 1);
    assert.equal(provider.pendingPeers.size, 0);
    provider.destroy();
});
