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

const validPeerId = (peerId) => peerId.padEnd(20, "\0").slice(0, 20);

const createProvider = (peerId, opts = {}) =>
    new WebtorrentProvider("provider-tests", new Y.Doc(), {
        peerId: validPeerId(peerId),
        trackers: [],
        ...opts,
    });

const copyBuffer = (message) => message.slice().buffer;

// Registers a fake transport the way `createPeer` does. `open: true` additionally promotes it,
// standing in for a data channel that has already opened.
const registerPeer = (provider, peer, peerId = null, { open = false } = {}) => {
    provider.registry.add(peer, peerId, Date.now());
    if (open) provider.registry.promote(peer);
    return peer;
};

const installSignalingPeers = (provider, createOffer) => {
    const created = [];
    provider.createPeer = (remotePeerId, initiator, generation) => {
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
        registerPeer(provider, peer, remotePeerId);
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
            second.onPeerMessage(secondPeer, copyBuffer(message), second.connectionGeneration);
            return true;
        },
        destroy: () => {
            if (firstDestroyed) return;
            firstDestroyed = true;
            first.removePeer(firstPeer, first.connectionGeneration);
        },
    };
    const secondPeer = {
        connected: true,
        send: (message) => {
            first.onPeerMessage(firstPeer, copyBuffer(message), first.connectionGeneration);
            return true;
        },
        destroy: () => {
            if (secondDestroyed) return;
            secondDestroyed = true;
            second.removePeer(secondPeer, second.connectionGeneration);
        },
    };
    // Both sides must be open before either handshake starts: `onPeerOpen` sends Sync Step 1
    // synchronously into the other provider, which drops frames from a peer it does not yet
    // consider open. Promoting one at a time would sync only one direction.
    registerPeer(first, firstPeer, second.peerId, { open: true });
    registerPeer(second, secondPeer, first.peerId, { open: true });
    first.onPeerOpen(firstPeer, true, first.connectionGeneration);
    second.onPeerOpen(secondPeer, false, second.connectionGeneration);
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
    registerPeer(first, unopenedPeer, "unopened");
    first.onPeerOpen(unopenedPeer, true, first.connectionGeneration);
    assert.equal(first.synced, false);
    first.removePeer(unopenedPeer, first.connectionGeneration);

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
    registerPeer(provider, sendFailurePeer, "send-failure");

    provider.onPeerOpen(sendFailurePeer, true, provider.connectionGeneration);
    assert.equal(sendFailureDestroyed, 1);

    let malformedDestroyed = 0;
    const malformedPeer = {
        connected: true,
        send: () => true,
        destroy: () => {
            malformedDestroyed++;
        },
    };
    registerPeer(provider, malformedPeer, "malformed", { open: true });
    provider.on("peer-error", () => {
        throw new Error("peer-error listener failed");
    });
    provider.onPeerMessage(
        malformedPeer,
        new Uint8Array([255]).buffer,
        provider.connectionGeneration,
    );
    assert.equal(malformedDestroyed, 1);
    provider.destroy();
});

test("disconnect removes and restores owned awareness", async () => {
    const provider = createProvider("owned-awareness");
    await provider.ready;
    const localState = { user: "local" };
    provider.awareness.setLocalState(localState);
    const sent = [];
    registerPeer(
        provider,
        {
            connected: true,
            send: (message) => {
                sent.push(message);
                return true;
            },
            destroy: () => {},
        },
        "presence-peer",
        { open: true },
    );

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
            lower.createOffers(lower.connectionGeneration),
            higher.createOffers(higher.connectionGeneration),
        ]);
        const deliverLowerOffer = () =>
            higher.receiveOffer(
                lower.peerId,
                lowerOffers[0].offer_id,
                lowerOffers[0].offer,
                higherTracker,
                higher.connectionGeneration,
            );
        const deliverHigherOffer = () =>
            lower.receiveOffer(
                higher.peerId,
                higherOffers[0].offer_id,
                higherOffers[0].offer,
                lowerTracker,
                lower.connectionGeneration,
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
            higher.connectionGeneration,
        );
        const routedAnswer = higherTracker.answers[0];
        await lower.receiveAnswer(
            higher.peerId,
            routedAnswer.offerId,
            routedAnswer.answer,
            lower.connectionGeneration,
        );

        const lowerPeer = lower.registry.pendingPeer(higher.peerId);
        const higherPeer = higher.registry.pendingPeer(lower.peerId);
        assert.equal(lowerPeer.initiator, true);
        assert.equal(higherPeer.initiator, false);
        lowerPeer.connected = true;
        higherPeer.connected = true;
        lowerPeer.send = (message) => {
            higher.onPeerMessage(higherPeer, copyBuffer(message), higher.connectionGeneration);
            return true;
        };
        higherPeer.send = (message) => {
            lower.onPeerMessage(lowerPeer, copyBuffer(message), lower.connectionGeneration);
            return true;
        };
        lower.doc.getText("shared").insert(0, "lower");
        // Open both before either handshake, as in `connectProviders`.
        lower.registry.promote(lowerPeer);
        higher.registry.promote(higherPeer);
        lower.onPeerOpen(lowerPeer, true, lower.connectionGeneration);
        higher.onPeerOpen(higherPeer, false, higher.connectionGeneration);

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
        const offers = await provider.createOffers(provider.connectionGeneration);
        await provider.receiveAnswer(
            remoteId,
            offers[0].offer_id,
            {
                type: "answer",
                sdp: "answer-first",
            },
            provider.connectionGeneration,
        );
        const outbound = provider.registry.pendingPeer(remoteId);

        await provider.receiveOffer(
            remoteId,
            "later-offer",
            { type: "offer", sdp: "later-offer" },
            tracker,
            provider.connectionGeneration,
        );

        const survivor = provider.registry.pendingPeer(remoteId);
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
        lower.connectionGeneration,
    );
    const inbound = lower.registry.pendingPeer("b".repeat(20));
    assert.equal(inbound.initiator, false);
    const offers = await lower.createOffers(lower.connectionGeneration);

    await lower.receiveAnswer(
        "b".repeat(20),
        offers[0].offer_id,
        { type: "answer", sdp: "local" },
        lower.connectionGeneration,
    );

    assert.equal(inbound.destroyed, true);
    assert.equal(lower.registry.pendingPeer("b".repeat(20)), peers.at(-1));
    assert.equal(peers.at(-1).initiator, true);
    lower.destroy();
});

test("peer opening during answer acceptance is preserved", async () => {
    const provider = createProvider("early-open", { maxConns: 1, numwant: 1 });
    await provider.ready;
    const peers = installSignalingPeers(provider);
    const [offer] = await provider.createOffers(provider.connectionGeneration);
    const peer = peers[0];
    peer.acceptAnswer = async () => {
        peer.connected = true;
        provider.onPeerOpen(peer, true, provider.connectionGeneration);
    };

    await provider.receiveAnswer(
        "remote",
        offer.offer_id,
        {
            type: "answer",
            sdp: "answer",
        },
        provider.connectionGeneration,
    );

    assert.equal(peer.destroyed, false);
    assert.equal(provider.registry.openPeer("remote"), peer);
    assert.equal(provider.registry.pendingCount, 0);
    assert.equal(provider.registry.pendingPeer("remote"), undefined);
    provider.destroy();
});

test("tracker announces share global pending-offer capacity", async () => {
    const provider = createProvider("tracker-capacity", { maxConns: 1, numwant: 1 });
    await provider.ready;
    installSignalingPeers(provider);

    const firstAnnounceOffers = await provider.createOffers(provider.connectionGeneration);
    const secondAnnounceOffers = await provider.createOffers(provider.connectionGeneration);
    assert.equal(firstAnnounceOffers.length, 1);
    assert.equal(secondAnnounceOffers.length, 0);

    provider.cancelOffers([firstAnnounceOffers[0].offer_id]);
    const secondAnnounceRetry = await provider.createOffers(provider.connectionGeneration);
    assert.equal(secondAnnounceRetry.length, 1);
    provider.destroy();
});

test("canceling an unpublished offer batch restores capacity", async () => {
    const provider = createProvider("cancel-offer-batch", { maxConns: 1, numwant: 1 });
    await provider.ready;
    installSignalingPeers(provider);
    const forgotten = [];
    provider.trackerConnections.push({
        forgetOffer: (offerId) => forgotten.push(offerId),
        destroy: () => {},
    });
    const [offer] = await provider.createOffers(provider.connectionGeneration);

    provider.cancelOffers([offer.offer_id]);

    assert.equal(provider.pendingOffers.size, 0);
    assert.equal(provider.registry.pendingCount, 0);
    assert.deepEqual(forgotten, [offer.offer_id]);
    const replacements = await provider.createOffers(provider.connectionGeneration);
    assert.equal(replacements.length, 1);
    provider.destroy();
});
test("canceled outbound peer cannot regain a timeout after acceptAnswer resolves", async () => {
    const higher = createProvider("b".repeat(20), { maxConns: 1, numwant: 1 });
    await higher.ready;
    installSignalingPeers(higher);
    const tracker = createFakeTracker();
    const offers = await higher.createOffers(higher.connectionGeneration);
    const outbound = higher.pendingOffers.get(offers[0].offer_id).peer;
    let resolveAnswer;
    outbound.acceptAnswer = () => new Promise((resolve) => (resolveAnswer = resolve));
    const receivingAnswer = higher.receiveAnswer(
        "a".repeat(20),
        offers[0].offer_id,
        {
            type: "answer",
            sdp: "deferred",
        },
        higher.connectionGeneration,
    );
    await Promise.resolve();

    await higher.receiveOffer(
        "a".repeat(20),
        "competing-offer",
        { type: "offer", sdp: "competing" },
        tracker,
        higher.connectionGeneration,
    );
    const inbound = higher.registry.pendingPeer("a".repeat(20));
    assert.equal(inbound.initiator, false);
    assert.equal(outbound.destroyed, true);
    resolveAnswer();
    await receivingAnswer;

    assert.equal(higher.registry.get(outbound)?.timer, undefined);
    assert.equal(higher.registry.pendingPeer("a".repeat(20)), inbound);
    higher.destroy();
});
test("canceled unresolved offers cannot later be announced", async () => {
    const higher = createProvider("b".repeat(20), { maxConns: 1, numwant: 1 });
    await higher.ready;
    let resolveOffer;
    installSignalingPeers(higher, () => new Promise((resolve) => (resolveOffer = resolve)));
    const tracker = createFakeTracker();
    const creatingOffers = higher.createOffers(higher.connectionGeneration);
    await Promise.resolve();

    await higher.receiveOffer(
        "a".repeat(20),
        "incoming",
        { type: "offer", sdp: "incoming" },
        tracker,
        higher.connectionGeneration,
    );
    resolveOffer();
    const offers = await creatingOffers;

    assert.deepEqual(offers, []);
    assert.equal(higher.pendingOffers.size, 0);
    assert.equal(higher.registry.pendingCount, 1);
    higher.destroy();
});

test("unrelated outbound answers cannot exceed connected capacity", async () => {
    const provider = createProvider("capacity-owner", { maxConns: 1, numwant: 1 });
    await provider.ready;
    installSignalingPeers(provider);
    const offers = await provider.createOffers(provider.connectionGeneration);
    const outbound = provider.pendingOffers.get(offers[0].offer_id).peer;
    const activePeer = { connected: true, initiator: false, send: () => true, destroy: () => {} };
    registerPeer(provider, activePeer, "already-connected", { open: true });

    await provider.receiveAnswer(
        "different-peer",
        offers[0].offer_id,
        {
            type: "answer",
            sdp: "different-peer",
        },
        provider.connectionGeneration,
    );

    assert.equal(outbound.destroyed, true);
    assert.deepEqual(provider.registry.openPeerIds, ["already-connected"]);

    const latePeer = {
        connected: true,
        initiator: false,
        send: () => true,
        destroyed: false,
        destroy() {
            this.destroyed = true;
        },
    };
    registerPeer(provider, latePeer, "late-unrelated");
    provider.onPeerOpen(latePeer, false, provider.connectionGeneration);
    assert.equal(latePeer.destroyed, true);
    assert.equal(provider.registry.openCount, 1);
    provider.destroy();
});
test("glare cancellation removes only one of multiple outstanding offers", async () => {
    const higher = createProvider("b".repeat(20), { maxConns: 2, numwant: 1 });
    await higher.ready;
    installSignalingPeers(higher);
    const firstTracker = createFakeTracker();
    await higher.createOffers(higher.connectionGeneration);
    await higher.createOffers(higher.connectionGeneration);
    assert.equal(higher.pendingOffers.size, 2);

    await higher.receiveOffer(
        "a".repeat(20),
        "incoming",
        { type: "offer", sdp: "incoming" },
        firstTracker,
        higher.connectionGeneration,
    );

    assert.equal(higher.pendingOffers.size, 1);
    assert.equal(higher.registry.pendingCount, 2);
    assert.ok(higher.registry.pendingPeer("a".repeat(20)));
    higher.destroy();
});

test("disconnect and destroy leave caller-owned awareness usable", async () => {
    const doc = new Y.Doc();
    const awareness = new Awareness(doc);
    awareness.setLocalState({ user: "external" });
    const provider = new WebtorrentProvider("external-awareness", doc, {
        peerId: validPeerId("external-awareness"),
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

test("uses normalized tracker URLs for every lifecycle status", async () => {
    class QuietWebSocket {
        static OPEN = 1;
        readyState = 0;
        addEventListener() {}
        close() {}
    }

    const provider = createProvider("normalized-tracker-status", {
        trackers: ["wss://tracker.example"],
        WebSocket: QuietWebSocket,
    });
    const statuses = [];
    provider.on("status", (event) => statuses.push(event));
    await provider.ready;

    provider.trackerConnections[0].onState("connected");
    provider.disconnect();

    assert.deepEqual(statuses, [
        { status: "connected", tracker: "wss://tracker.example/" },
        { status: "disconnected", tracker: "wss://tracker.example/" },
    ]);
    provider.destroy();
});

test("throwing status listeners cannot interrupt provider disconnect", async () => {
    const provider = createProvider("throwing-status");
    await provider.ready;
    let trackerDestroyed = false;
    provider.trackerConnections.push({
        url: "wss://tracker.test/",
        destroy: () => {
            trackerDestroyed = true;
        },
    });
    provider.on("status", () => {
        throw new Error("status listener failed");
    });

    assert.doesNotThrow(() => provider.disconnect());
    assert.equal(trackerDestroyed, true);
    assert.equal(provider.trackerConnections.length, 0);
    provider.destroy();
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
    const peer = provider.createPeer("remote", true, provider.connectionGeneration);

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
    const creatingOffers = provider.createOffers(provider.connectionGeneration);
    await Promise.resolve();
    await Promise.resolve();

    context.mock.timers.tick(100);
    const offers = await creatingOffers;
    await Promise.resolve();

    assert.deepEqual(offers, []);
    assert.deepEqual(errors, []);
    assert.equal(provider.registry.pendingCount, 0);
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
    const creatingOffers = provider.createOffers(provider.connectionGeneration);
    await Promise.resolve();
    await Promise.resolve();

    context.mock.timers.tick(100);
    const offers = await creatingOffers;

    assert.equal(offers.length, 1);
    assert.deepEqual(offers[0].offer, { type: "offer", sdp: "partial-offer" });
    provider.destroy();
});

test("genuine negotiation failures request recovery announces", async () => {
    const makeProvider = async (name) => {
        const provider = createProvider(name, { maxConns: 1, numwant: 1 });
        await provider.ready;
        let recoveries = 0;
        provider.trackerConnections.push({
            forgetOffer: () => {},
            requestRecoveryAnnounce: () => recoveries++,
            destroy: () => {},
        });
        return { provider, recoveries: () => recoveries };
    };

    const outbound = await makeProvider("failed-create-offer");
    installSignalingPeers(outbound.provider, async () => {
        throw new Error("createOffer failed");
    });
    assert.deepEqual(
        await outbound.provider.createOffers(outbound.provider.connectionGeneration),
        [],
    );
    assert.equal(outbound.recoveries(), 1);
    outbound.provider.destroy();

    const constructing = await makeProvider("failed-peer-construction");
    constructing.provider.createPeer = () => {
        throw new Error("peer construction failed");
    };
    assert.deepEqual(
        await constructing.provider.createOffers(constructing.provider.connectionGeneration),
        [],
    );
    assert.equal(constructing.recoveries(), 1);
    constructing.provider.destroy();

    const publishing = await makeProvider("failed-answer-publication");
    installSignalingPeers(publishing.provider);
    const failedTracker = createFakeTracker();
    failedTracker.sendAnswer = () => false;
    await publishing.provider.receiveOffer(
        "r".repeat(20),
        "incoming",
        { type: "offer", sdp: "offer" },
        failedTracker,
        publishing.provider.connectionGeneration,
    );
    assert.equal(publishing.recoveries(), 1);
    publishing.provider.destroy();

    const incoming = await makeProvider("failed-accept-offer");
    installSignalingPeers(incoming.provider);
    const incomingCreatePeer = incoming.provider.createPeer.bind(incoming.provider);
    incoming.provider.createPeer = (...args) => {
        const peer = incomingCreatePeer(...args);
        peer.acceptOffer = async () => {
            throw new Error("acceptOffer failed");
        };
        return peer;
    };
    await incoming.provider.receiveOffer(
        "r".repeat(20),
        "incoming",
        { type: "offer", sdp: "offer" },
        createFakeTracker(),
        incoming.provider.connectionGeneration,
    );
    assert.equal(incoming.recoveries(), 1);
    incoming.provider.destroy();

    const answering = await makeProvider("failed-accept-answer");
    const peers = installSignalingPeers(answering.provider);
    const [offer] = await answering.provider.createOffers(answering.provider.connectionGeneration);
    peers[0].acceptAnswer = async () => {
        throw new Error("acceptAnswer failed");
    };
    await answering.provider.receiveAnswer(
        "r".repeat(20),
        offer.offer_id,
        {
            type: "answer",
            sdp: "answer",
        },
        answering.provider.connectionGeneration,
    );
    assert.equal(answering.recoveries(), 1);
    answering.provider.destroy();
});
test("throwing peers listeners cannot prevent peer-loss recovery", async () => {
    const provider = createProvider("peer-loss-recovery", { maxConns: 1 });
    await provider.ready;
    let recoveryAnnounces = 0;
    provider.trackerConnections.push({
        requestRecoveryAnnounce: () => recoveryAnnounces++,
        destroy: () => {},
    });
    const peer = { connected: true, send: () => true, destroy: () => {} };
    registerPeer(provider, peer, "remote", { open: true });
    let laterListenerCalls = 0;
    const listenerFailure = new Error("peers listener failed");
    const listenerErrors = [];
    provider.on("peers", () => {
        throw listenerFailure;
    });
    provider.on("peers", () => laterListenerCalls++);
    provider.on("listener-error", () => {
        throw new Error("listener-error handler failed");
    });
    provider.on("listener-error", (event) => listenerErrors.push(event));

    assert.doesNotThrow(() => provider.removePeer(peer, provider.connectionGeneration));
    assert.equal(recoveryAnnounces, 1);
    assert.equal(laterListenerCalls, 1);
    assert.deepEqual(listenerErrors, [{ eventName: "peers", error: listenerFailure }]);
    provider.destroy();
});

test("late Sync Step 2 from a removed peer cannot restore synced", async () => {
    const provider = createProvider("stale-sync");
    await provider.ready;
    const peer = { connected: true, send: () => true, destroy: () => {} };
    registerPeer(provider, peer, "removed", { open: true });
    provider.removePeer(peer, provider.connectionGeneration);
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, 0);
    syncProtocol.writeSyncStep2(encoder, provider.doc);

    provider.onPeerMessage(
        peer,
        encoding.toUint8Array(encoder).buffer,
        provider.connectionGeneration,
    );

    assert.equal(provider.synced, false);
    assert.equal(provider.registry.hasSynced, false);
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
        peerId: validPeerId("late-connect"),
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
            provider.removePeer(peer, provider.connectionGeneration);
        },
    };
    registerPeer(provider, peer);
    provider.pendingOffers.set("offer", { peer, tracker: {} });
    provider.startPeerTimeout(peer);

    await provider.receiveAnswer(
        "remote",
        "offer",
        { type: "answer", sdp: "answer" },
        provider.connectionGeneration,
    );
    context.mock.timers.tick(100);

    assert.equal(destroyed, 1);
    assert.equal(provider.registry.pendingCount, 0);
    provider.destroy();
});
test("signal timeout frees capacity while accepting an offer is stuck", async (context) => {
    const provider = createProvider("stuck-accept-offer", { maxConns: 1, signalTimeout: 100 });
    await provider.ready;
    context.mock.timers.enable({ apis: ["setTimeout"] });
    installSignalingPeers(provider);
    const createPeer = provider.createPeer.bind(provider);
    let createdCount = 0;
    provider.createPeer = (...args) => {
        const peer = createPeer(...args);
        if (createdCount++ === 0) peer.acceptOffer = () => new Promise(() => {});
        return peer;
    };
    const errors = [];
    provider.on("peer-error", (error) => errors.push(error));
    const tracker = createFakeTracker();

    void provider.receiveOffer(
        "remote-1",
        "offer-1",
        { type: "offer", sdp: "stuck" },
        tracker,
        provider.connectionGeneration,
    );
    assert.equal(provider.registry.pendingCount, 1);
    assert.ok(provider.registry.pendingPeer("remote-1"));
    context.mock.timers.tick(100);

    assert.equal(provider.registry.pendingCount, 0);
    assert.equal(provider.registry.pendingPeer("remote-1"), undefined);
    assert.equal(provider.pendingOffers.size, 0);
    assert.deepEqual(errors, []);

    await provider.receiveOffer(
        "remote-2",
        "offer-2",
        { type: "offer", sdp: "usable" },
        tracker,
        provider.connectionGeneration,
    );
    assert.equal(tracker.answers.length, 1);
    assert.equal(provider.registry.pendingCount, 1);
    provider.destroy();
});

test("signal timeout frees capacity while accepting an answer is stuck", async (context) => {
    const provider = createProvider("stuck-accept-answer", {
        maxConns: 1,
        numwant: 1,
        signalTimeout: 100,
    });
    await provider.ready;
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const peers = installSignalingPeers(provider);
    let recoveryAnnounces = 0;
    provider.trackerConnections.push({
        forgetOffer: () => {},
        requestRecoveryAnnounce: () => recoveryAnnounces++,
        destroy: () => {},
    });
    const errors = [];
    provider.on("peer-error", (error) => errors.push(error));
    const [offer] = await provider.createOffers(provider.connectionGeneration);
    peers[0].acceptAnswer = () => new Promise(() => {});

    void provider.receiveAnswer(
        "remote-1",
        offer.offer_id,
        {
            type: "answer",
            sdp: "stuck",
        },
        provider.connectionGeneration,
    );
    assert.ok(provider.registry.pendingPeer("remote-1"));
    context.mock.timers.tick(100);

    assert.equal(provider.registry.pendingCount, 0);
    assert.equal(provider.registry.pendingPeer("remote-1"), undefined);
    assert.equal(provider.pendingOffers.size, 0);
    assert.deepEqual(errors, []);
    assert.equal(recoveryAnnounces, 1);

    const replacementOffers = await provider.createOffers(provider.connectionGeneration);
    assert.equal(replacementOffers.length, 1);
    provider.destroy();
});
