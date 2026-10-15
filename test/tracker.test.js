import assert from "node:assert/strict";
import test, { beforeEach } from "node:test";

const { TrackerConnection } = await import("../dist/tracker.js");

class MockWebSocket {
    static OPEN = 1;
    static sockets = [];
    readyState = 0;
    listeners = new Map();
    sent = [];

    constructor(url) {
        this.url = url;
        MockWebSocket.sockets.push(this);
    }

    addEventListener(type, listener) {
        const listeners = this.listeners.get(type) ?? [];
        listeners.push(listener);
        this.listeners.set(type, listeners);
    }

    receive(fields) {
        this.dispatch("message", {
            data: JSON.stringify({ action: "announce", info_hash: "info-hash", ...fields }),
        });
    }

    dispatch(type, event = {}) {
        for (const listener of this.listeners.get(type) ?? []) listener(event);
    }

    open() {
        this.readyState = MockWebSocket.OPEN;
        this.dispatch("open");
    }

    send(data) {
        this.sent.push(JSON.parse(data));
    }

    drop() {
        this.readyState = 3;
    }

    close() {
        if (this.readyState === 3) return;
        this.readyState = 3;
        this.dispatch("close");
    }
}

const createTracker = (overrides = {}, url = "wss://tracker.test") =>
    new TrackerConnection(url, {
        infoHash: "info-hash",
        peerId: "peer-id",
        numwant: 1,
        createOffers: () => [],
        cancelOffers: () => {},
        onOffer: () => {},
        onAnswer: () => {},
        onAnnounce: () => {},
        onState: () => {},
        onError: () => {},
        WebSocket: MockWebSocket,
        ...overrides,
    });

beforeEach(() => {
    MockWebSocket.sockets.length = 0;
});

test("validates tracker URLs and retries synchronous construction failures", (context) => {
    assert.throws(() => createTracker({}, "https://tracker.test"), /Invalid tracker URL/);

    class ThrowingWebSocket {
        static OPEN = 1;
        constructor() {
            throw new Error("construction failed");
        }
    }
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const errors = [];
    const tracker = createTracker({
        WebSocket: ThrowingWebSocket,
        onError: (error) => errors.push(error),
    });

    assert.match(String(errors[0]), /construction failed/);
    assert.ok(tracker.reconnectTimer);
    tracker.destroy();
});

test("reports tracker connection lifecycle states", () => {
    const states = [];
    const tracker = createTracker({ onState: (state) => states.push(state) });
    const first = MockWebSocket.sockets[0];
    first.open();
    first.close();
    tracker.connect();
    const second = MockWebSocket.sockets.at(-1);
    second.open();
    tracker.destroy();

    assert.deepEqual(states, ["connected", "reconnecting", "connected", "disconnected"]);
});

test("connect does not replace active sockets or leave reconnect timers", (context) => {
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const tracker = createTracker();
    const first = MockWebSocket.sockets[0];

    tracker.connect();
    assert.equal(MockWebSocket.sockets.length, 1);
    first.open();
    tracker.connect();
    assert.equal(MockWebSocket.sockets.length, 1);

    first.close();
    assert.ok(tracker.reconnectTimer);
    tracker.connect();
    assert.equal(tracker.reconnectTimer, undefined);
    assert.equal(MockWebSocket.sockets.length, 2);
    context.mock.timers.tick(1000);
    assert.equal(MockWebSocket.sockets.length, 2);
    tracker.destroy();
});

test("overlapping announce requests coalesce behind in-flight offer creation", async (context) => {
    context.mock.method(Date, "now", () => 0);
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    let resolveFirstOffers;
    let createCount = 0;
    const tracker = createTracker({
        createOffers: () => {
            createCount++;
            if (createCount === 1) {
                return new Promise((resolve) => (resolveFirstOffers = resolve));
            }
            return [];
        },
    });
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();
    assert.equal(createCount, 1);

    tracker.scheduleAnnounce(1);
    context.mock.timers.tick(30_000);
    tracker.requestRecoveryAnnounce();
    context.mock.timers.tick(0);
    tracker.requestRecoveryAnnounce();
    context.mock.timers.tick(0);
    assert.equal(createCount, 1);
    assert.equal(socket.sent.length, 0);

    resolveFirstOffers([]);
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(socket.sent.length, 1);
    assert.equal(createCount, 1);

    context.mock.timers.tick(4_999);
    assert.equal(createCount, 1);
    context.mock.timers.tick(1);
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(createCount, 2);
    assert.equal(socket.sent.length, 2);
    tracker.destroy();
});

test("recovery announces are debounced and rate limited", async (context) => {
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const tracker = createTracker();
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();
    socket.receive({ interval: 120 });
    await Promise.resolve();
    socket.sent.length = 0;

    tracker.requestRecoveryAnnounce();
    tracker.requestRecoveryAnnounce();
    context.mock.timers.tick(4_999);
    assert.equal(socket.sent.length, 0);
    context.mock.timers.tick(1);
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(socket.sent.length, 1);
    tracker.destroy();
});

test("times out sockets that never open", (context) => {
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const errors = [];
    const tracker = createTracker({ connectTimeout: 100, onError: (error) => errors.push(error) });

    context.mock.timers.tick(100);

    assert.match(String(errors[0]), /connection timed out/);
    assert.equal(MockWebSocket.sockets[0].readyState, 3);
    assert.ok(tracker.reconnectTimer);
    tracker.destroy();
});

test("cancels sent offers when their socket closes and announces replacements", async () => {
    let createCount = 0;
    const canceled = [];
    const tracker = createTracker({
        createOffers: () => {
            createCount++;
            const id = createCount === 1 ? "sent" : "replacement";
            return [{ offer_id: id, offer: { type: "offer", sdp: id } }];
        },
        cancelOffers: (offerIds) => canceled.push(...offerIds),
    });
    const firstSocket = MockWebSocket.sockets[0];
    firstSocket.open();
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(firstSocket.sent[0].offers[0].offer_id, "sent");

    firstSocket.close();
    assert.deepEqual(canceled, ["sent"]);

    tracker.connect();
    const replacementSocket = MockWebSocket.sockets.at(-1);
    replacementSocket.open();
    await Promise.resolve();
    await Promise.resolve();

    assert.equal(replacementSocket.sent[0].offers[0].offer_id, "replacement");
    tracker.destroy();
    assert.deepEqual(canceled, ["sent", "replacement"]);
});

test("forgets an announced offer when its answer arrives", async () => {
    const answers = [];
    const canceled = [];
    const tracker = createTracker({
        createOffers: () => [{ offer_id: "answered", offer: { type: "offer", sdp: "offer" } }],
        onAnswer: (_peerId, offerId) => answers.push(offerId),
        cancelOffers: (offerIds) => canceled.push(...offerIds),
    });
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();
    await Promise.resolve();

    socket.receive({
        peer_id: "remote",
        offer_id: "answered",
        answer: { type: "answer", sdp: "answer" },
    });
    await Promise.resolve();

    assert.deepEqual(answers, ["answered"]);
    socket.close();
    assert.deepEqual(canceled, []);
    tracker.destroy();
});
test("cancels stale offer batches and announces replacements after reconnect", async () => {
    let resolveFirstOffers;
    let createCount = 0;
    const canceled = [];
    const tracker = createTracker({
        createOffers: () => {
            createCount++;
            if (createCount === 1) {
                return new Promise((resolve) => (resolveFirstOffers = resolve));
            }
            return [{ offer_id: "replacement", offer: { type: "offer", sdp: "replacement" } }];
        },
        cancelOffers: (offerIds) => canceled.push(...offerIds),
    });
    const staleSocket = MockWebSocket.sockets[0];
    staleSocket.open();
    staleSocket.close();

    resolveFirstOffers([{ offer_id: "stale", offer: { type: "offer", sdp: "stale" } }]);
    await Promise.resolve();
    await Promise.resolve();
    assert.deepEqual(canceled, ["stale"]);

    tracker.connect();
    const replacementSocket = MockWebSocket.sockets.at(-1);
    replacementSocket.open();
    await Promise.resolve();
    await Promise.resolve();

    assert.deepEqual(replacementSocket.sent[0].offers, [
        { offer_id: "replacement", offer: { type: "offer", sdp: "replacement" } },
    ]);
    tracker.destroy();
});
test("does not publish offers completed after destruction", async () => {
    let resolveOffers;
    const tracker = createTracker({
        createOffers: () => new Promise((resolve) => (resolveOffers = resolve)),
    });
    const socket = MockWebSocket.sockets[0];
    socket.open();
    tracker.destroy();

    resolveOffers([{ offer_id: "late", offer: { type: "offer", sdp: "late" } }]);
    await Promise.resolve();

    assert.equal(
        socket.sent.some((message) => message.offers),
        false,
    );
});

test("send exceptions close the socket and cancel unsent offers", async () => {
    class ThrowingSendWebSocket extends MockWebSocket {
        send() {
            throw new Error("send failed");
        }
    }
    const errors = [];
    const canceled = [];
    const tracker = createTracker({
        WebSocket: ThrowingSendWebSocket,
        createOffers: () => [{ offer_id: "unsent", offer: { type: "offer", sdp: "offer" } }],
        cancelOffers: (offerIds) => canceled.push(...offerIds),
        onError: (error) => errors.push(error),
    });
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();
    await Promise.resolve();

    assert.match(String(errors[0]), /send failed/);
    assert.equal(socket.readyState, 3);
    assert.deepEqual(canceled, ["unsent"]);
    tracker.destroy();
});

test("silent trackers time out and reconnect with increasing backoff", async (context) => {
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const errors = [];
    const tracker = createTracker({
        announceResponseTimeout: 100,
        onError: (error) => errors.push(error),
    });
    const first = MockWebSocket.sockets[0];
    first.open();
    await Promise.resolve();

    context.mock.timers.tick(100);
    assert.equal(first.readyState, 3);
    assert.match(String(errors[0]), /announce response timed out/);
    assert.equal(tracker.reconnectDelay, 2000);

    context.mock.timers.tick(1000);
    const second = MockWebSocket.sockets.at(-1);
    second.open();
    await Promise.resolve();
    context.mock.timers.tick(100);
    assert.equal(second.readyState, 3);
    assert.equal(tracker.reconnectDelay, 4000);
    tracker.destroy();
});

test("valid tracker responses clear timeout and reset reconnect backoff", async (context) => {
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const tracker = createTracker({ announceResponseTimeout: 100 });
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();
    tracker.reconnectDelay = 8000;

    socket.receive({
        interval: 60,
        complete: 0,
    });
    await Promise.resolve();

    assert.equal(tracker.reconnectDelay, 1000);
    context.mock.timers.tick(100);
    assert.equal(socket.readyState, MockWebSocket.OPEN);
    tracker.destroy();
});

test("missing and invalid tracker intervals use the fallback schedule", async (context) => {
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const tracker = createTracker();
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();

    for (const interval of [undefined, -1, "invalid", 3_000_000]) {
        socket.receive({ complete: 0, interval });
        await Promise.resolve();
        socket.sent.length = 0;
        context.mock.timers.tick(119_999);
        assert.equal(socket.sent.length, 0);
        context.mock.timers.tick(1);
        await Promise.resolve();
        assert.equal(socket.sent.length, 1);
    }
    tracker.destroy();
});

test("signaling responses clear liveness timeout without postponing an existing announce", async (context) => {
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const tracker = createTracker({ announceResponseTimeout: 100 });
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();
    tracker.scheduleAnnounce(60);

    socket.receive({
        peer_id: "remote",
        offer_id: "incoming",
        offer: { type: "offer", sdp: "offer" },
    });
    await Promise.resolve();

    context.mock.timers.tick(100);
    assert.equal(socket.readyState, MockWebSocket.OPEN);
    context.mock.timers.tick(59_900);
    await Promise.resolve();
    assert.equal(socket.sent.length, 2);
    tracker.destroy();
});

test("a signaling response after a fired fallback schedules the next fallback", async (context) => {
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const tracker = createTracker();
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();
    socket.receive({ complete: 0 });
    await Promise.resolve();
    socket.sent.length = 0;

    context.mock.timers.tick(120_000);
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(socket.sent.length, 1);

    socket.receive({
        peer_id: "remote",
        offer_id: "incoming",
        offer: { type: "offer", sdp: "offer" },
    });
    await Promise.resolve();

    socket.sent.length = 0;
    context.mock.timers.tick(120_000);
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(socket.sent.length, 1);
    tracker.destroy();
});
test("signaling responses with intervals handle both signaling and scheduling", async (context) => {
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const offers = [];
    const tracker = createTracker({
        announceResponseTimeout: 100,
        onOffer: (_peerId, offerId) => offers.push(offerId),
    });
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();
    socket.sent.length = 0;

    socket.receive({
        interval: 60,
        peer_id: "remote",
        offer_id: "combined",
        offer: { type: "offer", sdp: "offer" },
    });
    await Promise.resolve();

    assert.deepEqual(offers, ["combined"]);
    context.mock.timers.tick(59_999);
    assert.equal(socket.sent.length, 0);
    context.mock.timers.tick(1);
    await Promise.resolve();
    assert.equal(socket.sent.length, 1);
    tracker.destroy();
});
test("a replacement socket stays open past the dropped socket's response deadline", async (context) => {
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const tracker = createTracker({ announceResponseTimeout: 100 });
    const stale = MockWebSocket.sockets[0];
    stale.open();
    await Promise.resolve();

    stale.drop();
    tracker.connect();
    const replacement = MockWebSocket.sockets.at(-1);
    context.mock.timers.tick(100);

    assert.equal(replacement.readyState, 0);
    tracker.destroy();
});

test("reports tracker failures and warnings before normal filtering", async (context) => {
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const errors = [];
    const announces = [];
    const tracker = createTracker({
        onError: (error) => errors.push(String(error)),
        onAnnounce: (message) => announces.push(message),
    });
    const failureSocket = MockWebSocket.sockets[0];
    failureSocket.open();
    await Promise.resolve();
    failureSocket.receive({ info_hash: undefined, "failure reason": "rejected" });
    await Promise.resolve();

    assert.match(errors[0], /Tracker failure: rejected/);
    assert.equal(failureSocket.readyState, 3);
    assert.ok(tracker.reconnectTimer);
    assert.equal(tracker.reconnectDelay, 2000);

    tracker.connect();
    const warningSocket = MockWebSocket.sockets.at(-1);
    warningSocket.open();
    await Promise.resolve();
    warningSocket.receive({ info_hash: "other-swarm", complete: 1 });
    await Promise.resolve();
    assert.equal(announces.length, 0);
    assert.equal(tracker.reconnectDelay, 2000);

    warningSocket.receive({
        complete: 1,
        "warning message": "degraded",
    });
    await Promise.resolve();

    assert.match(errors.at(-1), /Tracker warning: degraded/);
    assert.equal(announces.length, 1);
    assert.equal(warningSocket.readyState, MockWebSocket.OPEN);
    tracker.destroy();
});
test("floors and jitters tracker announce intervals", async (context) => {
    let random = 0;
    context.mock.method(Math, "random", () => random);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const tracker = createTracker();
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();
    socket.receive({ interval: 30 });
    await Promise.resolve();
    socket.sent.length = 0;

    tracker.scheduleAnnounce(1);
    context.mock.timers.tick(29_999);
    assert.equal(socket.sent.length, 0);
    context.mock.timers.tick(1);
    await Promise.resolve();
    assert.equal(socket.sent.length, 1);
    socket.receive({ interval: 30 });
    await Promise.resolve();

    socket.sent.length = 0;
    random = 0.5;
    tracker.scheduleAnnounce(1);
    context.mock.timers.tick(31_499);
    assert.equal(socket.sent.length, 0);
    context.mock.timers.tick(1);
    await Promise.resolve();
    assert.equal(socket.sent.length, 1);
    tracker.destroy();
});
