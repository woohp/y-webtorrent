import assert from "node:assert/strict";
import test from "node:test";

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

    close() {
        this.readyState = 3;
        this.dispatch("close");
    }
}

const createTracker = (overrides = {}) =>
    new TrackerConnection("wss://tracker.test", {
        infoHash: "info-hash",
        peerId: "peer-id",
        numwant: 1,
        createOffers: () => [],
        onOffer: () => {},
        onAnswer: () => {},
        WebSocket: MockWebSocket,
        ...overrides,
    });

test("validates tracker URLs and retries synchronous construction failures", (context) => {
    assert.throws(
        () =>
            new TrackerConnection("https://tracker.test", {
                infoHash: "info-hash",
                peerId: "peer-id",
                numwant: 1,
                createOffers: () => [],
                onOffer: () => {},
                onAnswer: () => {},
                WebSocket: MockWebSocket,
            }),
        /Invalid tracker URL/,
    );
    for (const option of ["connectTimeout", "announceResponseTimeout"]) {
        for (const value of [-1, Number.NaN, Number.POSITIVE_INFINITY, 2_147_483_648, "1"]) {
            assert.throws(() => createTracker({ [option]: value }), new RegExp(option));
        }
    }

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

test("times out sockets that never open", (context) => {
    MockWebSocket.sockets.length = 0;
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const errors = [];
    const tracker = createTracker({ connectTimeout: 100, onError: (error) => errors.push(error) });

    context.mock.timers.tick(100);

    assert.match(String(errors[0]), /connection timed out/);
    assert.equal(tracker.socket, null);
    assert.ok(tracker.reconnectTimer);
    tracker.destroy();
});

test("cancels sent offers when their socket closes and announces replacements", async () => {
    MockWebSocket.sockets.length = 0;
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
    MockWebSocket.sockets.length = 0;
    const answers = [];
    const tracker = createTracker({
        createOffers: () => [{ offer_id: "answered", offer: { type: "offer", sdp: "offer" } }],
        onAnswer: (_peerId, offerId) => answers.push(offerId),
    });
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();
    await Promise.resolve();

    socket.dispatch("message", {
        data: JSON.stringify({
            action: "announce",
            info_hash: "info-hash",
            peer_id: "remote",
            offer_id: "answered",
            answer: { type: "answer", sdp: "answer" },
        }),
    });
    await Promise.resolve();

    assert.deepEqual(answers, ["answered"]);
    assert.equal(tracker.announcedOffers.size, 0);
    tracker.destroy();
});
test("cancels stale offer batches and announces replacements after reconnect", async () => {
    MockWebSocket.sockets.length = 0;
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
    MockWebSocket.sockets.length = 0;
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

test("reports send exceptions and returns false", async () => {
    class ThrowingSendWebSocket extends MockWebSocket {
        send() {
            throw new Error("send failed");
        }
    }
    MockWebSocket.sockets.length = 0;
    const errors = [];
    const tracker = createTracker({
        WebSocket: ThrowingSendWebSocket,
        onError: (error) => errors.push(error),
    });
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();

    assert.equal(tracker.sendAnswer("remote", "offer", { type: "answer", sdp: "answer" }), false);
    assert.ok(errors.length >= 1);
    tracker.destroy();
});

test("silent trackers time out and reconnect with increasing backoff", async (context) => {
    MockWebSocket.sockets.length = 0;
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
    MockWebSocket.sockets.length = 0;
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const tracker = createTracker({ announceResponseTimeout: 100 });
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();
    tracker.reconnectDelay = 8000;

    socket.dispatch("message", {
        data: JSON.stringify({
            action: "announce",
            info_hash: "info-hash",
            interval: 60,
            complete: 0,
        }),
    });
    await Promise.resolve();

    assert.equal(tracker.announceResponseTimer, undefined);
    assert.equal(tracker.reconnectDelay, 1000);
    context.mock.timers.tick(100);
    assert.equal(socket.readyState, MockWebSocket.OPEN);
    tracker.destroy();
});

test("missing and invalid tracker intervals use the fallback schedule", async (context) => {
    MockWebSocket.sockets.length = 0;
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const tracker = createTracker();
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();

    for (const interval of [undefined, "invalid", Number.POSITIVE_INFINITY, 3_000_000]) {
        const message = { action: "announce", info_hash: "info-hash", complete: 0 };
        if (interval !== undefined) message.interval = interval;
        socket.dispatch("message", { data: JSON.stringify(message) });
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
    MockWebSocket.sockets.length = 0;
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const tracker = createTracker({ announceResponseTimeout: 100 });
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();
    tracker.scheduleAnnounce(60);
    const existingAnnounceTimer = tracker.announceTimer;

    socket.dispatch("message", {
        data: JSON.stringify({
            action: "announce",
            info_hash: "info-hash",
            peer_id: "remote",
            offer_id: "incoming",
            offer: { type: "offer", sdp: "offer" },
        }),
    });
    await Promise.resolve();

    assert.equal(tracker.announceResponseTimer, undefined);
    assert.equal(tracker.announceTimer, existingAnnounceTimer);
    context.mock.timers.tick(100);
    assert.equal(socket.readyState, MockWebSocket.OPEN);
    tracker.destroy();
});

test("an initial signaling-only response establishes the fallback schedule", async (context) => {
    MockWebSocket.sockets.length = 0;
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const tracker = createTracker({ announceResponseTimeout: 100 });
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();
    socket.sent.length = 0;

    socket.dispatch("message", {
        data: JSON.stringify({
            action: "announce",
            info_hash: "info-hash",
            peer_id: "remote",
            offer_id: "incoming",
            offer: { type: "offer", sdp: "offer" },
        }),
    });
    await Promise.resolve();

    assert.equal(tracker.announceResponseTimer, undefined);
    assert.ok(tracker.announceTimer);
    context.mock.timers.tick(119_999);
    assert.equal(socket.sent.length, 0);
    context.mock.timers.tick(1);
    await Promise.resolve();
    assert.equal(socket.sent.length, 1);
    tracker.destroy();
});

test("a signaling response after a fired fallback schedules the next fallback", async (context) => {
    MockWebSocket.sockets.length = 0;
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const tracker = createTracker();
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();
    socket.dispatch("message", {
        data: JSON.stringify({ action: "announce", info_hash: "info-hash", complete: 0 }),
    });
    await Promise.resolve();
    socket.sent.length = 0;

    context.mock.timers.tick(120_000);
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(socket.sent.length, 1);
    assert.equal(tracker.announceTimer, undefined);

    socket.dispatch("message", {
        data: JSON.stringify({
            action: "announce",
            info_hash: "info-hash",
            peer_id: "remote",
            offer_id: "incoming",
            offer: { type: "offer", sdp: "offer" },
        }),
    });
    await Promise.resolve();
    assert.ok(tracker.announceTimer);

    socket.sent.length = 0;
    context.mock.timers.tick(120_000);
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(socket.sent.length, 1);
    tracker.destroy();
});
test("signaling responses with intervals handle both signaling and scheduling", async (context) => {
    MockWebSocket.sockets.length = 0;
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

    socket.dispatch("message", {
        data: JSON.stringify({
            action: "announce",
            info_hash: "info-hash",
            interval: 60,
            peer_id: "remote",
            offer_id: "combined",
            offer: { type: "offer", sdp: "offer" },
        }),
    });
    await Promise.resolve();

    assert.deepEqual(offers, ["combined"]);
    assert.equal(tracker.announceResponseTimer, undefined);
    context.mock.timers.tick(59_999);
    assert.equal(socket.sent.length, 0);
    context.mock.timers.tick(1);
    await Promise.resolve();
    assert.equal(socket.sent.length, 1);
    tracker.destroy();
});
test("signaling messages do not postpone fallback announces", async (context) => {
    MockWebSocket.sockets.length = 0;
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const tracker = createTracker();
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();
    socket.dispatch("message", {
        data: JSON.stringify({ action: "announce", info_hash: "info-hash", complete: 0 }),
    });
    await Promise.resolve();
    socket.sent.length = 0;

    context.mock.timers.tick(60_000);
    socket.dispatch("message", {
        data: JSON.stringify({
            action: "announce",
            info_hash: "info-hash",
            peer_id: "remote",
            offer_id: "incoming",
            offer: { type: "offer", sdp: "offer" },
        }),
    });
    await Promise.resolve();
    context.mock.timers.tick(59_999);
    assert.equal(socket.sent.length, 0);
    context.mock.timers.tick(1);
    await Promise.resolve();
    assert.equal(socket.sent.length, 1);
    tracker.destroy();
});
test("a stale response timeout cannot close a replacement socket", async (context) => {
    MockWebSocket.sockets.length = 0;
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const tracker = createTracker({ announceResponseTimeout: 100 });
    const stale = MockWebSocket.sockets[0];
    stale.open();
    await Promise.resolve();

    tracker.connect();
    const replacement = MockWebSocket.sockets.at(-1);
    context.mock.timers.tick(100);

    assert.equal(replacement.readyState, 0);
    assert.equal(stale.readyState, MockWebSocket.OPEN);
    tracker.destroy();
});

test("reports tracker failures and warnings before normal filtering", async (context) => {
    MockWebSocket.sockets.length = 0;
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
    failureSocket.dispatch("message", {
        data: JSON.stringify({ action: "announce", "failure reason": "rejected" }),
    });
    await Promise.resolve();

    assert.match(errors[0], /Tracker failure: rejected/);
    assert.equal(failureSocket.readyState, 3);
    assert.ok(tracker.reconnectTimer);
    assert.equal(tracker.reconnectDelay, 2000);

    tracker.connect();
    const warningSocket = MockWebSocket.sockets.at(-1);
    warningSocket.open();
    await Promise.resolve();
    warningSocket.dispatch("message", {
        data: JSON.stringify({
            action: "announce",
            info_hash: "info-hash",
            complete: 1,
            "warning message": "degraded",
        }),
    });
    await Promise.resolve();

    assert.match(errors.at(-1), /Tracker warning: degraded/);
    assert.equal(announces.length, 1);
    assert.equal(warningSocket.readyState, MockWebSocket.OPEN);
    assert.ok(tracker.announceTimer);
    tracker.destroy();
});
test("validates, floors, and jitters tracker announce intervals", async (context) => {
    MockWebSocket.sockets.length = 0;
    let random = 0;
    context.mock.method(Math, "random", () => random);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const tracker = createTracker();
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();
    socket.dispatch("message", {
        data: JSON.stringify({ action: "announce", info_hash: "info-hash", interval: 30 }),
    });
    await Promise.resolve();
    socket.sent.length = 0;

    for (const interval of [-1, Number.POSITIVE_INFINITY, "1", 3_000_000]) {
        tracker.scheduleAnnounce(interval);
        assert.ok(tracker.announceTimer);
    }
    context.mock.timers.tick(119_999);
    assert.equal(socket.sent.length, 0);
    context.mock.timers.tick(1);
    await Promise.resolve();
    assert.equal(socket.sent.length, 1);
    socket.dispatch("message", {
        data: JSON.stringify({ action: "announce", info_hash: "info-hash", interval: 30 }),
    });
    await Promise.resolve();
    socket.sent.length = 0;

    tracker.scheduleAnnounce(1);
    context.mock.timers.tick(29_999);
    assert.equal(socket.sent.length, 0);
    context.mock.timers.tick(1);
    await Promise.resolve();
    assert.equal(socket.sent.length, 1);
    socket.dispatch("message", {
        data: JSON.stringify({ action: "announce", info_hash: "info-hash", interval: 30 }),
    });
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
