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
test("validates, floors, and jitters tracker announce intervals", async (context) => {
    MockWebSocket.sockets.length = 0;
    let random = 0;
    context.mock.method(Math, "random", () => random);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const tracker = createTracker();
    const socket = MockWebSocket.sockets[0];
    socket.open();
    await Promise.resolve();
    socket.sent.length = 0;

    for (const interval of [-1, Number.POSITIVE_INFINITY, "1"]) {
        tracker.scheduleAnnounce(interval);
        assert.equal(tracker.announceTimer, undefined);
    }

    tracker.scheduleAnnounce(30);
    assert.ok(tracker.announceTimer);
    tracker.scheduleAnnounce(3_000_000);
    assert.equal(tracker.announceTimer, undefined);

    tracker.scheduleAnnounce(1);
    context.mock.timers.tick(29_999);
    assert.equal(socket.sent.length, 0);
    context.mock.timers.tick(1);
    await Promise.resolve();
    assert.equal(socket.sent.length, 1);

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
