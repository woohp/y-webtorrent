import assert from "node:assert/strict";
import test from "node:test";

const { PeerRegistry } = await import("../dist/peer.js");

// The registry only ever uses transports as map keys, so a bare object stands in fine.
const fakePeer = (name) => ({ name });

// Waits past a 0 ms deadline. `setImmediate` is not enough — it runs before timers fire, so
// asserting on it would let an uncancelled timer look cancelled.
const afterDeadlines = () => new Promise((resolve) => setTimeout(resolve, 5));

test("registers a peer as pending and indexes it by id", () => {
    const registry = new PeerRegistry();
    const peer = fakePeer("a");
    const slot = registry.add(peer, "remote-a", 1000);

    assert.deepEqual(
        { peerId: slot.peerId, state: slot.state, synced: slot.synced, startedAt: slot.startedAt },
        { peerId: "remote-a", state: "pending", synced: false, startedAt: 1000 },
    );
    assert.equal(registry.get(peer), slot);
    assert.equal(registry.pendingPeer("remote-a"), peer);
    assert.equal(registry.openPeer("remote-a"), undefined);
    assert.deepEqual([registry.size, registry.pendingCount, registry.openCount], [1, 1, 0]);
});

test("counts an unidentified peer without indexing it", () => {
    const registry = new PeerRegistry();
    const peer = registry.add(fakePeer("outbound"), null, 0).peer;

    // Outbound offers have no id until an answer arrives, but they still occupy capacity.
    assert.equal(registry.size, 1);
    assert.equal(registry.peerIdOf(peer), null);
    assert.deepEqual(registry.allPeers, [peer]);
});

test("refuses to register the same peer twice", () => {
    const registry = new PeerRegistry();
    const peer = fakePeer("a");
    registry.add(peer, "remote-a", 0);

    assert.throws(() => registry.add(peer, null, 0), /already registered/);
    // The original slot survives the rejected call.
    assert.equal(registry.peerIdOf(peer), "remote-a");
    assert.equal(registry.size, 1);
});

test("identify re-files a peer under a new id and releases the old one", () => {
    const registry = new PeerRegistry();
    const peer = fakePeer("a");
    registry.add(peer, "provisional", 0);

    registry.identify(peer, "actual");

    assert.equal(registry.pendingPeer("provisional"), undefined);
    assert.equal(registry.pendingPeer("actual"), peer);
    assert.equal(registry.size, 1);
});

test("identify keeps an already-open peer in the open index", () => {
    const registry = new PeerRegistry();
    const peer = fakePeer("a");
    registry.add(peer, "first", 0);
    registry.promote(peer);

    registry.identify(peer, "second");

    assert.equal(registry.openPeer("second"), peer);
    assert.equal(registry.pendingPeer("second"), undefined);
    assert.equal(registry.openCount, 1);
});

test("promote moves a peer from pending to open and cancels its deadline", async () => {
    const registry = new PeerRegistry();
    const peer = fakePeer("a");
    registry.add(peer, "remote-a", 0);
    let expired = false;
    registry.setTimer(
        peer,
        setTimeout(() => {
            expired = true;
        }, 0),
    );

    registry.promote(peer);

    assert.equal(registry.get(peer).state, "open");
    assert.equal(registry.get(peer).timer, undefined);
    assert.equal(registry.openPeer("remote-a"), peer);
    assert.equal(registry.pendingPeer("remote-a"), undefined);
    assert.deepEqual([registry.size, registry.pendingCount, registry.openCount], [1, 0, 1]);
    await afterDeadlines();
    assert.equal(expired, false, "promotion must clear the signaling deadline");
});

test("promote will not open a peer that has no id yet", () => {
    const registry = new PeerRegistry();
    const peer = fakePeer("a");
    registry.add(peer, null, 0);

    registry.promote(peer);

    assert.equal(registry.get(peer).state, "pending");
    assert.equal(registry.openCount, 0);
});

test("mutators ignore a peer that was already forgotten", () => {
    const registry = new PeerRegistry();
    const stale = fakePeer("stale");

    // A transport's callbacks can outlive its slot, so these must not throw or resurrect it.
    registry.identify(stale, "remote-a");
    registry.promote(stale);
    registry.clearTimer(stale);
    assert.equal(registry.remove(stale), undefined);
    assert.equal(registry.size, 0);
    assert.equal(registry.pendingPeer("remote-a"), undefined);
});

test("setTimer replaces the previous deadline, and cancels one handed to a stranger", async () => {
    const registry = new PeerRegistry();
    const peer = fakePeer("a");
    registry.add(peer, "remote-a", 0);
    const fired = [];
    registry.setTimer(peer, setTimeout(() => fired.push("first"), 0));
    registry.setTimer(peer, setTimeout(() => fired.push("second"), 0));
    // Handing a timer to an unregistered peer must not leak it into the event loop.
    registry.setTimer(fakePeer("stranger"), setTimeout(() => fired.push("stranger"), 0));

    await afterDeadlines();
    assert.deepEqual(fired, ["second"]);
    registry.clear();
});

test("remove returns the final slot and drops both indexes", () => {
    const registry = new PeerRegistry();
    const open = fakePeer("open");
    const pending = fakePeer("pending");
    registry.add(open, "remote-open", 0);
    registry.promote(open);
    registry.add(pending, "remote-pending", 0);

    assert.equal(registry.remove(open).state, "open");
    assert.equal(registry.remove(pending).state, "pending");

    assert.deepEqual([registry.size, registry.openCount, registry.pendingCount], [0, 0, 0]);
    assert.equal(registry.openPeer("remote-open"), undefined);
    assert.equal(registry.pendingPeer("remote-pending"), undefined);
});

test("removing one peer leaves an id another peer holds alone", () => {
    const registry = new PeerRegistry();
    const winner = fakePeer("winner");
    const loser = fakePeer("loser");
    registry.add(winner, "shared", 0);
    registry.promote(winner);
    // Duplicate-connection glare briefly files two transports under one remote id.
    registry.add(loser, "shared", 0);

    registry.remove(loser);

    assert.equal(registry.openPeer("shared"), winner, "the loser must not evict the open peer");
    assert.equal(registry.size, 1);
});

test("clear forgets every peer and cancels their deadlines", async () => {
    const registry = new PeerRegistry();
    const first = fakePeer("first");
    const second = fakePeer("second");
    registry.add(first, "remote-first", 0);
    registry.promote(first);
    registry.add(second, "remote-second", 0);
    let expired = false;
    registry.setTimer(
        second,
        setTimeout(() => {
            expired = true;
        }, 0),
    );

    registry.clear();

    assert.deepEqual([registry.size, registry.openCount, registry.pendingCount], [0, 0, 0]);
    assert.deepEqual(registry.allPeers, []);
    assert.equal(registry.openPeer("remote-first"), undefined);
    await afterDeadlines();
    assert.equal(expired, false, "teardown must cancel pending deadlines");
});

test("hasSynced reflects the current slots rather than a running count", () => {
    const registry = new PeerRegistry();
    const peer = fakePeer("a");
    const slot = registry.add(peer, "remote-a", 0);
    assert.equal(registry.hasSynced, false);

    slot.synced = true;
    assert.equal(registry.hasSynced, true);

    // Forgetting the only synced peer has to take the flag with it.
    registry.remove(peer);
    assert.equal(registry.hasSynced, false);
});

test("openPeers and openPeerIds report only promoted peers", () => {
    const registry = new PeerRegistry();
    const open = fakePeer("open");
    registry.add(open, "remote-open", 0);
    registry.promote(open);
    registry.add(fakePeer("pending"), "remote-pending", 0);

    assert.deepEqual(registry.openPeerIds, ["remote-open"]);
    assert.deepEqual(registry.openPeers, [open]);
    assert.equal(registry.hasOpen("remote-open"), true);
    assert.equal(registry.hasOpen("remote-pending"), false);
    assert.equal(registry.allPeers.length, 2);
});
