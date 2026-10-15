import assert from "node:assert/strict";
import test from "node:test";

const { collectCandidateTypes } = await import("../dist/sdp.js");

const candidateLine = (type) => `a=candidate:1 1 UDP 2130706431 192.0.2.1 5000 typ ${type}`;

test("reports deduplicated, sorted candidate types across signals", () => {
    const offer = ["v=0", candidateLine("srflx"), candidateLine("host"), candidateLine("host")];
    const answer = [candidateLine("relay"), candidateLine("prflx")];
    assert.deepEqual(
        collectCandidateTypes([
            { type: "offer", sdp: offer.join("\r\n") },
            { type: "answer", sdp: answer.join("\r\n") },
        ]),
        ["host", "prflx", "relay", "srflx"],
    );
});

test("ignores missing SDP, unknown types, and partial word matches", () => {
    const sdp = [candidateLine("bogus"), "a=note:prototyp host", "a=note:relay"].join("\r\n");
    assert.deepEqual(collectCandidateTypes([{ type: "offer" }, { type: "offer", sdp }]), []);
});
