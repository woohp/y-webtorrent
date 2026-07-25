import assert from "node:assert/strict";
import test from "node:test";

const { collectCandidateTypes } = await import("../dist/sdp.js");

const candidateLine = (type, address = "192.0.2.1") =>
    `a=candidate:1 1 UDP 2130706431 ${address} 5000 typ ${type}`;

test("reports the candidate types present in a session description", () => {
    const sdp = ["v=0", candidateLine("host", "10.0.0.1"), candidateLine("srflx")].join("\r\n");
    assert.deepEqual(collectCandidateTypes([{ type: "offer", sdp }]), ["host", "srflx"]);
});

test("returns types sorted rather than in SDP order", () => {
    const sdp = [candidateLine("srflx"), candidateLine("relay"), candidateLine("host")].join(
        "\r\n",
    );
    assert.deepEqual(collectCandidateTypes([{ type: "offer", sdp }]), ["host", "relay", "srflx"]);
});

test("deduplicates across candidates and across signals", () => {
    const first = [candidateLine("host", "10.0.0.1"), candidateLine("host", "10.0.0.2")].join(
        "\r\n",
    );
    const second = [candidateLine("host", "10.0.0.3"), candidateLine("relay")].join("\r\n");
    assert.deepEqual(
        collectCandidateTypes([
            { type: "offer", sdp: first },
            { type: "answer", sdp: second },
        ]),
        ["host", "relay"],
    );
});

test("recognizes every ICE candidate type", () => {
    const sdp = ["host", "srflx", "prflx", "relay"].map((type) => candidateLine(type)).join("\r\n");
    assert.deepEqual(collectCandidateTypes([{ type: "offer", sdp }]), [
        "host",
        "prflx",
        "relay",
        "srflx",
    ]);
});

test("tolerates signals without SDP and empty input", () => {
    assert.deepEqual(collectCandidateTypes([]), []);
    assert.deepEqual(collectCandidateTypes([{ type: "offer" }]), []);
    assert.deepEqual(collectCandidateTypes([{ type: "offer", sdp: "" }]), []);
    assert.deepEqual(collectCandidateTypes([{ type: "offer", sdp: "v=0\r\nm=application 9" }]), []);
});

test("ignores unknown candidate types and partial word matches", () => {
    const sdp = [
        candidateLine("bogus"),
        "a=fingerprint:sha-256 AA:BB",
        // `typ` must stand alone: neither a longer word ending in it nor a bare type name counts.
        "a=note:prototyp host",
        "a=note:relay",
    ].join("\r\n");
    assert.deepEqual(collectCandidateTypes([{ type: "offer", sdp }]), []);
});
