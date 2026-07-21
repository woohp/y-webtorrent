import { expect, test } from "@playwright/test";
import { WebSocketServer } from "ws";

let tracker;
let trackerUrl;

test.beforeAll(async () => {
    tracker = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    await new Promise((resolve) => tracker.once("listening", resolve));
    trackerUrl = `ws://127.0.0.1:${tracker.address().port}/`;
    const peers = new Map();

    tracker.on("connection", (socket) => {
        socket.on("message", (data) => {
            const message = JSON.parse(data.toString());
            if (message.action !== "announce") return;

            peers.set(message.peer_id, { socket, infoHash: message.info_hash });
            socket.send(
                JSON.stringify({
                    action: "announce",
                    info_hash: message.info_hash,
                    interval: 120,
                    complete: peers.size,
                    incomplete: 0,
                }),
            );

            if (message.answer && message.to_peer_id) {
                const target = peers.get(message.to_peer_id);
                target?.socket.send(JSON.stringify(message));
                return;
            }

            const target = [...peers.entries()].find(
                ([peerId, peer]) =>
                    peerId !== message.peer_id && peer.infoHash === message.info_hash,
            );
            if (!target) return;
            for (const { offer_id, offer } of message.offers ?? []) {
                target[1].socket.send(
                    JSON.stringify({
                        action: "announce",
                        info_hash: message.info_hash,
                        peer_id: message.peer_id,
                        offer_id,
                        offer,
                    }),
                );
            }
        });
        socket.on("close", () => {
            for (const [peerId, peer] of peers) {
                if (peer.socket === socket) peers.delete(peerId);
            }
        });
    });
});

test.afterAll(() => {
    for (const client of tracker.clients) client.terminate();
    return new Promise((resolve) => tracker.close(resolve));
});

test("syncs text between two browser tabs over WebRTC", async ({ browser }) => {
    const context = await browser.newContext();
    const first = await context.newPage();
    const second = await context.newPage();
    const room = `ci-${Date.now()}`;
    const url = `/?room=${room}&trackers=${encodeURIComponent(trackerUrl)}`;

    await Promise.all([first.goto(url), second.goto(url)]);
    // bootstrap.js loads the application with a dynamic import, which can finish after `load`.
    await Promise.all([
        expect(first.locator("#status")).toContainText(`room: ${room}`),
        expect(second.locator("#status")).toContainText(`room: ${room}`),
    ]);
    await Promise.all([
        first.getByRole("button", { name: "Connect", exact: true }).click(),
        second.getByRole("button", { name: "Connect", exact: true }).click(),
    ]);

    await expect(first.locator("#status")).toContainText("connected: 1", { timeout: 20_000 });
    await expect(second.locator("#status")).toContainText("connected: 1", { timeout: 20_000 });

    await first.locator("#text").fill("browser e2e sync");
    await expect(second.locator("#text")).toHaveValue("browser e2e sync");

    await context.close();
});
