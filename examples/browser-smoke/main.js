import * as Y from "yjs";
import { WebtorrentProvider } from "../../src/index.ts";

const roomInput = document.querySelector("#room");
const trackersInput = document.querySelector("#trackers");
const connectButton = document.querySelector("#connect");
const disconnectButton = document.querySelector("#disconnect");
const textArea = document.querySelector("#text");
const statusEl = document.querySelector("#status");

roomInput.value = new URL(location.href).searchParams.get("room") || "y-webtorrent-smoke";
trackersInput.value = new URL(location.href).searchParams.get("trackers") || "ws://localhost:4000/";

let doc = null;
let provider = null;
let ytext = null;
let applyingRemote = false;
const logs = [];

const log = (...args) => {
  logs.unshift(`[${new Date().toLocaleTimeString()}] ${args.map(format).join(" ")}`);
  logs.splice(80);
  renderStatus();
};

const format = (value) => {
  if (typeof value === "string") return value;
  if (value instanceof Error) return value.stack || value.message;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};

const renderStatus = () => {
  statusEl.textContent = [
    `room: ${roomInput.value}`,
    `trackers: ${trackersInput.value}`,
    `connected: ${provider ? provider.peers.size : 0}`,
    `peer id: ${provider ? JSON.stringify(provider.peerId) : "-"}`,
    "",
    ...logs,
  ].join("\n");
};

const connect = () => {
  disconnect();

  doc = new Y.Doc();
  ytext = doc.getText("text");
  provider = new WebtorrentProvider(roomInput.value, doc, {
    trackers: trackersInput.value
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean),
    debug: true,
  });

  provider.on("status", (event) => log("tracker status", event));
  provider.on("peers", (peers) => log("peers", peers));
  provider.on("synced", (synced) => log("synced", synced));
  provider.on("connection-error", (event) => log("connection-error", event));
  provider.on("peer-error", (event) => log("peer-error", event));
  provider.on("debug", (event) => log("debug", event));

  ytext.observe(() => {
    applyingRemote = true;
    textArea.value = ytext.toString();
    applyingRemote = false;
  });

  textArea.disabled = false;
  connectButton.disabled = true;
  disconnectButton.disabled = false;
  log("connecting");
  provider.ready.then(() => log("provider ready"));
  renderStatus();
};

const disconnect = () => {
  if (provider) provider.destroy();
  if (doc) doc.destroy();
  provider = null;
  doc = null;
  ytext = null;
  textArea.disabled = true;
  connectButton.disabled = false;
  disconnectButton.disabled = true;
  renderStatus();
};

textArea.addEventListener("input", () => {
  if (!ytext || applyingRemote) return;
  doc.transact(() => {
    ytext.delete(0, ytext.length);
    ytext.insert(0, textArea.value);
  });
});

connectButton.addEventListener("click", connect);
disconnectButton.addEventListener("click", disconnect);
window.addEventListener("beforeunload", disconnect);

renderStatus();
