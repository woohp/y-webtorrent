import type * as Y from "yjs";
import * as syncProtocol from "y-protocols/sync";
import * as awarenessProtocol from "y-protocols/awareness";
import type { Awareness } from "y-protocols/awareness";
import * as encoding from "lib0/encoding";
import * as decoding from "lib0/decoding";
import { Observable } from "lib0/observable";
import { createInfoHash, createPeerId } from "./crypto.js";
import {
    TrackerConnection,
    defaultTrackerUrls,
    type OfferId,
    type PeerId,
    type TrackerOffer,
    type TrackerSignal,
} from "./tracker.js";
import { WebrtcPeer } from "./webrtc-peer.js";

export { defaultTrackerUrls };

const messageSync = 0;
const messageAwareness = 1;
const messageQueryAwareness = 3;

type PendingOffer = { peer: WebrtcPeer; tracker: TrackerConnection };
type OfferRecord = PendingOffer & { offerId: OfferId; offered: boolean };
type DocUpdateHandler = (update: Uint8Array, origin: unknown) => void;
type AwarenessUpdate = { added: number[]; updated: number[]; removed: number[] };
type AwarenessUpdateHandler = (update: AwarenessUpdate, origin: unknown) => void;

export interface WebtorrentProviderOptions {
    trackers?: readonly string[];
    password?: string;
    awareness?: Awareness;
    maxConns?: number;
    numwant?: number;
    offerTimeout?: number;
    rtcConfig?: RTCConfiguration | undefined;
    channelName?: string;
    peerId?: PeerId;
    WebSocket?: typeof WebSocket;
    debug?: boolean;
}

export interface DebugEvent {
    type: string;
    [key: string]: unknown;
}

const readMessage = (provider: WebtorrentProvider, peer: WebrtcPeer, data: ArrayBuffer): void => {
    const decoder = decoding.createDecoder(new Uint8Array(data));
    const encoder = encoding.createEncoder();
    const messageType = decoding.readVarUint(decoder);

    if (messageType === messageSync) {
        encoding.writeVarUint(encoder, messageSync);
        syncProtocol.readSyncMessage(decoder, encoder, provider.doc, peer);
        if (encoding.length(encoder) > 1) peer.send(encoding.toUint8Array(encoder));
    } else if (messageType === messageAwareness) {
        awarenessProtocol.applyAwarenessUpdate(
            provider.awareness,
            decoding.readVarUint8Array(decoder),
            peer,
        );
    } else if (messageType === messageQueryAwareness) {
        provider.sendAwareness(peer);
    }
};

export class WebtorrentProvider extends Observable<string> {
    readonly roomName: string;
    readonly doc: Y.Doc;
    readonly trackers: readonly string[];
    readonly password: string;
    readonly maxConns: number;
    readonly numwant: number;
    readonly offerTimeout: number;
    readonly rtcConfig: RTCConfiguration | undefined;
    readonly channelName: string;
    readonly peerId: PeerId;
    readonly debug: boolean;
    readonly awareness: Awareness;
    readonly ready: Promise<void>;
    shouldConnect: boolean = true;
    synced: boolean = false;
    infoHash: string | null = null;
    trackerConnections: TrackerConnection[] = [];
    peers: Map<PeerId, WebrtcPeer> = new Map();
    pendingOffers: Map<OfferId, PendingOffer> = new Map();
    private readonly _ownsAwareness: boolean;
    private readonly _docUpdateHandler: DocUpdateHandler;
    private readonly _awarenessUpdateHandler: AwarenessUpdateHandler;

    constructor(roomName: string, doc: Y.Doc, opts: WebtorrentProviderOptions = {}) {
        super();
        this.roomName = roomName;
        this.doc = doc;
        this.trackers = opts.trackers || defaultTrackerUrls;
        this.password = opts.password || "";
        this.maxConns = opts.maxConns ?? 20;
        this.numwant = opts.numwant ?? Math.min(3, Math.max(1, this.maxConns));
        this.offerTimeout = opts.offerTimeout ?? 5000;
        this.rtcConfig = opts.rtcConfig;
        this.channelName = opts.channelName ?? "y-webtorrent";
        this.peerId = opts.peerId || createPeerId();
        this.debug = !!opts.debug;
        this._ownsAwareness = !opts.awareness;
        this.awareness = opts.awareness || new awarenessProtocol.Awareness(doc);

        this._docUpdateHandler = (update, origin) => {
            if (origin !== this) this.broadcastSyncUpdate(update);
        };
        this._awarenessUpdateHandler = ({ added, updated, removed }) => {
            const changedClients = added.concat(updated, removed);
            const update = awarenessProtocol.encodeAwarenessUpdate(this.awareness, changedClients);
            this.broadcastAwareness(update);
        };
        this.doc.on("update", this._docUpdateHandler);
        this.awareness.on("update", this._awarenessUpdateHandler);

        this.ready = this.connect(opts);
    }

    async connect(opts: WebtorrentProviderOptions = {}): Promise<void> {
        this.infoHash = await createInfoHash(this.roomName, this.password);
        if (!this.shouldConnect) return;

        this.trackerConnections = this.trackers.map((url) => {
            const trackerOptions = {
                infoHash: this.infoHash!,
                peerId: this.peerId,
                numwant: this.numwant,
                createOffers: (tracker: TrackerConnection) => this.createOffers(tracker),
                onOffer: (
                    peerId: PeerId,
                    offerId: OfferId,
                    offer: TrackerSignal,
                    tracker: TrackerConnection,
                ) => void this.receiveOffer(peerId, offerId, offer, tracker),
                onAnswer: (peerId: PeerId, offerId: OfferId, answer: TrackerSignal) =>
                    void this.receiveAnswer(peerId, offerId, answer),
                onAnnounce: (message: Parameters<TrackerConnection["onAnnounce"]>[0]) =>
                    this.emit("status", [{ status: "connected", message }]),
                onError: (error: unknown) => this.emit("connection-error", [error]),
                ...(opts.WebSocket ? { WebSocket: opts.WebSocket } : {}),
            };
            return new TrackerConnection(url, trackerOptions);
        });
    }

    async createOffers(tracker: TrackerConnection): Promise<TrackerOffer[]> {
        const capacity = Math.max(0, this.maxConns - this.peers.size - this.pendingOffers.size);
        const count = Math.min(this.numwant, capacity);
        const records: OfferRecord[] = [];
        const offerPromises: Promise<void>[] = [];

        for (let i = 0; i < count; i++) {
            const offerId = createPeerId();
            const peer = this.createPeer(null, true);
            const record = { offerId, peer, tracker, offered: false };
            records.push(record);
            this.pendingOffers.set(offerId, { peer, tracker });
            offerPromises.push(
                peer.createOffer().then(() => {
                    record.offered = true;
                }),
            );
        }

        const offers = await collectOffers(records, offerPromises, this.offerTimeout, (error) =>
            this.emit("peer-error", [error]),
        );

        for (const record of records) {
            if (!record.offered) {
                this.pendingOffers.delete(record.offerId);
                record.peer.destroy();
            }
        }

        this.emitDebug({ type: "offers-created", count: offers.length, requested: count });
        return offers;
    }

    async receiveOffer(
        peerId: PeerId,
        offerId: OfferId,
        offer: TrackerSignal,
        tracker: TrackerConnection,
    ): Promise<void> {
        this.emitDebug({ type: "offer-received", peerId, offerId });
        if (peerId === this.peerId || this.peers.has(peerId) || this.peers.size >= this.maxConns)
            return;

        const peer = this.createPeer(peerId, false);
        try {
            const answer = await peer.acceptOffer(offer);
            this.emitDebug({ type: "answer-created", peerId, offerId });
            tracker.sendAnswer(peerId, offerId, answer);
        } catch (error) {
            this.emit("peer-error", [error]);
            peer.destroy();
        }
    }

    async receiveAnswer(peerId: PeerId, offerId: OfferId, answer: TrackerSignal): Promise<void> {
        this.emitDebug({
            type: "answer-received",
            peerId,
            offerId,
            knownOffer: this.pendingOffers.has(offerId),
        });
        const pending = this.pendingOffers.get(offerId);
        if (!pending || peerId === this.peerId) return;

        this.pendingOffers.delete(offerId);
        pending.peer.remotePeerId = peerId;
        this.peers.set(peerId, pending.peer);
        try {
            await pending.peer.acceptAnswer(answer);
        } catch (error) {
            this.emit("peer-error", [error]);
            pending.peer.destroy();
        }
    }

    createPeer(remotePeerId: PeerId | null, initiator: boolean): WebrtcPeer {
        const peer = new WebrtcPeer({
            initiator,
            remotePeerId,
            rtcConfig: this.rtcConfig,
            channelName: this.channelName,
            iceTimeout: this.offerTimeout,
            onOpen: (peer) => this.onPeerOpen(peer, initiator),
            onMessage: (peer, data) => readMessage(this, peer, data),
            onClose: (peer) => this.removePeer(peer),
            onError: (_peer, error) => this.emit("peer-error", [error]),
            onDebug: (event) => this.emitDebug({ type: String(event["type"]), ...event }),
        });
        this.emitDebug({
            type: "peer-created",
            initiator,
            hasRtcPeerConnection: typeof globalThis.RTCPeerConnection !== "undefined",
            hasRtcSessionDescription: typeof globalThis.RTCSessionDescription !== "undefined",
        });
        return peer;
    }

    onPeerOpen(peer: WebrtcPeer, initiator: boolean): void {
        this.emitDebug({ type: "peer-connect", peerId: peer.remotePeerId, initiator });
        if (peer.remotePeerId) this.peers.set(peer.remotePeerId, peer);
        this.sendSyncStep1(peer);
        this.sendAwareness(peer);
        this.synced = true;
        this.emit("synced", [true]);
        this.emit("peers", [Array.from(this.peers.keys())]);
    }

    removePeer(peer: WebrtcPeer): void {
        let changed = false;
        if (peer.remotePeerId && this.peers.delete(peer.remotePeerId)) changed = true;
        for (const [offerId, pending] of this.pendingOffers) {
            if (pending.peer === peer) this.pendingOffers.delete(offerId);
        }
        if (changed) this.emit("peers", [Array.from(this.peers.keys())]);
    }

    sendSyncStep1(peer: WebrtcPeer): void {
        const encoder = encoding.createEncoder();
        encoding.writeVarUint(encoder, messageSync);
        syncProtocol.writeSyncStep1(encoder, this.doc);
        peer.send(encoding.toUint8Array(encoder));
    }

    sendAwareness(peer: WebrtcPeer): void {
        const states = Array.from(this.awareness.getStates().keys());
        if (states.length === 0) return;
        const encoder = encoding.createEncoder();
        encoding.writeVarUint(encoder, messageAwareness);
        encoding.writeVarUint8Array(
            encoder,
            awarenessProtocol.encodeAwarenessUpdate(this.awareness, states),
        );
        peer.send(encoding.toUint8Array(encoder));
    }

    broadcastSyncUpdate(update: Uint8Array): void {
        const encoder = encoding.createEncoder();
        encoding.writeVarUint(encoder, messageSync);
        syncProtocol.writeUpdate(encoder, update);
        this.broadcast(encoding.toUint8Array(encoder));
    }

    broadcastAwareness(update: Uint8Array): void {
        const encoder = encoding.createEncoder();
        encoding.writeVarUint(encoder, messageAwareness);
        encoding.writeVarUint8Array(encoder, update);
        this.broadcast(encoding.toUint8Array(encoder));
    }

    broadcast(message: Uint8Array<ArrayBuffer>): void {
        for (const peer of this.peers.values()) {
            if (peer.connected) peer.send(message);
        }
    }

    emitDebug(event: DebugEvent): void {
        if (this.debug) this.emit("debug", [event]);
    }

    disconnect(): void {
        this.shouldConnect = false;
        for (const tracker of this.trackerConnections) tracker.destroy();
        this.trackerConnections = [];
        for (const peer of this.peers.values()) peer.destroy();
        for (const { peer } of this.pendingOffers.values()) peer.destroy();
        this.peers.clear();
        this.pendingOffers.clear();
        this.synced = false;
        this.emit("synced", [false]);
    }

    override destroy(): void {
        this.disconnect();
        this.doc.off("update", this._docUpdateHandler);
        this.awareness.off("update", this._awarenessUpdateHandler);
        if (this._ownsAwareness) this.awareness.destroy();
        super.destroy();
    }
}

const collectOffers = async (
    records: OfferRecord[],
    offerPromises: Promise<void>[],
    timeout: number,
    onError: (error: unknown) => void,
): Promise<TrackerOffer[]> => {
    const offers: TrackerOffer[] = [];
    await Promise.race([
        Promise.all(
            offerPromises.map((promise, index) =>
                promise.catch((error) => {
                    onError(error);
                    records[index]!.peer.destroy();
                }),
            ),
        ),
        new Promise((resolve) => setTimeout(resolve, timeout)),
    ]);

    for (const record of records) {
        if (record.offered && record.peer.pc.localDescription) {
            offers.push({
                offer_id: record.offerId,
                offer: record.peer.pc.localDescription.toJSON(),
            });
        }
    }
    return offers;
};
