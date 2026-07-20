import type * as Y from "yjs";
import * as syncProtocol from "y-protocols/sync";
import * as awarenessProtocol from "y-protocols/awareness";
import type { Awareness } from "y-protocols/awareness";
import * as encoding from "lib0/encoding";
import * as decoding from "lib0/decoding";
import { ObservableV2 } from "lib0/observable";
import { createInfoHash, createPeerId } from "./crypto.js";
import {
    TrackerConnection,
    defaultTrackerUrls,
    type OfferId,
    type PeerId,
    type TrackerAnnounceMessage,
    type TrackerOffer,
    type TrackerSignal,
} from "./tracker.js";
import { WebrtcDataPeer } from "webrtc-data-peer";

export { defaultTrackerUrls };

export const defaultRtcConfig: RTCConfiguration = {
    iceServers: [
        { urls: "stun:stun.l.google.com:19302" },
        { urls: "stun:stun.cloudflare.com:3478" },
    ],
};

const cloneDefaultRtcConfig = (): RTCConfiguration => {
    const iceServers = defaultRtcConfig.iceServers?.map((server) => ({
        ...server,
        urls: Array.isArray(server.urls) ? [...server.urls] : server.urls,
    }));
    return { ...defaultRtcConfig, ...(iceServers ? { iceServers } : {}) };
};

const maximumTimerDelay = 2_147_483_647;

const validatePeerId = (peerId: string): PeerId => {
    if (typeof peerId !== "string") throw new TypeError("peerId must be a string");
    if (peerId.length !== 20 || [...peerId].some((character) => character.charCodeAt(0) > 255)) {
        throw new RangeError("peerId must be a raw 20-character binary string");
    }
    return peerId;
};

const validateCount = (name: string, value: number): number => {
    if (typeof value !== "number") throw new TypeError(`${name} must be a number`);
    if (!Number.isFinite(value) || value < 0 || !Number.isInteger(value)) {
        throw new RangeError(`${name} must be a finite, non-negative integer`);
    }
    return value;
};

const validateTimeout = (name: string, value: number): number => {
    if (typeof value !== "number") throw new TypeError(`${name} must be a number`);
    if (!Number.isFinite(value) || value < 0 || value > maximumTimerDelay) {
        throw new RangeError(`${name} must be between 0 and ${maximumTimerDelay}`);
    }
    return value;
};

const validateLimit = (name: string, value: number): number => {
    if (typeof value !== "number") throw new TypeError(`${name} must be a number`);
    if (Number.isNaN(value) || value < 0) {
        throw new RangeError(`${name} must be non-negative or Infinity`);
    }
    return value;
};

const messageSync = 0;
const messageAwareness = 1;
const messageQueryAwareness = 3;
const messageDirect = 4;

type PendingOffer = {
    peer: WebrtcDataPeer;
    canceled: boolean;
};
type OfferRecord = PendingOffer & { offerId: OfferId; offered: boolean };
type DocUpdateHandler = (update: Uint8Array, origin: unknown) => void;
type AwarenessUpdate = { added: number[]; updated: number[]; removed: number[] };
type AwarenessUpdateHandler = (update: AwarenessUpdate, origin: unknown) => void;

export interface WebtorrentProviderOptions {
    trackers?: readonly string[];
    awareness?: Awareness;
    maxConns?: number;
    numwant?: number;
    offerTimeout?: number;
    offerCollectionTimeout?: number;
    signalTimeout?: number;
    trackerConnectTimeout?: number;
    announceResponseTimeout?: number;
    fallbackMaxMessageSize?: number;
    maxBufferedAmount?: number;
    rtcConfig?: RTCConfiguration | undefined;
    channelName?: string;
    peerId?: PeerId;
    WebSocket?: typeof WebSocket;
    RTCPeerConnection?: typeof RTCPeerConnection;
    debug?: boolean;
}

export interface DebugEvent {
    type: string;
    [key: string]: unknown;
}

export interface TrackerStatusEvent {
    status: "connected" | "reconnecting" | "disconnected";
    tracker: string;
}

export interface ListenerErrorEvent {
    eventName: keyof WebtorrentProviderEvents;
    error: unknown;
}

export interface WebtorrentProviderEvents {
    announce: (message: TrackerAnnounceMessage) => void;
    "connection-error": (error: unknown) => void;
    debug: (event: DebugEvent) => void;
    "direct-message": (peerId: PeerId, data: Uint8Array) => void;
    "listener-error": (event: ListenerErrorEvent) => void;
    peers: (peerIds: PeerId[]) => void;
    "peer-error": (error: unknown) => void;
    status: (event: TrackerStatusEvent) => void;
    synced: (synced: boolean) => void;
}

const readMessage = (
    provider: WebtorrentProvider,
    peer: WebrtcDataPeer,
    peerId: PeerId | null,
    data: ArrayBuffer,
    sendProtocolMessage: (message: Uint8Array<ArrayBuffer>) => boolean,
    sendAwareness: () => boolean,
    emitDirectMessage: (data: Uint8Array) => void,
): boolean => {
    const decoder = decoding.createDecoder(new Uint8Array(data));
    const encoder = encoding.createEncoder();
    const messageType = decoding.readVarUint(decoder);

    if (messageType === messageSync) {
        encoding.writeVarUint(encoder, messageSync);
        const syncType = syncProtocol.readSyncMessage(decoder, encoder, provider.doc, peer);
        if (encoding.length(encoder) > 1 && !sendProtocolMessage(encoding.toUint8Array(encoder))) {
            return false;
        }
        return syncType === syncProtocol.messageYjsSyncStep2;
    } else if (messageType === messageAwareness) {
        awarenessProtocol.applyAwarenessUpdate(
            provider.awareness,
            decoding.readVarUint8Array(decoder),
            peer,
        );
    } else if (messageType === messageQueryAwareness) {
        sendAwareness();
    } else if (messageType === messageDirect && peerId) {
        emitDirectMessage(decoding.readVarUint8Array(decoder));
    }
    return false;
};

export class WebtorrentProvider extends ObservableV2<WebtorrentProviderEvents> {
    readonly roomName: string;
    readonly doc: Y.Doc;
    readonly trackers: readonly string[];
    readonly maxConns: number;
    readonly numwant: number;
    readonly offerTimeout: number;
    readonly offerCollectionTimeout: number;
    readonly signalTimeout: number;
    readonly trackerConnectTimeout: number;
    readonly announceResponseTimeout: number;
    readonly fallbackMaxMessageSize: number;
    readonly maxBufferedAmount: number;
    readonly rtcConfig: RTCConfiguration;
    readonly channelName: string;
    readonly peerId: PeerId;
    readonly debug: boolean;
    readonly awareness: Awareness;
    readonly ready: Promise<void>;
    shouldConnect = true;
    synced = false;
    infoHash: string | null = null;
    private trackerConnections: TrackerConnection[] = [];
    private peers: Map<PeerId, WebrtcDataPeer> = new Map();
    private pendingOffers: Map<OfferId, PendingOffer> = new Map();
    private readonly pendingPeerIds = new Map<PeerId, WebrtcDataPeer>();
    private readonly pendingPeers = new Set<WebrtcDataPeer>();
    private readonly pendingTimers = new Map<WebrtcDataPeer, ReturnType<typeof setTimeout>>();
    private readonly syncedPeers = new Set<WebrtcDataPeer>();
    private readonly peerIds = new WeakMap<WebrtcDataPeer, PeerId | null>();
    private readonly _WebSocket: typeof WebSocket | undefined;
    private readonly _RTCPeerConnection: typeof RTCPeerConnection | undefined;
    private readonly _ownsAwareness: boolean;
    private readonly _docUpdateHandler: DocUpdateHandler;
    private readonly _awarenessUpdateHandler: AwarenessUpdateHandler;
    private connectionGeneration = 0;
    private connectionActive = false;
    private connectionPromise: Promise<void> | null = null;
    private destroyed = false;

    constructor(roomName: string, doc: Y.Doc, opts: WebtorrentProviderOptions = {}) {
        super();
        this.roomName = roomName;
        this.doc = doc;
        this.trackers = opts.trackers || defaultTrackerUrls;
        this.maxConns = validateCount("maxConns", opts.maxConns ?? 20);
        this.numwant = validateCount("numwant", opts.numwant ?? Math.min(3, this.maxConns));
        this.offerTimeout = validateTimeout("offerTimeout", opts.offerTimeout ?? 5000);
        this.offerCollectionTimeout = validateTimeout(
            "offerCollectionTimeout",
            opts.offerCollectionTimeout ?? this.offerTimeout + 5000,
        );
        this.signalTimeout = validateTimeout("signalTimeout", opts.signalTimeout ?? 15000);
        this.trackerConnectTimeout = validateTimeout(
            "trackerConnectTimeout",
            opts.trackerConnectTimeout ?? 10000,
        );
        this.announceResponseTimeout = validateTimeout(
            "announceResponseTimeout",
            opts.announceResponseTimeout ?? 15000,
        );
        this.fallbackMaxMessageSize = validateLimit(
            "fallbackMaxMessageSize",
            opts.fallbackMaxMessageSize ?? 256 * 1024,
        );
        this.maxBufferedAmount = validateLimit(
            "maxBufferedAmount",
            opts.maxBufferedAmount ?? 4 * 1024 * 1024,
        );
        this.rtcConfig = opts.rtcConfig ?? cloneDefaultRtcConfig();
        this.channelName = opts.channelName ?? "y-webtorrent";
        this.peerId = opts.peerId === undefined ? createPeerId() : validatePeerId(opts.peerId);
        this.debug = !!opts.debug;
        this._WebSocket = opts.WebSocket;
        this._RTCPeerConnection = opts.RTCPeerConnection;
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

        this.ready = this.connect();
    }

    connect(): Promise<void> {
        if (this.destroyed) return Promise.resolve();
        if (this.connectionPromise) return this.connectionPromise;
        if (this.connectionActive) return Promise.resolve();

        this.shouldConnect = true;
        this.connectionActive = true;
        const generation = ++this.connectionGeneration;
        const attempt = this.initializeConnection(generation);
        const tracked = attempt
            .catch((error: unknown) => {
                if (generation === this.connectionGeneration) this.connectionActive = false;
                throw error;
            })
            .finally(() => {
                if (this.connectionPromise === tracked) this.connectionPromise = null;
            });
        this.connectionPromise = tracked;
        return tracked;
    }

    private async initializeConnection(generation: number): Promise<void> {
        const infoHash = await createInfoHash(this.roomName);
        if (!this.isCurrentGeneration(generation)) return;
        this.infoHash = infoHash;

        for (const url of this.trackers) {
            if (!this.isCurrentGeneration(generation)) break;
            try {
                const trackerUrl = new URL(url).href;
                const trackerOptions = {
                    infoHash,
                    peerId: this.peerId,
                    numwant: this.numwant,
                    createOffers: () => this.createOffers(generation),
                    cancelOffers: (offerIds: readonly OfferId[]) => this.cancelOffers(offerIds),
                    onOffer: (
                        peerId: PeerId,
                        offerId: OfferId,
                        offer: TrackerSignal,
                        tracker: TrackerConnection,
                    ) => {
                        void this.receiveOffer(peerId, offerId, offer, tracker, generation).catch(
                            (error: unknown) => {
                                if (this.isCurrentGeneration(generation)) {
                                    this.emitSafely("connection-error", [error]);
                                }
                            },
                        );
                    },
                    onAnswer: (peerId: PeerId, offerId: OfferId, answer: TrackerSignal) => {
                        void this.receiveAnswer(peerId, offerId, answer, generation).catch(
                            (error: unknown) => {
                                if (this.isCurrentGeneration(generation)) {
                                    this.emitSafely("connection-error", [error]);
                                }
                            },
                        );
                    },
                    onAnnounce: (message: Parameters<TrackerConnection["onAnnounce"]>[0]) => {
                        if (this.isCurrentGeneration(generation))
                            this.emitSafely("announce", [message]);
                    },
                    onState: (status: Parameters<TrackerConnection["onState"]>[0]) => {
                        if (generation === this.connectionGeneration) {
                            this.emitSafely("status", [{ status, tracker: trackerUrl }]);
                        }
                    },
                    onError: (error: unknown) => {
                        if (this.isCurrentGeneration(generation)) {
                            this.emitSafely("connection-error", [error]);
                        }
                    },
                    connectTimeout: this.trackerConnectTimeout,
                    announceResponseTimeout: this.announceResponseTimeout,
                    ...(this._WebSocket ? { WebSocket: this._WebSocket } : {}),
                };
                this.trackerConnections.push(new TrackerConnection(trackerUrl, trackerOptions));
            } catch (error) {
                this.emitSafely("connection-error", [error]);
            }
        }
    }

    private async createOffers(
        generation: number = this.connectionGeneration,
    ): Promise<TrackerOffer[]> {
        if (!this.isCurrentGeneration(generation)) return [];
        const capacity = Math.max(0, this.maxConns - this.peers.size - this.pendingPeers.size);
        const count = Math.min(this.numwant, capacity);
        const records: OfferRecord[] = [];
        const offerPromises: Promise<void>[] = [];
        let negotiationFailed = false;

        for (let i = 0; i < count; i++) {
            const offerId = createPeerId();
            let peer: WebrtcDataPeer;
            try {
                peer = this.createPeer(null, true, generation);
            } catch (error) {
                this.emitSafely("peer-error", [error]);
                negotiationFailed = true;
                continue;
            }
            this.pendingPeers.add(peer);
            const record: OfferRecord = {
                offerId,
                peer,
                offered: false,
                canceled: false,
            };
            records.push(record);
            this.pendingOffers.set(offerId, record);
            offerPromises.push(
                peer.createOffer().then(() => {
                    if (!record.canceled && this.pendingOffers.get(offerId) === record) {
                        record.offered = true;
                    }
                }),
            );
        }

        const offers = await collectOffers(
            records,
            offerPromises,
            this.offerCollectionTimeout,
            (error) => {
                if (this.isCurrentGeneration(generation)) {
                    negotiationFailed = true;
                    this.emitSafely("peer-error", [error]);
                }
            },
        );

        if (!this.isCurrentGeneration(generation)) {
            for (const record of records) this.cancelPendingPeer(record.peer);
            return [];
        }

        for (const record of records) {
            if (!record.offered || record.canceled) {
                this.cancelPendingPeer(record.peer);
            } else {
                this.startPeerTimeout(record.peer);
            }
        }

        if (negotiationFailed) this.requestRecoveryAnnounce();
        this.emitDebug({ type: "offers-created", count: offers.length, requested: count });
        return offers;
    }

    private cancelOffers(offerIds: readonly OfferId[]): void {
        for (const offerId of offerIds) {
            const pending = this.pendingOffers.get(offerId);
            if (pending) this.cancelPendingPeer(pending.peer);
        }
    }

    private async receiveOffer(
        peerId: PeerId,
        offerId: OfferId,
        offer: TrackerSignal,
        tracker: TrackerConnection,
        generation: number = this.connectionGeneration,
    ): Promise<void> {
        if (!this.isCurrentGeneration(generation)) return;
        this.emitDebug({ type: "offer-received", peerId, offerId });
        if (peerId === this.peerId || this.peers.has(peerId)) return;
        const identifiedPending = this.pendingPeerIds.get(peerId);
        if (identifiedPending) {
            if (this.peerId > peerId && identifiedPending.initiator) {
                this.cancelPendingPeer(identifiedPending);
            } else {
                return;
            }
        }

        const competingOffer = this.pendingOffers.entries().next().value as
            | [OfferId, PendingOffer]
            | undefined;
        const peerCount = this.peers.size + this.pendingPeers.size;
        if (peerCount >= this.maxConns) {
            if (!competingOffer || this.maxConns === 0) return;
            if (this.peerId > peerId) {
                this.cancelPendingPeer(competingOffer[1].peer);
            } else {
                // Allow one temporary inbound peer so one-way offer delivery can still connect.
                // A later answer deterministically removes the losing duplicate.
                if (peerCount >= this.maxConns + 1) return;
            }
        }
        if (this.peers.size + this.pendingPeers.size > this.maxConns) return;

        let peer: WebrtcDataPeer;
        try {
            peer = this.createPeer(peerId, false, generation);
        } catch (error) {
            this.emitSafely("peer-error", [error]);
            this.requestRecoveryAnnounce();
            return;
        }
        this.pendingPeers.add(peer);
        this.pendingPeerIds.set(peerId, peer);
        this.startPeerTimeout(peer);
        try {
            const answer = await peer.acceptOffer(offer);
            this.emitDebug({ type: "answer-created", peerId, offerId });
            if (!this.isCurrentGeneration(generation) || this.pendingPeerIds.get(peerId) !== peer) {
                this.cancelPendingPeer(peer);
                return;
            }
            const trackerWasOpen = tracker.isOpen();
            if (!tracker.sendAnswer(peerId, offerId, answer)) {
                if (!trackerWasOpen) {
                    this.emitSafely("connection-error", [
                        new Error("No open tracker accepted the answer"),
                    ]);
                }
                this.cancelPendingPeer(peer);
                this.requestRecoveryAnnounce();
                return;
            }
        } catch (error) {
            if (!this.isCurrentGeneration(generation) || this.pendingPeerIds.get(peerId) !== peer) {
                return;
            }
            this.emitSafely("peer-error", [error]);
            this.cancelPendingPeer(peer);
            this.requestRecoveryAnnounce();
        }
    }

    private async receiveAnswer(
        peerId: PeerId,
        offerId: OfferId,
        answer: TrackerSignal,
        generation: number = this.connectionGeneration,
    ): Promise<void> {
        if (!this.isCurrentGeneration(generation)) return;
        this.emitDebug({
            type: "answer-received",
            peerId,
            offerId,
            knownOffer: this.pendingOffers.has(offerId),
        });
        const pending = this.pendingOffers.get(offerId);
        if (!pending || peerId === this.peerId) return;

        if (this.peers.has(peerId)) {
            this.cancelPendingPeer(pending.peer);
            return;
        }
        const existingPending = this.pendingPeerIds.get(peerId);
        if (existingPending) {
            if (this.peerId < peerId && !existingPending.initiator) {
                this.cancelPendingPeer(existingPending);
            } else {
                this.cancelPendingPeer(pending.peer);
                return;
            }
        }

        if (this.peers.size >= this.maxConns) {
            this.cancelPendingPeer(pending.peer);
            return;
        }

        this.pendingOffers.delete(offerId);
        this.peerIds.set(pending.peer, peerId);
        this.pendingPeerIds.set(peerId, pending.peer);
        try {
            await pending.peer.acceptAnswer(answer);
            if (!this.isCurrentGeneration(generation)) {
                this.cancelPendingPeer(pending.peer);
                return;
            }
            if (this.peers.get(peerId) === pending.peer) return;
            if (this.pendingPeerIds.get(peerId) !== pending.peer) {
                this.cancelPendingPeer(pending.peer);
                return;
            }
        } catch (error) {
            if (
                !this.isCurrentGeneration(generation) ||
                this.pendingPeerIds.get(peerId) !== pending.peer
            ) {
                return;
            }
            this.emitSafely("peer-error", [error]);
            this.cancelPendingPeer(pending.peer);
            this.requestRecoveryAnnounce();
        }
    }

    private createPeer(
        remotePeerId: PeerId | null,
        initiator: boolean,
        generation: number = this.connectionGeneration,
    ): WebrtcDataPeer {
        const peer = new WebrtcDataPeer({
            initiator,
            rtcConfig: this.rtcConfig,
            channelLabel: this.channelName,
            iceGatheringTimeout: this.offerTimeout,
            fallbackMaxMessageSize: this.fallbackMaxMessageSize,
            maxBufferedAmount: this.maxBufferedAmount,
            ...(this._RTCPeerConnection ? { RTCPeerConnection: this._RTCPeerConnection } : {}),
            onOpen: (peer) => this.onPeerOpen(peer, initiator, generation),
            onMessage: (peer, data) => this.onPeerMessage(peer, data, generation),
            onClose: (peer) => this.removePeer(peer, generation),
            onError: (_peer, error) => {
                if (this.isCurrentGeneration(generation)) this.emitSafely("peer-error", [error]);
            },
            onDebug: (event) => this.emitDebug({ ...event, type: String(event["type"]) }),
        });
        this.peerIds.set(peer, remotePeerId);
        this.emitDebug({
            type: "peer-created",
            initiator,
            hasRtcPeerConnection:
                this._RTCPeerConnection !== undefined ||
                typeof globalThis.RTCPeerConnection !== "undefined",
            hasRtcSessionDescription: typeof globalThis.RTCSessionDescription !== "undefined",
        });
        return peer;
    }

    private onPeerOpen(
        peer: WebrtcDataPeer,
        initiator: boolean,
        generation: number = this.connectionGeneration,
    ): void {
        if (!this.isCurrentGeneration(generation)) {
            this.cancelPendingPeer(peer);
            return;
        }
        const peerId = this.peerIds.get(peer) ?? null;
        if (!peerId) {
            this.cancelPendingPeer(peer);
            return;
        }
        const existing = this.peers.get(peerId);
        if (existing && existing !== peer) {
            this.cancelPendingPeer(peer);
            return;
        }
        if (!existing && this.peers.size >= this.maxConns) {
            this.cancelPendingPeer(peer);
            return;
        }
        this.clearPendingPeer(peer);
        this.peers.set(peerId, peer);
        this.emitDebug({ type: "peer-connect", peerId, initiator });
        if (!this.sendSyncStep1(peer) || !this.sendAwareness(peer)) return;
        this.emitSafely("peers", [Array.from(this.peers.keys())]);
    }

    private onPeerMessage(
        peer: WebrtcDataPeer,
        data: ArrayBuffer,
        generation: number = this.connectionGeneration,
    ): void {
        if (!this.isCurrentGeneration(generation)) return;
        const peerId = this.peerIds.get(peer) ?? null;
        if (!peerId || this.peers.get(peerId) !== peer || !peer.connected) return;
        try {
            if (
                readMessage(
                    this,
                    peer,
                    peerId,
                    data,
                    (message) => this.sendProtocolMessage(peer, message),
                    () => this.sendAwareness(peer),
                    (message) => this.emitSafely("direct-message", [peerId, message]),
                )
            ) {
                this.syncedPeers.add(peer);
                this.updateSyncedState();
            }
        } catch (error) {
            this.emitSafely("peer-error", [error]);
            peer.destroy();
        }
    }

    private removePeer(peer: WebrtcDataPeer, generation: number = this.connectionGeneration): void {
        if (generation !== this.connectionGeneration) return;
        this.syncedPeers.delete(peer);
        this.updateSyncedState();
        this.clearPendingPeer(peer);
        let changed = false;
        const peerId = this.peerIds.get(peer) ?? null;
        if (peerId && this.peers.get(peerId) === peer) {
            this.peers.delete(peerId);
            changed = true;
        }
        if (changed) {
            this.requestRecoveryAnnounce();
            this.emitSafely("peers", [Array.from(this.peers.keys())]);
        }
    }

    private sendProtocolMessage(peer: WebrtcDataPeer, message: Uint8Array<ArrayBuffer>): boolean {
        try {
            if (peer.send(message)) return true;
        } catch (error) {
            this.emitSafely("peer-error", [error]);
        }
        peer.destroy();
        return false;
    }

    private sendSyncStep1(peer: WebrtcDataPeer): boolean {
        const encoder = encoding.createEncoder();
        encoding.writeVarUint(encoder, messageSync);
        syncProtocol.writeSyncStep1(encoder, this.doc);
        return this.sendProtocolMessage(peer, encoding.toUint8Array(encoder));
    }

    private sendAwareness(peer: WebrtcDataPeer): boolean {
        const states = Array.from(this.awareness.getStates().keys());
        if (states.length === 0) return true;
        const encoder = encoding.createEncoder();
        encoding.writeVarUint(encoder, messageAwareness);
        encoding.writeVarUint8Array(
            encoder,
            awarenessProtocol.encodeAwarenessUpdate(this.awareness, states),
        );
        return this.sendProtocolMessage(peer, encoding.toUint8Array(encoder));
    }

    private broadcastSyncUpdate(update: Uint8Array): void {
        const encoder = encoding.createEncoder();
        encoding.writeVarUint(encoder, messageSync);
        syncProtocol.writeUpdate(encoder, update);
        this.broadcast(encoding.toUint8Array(encoder));
    }

    private broadcastAwareness(update: Uint8Array): void {
        const encoder = encoding.createEncoder();
        encoding.writeVarUint(encoder, messageAwareness);
        encoding.writeVarUint8Array(encoder, update);
        this.broadcast(encoding.toUint8Array(encoder));
    }

    sendToPeer(peerId: PeerId, message: Uint8Array<ArrayBuffer>): boolean {
        const peer = this.peers.get(peerId);
        if (!peer?.connected) return false;

        const encoder = encoding.createEncoder();
        encoding.writeVarUint(encoder, messageDirect);
        encoding.writeVarUint8Array(encoder, message);
        return peer.send(encoding.toUint8Array(encoder));
    }

    private broadcast(message: Uint8Array<ArrayBuffer>): void {
        for (const peer of this.peers.values()) {
            if (peer.connected) this.sendProtocolMessage(peer, message);
        }
    }

    private emitSafely<Name extends keyof WebtorrentProviderEvents & string>(
        eventName: Name,
        args: Parameters<WebtorrentProviderEvents[Name]>,
    ): void {
        const observers = this._observers.get(eventName) as
            | Set<(...observerArgs: Parameters<WebtorrentProviderEvents[Name]>) => void>
            | undefined;
        if (!observers) return;
        for (const observer of Array.from(observers)) {
            try {
                observer(...args);
            } catch (error) {
                if (eventName !== "listener-error") {
                    this.emitSafely("listener-error", [{ eventName, error }]);
                }
            }
        }
    }

    private emitDebug(event: DebugEvent): void {
        if (this.debug) this.emitSafely("debug", [event]);
    }

    disconnect(): void {
        const localAwarenessState =
            this._ownsAwareness && !this.destroyed ? this.awareness.getLocalState() : null;
        if (localAwarenessState !== null) this.awareness.setLocalState(null);
        this.shouldConnect = false;
        this.connectionActive = false;
        this.connectionPromise = null;
        this.connectionGeneration++;
        const disconnectedTrackers = this.trackerConnections;
        for (const tracker of disconnectedTrackers) tracker.destroy();
        this.trackerConnections = [];
        for (const peer of this.peers.values()) peer.destroy();
        for (const peer of this.pendingPeers) peer.destroy();
        this.peers.clear();
        this.pendingOffers.clear();
        this.pendingPeerIds.clear();
        this.pendingPeers.clear();
        for (const timer of this.pendingTimers.values()) clearTimeout(timer);
        this.pendingTimers.clear();
        this.syncedPeers.clear();
        if (this.synced) {
            this.synced = false;
            this.emitSafely("synced", [false]);
        }
        if (localAwarenessState !== null) this.awareness.setLocalState(localAwarenessState);
        for (const tracker of disconnectedTrackers) {
            this.emitSafely("status", [{ status: "disconnected", tracker: tracker.url }]);
        }
    }

    override destroy(): void {
        if (this.destroyed) return;
        this.destroyed = true;
        if (this._ownsAwareness && this.awareness.getLocalState() !== null) {
            this.awareness.setLocalState(null);
        }
        this.disconnect();
        this.doc.off("update", this._docUpdateHandler);
        this.awareness.off("update", this._awarenessUpdateHandler);
        if (this._ownsAwareness) this.awareness.destroy();
        super.destroy();
    }

    private isCurrentGeneration(generation: number): boolean {
        return !this.destroyed && this.shouldConnect && generation === this.connectionGeneration;
    }

    private requestRecoveryAnnounce(): void {
        if (
            this.destroyed ||
            !this.shouldConnect ||
            this.peers.size + this.pendingPeers.size >= this.maxConns
        ) {
            return;
        }
        for (const tracker of this.trackerConnections) tracker.requestRecoveryAnnounce();
    }

    private cancelPendingPeer(peer: WebrtcDataPeer): void {
        this.clearPendingPeer(peer);
        peer.destroy();
    }

    private clearPendingPeer(peer: WebrtcDataPeer): void {
        this.pendingPeers.delete(peer);
        this.clearPeerTimeout(peer);
        for (const [peerId, pending] of this.pendingPeerIds) {
            if (pending === peer) this.pendingPeerIds.delete(peerId);
        }
        for (const [offerId, pending] of this.pendingOffers) {
            if (pending.peer !== peer) continue;
            pending.canceled = true;
            this.pendingOffers.delete(offerId);
            for (const tracker of this.trackerConnections) tracker.forgetOffer(offerId);
        }
    }

    private startPeerTimeout(peer: WebrtcDataPeer): void {
        this.clearPeerTimeout(peer);
        this.pendingTimers.set(
            peer,
            setTimeout(() => {
                this.pendingTimers.delete(peer);
                this.cancelPendingPeer(peer);
                this.requestRecoveryAnnounce();
            }, this.signalTimeout),
        );
    }

    private clearPeerTimeout(peer: WebrtcDataPeer): void {
        clearTimeout(this.pendingTimers.get(peer));
        this.pendingTimers.delete(peer);
    }

    private updateSyncedState(): void {
        const synced = this.syncedPeers.size > 0;
        if (synced === this.synced) return;
        this.synced = synced;
        this.emitSafely("synced", [synced]);
    }
}

const collectOffers = async (
    records: OfferRecord[],
    offerPromises: Promise<void>[],
    timeout: number,
    onError: (error: unknown) => void,
): Promise<TrackerOffer[]> => {
    const offers: TrackerOffer[] = [];
    let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
    try {
        await Promise.race([
            Promise.all(
                offerPromises.map((promise, index) =>
                    promise.catch((error) => {
                        const record = records[index]!;
                        if (!record.canceled) onError(error);
                        record.canceled = true;
                        record.peer.destroy();
                    }),
                ),
            ),
            new Promise((resolve) => {
                timeoutHandle = setTimeout(resolve, timeout);
            }),
        ]);
    } finally {
        clearTimeout(timeoutHandle);
    }

    for (const record of records) {
        if (!record.canceled && record.offered && record.peer.pc.localDescription) {
            offers.push({
                offer_id: record.offerId,
                offer: record.peer.pc.localDescription.toJSON(),
            });
        }
    }
    return offers;
};
