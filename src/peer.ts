import type { PeerId } from "./tracker.js";

/** @internal Transport contract and peer bookkeeping; not exported from the package entry point. */

/** The transport surface the provider depends on; `WebrtcDataPeer` satisfies it structurally. */
export interface DataPeer {
    readonly initiator: boolean;
    readonly connected: boolean;
    /** Read late by `collectOffers` to pick up candidates gathered after `createOffer`. */
    readonly pc: { readonly localDescription: RTCSessionDescription | null };
    createOffer(): Promise<RTCSessionDescriptionInit>;
    acceptOffer(offer: RTCSessionDescriptionInit): Promise<RTCSessionDescriptionInit>;
    acceptAnswer(answer: RTCSessionDescriptionInit): Promise<void>;
    send(data: Uint8Array<ArrayBuffer>): boolean;
    destroy(): void;
}

/** Everything tracked about a single transport, in one record. */
export interface PeerSlot {
    readonly peer: DataPeer;
    /** Null until a tracker exchange identifies the remote end. */
    peerId: PeerId | null;
    /** `pending` from creation until the data channel opens and the peer is promoted. */
    state: "pending" | "open";
    /** Set once this peer has answered Sync Step 1; drives the provider's `synced` flag. */
    synced: boolean;
    /** Creation time, used to report how long connecting took. */
    readonly startedAt: number;
    /** Signaling deadline for a pending peer; cleared on promotion and removal. */
    timer: ReturnType<typeof setTimeout> | undefined;
}

/**
 * Owns per-peer state and the id indexes over it. Never calls `destroy` on a transport; peer
 * lifecycle stays with the provider.
 *
 * Registering a peer twice throws; every other mutator is a no-op on an unregistered peer,
 * because transport callbacks can outlive their slot (e.g. a channel opening after
 * `disconnect` forgot it).
 */
export class PeerRegistry {
    private readonly slots = new Map<DataPeer, PeerSlot>();
    /** Open peers are always identified, so this doubles as the open-peer count. */
    private readonly openByPeerId = new Map<PeerId, DataPeer>();
    private readonly pendingByPeerId = new Map<PeerId, DataPeer>();

    /** Connected peers plus those still negotiating — the figure `maxConns` limits. */
    get size(): number {
        return this.slots.size;
    }

    get openCount(): number {
        return this.openByPeerId.size;
    }

    get pendingCount(): number {
        return this.slots.size - this.openByPeerId.size;
    }

    get openPeerIds(): PeerId[] {
        return [...this.openByPeerId.keys()];
    }

    get openPeers(): DataPeer[] {
        return [...this.openByPeerId.values()];
    }

    /** Every tracked transport, open or still negotiating. */
    get allPeers(): DataPeer[] {
        return [...this.slots.keys()];
    }

    /** Whether any peer has completed Sync Step 2. Recomputed to avoid a counter drifting. */
    get hasSynced(): boolean {
        for (const slot of this.slots.values()) {
            if (slot.synced) return true;
        }
        return false;
    }

    get(peer: DataPeer): PeerSlot | undefined {
        return this.slots.get(peer);
    }

    peerIdOf(peer: DataPeer): PeerId | null {
        return this.slots.get(peer)?.peerId ?? null;
    }

    openPeer(peerId: PeerId): DataPeer | undefined {
        return this.openByPeerId.get(peerId);
    }

    hasOpen(peerId: PeerId): boolean {
        return this.openByPeerId.has(peerId);
    }

    pendingPeer(peerId: PeerId): DataPeer | undefined {
        return this.pendingByPeerId.get(peerId);
    }

    /**
     * Registers a newly created transport as pending. `peerId` is null for outbound offers.
     * Throws on re-registration, since replacing the slot would orphan its timer and index.
     */
    add(peer: DataPeer, peerId: PeerId | null, startedAt: number): PeerSlot {
        if (this.slots.has(peer)) throw new Error("peer is already registered");
        const slot: PeerSlot = {
            peer,
            peerId,
            state: "pending",
            synced: false,
            startedAt,
            timer: undefined,
        };
        this.slots.set(peer, slot);
        if (peerId !== null) this.pendingByPeerId.set(peerId, peer);
        return slot;
    }

    /** Attaches a remote id to a pending outbound peer once a tracker answer names it. */
    identify(peer: DataPeer, peerId: PeerId): void {
        const slot = this.slots.get(peer);
        if (!slot) return;
        slot.peerId = peerId;
        this.pendingByPeerId.set(peerId, peer);
    }

    /** Promotes a pending peer to open once its data channel is usable. */
    promote(peer: DataPeer): void {
        const slot = this.slots.get(peer);
        if (!slot || slot.peerId === null || slot.state === "open") return;
        clearTimeout(slot.timer);
        slot.timer = undefined;
        this.pendingByPeerId.delete(slot.peerId);
        slot.state = "open";
        this.openByPeerId.set(slot.peerId, peer);
    }

    setTimer(peer: DataPeer, timer: ReturnType<typeof setTimeout>): void {
        const slot = this.slots.get(peer);
        if (!slot) {
            clearTimeout(timer);
            return;
        }
        clearTimeout(slot.timer);
        slot.timer = timer;
    }

    /** Forgets a peer. Returns its final slot so callers can see whether it had been open. */
    remove(peer: DataPeer): PeerSlot | undefined {
        const slot = this.slots.get(peer);
        if (!slot) return undefined;
        clearTimeout(slot.timer);
        slot.timer = undefined;
        this.unindex(slot);
        this.slots.delete(peer);
        return slot;
    }

    /** Forgets every peer and cancels their signaling deadlines. Does not destroy transports. */
    clear(): void {
        for (const slot of this.slots.values()) clearTimeout(slot.timer);
        this.slots.clear();
        this.openByPeerId.clear();
        this.pendingByPeerId.clear();
    }

    /** Drops both id indexes for a slot, leaving `slots` alone. */
    private unindex(slot: PeerSlot): void {
        if (slot.peerId === null) return;
        if (this.openByPeerId.get(slot.peerId) === slot.peer) {
            this.openByPeerId.delete(slot.peerId);
        }
        if (this.pendingByPeerId.get(slot.peerId) === slot.peer) {
            this.pendingByPeerId.delete(slot.peerId);
        }
    }
}
