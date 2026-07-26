import type { PeerId } from "./tracker.js";

/** @internal Transport contract and peer bookkeeping; not exported from the package entry point. */

/**
 * The transport surface the provider actually depends on. `WebrtcDataPeer` satisfies this
 * structurally, so naming it costs nothing at runtime; it exists to keep provider logic
 * independent of the concrete transport and to state plainly how little of it is used.
 */
export interface DataPeer {
    readonly initiator: boolean;
    readonly connected: boolean;
    /**
     * Narrowed to the one member the provider reads. The read is deliberately late — see
     * `collectOffers`, which prefers the description as amended by ongoing ICE gathering
     * over whatever `createOffer` resolved with.
     */
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
 * Owns per-peer state and the indexes over it.
 *
 * The provider previously kept this in seven parallel collections keyed on the peer object
 * and on its id, which meant removal had to scan for reverse mappings, capacity was
 * re-derived at each call site, and teardown depended on clearing every collection in
 * order. Keeping one record per peer and maintaining the id indexes here makes those
 * operations single lookups and gives the "connected plus pending" invariant one home.
 *
 * Peer lifecycle stays with the provider: this class never calls `destroy` on a transport,
 * it only forgets it.
 *
 * Registering a peer twice throws; every other mutator is a no-op on an unregistered peer.
 * That asymmetry is deliberate. A transport's callbacks can outlive the slot — a generation
 * bump during `disconnect` forgets peers whose channels then open anyway — so the mutators
 * have to tolerate arriving late. Nothing legitimately registers the same peer twice.
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
     *
     * Throws on re-registration rather than replacing the slot. Overwriting silently would
     * orphan the old slot's timer and leave its id indexed, and the symptom surfaces far from
     * the second `add` — during this registry's own rollout a double-registered test peer
     * reverted to `peerId: null` and simply stopped being findable.
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

    /**
     * Attaches a remote id to a peer discovered through a tracker answer, releasing any id
     * it was previously filed under.
     */
    identify(peer: DataPeer, peerId: PeerId): void {
        const slot = this.slots.get(peer);
        if (!slot) return;
        if (slot.peerId !== null && slot.peerId !== peerId) this.unindex(slot);
        slot.peerId = peerId;
        if (slot.state === "open") this.openByPeerId.set(peerId, peer);
        else this.pendingByPeerId.set(peerId, peer);
    }

    /** Promotes a pending peer to open once its data channel is usable. */
    promote(peer: DataPeer): void {
        const slot = this.slots.get(peer);
        if (!slot || slot.peerId === null || slot.state === "open") return;
        this.clearTimer(peer);
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

    clearTimer(peer: DataPeer): void {
        const slot = this.slots.get(peer);
        if (!slot) return;
        clearTimeout(slot.timer);
        slot.timer = undefined;
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
