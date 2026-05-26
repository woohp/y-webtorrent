export const defaultTrackerUrls = [
    "wss://tracker.openwebtorrent.com",
    "wss://tracker.btorrent.xyz",
] as const;

export type PeerId = string;
export type InfoHash = string;
export type OfferId = string;
export type TrackerSignal = RTCSessionDescriptionInit;

export interface TrackerOffer {
    offer_id: OfferId;
    offer: TrackerSignal;
}

export interface TrackerAnnounceMessage {
    action: "announce";
    info_hash: InfoHash;
    peer_id: PeerId;
    numwant?: number;
    event?: "started" | "stopped" | "completed";
    offers?: TrackerOffer[];
    to_peer_id?: PeerId;
    offer_id?: OfferId;
    offer?: TrackerSignal;
    answer?: TrackerSignal;
    interval?: number;
    complete?: number;
    incomplete?: number;
}

export interface TrackerConnectionOptions {
    infoHash: InfoHash;
    peerId: PeerId;
    numwant: number;
    createOffers: (tracker: TrackerConnection) => Promise<TrackerOffer[]> | TrackerOffer[];
    onOffer: (
        peerId: PeerId,
        offerId: OfferId,
        offer: TrackerSignal,
        tracker: TrackerConnection,
    ) => void;
    onAnswer: (peerId: PeerId, offerId: OfferId, answer: TrackerSignal) => void;
    onAnnounce?: (message: TrackerAnnounceMessage) => void;
    onError?: (error: unknown) => void;
    WebSocket?: typeof WebSocket;
}

export class TrackerConnection {
    readonly url: string;
    readonly infoHash: InfoHash;
    readonly peerId: PeerId;
    readonly numwant: number;
    readonly createOffers: TrackerConnectionOptions["createOffers"];
    readonly onOffer: TrackerConnectionOptions["onOffer"];
    readonly onAnswer: TrackerConnectionOptions["onAnswer"];
    readonly onAnnounce: NonNullable<TrackerConnectionOptions["onAnnounce"]>;
    readonly onError: NonNullable<TrackerConnectionOptions["onError"]>;
    readonly WebSocket?: typeof WebSocket;
    destroyed = false;
    reconnectDelay = 1000;
    socket: WebSocket | null = null;
    announceTimer: ReturnType<typeof setTimeout> | undefined;
    reconnectTimer: ReturnType<typeof setTimeout> | undefined;

    constructor(url: string, opts: TrackerConnectionOptions) {
        this.url = url;
        this.infoHash = opts.infoHash;
        this.peerId = opts.peerId;
        this.numwant = opts.numwant;
        this.createOffers = opts.createOffers;
        this.onOffer = opts.onOffer;
        this.onAnswer = opts.onAnswer;
        this.onAnnounce = opts.onAnnounce || (() => {});
        this.onError = opts.onError || (() => {});
        this.WebSocket = opts.WebSocket || globalThis.WebSocket;
        this.connect();
    }

    connect(): void {
        if (this.destroyed || !this.WebSocket) return;

        const socket = (this.socket = new this.WebSocket(this.url));
        socket.addEventListener("open", () => {
            this.reconnectDelay = 1000;
            void this.announce("started");
        });
        socket.addEventListener("message", (event) => this.handleMessage(event.data));
        socket.addEventListener("error", (event) => this.onError(event));
        socket.addEventListener("close", () => this.scheduleReconnect());
    }

    async announce(event?: "started" | "stopped" | "completed"): Promise<void> {
        if (!this.isOpen()) return;

        const message: TrackerAnnounceMessage = {
            action: "announce",
            info_hash: this.infoHash,
            peer_id: this.peerId,
            numwant: this.numwant,
        };

        if (event) message.event = event;
        if (event !== "stopped") {
            const offers = await this.createOffers(this);
            if (offers.length > 0) message.offers = offers;
        }

        this.send(message);
    }

    sendAnswer(toPeerId: PeerId, offerId: OfferId, answer: TrackerSignal): void {
        this.send({
            action: "announce",
            info_hash: this.infoHash,
            peer_id: this.peerId,
            to_peer_id: toPeerId,
            offer_id: offerId,
            answer,
        });
    }

    handleMessage(data: string | Blob | ArrayBufferLike): void {
        let message: TrackerAnnounceMessage;
        try {
            message = JSON.parse(
                typeof data === "string"
                    ? data
                    : new TextDecoder().decode(data as AllowSharedBufferSource),
            );
        } catch (error) {
            this.onError(error);
            return;
        }

        if (message.action !== "announce" || message.info_hash !== this.infoHash) return;

        if (message.interval) this.scheduleAnnounce(message.interval);
        if ("complete" in message || "incomplete" in message) this.onAnnounce(message);
        if (message.offer && message.offer_id && message.peer_id) {
            this.onOffer(message.peer_id, message.offer_id, message.offer, this);
        } else if (message.answer && message.offer_id && message.peer_id) {
            this.onAnswer(message.peer_id, message.offer_id, message.answer);
        }
    }

    scheduleAnnounce(intervalSeconds: number): void {
        clearTimeout(this.announceTimer);
        this.announceTimer = setTimeout(() => void this.announce(), intervalSeconds * 1000);
    }

    scheduleReconnect(): void {
        clearTimeout(this.announceTimer);
        if (this.destroyed) return;
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = setTimeout(() => this.connect(), this.reconnectDelay);
        this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30000);
    }

    send(message: TrackerAnnounceMessage): void {
        if (this.isOpen()) this.socket.send(JSON.stringify(message));
    }

    isOpen(): this is this & { socket: WebSocket; WebSocket: typeof WebSocket } {
        return !!this.socket && !!this.WebSocket && this.socket.readyState === this.WebSocket.OPEN;
    }

    destroy(): void {
        this.destroyed = true;
        clearTimeout(this.announceTimer);
        clearTimeout(this.reconnectTimer);
        if (this.isOpen()) {
            this.send({
                action: "announce",
                event: "stopped",
                info_hash: this.infoHash,
                peer_id: this.peerId,
            });
        }
        this.socket?.close();
    }
}
