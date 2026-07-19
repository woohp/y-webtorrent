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
    connectTimeout?: number;
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
    readonly connectTimeout: number;
    destroyed = false;
    reconnectDelay = 1000;
    socket: WebSocket | null = null;
    announceTimer: ReturnType<typeof setTimeout> | undefined;
    reconnectTimer: ReturnType<typeof setTimeout> | undefined;
    connectTimer: ReturnType<typeof setTimeout> | undefined;

    constructor(url: string, opts: TrackerConnectionOptions) {
        const parsedUrl = new URL(url);
        if (parsedUrl.protocol !== "ws:" && parsedUrl.protocol !== "wss:") {
            throw new Error(`Invalid tracker URL: ${url}`);
        }
        this.url = parsedUrl.href;
        this.infoHash = opts.infoHash;
        this.peerId = opts.peerId;
        this.numwant = opts.numwant;
        this.createOffers = opts.createOffers;
        this.onOffer = opts.onOffer;
        this.onAnswer = opts.onAnswer;
        this.onAnnounce = opts.onAnnounce ?? (() => {});
        this.onError = opts.onError ?? (() => {});
        this.WebSocket = opts.WebSocket ?? globalThis.WebSocket;
        this.connectTimeout = opts.connectTimeout ?? 10000;
        this.connect();
    }

    connect(): void {
        if (this.destroyed || !this.WebSocket) return;
        clearTimeout(this.connectTimer);
        this.connectTimer = undefined;

        let socket: WebSocket;
        try {
            socket = new this.WebSocket(this.url);
            this.socket = socket;
        } catch (error) {
            this.socket = null;
            this.onError(error);
            this.scheduleReconnect();
            return;
        }

        this.connectTimer = setTimeout(() => {
            if (
                socket !== this.socket ||
                this.destroyed ||
                socket.readyState === this.WebSocket!.OPEN
            ) {
                return;
            }
            this.socket = null;
            this.connectTimer = undefined;
            this.onError(new Error(`WebSocket connection timed out: ${this.url}`));
            try {
                socket.close();
            } catch {
                // Reconnection is still required if closing the stale socket fails.
            }
            this.scheduleReconnect();
        }, this.connectTimeout);

        socket.addEventListener("open", () => {
            if (socket !== this.socket || this.destroyed) return;
            clearTimeout(this.connectTimer);
            this.connectTimer = undefined;
            this.reconnectDelay = 1000;
            void this.announce("started", socket).catch((error: unknown) => this.onError(error));
        });
        socket.addEventListener("message", (event) => {
            if (socket !== this.socket) return;
            void this.handleMessage(event.data, socket).catch((error: unknown) => {
                if (socket === this.socket && !this.destroyed) this.onError(error);
            });
        });
        socket.addEventListener("error", (event) => {
            if (socket === this.socket && !this.destroyed) this.onError(event);
        });
        socket.addEventListener("close", () => {
            if (socket !== this.socket || this.destroyed) return;
            clearTimeout(this.connectTimer);
            this.connectTimer = undefined;
            this.scheduleReconnect();
        });
    }

    async announce(
        event?: "started" | "stopped" | "completed",
        socket: WebSocket | null = this.socket,
    ): Promise<boolean> {
        if (!socket || socket !== this.socket || !this.isOpen()) return false;

        const message: TrackerAnnounceMessage = {
            action: "announce",
            info_hash: this.infoHash,
            peer_id: this.peerId,
            numwant: this.numwant,
        };
        if (event) message.event = event;
        if (event !== "stopped") {
            const offers = await this.createOffers(this);
            if (socket !== this.socket || this.destroyed || !this.isOpen()) return false;
            if (offers.length > 0) message.offers = offers;
        }
        return this.send(message);
    }

    sendAnswer(toPeerId: PeerId, offerId: OfferId, answer: TrackerSignal): boolean {
        return this.send({
            action: "announce",
            info_hash: this.infoHash,
            peer_id: this.peerId,
            to_peer_id: toPeerId,
            offer_id: offerId,
            answer,
        });
    }

    async handleMessage(
        data: string | Blob | ArrayBufferLike,
        socket: WebSocket | null = this.socket,
    ): Promise<void> {
        if (!socket || socket !== this.socket || this.destroyed) return;
        let message: TrackerAnnounceMessage;
        try {
            const text =
                typeof data === "string"
                    ? data
                    : data instanceof Blob
                      ? await data.text()
                      : new TextDecoder().decode(data as AllowSharedBufferSource);
            if (socket !== this.socket || this.destroyed) return;
            message = JSON.parse(text) as TrackerAnnounceMessage;
        } catch (error) {
            this.onError(error);
            return;
        }

        if (
            !message ||
            typeof message !== "object" ||
            message.action !== "announce" ||
            message.info_hash !== this.infoHash
        ) {
            return;
        }
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
        this.announceTimer = setTimeout(() => {
            void this.announce().catch((error: unknown) => this.onError(error));
        }, intervalSeconds * 1000);
    }

    scheduleReconnect(): void {
        clearTimeout(this.announceTimer);
        if (this.destroyed) return;
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = setTimeout(() => this.connect(), this.reconnectDelay);
        this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30000);
    }

    send(message: TrackerAnnounceMessage): boolean {
        if (!this.isOpen()) return false;
        try {
            this.socket.send(JSON.stringify(message));
            return true;
        } catch (error) {
            this.onError(error);
            this.socket?.close();
            return false;
        }
    }

    isOpen(): this is this & { socket: WebSocket; WebSocket: typeof WebSocket } {
        return !!this.socket && !!this.WebSocket && this.socket.readyState === this.WebSocket.OPEN;
    }

    destroy(): void {
        if (this.destroyed) return;
        this.destroyed = true;
        clearTimeout(this.announceTimer);
        clearTimeout(this.reconnectTimer);
        clearTimeout(this.connectTimer);
        this.connectTimer = undefined;
        if (this.isOpen()) {
            this.send({
                action: "announce",
                event: "stopped",
                info_hash: this.infoHash,
                peer_id: this.peerId,
            });
        }
        this.socket?.close();
        this.socket = null;
    }
}
