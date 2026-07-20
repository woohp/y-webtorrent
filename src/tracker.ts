const minimumAnnounceIntervalSeconds = 30;
const fallbackAnnounceIntervalSeconds = 120;
const minimumRecoveryAnnounceDelay = 5000;
const recoveryAnnounceJitter = 1000;
const maximumTimerDelay = 2_147_483_647;

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
    "failure reason"?: string;
    "warning message"?: string;
}

export interface TrackerConnectionOptions {
    infoHash: InfoHash;
    peerId: PeerId;
    numwant: number;
    createOffers: (tracker: TrackerConnection) => Promise<TrackerOffer[]> | TrackerOffer[];
    cancelOffers?: (offerIds: readonly OfferId[]) => void;
    onOffer: (
        peerId: PeerId,
        offerId: OfferId,
        offer: TrackerSignal,
        tracker: TrackerConnection,
    ) => void;
    onAnswer: (peerId: PeerId, offerId: OfferId, answer: TrackerSignal) => void;
    onAnnounce?: (message: TrackerAnnounceMessage) => void;
    onState?: (state: "connected" | "reconnecting" | "disconnected") => void;
    onError?: (error: unknown) => void;
    connectTimeout?: number;
    announceResponseTimeout?: number;
    WebSocket?: typeof WebSocket;
}

export class TrackerConnection {
    readonly url: string;
    readonly infoHash: InfoHash;
    readonly peerId: PeerId;
    readonly numwant: number;
    readonly createOffers: TrackerConnectionOptions["createOffers"];
    readonly cancelOffers: NonNullable<TrackerConnectionOptions["cancelOffers"]>;
    readonly onOffer: TrackerConnectionOptions["onOffer"];
    readonly onAnswer: TrackerConnectionOptions["onAnswer"];
    readonly onAnnounce: NonNullable<TrackerConnectionOptions["onAnnounce"]>;
    readonly onState: NonNullable<TrackerConnectionOptions["onState"]>;
    readonly onError: NonNullable<TrackerConnectionOptions["onError"]>;
    readonly WebSocket?: typeof WebSocket;
    readonly connectTimeout: number;
    readonly announceResponseTimeout: number;
    destroyed = false;
    reconnectDelay = 1000;
    socket: WebSocket | null = null;
    announceTimer: ReturnType<typeof setTimeout> | undefined;
    reconnectTimer: ReturnType<typeof setTimeout> | undefined;
    connectTimer: ReturnType<typeof setTimeout> | undefined;
    announceResponseTimer: ReturnType<typeof setTimeout> | undefined;
    private recoveryAnnounceTimer: ReturnType<typeof setTimeout> | undefined;
    private recoveryAnnounceSocket: WebSocket | null = null;
    private lastAnnounceAt = 0;
    private announceInFlight: {
        socket: WebSocket;
        promise: Promise<boolean>;
        trailing: boolean;
    } | null = null;
    private announceResponseSocket: WebSocket | null = null;
    private readonly announcedOffers = new Map<WebSocket, Set<OfferId>>();

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
        this.cancelOffers = opts.cancelOffers ?? (() => {});
        this.onOffer = opts.onOffer;
        this.onAnswer = opts.onAnswer;
        this.onAnnounce = opts.onAnnounce ?? (() => {});
        this.onState = opts.onState ?? (() => {});
        this.onError = opts.onError ?? (() => {});
        this.WebSocket = opts.WebSocket ?? globalThis.WebSocket;
        this.connectTimeout = validateTimeout("connectTimeout", opts.connectTimeout ?? 10000);
        this.announceResponseTimeout = validateTimeout(
            "announceResponseTimeout",
            opts.announceResponseTimeout ?? 15000,
        );
        this.connect();
    }

    connect(): void {
        if (this.destroyed || !this.WebSocket) return;
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = undefined;
        if (this.socket) {
            if (this.socket.readyState === 0 || this.socket.readyState === this.WebSocket.OPEN)
                return;
            const staleSocket = this.socket;
            this.socket = null;
            try {
                staleSocket.close();
            } catch {
                this.cancelSocketOffers(staleSocket);
            }
        }
        clearTimeout(this.connectTimer);
        this.connectTimer = undefined;

        let socket: WebSocket;
        try {
            socket = new this.WebSocket(this.url);
            this.socket = socket;
        } catch (error) {
            this.socket = null;
            this.scheduleReconnect();
            try {
                this.onError(error);
            } finally {
                this.reportState("reconnecting");
            }
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
            try {
                socket.close();
            } catch {
                // Reconnection is still required if closing the stale socket fails.
            }
            this.scheduleReconnect();
            try {
                this.onError(new Error(`WebSocket connection timed out: ${this.url}`));
            } finally {
                this.reportState("reconnecting");
            }
        }, this.connectTimeout);

        socket.addEventListener("open", () => {
            if (socket !== this.socket || this.destroyed) return;
            clearTimeout(this.connectTimer);
            this.connectTimer = undefined;
            this.reportState("connected");
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
            this.cancelSocketOffers(socket);
            this.clearAnnounceResponseTimeout(socket);
            this.clearRecoveryAnnounceTimer(socket);
            if (socket !== this.socket || this.destroyed) return;
            clearTimeout(this.connectTimer);
            this.connectTimer = undefined;
            this.scheduleReconnect();
            this.reportState("reconnecting");
        });
    }

    private announce(
        event?: "started" | "stopped" | "completed",
        socket: WebSocket | null = this.socket,
    ): Promise<boolean> {
        if (!socket || socket !== this.socket || !this.isOpen()) return Promise.resolve(false);
        const active = this.announceInFlight;
        if (active?.socket === socket) {
            if (event !== "stopped") active.trailing = true;
            return active.promise;
        }

        const promise = this.performAnnounce(event, socket);
        const state = { socket, promise, trailing: false };
        this.announceInFlight = state;
        const finish = (): void => {
            if (this.announceInFlight !== state) return;
            this.announceInFlight = null;
            if (state.trailing && socket === this.socket && !this.destroyed) {
                this.requestRecoveryAnnounce();
            }
        };
        void promise.then(finish, finish);
        return promise;
    }

    private async performAnnounce(
        event: "started" | "stopped" | "completed" | undefined,
        socket: WebSocket,
    ): Promise<boolean> {
        if (socket !== this.socket || !this.isOpen()) return false;
        this.clearRecoveryAnnounceTimer(socket);

        const message: TrackerAnnounceMessage = {
            action: "announce",
            info_hash: this.infoHash,
            peer_id: this.peerId,
            numwant: this.numwant,
        };
        if (event) message.event = event;
        let offers: TrackerOffer[] = [];
        if (event !== "stopped") {
            offers = await this.createOffers(this);
            if (socket !== this.socket || this.destroyed || !this.isOpen()) {
                this.cancelOfferBatch(offers);
                return false;
            }
            if (offers.length > 0) message.offers = offers;
        }
        try {
            const sent = this.send(message);
            if (sent) {
                this.lastAnnounceAt = Date.now();
                this.trackOfferBatch(socket, offers);
                if (event !== "stopped") this.startAnnounceResponseTimeout(socket);
            } else {
                this.cancelOfferBatch(offers);
            }
            return sent;
        } catch (error) {
            this.cancelOfferBatch(offers);
            throw error;
        }
    }

    private startAnnounceResponseTimeout(socket: WebSocket): void {
        this.clearAnnounceResponseTimeout();
        this.announceResponseSocket = socket;
        this.announceResponseTimer = setTimeout(() => {
            if (
                this.destroyed ||
                socket !== this.socket ||
                socket !== this.announceResponseSocket
            ) {
                if (socket === this.announceResponseSocket) {
                    this.announceResponseTimer = undefined;
                    this.announceResponseSocket = null;
                }
                return;
            }
            this.announceResponseTimer = undefined;
            this.announceResponseSocket = null;
            try {
                this.onError(new Error(`Tracker announce response timed out: ${this.url}`));
            } finally {
                socket.close();
            }
        }, this.announceResponseTimeout);
    }

    private clearAnnounceResponseTimeout(socket?: WebSocket): void {
        if (socket && socket !== this.announceResponseSocket) return;
        clearTimeout(this.announceResponseTimer);
        this.announceResponseTimer = undefined;
        this.announceResponseSocket = null;
    }

    private trackOfferBatch(socket: WebSocket, offers: readonly TrackerOffer[]): void {
        if (offers.length === 0) return;
        const offerIds = this.announcedOffers.get(socket) ?? new Set<OfferId>();
        for (const offer of offers) offerIds.add(offer.offer_id);
        this.announcedOffers.set(socket, offerIds);
    }

    private cancelOfferBatch(offers: readonly TrackerOffer[]): void {
        if (offers.length > 0) this.cancelOffers(offers.map((offer) => offer.offer_id));
    }

    private cancelSocketOffers(socket: WebSocket): void {
        const offerIds = this.announcedOffers.get(socket);
        if (!offerIds) return;
        this.announcedOffers.delete(socket);
        this.cancelOffers([...offerIds]);
    }

    forgetOffer(offerId: OfferId): void {
        for (const [socket, offerIds] of this.announcedOffers) {
            offerIds.delete(offerId);
            if (offerIds.size === 0) this.announcedOffers.delete(socket);
        }
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

        if (!message || typeof message !== "object" || message.action !== "announce") return;

        const failure = message["failure reason"];
        if (typeof failure === "string") {
            this.clearAnnounceResponseTimeout(socket);
            try {
                this.onError(new Error(`Tracker failure: ${failure}`));
            } finally {
                socket.close();
            }
            return;
        }
        if (message.info_hash !== this.infoHash) return;

        this.clearAnnounceResponseTimeout(socket);
        this.reconnectDelay = 1000;
        const hasAnnounceMetadata =
            message.interval !== undefined || "complete" in message || "incomplete" in message;
        if (hasAnnounceMetadata) {
            this.scheduleAnnounce(message.interval);
        } else if (!this.announceTimer) {
            this.scheduleAnnounce(undefined);
        }
        const warning = message["warning message"];
        if (typeof warning === "string") this.onError(new Error(`Tracker warning: ${warning}`));
        if ("complete" in message || "incomplete" in message) this.onAnnounce(message);
        if (message.offer && message.offer_id && message.peer_id) {
            this.onOffer(message.peer_id, message.offer_id, message.offer, this);
        } else if (message.answer && message.offer_id && message.peer_id) {
            this.forgetOffer(message.offer_id);
            this.onAnswer(message.peer_id, message.offer_id, message.answer);
        }
    }

    scheduleAnnounce(intervalSeconds: unknown): void {
        const requestedSeconds =
            typeof intervalSeconds === "number" &&
            Number.isFinite(intervalSeconds) &&
            intervalSeconds > 0
                ? Math.max(intervalSeconds, minimumAnnounceIntervalSeconds)
                : fallbackAnnounceIntervalSeconds;
        const requestedDelay = requestedSeconds * 1000 * (1 + Math.random() * 0.1);
        const delay =
            requestedDelay <= maximumTimerDelay
                ? requestedDelay
                : fallbackAnnounceIntervalSeconds * 1000 * (1 + Math.random() * 0.1);
        this.clearAnnounceTimer();
        this.announceTimer = setTimeout(() => {
            this.announceTimer = undefined;
            void this.announce().catch((error: unknown) => this.onError(error));
        }, delay);
    }

    requestRecoveryAnnounce(): void {
        if (this.destroyed || !this.isOpen()) return;
        const socket = this.socket;
        if (this.announceInFlight?.socket === socket) {
            this.announceInFlight.trailing = true;
            return;
        }
        if (this.recoveryAnnounceTimer) return;
        const delay =
            Math.max(0, this.lastAnnounceAt + minimumRecoveryAnnounceDelay - Date.now()) +
            Math.random() * recoveryAnnounceJitter;
        this.recoveryAnnounceSocket = socket;
        this.recoveryAnnounceTimer = setTimeout(() => {
            this.recoveryAnnounceTimer = undefined;
            this.recoveryAnnounceSocket = null;
            if (!this.destroyed && socket === this.socket && this.isOpen()) {
                void this.announce().catch((error: unknown) => this.onError(error));
            }
        }, delay);
    }

    private clearAnnounceTimer(): void {
        clearTimeout(this.announceTimer);
        this.announceTimer = undefined;
    }

    private clearRecoveryAnnounceTimer(socket?: WebSocket): void {
        if (socket && socket !== this.recoveryAnnounceSocket) return;
        clearTimeout(this.recoveryAnnounceTimer);
        this.recoveryAnnounceTimer = undefined;
        this.recoveryAnnounceSocket = null;
    }

    scheduleReconnect(): void {
        this.clearAnnounceTimer();
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

    private reportState(state: "connected" | "reconnecting" | "disconnected"): void {
        try {
            this.onState(state);
        } catch {
            // Observer failures must not alter tracker lifecycle transitions.
        }
    }

    isOpen(): this is this & { socket: WebSocket; WebSocket: typeof WebSocket } {
        return !!this.socket && !!this.WebSocket && this.socket.readyState === this.WebSocket.OPEN;
    }

    destroy(): void {
        if (this.destroyed) return;
        this.destroyed = true;
        this.clearAnnounceTimer();
        this.clearRecoveryAnnounceTimer();
        clearTimeout(this.reconnectTimer);
        clearTimeout(this.connectTimer);
        this.connectTimer = undefined;
        this.clearAnnounceResponseTimeout();
        if (this.isOpen()) {
            try {
                this.send({
                    action: "announce",
                    event: "stopped",
                    info_hash: this.infoHash,
                    peer_id: this.peerId,
                });
            } catch {
                // Final cleanup must continue if diagnostics from the stopped announce fail.
            }
        }
        for (const socket of this.announcedOffers.keys()) this.cancelSocketOffers(socket);
        try {
            this.socket?.close();
        } catch {
            // The wrapper is still terminal if the injected socket throws while closing.
        }
        this.socket = null;
        this.reportState("disconnected");
    }
}

const validateTimeout = (name: string, value: number): number => {
    if (typeof value !== "number") throw new TypeError(`${name} must be a number`);
    if (!Number.isFinite(value) || value < 0 || value > maximumTimerDelay) {
        throw new RangeError(`${name} must be between 0 and ${maximumTimerDelay}`);
    }
    return value;
};
