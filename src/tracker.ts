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

type AnnounceInFlight = {
    promise: Promise<boolean>;
    trailing: boolean;
};

type TrackerSession = {
    socket: WebSocket;
    offerIds: Set<OfferId>;
    announceInFlight: AnnounceInFlight | null;
    connectTimer?: ReturnType<typeof setTimeout>;
    announceTimer?: ReturnType<typeof setTimeout>;
    announceResponseTimer?: ReturnType<typeof setTimeout>;
    recoveryAnnounceTimer?: ReturnType<typeof setTimeout>;
};

/** @internal Tracker signaling implementation; not exported from the package entry point. */
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
    reconnectTimer: ReturnType<typeof setTimeout> | undefined;
    private session: TrackerSession | null = null;
    private lastAnnounceAt = 0;

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

    private get socket(): WebSocket | null {
        return this.session?.socket ?? null;
    }

    connect(): void {
        if (this.destroyed || !this.WebSocket) return;
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = undefined;

        const existing = this.session;
        if (existing) {
            if (
                existing.socket.readyState === 0 ||
                existing.socket.readyState === this.WebSocket.OPEN
            ) {
                return;
            }
            this.closeSession(existing);
        }

        let socket: WebSocket;
        try {
            socket = new this.WebSocket(this.url);
        } catch (error) {
            this.scheduleReconnect();
            try {
                this.onError(error);
            } finally {
                this.reportState("reconnecting");
            }
            return;
        }

        const session: TrackerSession = {
            socket,
            offerIds: new Set(),
            announceInFlight: null,
        };
        this.session = session;
        session.connectTimer = setTimeout(() => {
            if (
                session !== this.session ||
                this.destroyed ||
                socket.readyState === this.WebSocket!.OPEN
            ) {
                return;
            }
            delete session.connectTimer;
            this.closeSession(session);
            this.scheduleReconnect();
            try {
                this.onError(new Error(`WebSocket connection timed out: ${this.url}`));
            } finally {
                this.reportState("reconnecting");
            }
        }, this.connectTimeout);

        socket.addEventListener("open", () => {
            if (session !== this.session || this.destroyed) return;
            clearTimeout(session.connectTimer);
            delete session.connectTimer;
            this.reportState("connected");
            void this.announce("started", session).catch((error: unknown) => this.onError(error));
        });
        socket.addEventListener("message", (event) => {
            if (session !== this.session) return;
            void this.handleMessage(event.data, socket).catch((error: unknown) => {
                if (session === this.session && !this.destroyed) this.onError(error);
            });
        });
        socket.addEventListener("error", (event) => {
            if (session === this.session && !this.destroyed) this.onError(event);
        });
        socket.addEventListener("close", () => {
            const wasCurrent = session === this.session;
            if (wasCurrent) this.session = null;
            this.clearSession(session);
            if (!wasCurrent || this.destroyed) return;
            this.scheduleReconnect();
            this.reportState("reconnecting");
        });
    }

    private announce(
        event?: "started" | "stopped" | "completed",
        session: TrackerSession | null = this.session,
    ): Promise<boolean> {
        if (!session || !this.isSessionOpen(session)) return Promise.resolve(false);
        const active = session.announceInFlight;
        if (active) {
            if (event !== "stopped") active.trailing = true;
            return active.promise;
        }

        const promise = this.performAnnounce(event, session);
        const state: AnnounceInFlight = { promise, trailing: false };
        session.announceInFlight = state;
        const finish = (): void => {
            if (session.announceInFlight !== state) return;
            session.announceInFlight = null;
            if (state.trailing && session === this.session && !this.destroyed) {
                this.requestRecoveryAnnounce();
            }
        };
        void promise.then(finish, finish);
        return promise;
    }

    private async performAnnounce(
        event: "started" | "stopped" | "completed" | undefined,
        session: TrackerSession,
    ): Promise<boolean> {
        if (!this.isSessionOpen(session)) return false;
        this.clearRecoveryAnnounceTimer(session);

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
            if (!this.isSessionOpen(session) || this.destroyed) {
                this.cancelOfferBatch(offers);
                return false;
            }
            if (offers.length > 0) message.offers = offers;
        }
        try {
            const sent = this.send(message, session);
            if (sent) {
                this.lastAnnounceAt = Date.now();
                this.trackOfferBatch(session, offers);
                if (event !== "stopped") this.startAnnounceResponseTimeout(session);
            } else {
                this.cancelOfferBatch(offers);
            }
            return sent;
        } catch (error) {
            this.cancelOfferBatch(offers);
            throw error;
        }
    }

    private startAnnounceResponseTimeout(session: TrackerSession): void {
        this.clearAnnounceResponseTimeout(session);
        session.announceResponseTimer = setTimeout(() => {
            delete session.announceResponseTimer;
            if (this.destroyed || session !== this.session) return;
            try {
                this.onError(new Error(`Tracker announce response timed out: ${this.url}`));
            } finally {
                session.socket.close();
            }
        }, this.announceResponseTimeout);
    }

    private clearAnnounceResponseTimeout(session: TrackerSession): void {
        clearTimeout(session.announceResponseTimer);
        delete session.announceResponseTimer;
    }

    private trackOfferBatch(session: TrackerSession, offers: readonly TrackerOffer[]): void {
        for (const offer of offers) session.offerIds.add(offer.offer_id);
    }

    private cancelOfferBatch(offers: readonly TrackerOffer[]): void {
        if (offers.length > 0) this.cancelOffers(offers.map((offer) => offer.offer_id));
    }

    private cancelSessionOffers(session: TrackerSession): void {
        if (session.offerIds.size === 0) return;
        const offerIds = [...session.offerIds];
        session.offerIds.clear();
        this.cancelOffers(offerIds);
    }

    forgetOffer(offerId: OfferId): void {
        this.session?.offerIds.delete(offerId);
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
        const session = this.session;
        if (!socket || !session || session.socket !== socket || this.destroyed) return;
        let message: TrackerAnnounceMessage;
        try {
            const text =
                typeof data === "string"
                    ? data
                    : data instanceof Blob
                      ? await data.text()
                      : new TextDecoder().decode(data as AllowSharedBufferSource);
            if (session !== this.session || this.destroyed) return;
            message = JSON.parse(text) as TrackerAnnounceMessage;
        } catch (error) {
            this.onError(error);
            return;
        }

        if (!message || typeof message !== "object" || message.action !== "announce") return;

        const failure = message["failure reason"];
        if (typeof failure === "string") {
            this.clearAnnounceResponseTimeout(session);
            try {
                this.onError(new Error(`Tracker failure: ${failure}`));
            } finally {
                socket.close();
            }
            return;
        }
        if (message.info_hash !== this.infoHash) return;

        this.clearAnnounceResponseTimeout(session);
        this.reconnectDelay = 1000;
        const hasAnnounceMetadata =
            message.interval !== undefined || "complete" in message || "incomplete" in message;
        if (hasAnnounceMetadata) {
            this.scheduleAnnounce(message.interval);
        } else if (!session.announceTimer) {
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
        const session = this.session;
        if (!session) return;
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
        this.clearAnnounceTimer(session);
        session.announceTimer = setTimeout(() => {
            delete session.announceTimer;
            if (session === this.session) {
                void this.announce(undefined, session).catch((error: unknown) =>
                    this.onError(error),
                );
            }
        }, delay);
    }

    requestRecoveryAnnounce(): void {
        const session = this.session;
        if (this.destroyed || !session || !this.isSessionOpen(session)) return;
        if (session.announceInFlight) {
            session.announceInFlight.trailing = true;
            return;
        }
        if (session.recoveryAnnounceTimer) return;
        const delay =
            Math.max(0, this.lastAnnounceAt + minimumRecoveryAnnounceDelay - Date.now()) +
            Math.random() * recoveryAnnounceJitter;
        session.recoveryAnnounceTimer = setTimeout(() => {
            delete session.recoveryAnnounceTimer;
            if (!this.destroyed && this.isSessionOpen(session)) {
                void this.announce(undefined, session).catch((error: unknown) =>
                    this.onError(error),
                );
            }
        }, delay);
    }

    private clearAnnounceTimer(session: TrackerSession): void {
        clearTimeout(session.announceTimer);
        delete session.announceTimer;
    }

    private clearRecoveryAnnounceTimer(session: TrackerSession): void {
        clearTimeout(session.recoveryAnnounceTimer);
        delete session.recoveryAnnounceTimer;
    }

    scheduleReconnect(): void {
        if (this.session) this.clearAnnounceTimer(this.session);
        if (this.destroyed) return;
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = setTimeout(() => this.connect(), this.reconnectDelay);
        this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30000);
    }

    send(message: TrackerAnnounceMessage, session: TrackerSession | null = this.session): boolean {
        if (!session || !this.isSessionOpen(session)) return false;
        try {
            session.socket.send(JSON.stringify(message));
            return true;
        } catch (error) {
            this.onError(error);
            session.socket.close();
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

    isOpen(): boolean {
        return !!this.session && this.isSessionOpen(this.session);
    }

    destroy(): void {
        if (this.destroyed) return;
        this.destroyed = true;
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = undefined;
        const session = this.session;
        if (session && this.isSessionOpen(session)) {
            try {
                this.send(
                    {
                        action: "announce",
                        event: "stopped",
                        info_hash: this.infoHash,
                        peer_id: this.peerId,
                    },
                    session,
                );
            } catch {
                // Final cleanup must continue if diagnostics from the stopped announce fail.
            }
        }
        if (session) this.closeSession(session);
        this.reportState("disconnected");
    }

    private isSessionOpen(session: TrackerSession): boolean {
        return (
            session === this.session &&
            !!this.WebSocket &&
            session.socket.readyState === this.WebSocket.OPEN
        );
    }

    private closeSession(session: TrackerSession): void {
        if (session === this.session) this.session = null;
        this.clearSession(session);
        try {
            session.socket.close();
        } catch {
            // The tracker can continue or terminate even if an injected socket throws while closing.
        }
    }

    private clearSession(session: TrackerSession): void {
        // Cancel timers that have not fired; session identity checks still neutralize callbacks
        // already queued when teardown began.
        clearTimeout(session.connectTimer);
        delete session.connectTimer;
        this.clearAnnounceTimer(session);
        this.clearAnnounceResponseTimeout(session);
        this.clearRecoveryAnnounceTimer(session);
        session.announceInFlight = null;
        this.cancelSessionOffers(session);
    }
}

const validateTimeout = (name: string, value: number): number => {
    if (typeof value !== "number") throw new TypeError(`${name} must be a number`);
    if (!Number.isFinite(value) || value < 0 || value > maximumTimerDelay) {
        throw new RangeError(`${name} must be between 0 and ${maximumTimerDelay}`);
    }
    return value;
};
