import type { PeerId } from "./tracker.js";

export interface WebrtcPeerOptions {
    initiator: boolean;
    remotePeerId: PeerId | null;
    rtcConfig?: RTCConfiguration | undefined;
    channelName?: string;
    iceTimeout?: number;
    onOpen: (peer: WebrtcPeer) => void;
    onMessage: (peer: WebrtcPeer, data: ArrayBuffer) => void;
    onClose: (peer: WebrtcPeer) => void;
    onError: (peer: WebrtcPeer, error: unknown) => void;
    onDebug?: (event: Record<string, unknown>) => void;
}

export class WebrtcPeer {
    readonly pc: RTCPeerConnection;
    readonly initiator: boolean;
    readonly channelName: string;
    readonly iceTimeout: number;
    readonly onOpen: WebrtcPeerOptions["onOpen"];
    readonly onMessage: WebrtcPeerOptions["onMessage"];
    readonly onClose: WebrtcPeerOptions["onClose"];
    readonly onError: WebrtcPeerOptions["onError"];
    readonly onDebug: NonNullable<WebrtcPeerOptions["onDebug"]>;
    remotePeerId: PeerId | null;
    channel: RTCDataChannel | null = null;
    closed = false;
    synced = false;

    constructor(opts: WebrtcPeerOptions) {
        this.initiator = opts.initiator;
        this.remotePeerId = opts.remotePeerId;
        this.channelName = opts.channelName ?? "y-webtorrent";
        this.iceTimeout = opts.iceTimeout ?? 5000;
        this.onOpen = opts.onOpen;
        this.onMessage = opts.onMessage;
        this.onClose = opts.onClose;
        this.onError = opts.onError;
        this.onDebug = opts.onDebug ?? (() => {});
        this.pc = new RTCPeerConnection(opts.rtcConfig);
        this.attachPeerConnectionHandlers();

        if (this.initiator) {
            this.attachChannel(this.pc.createDataChannel(this.channelName, { ordered: true }));
        } else {
            this.pc.addEventListener("datachannel", (event) => this.attachChannel(event.channel));
        }
    }

    get connected(): boolean {
        return this.channel?.readyState === "open";
    }

    async createOffer(): Promise<RTCSessionDescriptionInit> {
        await this.pc.setLocalDescription(await this.pc.createOffer());
        await this.waitForIceGatheringComplete();
        return descriptionToJSON(this.pc.localDescription);
    }

    async acceptOffer(offer: RTCSessionDescriptionInit): Promise<RTCSessionDescriptionInit> {
        await this.pc.setRemoteDescription(offer);
        await this.pc.setLocalDescription(await this.pc.createAnswer());
        await this.waitForIceGatheringComplete();
        return descriptionToJSON(this.pc.localDescription);
    }

    async acceptAnswer(answer: RTCSessionDescriptionInit): Promise<void> {
        await this.pc.setRemoteDescription(answer);
    }

    send(data: Uint8Array<ArrayBuffer>): void {
        if (!this.channel || this.channel.readyState !== "open") return;
        this.channel.send(data);
    }

    destroy(): void {
        if (this.closed) return;
        this.closed = true;
        this.channel?.close();
        this.pc.close();
        this.onClose(this);
    }

    private attachPeerConnectionHandlers(): void {
        this.pc.addEventListener("connectionstatechange", () => {
            this.onDebug({ type: "rtc-connection-state", state: this.pc.connectionState });
            if (this.pc.connectionState === "failed" || this.pc.connectionState === "closed") {
                this.destroy();
            }
        });
        this.pc.addEventListener("iceconnectionstatechange", () => {
            this.onDebug({ type: "rtc-ice-connection-state", state: this.pc.iceConnectionState });
            if (
                this.pc.iceConnectionState === "failed" ||
                this.pc.iceConnectionState === "closed"
            ) {
                this.destroy();
            }
        });
    }

    private attachChannel(channel: RTCDataChannel): void {
        this.channel = channel;
        channel.binaryType = "arraybuffer";
        channel.addEventListener("open", () => this.onOpen(this));
        channel.addEventListener("message", (event) => this.onMessage(this, event.data));
        channel.addEventListener("close", () => this.destroy());
        channel.addEventListener("error", (event) => this.onError(this, event));
    }

    private waitForIceGatheringComplete(): Promise<void> {
        if (this.pc.iceGatheringState === "complete") return Promise.resolve();

        return new Promise((resolve) => {
            const timeout = setTimeout(done, this.iceTimeout);
            const pc = this.pc;

            function done() {
                clearTimeout(timeout);
                pc.removeEventListener("icegatheringstatechange", onStateChange);
                resolve();
            }

            function onStateChange() {
                if (pc.iceGatheringState === "complete") done();
            }

            pc.addEventListener("icegatheringstatechange", onStateChange);
        });
    }
}

const descriptionToJSON = (
    description: RTCSessionDescription | null,
): RTCSessionDescriptionInit => {
    if (!description) throw new Error("Missing local RTC session description");
    return description.toJSON();
};
