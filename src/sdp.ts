/** @internal SDP inspection for diagnostics; not exported from the package entry point. */

/**
 * Reports which ICE candidate types appear in the given session descriptions, deduplicated
 * and sorted so the result is stable enough to compare across events.
 *
 * Intended for diagnostics only: the presence of `relay` (or the absence of anything but
 * `host`) is what usually explains a NAT traversal failure. This reads the SDP text rather
 * than the parsed candidates because non-trickle ICE means the completed offer/answer is the
 * only artifact available at signaling time.
 */
export const collectCandidateTypes = (signals: readonly RTCSessionDescriptionInit[]): string[] => {
    const candidateTypes = new Set<string>();
    for (const signal of signals) {
        for (const match of signal.sdp?.matchAll(/\btyp\s+(host|srflx|prflx|relay)\b/g) ?? []) {
            candidateTypes.add(match[1]!);
        }
    }
    return [...candidateTypes].sort();
};
