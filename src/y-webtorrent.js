import SimplePeerImport from 'simple-peer/simplepeer.min.js'
import * as Y from 'yjs'
import * as syncProtocol from 'y-protocols/sync'
import * as awarenessProtocol from 'y-protocols/awareness'
import * as encoding from 'lib0/encoding'
import * as decoding from 'lib0/decoding'
import { Observable } from 'lib0/observable'
import { createInfoHash, createPeerId } from './crypto.js'
import { TrackerConnection, defaultTrackerUrls } from './tracker.js'

export { defaultTrackerUrls }

const SimplePeer = SimplePeerImport && SimplePeerImport.default ? SimplePeerImport.default : (globalThis.SimplePeer || SimplePeerImport)

const messageSync = 0
const messageAwareness = 1
const messageQueryAwareness = 3

const readMessage = (provider, peer, data) => {
  const decoder = decoding.createDecoder(new Uint8Array(data))
  const encoder = encoding.createEncoder()
  const messageType = decoding.readVarUint(decoder)

  if (messageType === messageSync) {
    encoding.writeVarUint(encoder, messageSync)
    syncProtocol.readSyncMessage(decoder, encoder, provider.doc, peer)
    if (encoding.length(encoder) > 1) peer.send(encoding.toUint8Array(encoder))
  } else if (messageType === messageAwareness) {
    awarenessProtocol.applyAwarenessUpdate(provider.awareness, decoding.readVarUint8Array(decoder), peer)
  } else if (messageType === messageQueryAwareness) {
    provider.sendAwareness(peer)
  }
}

export class WebtorrentProvider extends Observable {
  constructor (roomName, doc, opts = {}) {
    super()
    this.roomName = roomName
    this.doc = doc
    this.trackers = opts.trackers || defaultTrackerUrls
    this.password = opts.password || ''
    this.maxConns = opts.maxConns ?? 20
    this.peerOpts = opts.peerOpts || {}
    this.numwant = opts.numwant ?? Math.min(3, Math.max(1, this.maxConns))
    this.offerTimeout = opts.offerTimeout ?? 5000
    this.peerId = opts.peerId || createPeerId()
    this.debug = !!opts.debug
    this._ownsAwareness = !opts.awareness
    this.awareness = opts.awareness || new awarenessProtocol.Awareness(doc)
    this.shouldConnect = true
    this.synced = false
    this.infoHash = null
    this.trackerConnections = []
    this.peers = new Map()
    this.pendingOffers = new Map()

    this._docUpdateHandler = (update, origin) => {
      if (origin !== this) this.broadcastSyncUpdate(update)
    }
    this._awarenessUpdateHandler = ({ added, updated, removed }, origin) => {
      const changedClients = added.concat(updated, removed)
      const update = awarenessProtocol.encodeAwarenessUpdate(this.awareness, changedClients)
      this.broadcastAwareness(update)
    }
    this.doc.on('update', this._docUpdateHandler)
    this.awareness.on('update', this._awarenessUpdateHandler)

    this.ready = this.connect(opts)
  }

  async connect (opts = {}) {
    this.infoHash = await createInfoHash(this.roomName, this.password)
    if (!this.shouldConnect) return

    this.trackerConnections = this.trackers.map(url => new TrackerConnection(url, {
      infoHash: this.infoHash,
      peerId: this.peerId,
      numwant: this.numwant,
      WebSocket: opts.WebSocket,
      createOffers: tracker => this.createOffers(tracker),
      onOffer: (peerId, offerId, offer, tracker) => this.receiveOffer(peerId, offerId, offer, tracker),
      onAnswer: (peerId, offerId, answer) => this.receiveAnswer(peerId, offerId, answer),
      onAnnounce: message => this.emit('status', [{ status: 'connected', message }]),
      onError: error => this.emit('connection-error', [error])
    }))
  }

  createOffers (tracker) {
    const capacity = Math.max(0, this.maxConns - this.peers.size - this.pendingOffers.size)
    const count = Math.min(this.numwant, capacity)
    const offers = []

    const records = []

    for (let i = 0; i < count; i++) {
      const offerId = createPeerId()
      const peer = this.createPeer(null, true)
      const record = { offerId, peer, tracker, offered: false }
      records.push(record)
      this.pendingOffers.set(offerId, { peer, tracker })
      peer.once('signal', offer => {
        record.offered = true
        offers.push({ offer_id: offerId, offer })
      })
    }

    return new Promise(resolve => {
      if (count === 0) resolve([])
      const started = Date.now()
      const wait = () => {
        const elapsed = Date.now() - started
        if (offers.length === count || elapsed > this.offerTimeout) {
          for (const record of records) {
            if (!record.offered) {
              this.pendingOffers.delete(record.offerId)
              record.peer.destroy()
            }
          }
          this.emitDebug({ type: 'offers-created', count: offers.length, requested: count })
          resolve(offers)
        } else {
          setTimeout(wait, 20)
        }
      }
      wait()
    })
  }

  receiveOffer (peerId, offerId, offer, tracker) {
    this.emitDebug({ type: 'offer-received', peerId, offerId })
    if (peerId === this.peerId || this.peers.has(peerId) || this.peers.size >= this.maxConns) return

    const peer = this.createPeer(peerId, false)
    peer.once('signal', answer => {
      this.emitDebug({ type: 'answer-created', peerId, offerId })
      tracker.sendAnswer(peerId, offerId, answer)
    })
    peer.signal(offer)
  }

  receiveAnswer (peerId, offerId, answer) {
    this.emitDebug({ type: 'answer-received', peerId, offerId, knownOffer: this.pendingOffers.has(offerId) })
    const pending = this.pendingOffers.get(offerId)
    if (!pending || peerId === this.peerId) return

    this.pendingOffers.delete(offerId)
    pending.peer.remotePeerId = peerId
    this.peers.set(peerId, pending.peer)
    pending.peer.signal(answer)
  }

  createPeer (remotePeerId, initiator) {
    const peer = new SimplePeer({ initiator, trickle: false, ...this.peerOpts })
    this.emitDebug({
      type: 'peer-created',
      initiator,
      peerType: typeof SimplePeer,
      hasRtcPeerConnection: typeof globalThis.RTCPeerConnection !== 'undefined',
      hasRtcSessionDescription: typeof globalThis.RTCSessionDescription !== 'undefined'
    })
    peer.remotePeerId = remotePeerId

    peer.on('connect', () => {
      this.emitDebug({ type: 'peer-connect', peerId: peer.remotePeerId, initiator })
      if (peer.remotePeerId) this.peers.set(peer.remotePeerId, peer)
      this.sendSyncStep1(peer)
      this.sendAwareness(peer)
      this.synced = true
      this.emit('synced', [true])
      this.emit('peers', [Array.from(this.peers.keys())])
    })
    peer.on('signal', signal => this.emitDebug({ type: 'peer-signal', signalType: signal.type, initiator }))
    peer.on('_debug', message => this.emitDebug({ type: 'simple-peer-debug', message, initiator }))
    peer.on('data', data => readMessage(this, peer, data))
    peer.on('close', () => this.removePeer(peer))
    peer.on('error', error => {
      this.emit('peer-error', [error])
      this.removePeer(peer)
    })

    return peer
  }

  removePeer (peer) {
    let changed = false
    if (peer.remotePeerId && this.peers.delete(peer.remotePeerId)) changed = true
    for (const [offerId, pending] of this.pendingOffers) {
      if (pending.peer === peer) this.pendingOffers.delete(offerId)
    }
    if (changed) this.emit('peers', [Array.from(this.peers.keys())])
  }

  sendSyncStep1 (peer) {
    const encoder = encoding.createEncoder()
    encoding.writeVarUint(encoder, messageSync)
    syncProtocol.writeSyncStep1(encoder, this.doc)
    peer.send(encoding.toUint8Array(encoder))
  }

  sendAwareness (peer) {
    const states = Array.from(this.awareness.getStates().keys())
    if (states.length === 0) return
    const encoder = encoding.createEncoder()
    encoding.writeVarUint(encoder, messageAwareness)
    encoding.writeVarUint8Array(encoder, awarenessProtocol.encodeAwarenessUpdate(this.awareness, states))
    peer.send(encoding.toUint8Array(encoder))
  }

  broadcastSyncUpdate (update) {
    const encoder = encoding.createEncoder()
    encoding.writeVarUint(encoder, messageSync)
    syncProtocol.writeUpdate(encoder, update)
    this.broadcast(encoding.toUint8Array(encoder))
  }

  broadcastAwareness (update) {
    const encoder = encoding.createEncoder()
    encoding.writeVarUint(encoder, messageAwareness)
    encoding.writeVarUint8Array(encoder, update)
    this.broadcast(encoding.toUint8Array(encoder))
  }

  broadcast (message) {
    for (const peer of this.peers.values()) {
      if (peer.connected) peer.send(message)
    }
  }

  emitDebug (event) {
    if (this.debug) this.emit('debug', [event])
  }

  disconnect () {
    this.shouldConnect = false
    for (const tracker of this.trackerConnections) tracker.destroy()
    this.trackerConnections = []
    for (const peer of this.peers.values()) peer.destroy()
    for (const { peer } of this.pendingOffers.values()) peer.destroy()
    this.peers.clear()
    this.pendingOffers.clear()
    this.synced = false
    this.emit('synced', [false])
  }

  destroy () {
    this.disconnect()
    this.doc.off('update', this._docUpdateHandler)
    this.awareness.off('update', this._awarenessUpdateHandler)
    if (this._ownsAwareness) this.awareness.destroy()
    super.destroy()
  }
}
