export const defaultTrackerUrls = [
  'wss://tracker.openwebtorrent.com',
  'wss://tracker.btorrent.xyz'
]

export class TrackerConnection {
  constructor (url, opts) {
    this.url = url
    this.infoHash = opts.infoHash
    this.peerId = opts.peerId
    this.numwant = opts.numwant
    this.createOffers = opts.createOffers
    this.onOffer = opts.onOffer
    this.onAnswer = opts.onAnswer
    this.onAnnounce = opts.onAnnounce || (() => {})
    this.onError = opts.onError || (() => {})
    this.WebSocket = opts.WebSocket || globalThis.WebSocket
    this.destroyed = false
    this.reconnectDelay = 1000
    this.socket = null
    this.announceTimer = 0
    this.reconnectTimer = 0
    this.connect()
  }

  connect () {
    if (this.destroyed || !this.WebSocket) return

    const socket = this.socket = new this.WebSocket(this.url)
    socket.addEventListener('open', () => {
      this.reconnectDelay = 1000
      this.announce('started')
    })
    socket.addEventListener('message', event => this.handleMessage(event.data))
    socket.addEventListener('error', event => this.onError(event))
    socket.addEventListener('close', () => this.scheduleReconnect())
  }

  async announce (event) {
    if (!this.isOpen()) return

    const message = {
      action: 'announce',
      info_hash: this.infoHash,
      peer_id: this.peerId,
      numwant: this.numwant
    }

    if (event) message.event = event
    if (event !== 'stopped') {
      const offers = await this.createOffers(this)
      if (offers.length > 0) message.offers = offers
    }

    this.send(message)
  }

  sendAnswer (toPeerId, offerId, answer) {
    this.send({
      action: 'announce',
      info_hash: this.infoHash,
      peer_id: this.peerId,
      to_peer_id: toPeerId,
      offer_id: offerId,
      answer
    })
  }

  handleMessage (data) {
    let message
    try {
      message = typeof data === 'string' ? JSON.parse(data) : JSON.parse(new TextDecoder().decode(data))
    } catch (error) {
      this.onError(error)
      return
    }

    if (message.action !== 'announce' || message.info_hash !== this.infoHash) return

    if (message.interval) this.scheduleAnnounce(message.interval)
    if ('complete' in message || 'incomplete' in message) this.onAnnounce(message)
    if (message.offer && message.offer_id && message.peer_id) {
      this.onOffer(message.peer_id, message.offer_id, message.offer, this)
    } else if (message.answer && message.offer_id && message.peer_id) {
      this.onAnswer(message.peer_id, message.offer_id, message.answer)
    }
  }

  scheduleAnnounce (intervalSeconds) {
    clearTimeout(this.announceTimer)
    this.announceTimer = setTimeout(() => this.announce(), intervalSeconds * 1000)
  }

  scheduleReconnect () {
    clearTimeout(this.announceTimer)
    if (this.destroyed) return
    clearTimeout(this.reconnectTimer)
    this.reconnectTimer = setTimeout(() => this.connect(), this.reconnectDelay)
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30000)
  }

  send (message) {
    if (this.isOpen()) this.socket.send(JSON.stringify(message))
  }

  isOpen () {
    return this.socket && this.socket.readyState === this.WebSocket.OPEN
  }

  destroy () {
    this.destroyed = true
    clearTimeout(this.announceTimer)
    clearTimeout(this.reconnectTimer)
    if (this.isOpen()) this.send({ action: 'announce', event: 'stopped', info_hash: this.infoHash, peer_id: this.peerId })
    if (this.socket) this.socket.close()
  }
}
