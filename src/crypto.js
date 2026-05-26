const textEncoder = new TextEncoder()

export const binaryStringFromBytes = bytes => {
  let out = ''
  for (const byte of bytes) out += String.fromCharCode(byte)
  return out
}

export const createInfoHash = async (roomName, password = '') => {
  const input = password ? `${password}:${roomName}` : roomName
  const digest = await crypto.subtle.digest('SHA-1', textEncoder.encode(input))
  return binaryStringFromBytes(new Uint8Array(digest))
}

export const createPeerId = () => {
  const bytes = new Uint8Array(20)
  bytes.set(textEncoder.encode('-YW0001-'))
  crypto.getRandomValues(bytes.subarray(8))
  return binaryStringFromBytes(bytes)
}
