/** UUID v4 for browsers, WebViews and Node without importing application utilities. */
export function randomUUID(): string {
    try {
        const native = globalThis.crypto?.randomUUID?.();
        if (native) return native;
    } catch { /* Restricted WebViews can expose a throwing implementation. */ }
    const bytes = new Uint8Array(16);
    try {
        if (!globalThis.crypto?.getRandomValues) throw new Error('Secure random unavailable');
        globalThis.crypto.getRandomValues(bytes);
    } catch { for (let index = 0; index < bytes.length; index++) bytes[index] = Math.floor(Math.random() * 256); }
    bytes[6] = (bytes[6]! & 0x0f) | 0x40;
    bytes[8] = (bytes[8]! & 0x3f) | 0x80;
    const hex = [...bytes].map(value => value.toString(16).padStart(2, '0')).join('');
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
