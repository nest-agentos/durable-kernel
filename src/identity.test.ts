import { afterEach, expect, it, vi } from 'vitest';
import { randomUUID } from './identity';

afterEach(() => vi.unstubAllGlobals());
it.each([undefined, {}, { randomUUID() { throw new Error('restricted'); } }, {
    randomUUID() { throw new Error('restricted'); }, getRandomValues() { throw new Error('restricted'); },
}])('generates UUID v4 when native randomness is missing or throws: %j', crypto => {
    vi.stubGlobal('crypto', crypto);
    const values = Array.from({ length: 20 }, () => randomUUID());
    expect(new Set(values).size).toBe(20);
    for (const value of values) expect(value).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
});
it('uses native UUID when available', () => {
    vi.stubGlobal('crypto', { randomUUID: () => '00000000-0000-4000-8000-000000000000' });
    expect(randomUUID()).toBe('00000000-0000-4000-8000-000000000000');
});
