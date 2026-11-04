import { expect, it, vi } from 'vitest';
import type { Kernel } from '../application/kernel';
import type { EventEnvelope } from '../domain/types';
import { KernelErrorCode } from '../domain/errors';
import { eventStream } from './event-stream';

const event = (sequence: number): EventEnvelope => ({ sequence, sessionId: 's', taskId: 't', type: 'app.progress', occurredAt: 0 });
const terminal = { status: 'succeeded', exit: { status: 'succeeded', taskId: 't', completedAt: 0 }, effects: {} };

it('uses bounded sequence pages without walking unrelated Session history', async () => {
    const readEvents = vi.fn().mockResolvedValueOnce({ items: [event(30)], nextAfterSequence: 30, throughSequence: 100 })
        .mockResolvedValueOnce({ items: [event(100)], throughSequence: 100 })
        .mockResolvedValueOnce({ items: [], throughSequence: 100 });
    const eventList = vi.fn(() => { throw new Error('Whole Session scan'); });
    const kernel = { readEvents, eventList, task: async () => terminal } as unknown as Kernel;
    const received: number[] = [];
    for await (const item of eventStream(kernel, 's', 't', 20)) received.push(item.sequence);
    expect(received).toEqual([30, 100]);
    expect(eventList).not.toHaveBeenCalled();
    expect(readEvents.mock.calls.map(call => call[1].afterSequence)).toEqual([20, 30, 100]);
    expect(readEvents.mock.calls.every(call => call[1].limit === 100 && call[2] === 't')).toBe(true);
});

it('drains every page of the transaction tail after observing the terminal boundary', async () => {
    const readEvents = vi.fn().mockResolvedValueOnce({ items: [event(100)], throughSequence: 100 })
        .mockResolvedValueOnce({ items: [event(105)], nextAfterSequence: 105, throughSequence: 110 })
        .mockResolvedValueOnce({ items: [event(110)], throughSequence: 110 });
    const received: number[] = [];
    for await (const item of eventStream({ readEvents, task: async () => terminal } as unknown as Kernel, 's', 't', 0)) received.push(item.sequence);
    expect(received).toEqual([100, 105, 110]);
});

it('aborts observation promptly even while an uncancellable storage read is pending', async () => {
    const controller = new AbortController(), lifetime = new AbortController();
    const kernel = { readEvents: () => new Promise(() => {}), abortSignal: lifetime.signal } as unknown as Kernel;
    const stream = eventStream(kernel, 's', undefined, 0, { signal: controller.signal });
    const next = stream.next(); controller.abort();
    await expect(next).rejects.toMatchObject({ code: KernelErrorCode.ABORTED });
});
