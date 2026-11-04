import {expect, it, vi} from 'vitest';
import {EffectWorkSlots} from './effect-work-slots';

function deferred<T = void>() {
    let resolve!: (value: T | PromiseLike<T>) => void;
    const promise = new Promise<T>(done => {resolve = done;}); return {promise, resolve};
}
it('releases work capacity during observation and reacquires before returning', async () => {
    const slots = new EffectWorkSlots(1, () => {}), parent = slots.tryAcquire()!, observed = deferred<number>();
    const complete = vi.fn(), waiting = parent.observe(() => observed.promise, new AbortController().signal).then(complete);
    const child = slots.tryAcquire()!; expect(child).toBeDefined(); expect(slots.tryAcquire()).toBeUndefined();
    observed.resolve(42); await Promise.resolve(); await Promise.resolve();
    expect(complete).not.toHaveBeenCalled(); child.close(); await waiting; expect(complete).toHaveBeenCalledWith(42);
    expect(slots.tryAcquire()).toBeUndefined(); parent.close(); expect(slots.tryAcquire()).toBeDefined();
});
it('restores capacity before an observation error reaches adapter catch code', async () => {
    const slots = new EffectWorkSlots(1, () => {}), parent = slots.tryAcquire()!, observed = deferred();
    const error = new Error('Private observation detail'), caught = vi.fn();
    const waiting = parent.observe(async () => {await observed.promise; throw error;}, new AbortController().signal).catch(caught);
    const child = slots.tryAcquire()!; observed.resolve(); await Promise.resolve(); await Promise.resolve();
    expect(caught).not.toHaveBeenCalled(); child.close(); await waiting;
    expect(caught).toHaveBeenCalledWith(error); expect(slots.tryAcquire()).toBeUndefined(); parent.close();
});
it('removes an aborted resume without taking another Effect work slot', async () => {
    const slots = new EffectWorkSlots(1, () => {}), parent = slots.tryAcquire()!, controller = new AbortController();
    const observed = deferred(), waiting = parent.observe(() => observed.promise, controller.signal);
    const child = slots.tryAcquire()!; observed.resolve(); await Promise.resolve(); await Promise.resolve();
    const stopped = expect(waiting).rejects.toBe('Stopped'); controller.abort('Stopped'); await stopped;
    parent.close(); child.close(); const next = slots.tryAcquire()!;
    expect(next).toBeDefined(); expect(slots.tryAcquire()).toBeUndefined(); next.close();
});
it('closes a queued resume and never leaks a later capacity grant', async () => {
    const slots = new EffectWorkSlots(1, () => {}), parent = slots.tryAcquire()!, observed = deferred();
    const waiting = parent.observe(() => observed.promise, new AbortController().signal), child = slots.tryAcquire()!;
    observed.resolve(); await Promise.resolve(); await Promise.resolve();
    const stopped = expect(waiting).rejects.toHaveProperty('code', 'EFFECT_WORK_PERMIT_CLOSED'); parent.close(); await stopped;
    child.close(); const next = slots.tryAcquire()!; expect(next).toBeDefined(); expect(slots.tryAcquire()).toBeUndefined(); next.close();
});
it('does not resume a closed permit after its observation settles late', async () => {
    const slots = new EffectWorkSlots(1, () => {}), parent = slots.tryAcquire()!, observed = deferred();
    const waiting = parent.observe(() => observed.promise, new AbortController().signal);
    parent.close(); observed.resolve(); await expect(waiting).rejects.toHaveProperty('code', 'EFFECT_WORK_PERMIT_CLOSED');
    expect(slots.tryAcquire()).toBeDefined(); expect(slots.tryAcquire()).toBeUndefined();
});
it('aborts a noncooperative observation and rejects overlapping observations', async () => {
    const slots = new EffectWorkSlots(1, () => {}), parent = slots.tryAcquire()!, controller = new AbortController();
    const read = vi.fn(() => new Promise<never>(() => {})), waiting = parent.observe(read, controller.signal);
    await expect(parent.observe(read, controller.signal)).rejects.toHaveProperty('code', 'EFFECT_OBSERVATION_ACTIVE');
    const stopped = expect(waiting).rejects.toBe('Stopped'); controller.abort('Stopped'); await stopped;
    parent.close(); expect(read).toHaveBeenCalledOnce(); expect(slots.tryAcquire()).toBeDefined(); expect(slots.tryAcquire()).toBeUndefined();
});
it('retains the original limit across repeated closes and zero-capacity configurations', () => {
    const available = vi.fn(), slots = new EffectWorkSlots(1, available), permit = slots.tryAcquire()!;
    permit.close(); permit.close(); expect(available).toHaveBeenCalledOnce();
    expect(slots.tryAcquire()).toBeDefined(); expect(slots.tryAcquire()).toBeUndefined();
    expect(new EffectWorkSlots(0, available).tryAcquire()).toBeUndefined();
});
it('releases a granted resume that closes before its continuation runs', async () => {
    const slots = new EffectWorkSlots(1, () => {}), parent = slots.tryAcquire()!, observed = deferred();
    const waiting = parent.observe(() => observed.promise, new AbortController().signal), child = slots.tryAcquire()!;
    observed.resolve(); await Promise.resolve(); await Promise.resolve(); await Promise.resolve();
    const stopped = expect(waiting).rejects.toHaveProperty('code', 'EFFECT_WORK_PERMIT_CLOSED');
    child.close(); parent.close(); await stopped;
    expect(slots.tryAcquire()).toBeDefined(); expect(slots.tryAcquire()).toBeUndefined();
});
