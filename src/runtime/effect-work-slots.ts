interface Waiter {grant(): void; cancel(reason: unknown): void}

/** Execution capacity is local; durable Effect ownership never lives in this pool. */
export class EffectWorkSlots {
    private used = 0;
    private readonly pending = new Map<EffectWorkPermit, Waiter>();
    constructor(private readonly limit: number, private readonly available: () => void) {}

    tryAcquire(): EffectWorkPermit | undefined {
        if (this.used >= this.limit || this.pending.size) return;
        this.used++;
        return new EffectWorkPermit(this);
    }
    resume(permit: EffectWorkPermit, signal: AbortSignal): Promise<void> {
        signal.throwIfAborted();
        if (permit.closed) return Promise.reject(closedError());
        return new Promise<void>((resolve, reject) => {
            const remove = () => {this.pending.delete(permit); signal.removeEventListener('abort', abort);};
            const abort = () => waiter.cancel(signal.reason);
            const waiter: Waiter = {
                grant: () => {remove(); this.used++; permit.grant(); resolve();},
                cancel: reason => {remove(); reject(reason); this.drain();},
            };
            this.pending.set(permit, waiter); signal.addEventListener('abort', abort, {once: true});
            this.drain();
        });
    }
    cancel(permit: EffectWorkPermit): void {this.pending.get(permit)?.cancel(closedError());}
    release(): void {this.used--; this.drain(); this.available();}
    private drain(): void {
        while (this.used < this.limit && this.pending.size) this.pending.values().next().value!.grant();
    }
}

/** A suspended observation retains its Effect lease but releases only execution capacity. */
export class EffectWorkPermit {
    closed = false;
    private held = true;
    private observing = false;
    constructor(private readonly slots: EffectWorkSlots) {}
    grant(): void {this.held = true;}
    close(): void {
        if (this.closed) return;
        this.closed = true; this.slots.cancel(this);
        if (this.held) {this.held = false; this.slots.release();}
    }
    async observe<T>(read: (signal: AbortSignal) => Promise<T>, signal: AbortSignal, confirmLocalStop = false): Promise<T> {
        signal.throwIfAborted();
        if (this.closed) throw closedError();
        if (this.observing) throw Object.assign(new Error('Effect observation already active'), {code: 'EFFECT_OBSERVATION_ACTIVE'});
        this.observing = true; this.held = false; this.slots.release();
        let value: T | undefined, error: unknown, failed = false;
        try {value = await (confirmLocalStop ? Promise.resolve().then(() => {signal.throwIfAborted(); return read(signal);}) : observation(read, signal));}
        catch (cause) {error = cause; failed = true;}
        try {await this.slots.resume(this, signal);}
        finally {this.observing = false;}
        if (this.closed) throw closedError();
        if (failed) throw error;
        return value as T;
    }
}

async function observation<T>(read: (signal: AbortSignal) => Promise<T>, signal: AbortSignal): Promise<T> {
    let abort!: () => void;
    const stopped = new Promise<never>((_, reject) => {
        abort = () => reject(signal.reason);
        signal.addEventListener('abort', abort, {once: true});
        if (signal.aborted) abort();
    });
    try {return await Promise.race([Promise.resolve().then(() => {signal.throwIfAborted(); return read(signal);}), stopped]);}
    finally {signal.removeEventListener('abort', abort);}
}
function closedError(): Error {
    return Object.assign(new Error('Effect work permit closed'), {code: 'EFFECT_WORK_PERMIT_CLOSED'});
}
