import {expect, it, vi} from 'vitest';
import type {IFileSystem} from '@nestos/vfs-core';
import {SeqCommitSubscriptions} from './infrastructure/seq-commit-subscriptions';
it('subscribes once per filesystem and routes only matching roots, once per commit', () => {
    let receive!: (event: {payload: {paths: string[]}}) => void;
    const release = vi.fn(), on = vi.fn((_type, callback) => {receive = callback; return release;});
    const fs = {on} as unknown as IFileSystem, routes = new SeqCommitSubscriptions(), callbacks = Array.from({length: 100}, () => vi.fn());
    const stops = callbacks.map((callback, index) => routes.watch(fs, '/sessions/s' + index, callback));
    expect(on).toHaveBeenCalledTimes(1);
    receive({payload: {paths: ['/sessions/s10/tasks.seq', '/sessions/s10/events.seq', '/sessions/s100/tasks.seq']}});
    expect(callbacks[10]).toHaveBeenCalledExactlyOnceWith(['/sessions/s10/tasks.seq', '/sessions/s10/events.seq']);
    expect(callbacks.filter(callback => callback.mock.calls.length)).toHaveLength(1);
    stops.slice(0, -1).forEach(stop => stop()); expect(release).not.toHaveBeenCalled();
    stops.at(-1)!(); expect(release).toHaveBeenCalledOnce(); stops.at(-1)!(); expect(release).toHaveBeenCalledOnce();
});
it('supports parent scopes, independent filesystems and isolation of failing callbacks', () => {
    const handlers: Array<(event: {payload: {paths: string[]}}) => void> = [];
    const fs = () => ({on: (_type, callback) => {handlers.push(callback); return () => {};}} as unknown as IFileSystem);
    const router = new SeqCommitSubscriptions(), first = fs(), second = fs(), root = vi.fn(), child = vi.fn(), other = vi.fn();
    router.watch(first, '/', root); router.watch(first, '/sessions/a', () => {throw Error('FAILED');}); router.watch(first, '/sessions/a', child); router.watch(second, '/', other);
    expect(() => handlers[0]({payload: {paths: ['/sessions/a/tasks.seq']}})).toThrow('Commit subscribers failed');
    expect(root).toHaveBeenCalledOnce(); expect(child).toHaveBeenCalledOnce(); expect(other).not.toHaveBeenCalled();
});
