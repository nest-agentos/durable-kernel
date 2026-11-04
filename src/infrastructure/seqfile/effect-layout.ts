import type {ISeqFileTransaction} from '@itookit/vfs-core';
import type {SessionRecord, TaskRecord} from '../../domain/types';
import {hasUnboundedEffect} from '../../domain/effect-deadline';
import {assertSessionLayout, currentSessionLayout, UNBOUNDED_EFFECT_CAPABILITY} from './session-layout';
import {decode, encode, sessionPath} from './seqfile-core';

/** Promote in the same transaction as the first unbounded Effect; old writers must fail closed. */
export async function declareEffectLayoutTx(tx: ISeqFileTransaction, root: string, task: TaskRecord): Promise<void> {
    if (!hasUnboundedEffect(task)) return;
    const path = sessionPath(root), raw = await tx.getEntry(path, 'record');
    if (!raw) throw new Error('Session record missing while declaring Effect deadline contract');
    const session = decode<SessionRecord>(raw); assertSessionLayout(session);
    if (session.layout?.recordSchemas.effect === 2) return;
    const layout = session.layout ?? currentSessionLayout();
    await tx.setEntry(path, 'record', encode({...session, version: session.version + 1,
        layout: {...layout, recordSchemas: {...layout.recordSchemas, effect: 2},
            requiredCapabilities: [...new Set([...layout.requiredCapabilities, UNBOUNDED_EFFECT_CAPABILITY])].sort()}}));
}
