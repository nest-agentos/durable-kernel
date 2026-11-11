import {expect,it} from 'vitest';
import {taskExecutionStat} from './index';
const task=(id:string,changes:Record<string,unknown>={})=>({id,sessionId:'s',rootTaskId:'root',effects:{},exit:{status:'succeeded'},...changes}) as any;
it('does not call a terminal root settled while a descendant still owns an external effect',()=>{
 const root=task('root'),child=task('child',{effects:{work:{status:'leased'}}});
 expect(taskExecutionStat(root,[root,child])).toMatchObject({settled:false,indeterminate:false});
 child.effects={};expect(taskExecutionStat(root,[root,child]).settled).toBe(true);
});
it('isolates another execution and retains uncertainty without exposing arbitrary error text',()=>{
 const root=task('root'),foreign=task('other',{sessionId:'another',effects:{work:{status:'indeterminate',error:{code:'PRIVATE TEXT'}}}});
 expect(taskExecutionStat(root,[root,foreign])).toMatchObject({settled:true,indeterminate:false});
 const unknown=task('child',{effects:{work:{status:'indeterminate',error:{code:'PRIVATE TEXT'}}}});
 expect(taskExecutionStat(root,[root,unknown])).toMatchObject({settled:false,indeterminate:true,code:undefined});
 unknown.effects.work.error.code='CONNECTION_LOST';expect(taskExecutionStat(root,[root,unknown]).code).toBe('CONNECTION_LOST');
 expect(taskExecutionStat(root,[]).settled).toBe(false);
});
