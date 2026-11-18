/** @typedef {{id:number,name:string,sensor_id:number,start_at:string,end_at:string,device_type:string|null}} Workload */
/** @typedef {{id:number,name:string,workload_id:number,columns:string[]}} Candidate */
/** @typedef {{id:number,acceptance_no:string,name:string,status:'queued'|'running'|'completed'|'failed',result:object|null,worker_id:string|null,lease_until:string|null}} Experiment */
export {};
