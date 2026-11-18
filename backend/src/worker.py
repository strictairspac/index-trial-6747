"""Single-consumer trial worker with crash recovery.

Acceptance is always persisted in the database first. This worker:
* pops queued ids from Redis but also reconciles queued rows when Redis lost
  data or was unavailable at acceptance time;
* keeps a lease with heartbeats while running;
* reclaims stale leases only when the old holder's database lock is gone, so a
  live connection that is still working can never have its table taken over;
* never publishes more than one result per experiment (status CAS guards).
"""
import os,time,uuid,logging,redis
from sqlalchemy import text
from src.config.settings import REDIS_URL
from src.config.database import Session,engine
from src.models.experiment import Experiment
from src.services import experiments as svc
from src.services import index_algorithm
from src.services import trial_locks
from src.services.audit import log
WORKER_ID=f"{os.uname().nodename}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
q=redis.Redis.from_url(REDIS_URL)
log_handler=logging.getLogger('worker')

def _enqueue_now(experiment_id):
 try:
  q.rpush(svc.QUEUE_KEY,experiment_id)
 except redis.RedisError:log_handler.warning('queue still unavailable for %s',experiment_id)

def reconcile():
 """Push DB-queued experiments missing from Redis; return stale running ids."""
 for experiment_id in svc.due_queue_ids():_enqueue_now(experiment_id)
 stale=svc.stale_running_ids()
 return stale

def claim(experiment_id):
 """CAS queued -> running. Returns True only for the winning worker."""
 with engine.begin() as conn:
  row=conn.execute(text(
   "SELECT status FROM experiments WHERE id=:id"),{'id':experiment_id}).mappings().first()
  if not row or row['status']!='queued':return False
  conn.execute(text(
   "UPDATE experiments SET status='running', worker_id=:w, lease_owner=:w, "
   "lease_until=DATE_ADD(CURRENT_TIMESTAMP, INTERVAL :sec SECOND) "
   "WHERE id=:id AND status='queued'" if engine.dialect.name=='mysql' else
   "UPDATE experiments SET status='running', worker_id=:w, lease_owner=:w, "
   "lease_until=DATETIME('now','+'||:sec||' seconds') "
   "WHERE id=:id AND status='queued'"),
   {'id':experiment_id,'w':WORKER_ID,'sec':svc.LEASE_SECONDS})
  return conn.execute(text("SELECT ROW_COUNT()" if engine.dialect.name=='mysql'
   else "SELECT changes()")).scalar()==1

def reclaim(experiment_id):
 """Take over a stale run only if no live holder keeps the DB trial lock."""
 if trial_locks.serial_lock_held_elsewhere():
  log_handler.warning('experiment %s lease stale but old connection still working; skip takeover',experiment_id)
  return False
 with engine.begin() as conn:
  conn.execute(text(
   "UPDATE experiments SET status='queued', worker_id=NULL, lease_owner=NULL, lease_until=NULL, "
   "error=CONCAT(COALESCE(error,''),' : 租约过期，重新排队接管') "
   "WHERE id=:id AND status='running'" if engine.dialect.name=='mysql' else
   "UPDATE experiments SET status='queued', worker_id=NULL, lease_owner=NULL, lease_until=NULL, "
   "error=COALESCE(error,'')||' : 租约过期，重新排队接管' "
   "WHERE id=:id AND status='running'"),{'id':experiment_id})
  changed=conn.execute(text("SELECT ROW_COUNT()" if engine.dialect.name=='mysql'
   else "SELECT changes()")).scalar()==1
 if changed:
  log(WORKER_ID,'experiment.reclaim',str(experiment_id))
  _enqueue_now(experiment_id)
 return changed

def execute(experiment_id):
 with Session() as s:
  row=s.get(Experiment,experiment_id)
  if not row or row.status!='running' or row.lease_owner!=WORKER_ID:return
  snapshot=row.snapshot
 try:
  result=index_algorithm.run(snapshot,heartbeat=lambda:svc.heartbeat(experiment_id,WORKER_ID))
  with Session() as s:
   r=s.get(Experiment,experiment_id)
   if r and r.status=='running' and r.lease_owner==WORKER_ID:
    r.result=result;r.status='completed';r.lease_until=None;r.error=None;s.commit()
    log(WORKER_ID,'experiment.completed',str(experiment_id))
   else:
    log_handler.warning('experiment %s no longer owned; result discarded',experiment_id)
 except Exception as exc:
  logging.exception('trial failed')
  with Session() as s:
   r=s.get(Experiment,experiment_id)
   if r and r.status=='running' and r.lease_owner==WORKER_ID:
    r.status='failed';r.error=str(exc)[:1000];r.lease_until=None;s.commit()
    log(WORKER_ID,'experiment.failed',str(experiment_id))

def _pop():
 try:
  item=q.blpop(svc.QUEUE_KEY,timeout=2)
 except redis.RedisError:
  log_handler.warning('queue disconnected; relying on DB reconciliation');time.sleep(2);return None
 if not item:return None
 return int(item[1])

def cycle():
 """One reconciliation + one queue item. Exposed for tests."""
 stale=reconcile()
 for experiment_id in stale:reclaim(experiment_id)
 experiment_id=_pop()
 if experiment_id is None:return
 if claim(experiment_id):execute(experiment_id)

def main():
 Path=__import__('pathlib').Path
 while True:
  Path('/tmp/worker-heartbeat').touch()
  try:cycle()
  except Exception:
   logging.exception('worker loop');time.sleep(2)

if __name__=='__main__':main()
