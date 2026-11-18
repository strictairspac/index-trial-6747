import logging,redis
from fastapi import HTTPException
from sqlalchemy import text
from src.config.database import Session,engine
from src.config.settings import REDIS_URL
from src.models.workload import Workload
from src.models.candidate import Candidate
from src.models.experiment import Experiment
from src.utils.serialization import record
queue=redis.Redis.from_url(REDIS_URL)
QUEUE_KEY='index-trials'
LEASE_SECONDS=60
SWEEP_INTERVAL_SECONDS=5

def list_items():
 with Session() as s:return [record(r) for r in s.query(Experiment).order_by(Experiment.id.desc()).limit(100).all()]
def detail(item_id):
 with Session() as s:
  row=s.get(Experiment,item_id)
  if not row:raise HTTPException(404,'未找到试验')
  return record(row)

def _enqueue(experiment_id):
 """Best-effort enqueue. The database is the source of truth; the worker
 reconciles queued rows, so a queue outage never blocks acceptance."""
 try:
  queue.rpush(QUEUE_KEY,experiment_id)
  return True
 except redis.RedisError:
  logging.getLogger('experiments').warning('queue unavailable; experiment %s stays queued in DB',experiment_id)
  return False

def create(data):
 acceptance_no=data.name
 with Session() as s:
  existing=s.query(Experiment).filter_by(acceptance_no=acceptance_no).one_or_none()
  if existing:
   # Same acceptance number is a retry, never a new experiment. A changed
   # workload or candidate is a conflicting request and must be rejected.
   if existing.candidate_id!=data.candidate_id:
    c=s.get(Candidate,data.candidate_id)
    conflict_workload=c.workload_id if c else None
    if conflict_workload!=existing.workload_id:
     raise HTTPException(409,f'受理号 {acceptance_no} 已用于其他负载，不能重复受理')
    raise HTTPException(409,f'受理号 {acceptance_no} 已绑定其他候选，不能重复受理')
   return record(existing)
  c=s.get(Candidate,data.candidate_id)
  if not c:raise HTTPException(404,'未找到候选')
  w=s.get(Workload,c.workload_id)
  row=Experiment(acceptance_no=acceptance_no,name=acceptance_no,candidate_id=c.id,workload_id=w.id,status='queued',snapshot={'workload':record(w),'candidate':record(c)})
  s.add(row);s.commit();s.refresh(row);result=record(row)
 _enqueue(result['id'])
 return result

def due_queue_ids(limit=100):
 """Queued rows absent from the actual queue list (never pushed, Redis lost
 data, or a worker died between BLPOP and claim). Membership is read from the
 queue itself, so no separate marker can go stale and wedge an experiment."""
 try:
  in_queue={int(x) for x in queue.lrange(QUEUE_KEY,0,-1)}
 except redis.RedisError:
  in_queue=set()
 with Session() as s:
  ids=[r[0] for r in s.query(Experiment.id).filter(Experiment.status=='queued').order_by(Experiment.id).limit(limit).all()]
 return [i for i in ids if i not in in_queue]

def stale_running_ids(lease_seconds=LEASE_SECONDS,limit=20):
 with engine.connect() as conn:
  rows=conn.execute(text(
   "SELECT id FROM experiments WHERE status='running' AND "
   "(lease_until IS NULL OR lease_until<CURRENT_TIMESTAMP) ORDER BY id LIMIT :lim"),{'lim':limit})
  return [r[0] for r in rows]

def heartbeat(experiment_id,worker_id):
 with engine.connect() as conn:
  conn.execute(text(
   "UPDATE experiments SET lease_until=DATE_ADD(CURRENT_TIMESTAMP, INTERVAL :sec SECOND) "
   "WHERE id=:id AND lease_owner=:owner" if engine.dialect.name=='mysql' else
   "UPDATE experiments SET lease_until=DATETIME('now','+'||:sec||' seconds') "
   "WHERE id=:id AND lease_owner=:owner"),
   {'id':experiment_id,'owner':worker_id,'sec':LEASE_SECONDS})
  conn.commit()
