"""端到端演练（真实 HTTP 服务 + 真实 worker 循环，SQLite/fakeredis 替身）。

覆盖题目三问：
  1. 受理后断队列：POST 仍 200 落库，恢复后 worker 自动开跑；
  2. 建完索引就退出：重建 running+残留索引+过期租约，新 worker 接管原试验号并清理；
  3. 旧连接未退出：命名锁仍被持有时拒绝接管，锁释放后才恢复。
另含同号幂等、换候选/负载 409、最终只公布一份结果。
"""
import os,sys,time,threading,json
from pathlib import Path
import fakeredis,redis,httpx

DB='/tmp/gb4102_e2e_live.db'
if os.path.exists(DB):os.remove(DB)
os.environ.update(JWT_SECRET='e2e-secret',DATABASE_URL=f'sqlite:///{DB}',
                  REDIS_URL='redis://unused:6379/0',SEED_DEMO='true')
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'backend'))

from fastapi.testclient import TestClient
from src.config.database import Base,engine,Session
from src.config.migrations import upgrade
from src.services import experiments as svc
from src.services import trial_locks
from src import worker
from src.main import app
from src.models.candidate import Candidate
from sqlalchemy import text

Base.metadata.create_all(engine);upgrade()
from scripts.seed_demo import seed
seed()
fake=fakeredis.FakeStrictRedis()
svc.queue=fake;worker.q=fake
with trial_locks._sqlite_lock:trial_locks._sqlite_locks.clear()

stop=threading.Event();paused=threading.Event()
def worker_loop():
 while not stop.is_set():
  if paused.is_set():time.sleep(0.1);continue
  try:worker.cycle()
  except Exception:import traceback;traceback.print_exc()
  time.sleep(0.2)

def pause_worker():
 paused.set();time.sleep(0.6)  # 等当前 cycle 收尾
def resume_worker():paused.clear()
def detach_from_queue(eid):
 fake.lrem('index-trials',0,eid)

class DeadRedis(fakeredis.FakeStrictRedis):
 def _dead(self,*a,**k):raise redis.RedisError('queue outage')
 sadd=rpush=blpop=smembers=srem=ping=_dead

c=TestClient(app)
tok=c.post('/auth/login',json={'email':'operator@index.local','password':'Trial2026!'}).json()['token']
H={'Authorization':f'Bearer {tok}'}
with Session() as s:cand=s.query(Candidate).order_by(Candidate.id).first();cid=cand.id
def wait_for(eid,status,timeout=15):
 t=time.time()
 while time.time()-t<timeout:
  r=c.get(f'/experiments/{eid}',headers=H).json()
  if r['status']==status:return r
  time.sleep(0.2)
 raise AssertionError(f'{eid} 未在 {timeout}s 内到达 {status}，实际 {r}')

print('== 场景1：受理后断队列 ==')
svc.queue=DeadRedis()
r=c.post('/experiments',json={'name':'E2E-QUEUE-DOWN','candidate_id':cid},headers=H)
assert r.status_code==200,r.text;eid1=r.json()['id']
assert r.json()['status']=='queued'
print(f'  队列断开仍受理：试验 #{eid1} 状态 queued，健康检查 {c.get("/health").json()}')
svc.queue=fake;worker.q=fake
th=threading.Thread(target=worker_loop,daemon=True);th.start()
row=wait_for(eid1,'completed')
print(f'  队列恢复后自动开跑并完成：#{eid1} completed，结果行数 {row["result"]["rows"]}')

print('== 规则：同号幂等 / 换候选或负载拒绝 ==')
r1=c.post('/experiments',json={'name':'E2E-IDEM','candidate_id':cid},headers=H)
r2=c.post('/experiments',json={'name':'E2E-IDEM','candidate_id':cid},headers=H)
assert r1.json()['id']==r2.json()['id'];eid2=r1.json()['id']
with Session() as s:
 other=s.query(Candidate).filter(Candidate.id!=cid).first()
r409=c.post('/experiments',json={'name':'E2E-IDEM','candidate_id':other.id},headers=H)
assert r409.status_code==409
print(f'  同号重试返回同一试验 #{eid2}；换候选被 409：{r409.json()["detail"]}')
wait_for(eid2,'completed')
r3=c.post('/experiments',json={'name':'E2E-IDEM','candidate_id':cid},headers=H)
assert r3.json()['id']==eid2 and r3.json()['status']=='completed'
print(f'  完成后同号重试不新增、不覆盖：仍是 #{eid2} 同一份结果')

print('== 场景2：建完索引就退出（硬杀死） ==')
r=c.post('/experiments',json={'name':'E2E-CRASH','candidate_id':cid},headers=H);eid3=r.json()['id']
pause_worker();detach_from_queue(eid3)
assert worker.claim(eid3)
with engine.begin() as conn:trial_locks.create_trial_index(conn,['sensor_id','recorded_at'])
with engine.begin() as conn:conn.execute(text("UPDATE experiments SET lease_until=DATETIME('now','-1 hour') WHERE id=:i"),{'i':eid3})
assert trial_locks.trial_index_exists(engine.connect())
print(f'  重建崩溃现场：#{eid3} running、租约过期、trial_candidate 残留')
resume_worker()
row=wait_for(eid3,'completed')
assert not trial_locks.trial_index_exists(engine.connect())
assert c.get('/experiments',headers=H).json() and len([x for x in c.get('/experiments',headers=H).json() if x['acceptance_no']=='E2E-CRASH'])==1
print(f'  新 worker 接管同一试验 #{eid3} 并完成，遗留索引已清理，只此一份结果')

print('== 场景3：旧执行连接未退出 ==')
r=c.post('/experiments',json={'name':'E2E-LIVE','candidate_id':cid},headers=H);eid4=r.json()['id']
pause_worker();detach_from_queue(eid4)
assert worker.claim(eid4)
with engine.begin() as conn:conn.execute(text("UPDATE experiments SET lease_until=DATETIME('now','-1 hour') WHERE id=:i"),{'i':eid4})
resume_worker()
started=threading.Event();proceed=threading.Event()
def hold():
 with engine.connect() as conn:
  assert trial_locks.acquire_serial_lock(conn) is True
  started.set();proceed.wait(10)
  trial_locks.release_serial_lock(conn)
holder=threading.Thread(target=hold);holder.start();started.wait(5)
assert trial_locks.serial_lock_held_elsewhere() is True
assert worker.reclaim(eid4) is False
print(f'  旧连接持锁工作中：#{eid4} 接管被拒绝，状态保持 {c.get(f"/experiments/{eid4}",headers=H).json()["status"]}')
proceed.set();holder.join(5)
# 释放后下一轮 worker 自动接管跑完
final=wait_for(eid4,'completed')
print(f'  旧连接退出后自动接管：#{eid4} completed，worker={final["worker_id"]}')

stop.set();th.join(3)
print('\n全部端到端断言通过。')
