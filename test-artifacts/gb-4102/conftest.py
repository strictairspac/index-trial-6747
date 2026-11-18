"""Integration test harness: SQLite stand-in for MySQL 8.0 + fakeredis.

MySQL-only SQL is branched inside src/services/trial_locks.py and the worker;
the control flow (acceptance, idempotency, lease takeover, index hygiene) is
dialect-independent and fully exercised here.
"""
import os,sys,datetime
from pathlib import Path
import fakeredis,redis,pytest

BACKEND=Path(__file__).resolve().parents[2]/'backend'
sys.path.insert(0,str(BACKEND))
DB_PATH='/tmp/gb4102_pytest.db'

# src.config.settings reads environment at import time; pin everything first.
os.environ.setdefault('JWT_SECRET','test-secret')
os.environ.setdefault('DATABASE_URL',f'sqlite:///{DB_PATH}')
os.environ.setdefault('REDIS_URL','redis://unused:6379/0')
os.environ.setdefault('SEED_DEMO','false')

@pytest.fixture()
def h(monkeypatch):
 if os.path.exists(DB_PATH):os.remove(DB_PATH)
 from src.config.database import Base,engine,Session
 from src.config import migrations
 from src.models.identity import User
 from src.models.workload import Workload
 from src.models.candidate import Candidate
 from src.models.telemetry import Telemetry
 from src.utils.security import hash_password
 from src.services import experiments as svc
 from src.services import trial_locks
 from src.services import index_algorithm
 from src import worker
 from src.main import app

 Base.metadata.create_all(engine);migrations.upgrade()
 # no in-process named lock may survive from a previous test
 with trial_locks._sqlite_lock:trial_locks._sqlite_locks.clear()
 fake=fakeredis.FakeStrictRedis()
 monkeypatch.setattr(svc,'queue',fake)
 monkeypatch.setattr(worker,'q',fake)
 with Session() as s:
  s.add(User(email='operator@index.local',role='operator',password=hash_password('Trial2026!')))
  s.add(User(email='admin@index.local',role='admin',password=hash_password('Trial2026!')))
  s.add(User(email='viewer@index.local',role='viewer',password=hash_password('Trial2026!')))
  s.add(Workload(name='wl-17',sensor_id=17,start_at='2026-11-01T00:00:00',end_at='2026-11-30T23:59:59',device_type=None,description='t'))
  s.add(Workload(name='wl-36',sensor_id=36,start_at='2026-11-01T00:00:00',end_at='2026-11-30T23:59:59',device_type='motor',description='t'))
  s.flush()
  w1=s.query(Workload).filter_by(name='wl-17').one();w2=s.query(Workload).filter_by(name='wl-36').one()
  s.add(Candidate(name='cand-a',workload_id=w1.id,columns=['sensor_id','recorded_at'],note=''))
  s.add(Candidate(name='cand-b',workload_id=w1.id,columns=['recorded_at','sensor_id'],note=''))
  s.add(Candidate(name='cand-c',workload_id=w2.id,columns=['device_type','sensor_id','recorded_at'],note=''))
  s.flush()
  base=datetime.datetime(2026,11,1)
  rows=[{'id':i+1,'sensor_id':i%20+1,'recorded_at':(base+datetime.timedelta(minutes=i*8)).isoformat(),
         'device_type':['pump','motor','valve'][i%3],'value':30.0+i} for i in range(300)]
  s.bulk_insert_mappings(Telemetry,rows);s.commit()
  cand_a=s.query(Candidate).filter_by(name='cand-a').one()
  cand_b=s.query(Candidate).filter_by(name='cand-b').one()
  cand_c=s.query(Candidate).filter_by(name='cand-c').one()
 from fastapi.testclient import TestClient
 client=TestClient(app)
 token=client.post('/auth/login',json={'email':'operator@index.local','password':'Trial2026!'}).json()['token']
 vtoken=client.post('/auth/login',json={'email':'viewer@index.local','password':'Trial2026!'}).json()['token']
 class DeadRedis(fakeredis.FakeStrictRedis):
  def _dead(self,*a,**k):raise redis.RedisError('simulated queue outage')
  sadd=rpush=blpop=smembers=srem=ping=lrange=_dead
 yield {'client':client,'headers':{'Authorization':f'Bearer {token}'},'vheaders':{'Authorization':f'Bearer {vtoken}'},
        'fake':fake,'DeadRedis':DeadRedis,'Session':Session,'engine':engine,
        'cand_a':cand_a.id,'cand_b':cand_b.id,'cand_c':cand_c.id,
        'svc':svc,'worker':worker,'trial_locks':trial_locks,'index_algorithm':index_algorithm}
 engine.dispose()
