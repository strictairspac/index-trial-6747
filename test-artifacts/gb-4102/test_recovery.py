"""验收脚本：受理断队列 / 建完索引即退出 / 旧连接未退出 三类故障演练。

运行：
  PYTHONPATH=/tmp/pylibs/lib/python3.11/site-packages \
  python3 -m pytest test-artifacts/gb-4102 -v
"""
import time,threading,datetime
from sqlalchemy import text


def _accept(h,no,cand):
 return h['client'].post('/experiments',json={'name':no,'candidate_id':cand},headers=h['headers'])


def _get(h,eid):
 return h['client'].get(f'/experiments/{eid}',headers=h['headers']).json()


# ---------- 规则 1：队列断开仍受理，恢复后自动开跑 ----------

def test_accept_while_queue_down_then_auto_run(h,monkeypatch):
 dead=h['DeadRedis']()
 monkeypatch.setattr(h['svc'],'queue',dead)
 r=_accept(h,'ACC-001',h['cand_a'])
 assert r.status_code==200,r.text
 eid=r.json()['id']
 # 受理成功且落库为 queued，没有因队列故障变 failed
 row=_get(h,eid)
 assert row['status']=='queued' and row['acceptance_no']=='ACC-001'
 assert row['result'] is None
 # 队列里确实什么都没进去
 assert h['fake'].llen('index-trials')==0
 # 恢复队列：worker 对账把 DB 里 queued 但未入队的试验补推并执行
 monkeypatch.setattr(h['svc'],'queue',h['fake'])
 monkeypatch.setattr(h['worker'],'q',h['fake'])
 h['worker'].cycle()
 row=_get(h,eid)
 assert row['status']=='completed',row
 assert row['result'] is not None and row['result']['rows']>=0
 # 队列标记被消费
 assert h['fake'].llen('index-trials')==0


def test_health_degraded_when_queue_down(h,monkeypatch):
 monkeypatch.setattr(h['svc'],'queue',h['DeadRedis']())
 r=h['client'].get('/health')
 assert r.status_code==200 and r.json()['queue']=='degraded'


# ---------- 规则 2：同一受理号重试不新增；换候选/负载拒绝 ----------

def test_same_acceptance_no_is_idempotent(h):
 r1=_accept(h,'ACC-DUP',h['cand_a']);assert r1.status_code==200
 r2=_accept(h,'ACC-DUP',h['cand_a']);assert r2.status_code==200
 assert r1.json()['id']==r2.json()['id']
 rows=h['client'].get('/experiments',headers=h['headers']).json()
 assert sum(1 for x in rows if x['acceptance_no']=='ACC-DUP')==1
 assert h['fake'].llen('index-trials')==1


def test_same_no_different_candidate_rejected(h):
 _accept(h,'ACC-CONF',h['cand_a'])
 r=_accept(h,'ACC-CONF',h['cand_b'])
 assert r.status_code==409 and '候选' in r.json()['detail']


def test_same_no_different_workload_rejected(h):
 _accept(h,'ACC-WL',h['cand_a'])
 r=_accept(h,'ACC-WL',h['cand_c'])
 assert r.status_code==409 and '负载' in r.json()['detail']
 # 被拒绝的受理没有产生第二条试验
 rows=h['client'].get('/experiments',headers=h['headers']).json()
 assert sum(1 for x in rows if x['acceptance_no']=='ACC-WL')==1


def test_viewer_cannot_accept(h):
 r=h['client'].post('/experiments',json={'name':'ACC-V','candidate_id':h['cand_a']},headers=h['vheaders'])
 assert r.status_code==403


# ---------- 规则 3：建完索引就退出 —— 恢复原试验、清理遗留索引、同号一份结果 ----------

def test_crash_after_index_create_is_resumed_with_same_id(h,monkeypatch):
 r=_accept(h,'ACC-CRASH',h['cand_a']);eid=r.json()['id']
 # 重建“硬杀死（SIGKILL/掉电）后”的磁盘状态：行停在 running、租约过期、
 # trial_candidate 残留在 telemetry 上。硬杀死不执行 Python finally，故直接造状态。
 assert h['worker'].claim(eid) is True
 with h['engine'].begin() as c:h['trial_locks'].create_trial_index(c,['sensor_id','recorded_at'])
 with h['engine'].begin() as c:
  c.execute(text("UPDATE experiments SET lease_until=DATETIME('now','-1 hour') WHERE id=:i"),{'i':eid})
 assert h['trial_locks'].trial_index_exists(h['engine'].connect())
 events={'drop':0}
 real_drop=h['trial_locks'].drop_trial_index
 def counting_drop(conn):
  events['drop']+=1;return real_drop(conn)
 monkeypatch.setattr(h['trial_locks'],'drop_trial_index',counting_drop)
 # 旧连接已不存在：DB 锁空闲，新 worker 可以接管
 assert h['trial_locks'].serial_lock_held_elsewhere() is False
 h['worker'].cycle()  # reconcile：发现过期租约 -> 重新排队
 h['worker'].cycle()  # 弹出并执行：先清遗留索引，再测基线
 row=_get(h,eid)
 assert row['status']=='completed',row
 assert events['drop']>=1,'接管时必须核对并清理遗留索引'
 assert not h['trial_locks'].trial_index_exists(h['engine'].connect()),'结束后索引应再次清理'
 # 沿用同一试验号，且只有一份结果
 rows=h['client'].get('/experiments',headers=h['headers']).json()
 same=[x for x in rows if x['acceptance_no']=='ACC-CRASH']
 assert len(same)==1 and same[0]['id']==eid
 assert same[0]['result'] is not None
 # 导出的也是这同一份
 exported=h['client'].get(f'/experiments/{eid}/export',headers=h['headers'])
 assert exported.status_code==200 and f'trial-{eid}' in exported.headers['content-disposition']


def test_baseline_never_runs_with_residual_index(h,monkeypatch):
 # 直接在表上制造一个残留索引，再跑全新试验
 r=_accept(h,'ACC-DIRTY',h['cand_a']);eid=r.json()['id']
 with h['engine'].begin() as c:h['trial_locks'].create_trial_index(c,['recorded_at'])
 order=[]
 real_drop=h['trial_locks'].drop_trial_index
 real_timed=h['index_algorithm']._timed
 def drop_recorder(conn):order.append('drop');return real_drop(conn)
 def timed_recorder(conn,q,p,heartbeat=None):order.append('timed');return real_timed(conn,q,p,heartbeat)
 monkeypatch.setattr(h['trial_locks'],'drop_trial_index',drop_recorder)
 monkeypatch.setattr(h['index_algorithm'],'_timed',timed_recorder)
 h['worker'].cycle()
 assert _get(h,eid)['status']=='completed'
 # 第一次计时查询之前，残留索引已被清掉（其他试验不能带着残留索引测基线）
 assert order.index('drop')<order.index('timed')


# ---------- 规则 4：旧执行连接仍在工作 —— 不能接管改表 ----------

def test_live_old_connection_blocks_takeover(h,monkeypatch):
 r=_accept(h,'ACC-LIVE',h['cand_a']);eid=r.json()['id']
 started=threading.Event();proceed=threading.Event()
 def hold_lock_then_run(snapshot,heartbeat=None):
  with h['engine'].connect() as conn:
   assert h['trial_locks'].acquire_serial_lock(conn) is True
   started.set()
   proceed.wait(5)
   h['trial_locks'].release_serial_lock(conn)
  return _real_run(snapshot,heartbeat)
 _real_run=h['index_algorithm'].run
 monkeypatch.setattr(h['index_algorithm'],'run',hold_lock_then_run)
 t=threading.Thread(target=h['worker'].cycle);t.start()
 started.wait(5)
 # 旧 worker 仍持锁、仍在工作；把租约伪装成过期
 with h['engine'].begin() as c:
  c.execute(text("UPDATE experiments SET lease_until=DATETIME('now','-1 hour') WHERE id=:i"),{'i':eid})
 assert h['trial_locks'].serial_lock_held_elsewhere() is True
 # 新 worker 尝试接管：必须跳过，不能改表
 assert h['worker'].reclaim(eid) is False
 row=_get(h,eid);assert row['status']=='running'
 # 放行旧连接
 proceed.set();t.join(5)
 row=_get(h,eid)
 assert row['status']=='completed',row  # 原执行者自己公布唯一结果
 assert row['worker_id'] is not None


def test_two_workers_only_one_claims(h):
 r=_accept(h,'ACC-RACE',h['cand_a']);eid=r.json()['id']
 outcomes=[]
 def claim_once():outcomes.append(h['worker'].claim(eid))
 ts=[threading.Thread(target=claim_once) for _ in range(2)]
 [x.start() for x in ts];[x.join(5) for x in ts]
 assert sorted(outcomes)==[False,True]
 assert _get(h,eid)['status']=='running'


# ---------- 规则 6：队列消息丢失 / 迁移幂等 ----------

def test_reconcile_when_redis_lost_data(h):
 r=_accept(h,'ACC-VANISH',h['cand_a']);eid=r.json()['id']
 # Redis 被清空（容器重建/未持久化），DB 里仍是 queued
 h['fake'].flushall()
 assert h['fake'].llen('index-trials')==0
 h['worker'].cycle()  # 对账补推并在同一周期弹出执行
 assert _get(h,eid)['status']=='completed'


def test_reconcile_after_pop_without_claim(h):
 r=_accept(h,'ACC-POPCRASH',h['cand_a']);eid=r.json()['id']
 # 旧 worker 已 BLPOP 拿到消息但在 CAS 认领前被杀死：队列空、DB 仍 queued
 assert h['fake'].blpop('index-trials',timeout=1) is not None
 assert h['fake'].llen('index-trials')==0
 h['worker'].cycle()  # 对账发现 DB queued 但不在队列 -> 补推并执行
 assert _get(h,eid)['status']=='completed'


def test_duplicate_queue_messages_run_once(h):
 r=_accept(h,'ACC-DUPMSG',h['cand_a']);eid=r.json()['id']
 h['fake'].rpush('index-trials',eid)  # 队列里出现重复消息
 assert [int(x) for x in h['fake'].lrange('index-trials',0,-1)]==[eid,eid]
 h['worker'].cycle();h['worker'].cycle()
 row=_get(h,eid)
 assert row['status']=='completed'
 rows=h['client'].get('/experiments',headers=h['headers']).json()
 assert len([x for x in rows if x['acceptance_no']=='ACC-DUPMSG'])==1


def test_migration_idempotent_and_backfills_acceptance_no(h):
 from src.config import migrations
 from src.models.experiment import Experiment
 # 旧库形态：受理号列清空（模拟历史行）
 r=_accept(h,'ACC-LEGACY',h['cand_a']);eid=r.json()['id']
 with h['engine'].begin() as c:
  c.execute(text("UPDATE experiments SET acceptance_no='' WHERE id=:i"),{'i':eid})
 migrations.upgrade();migrations.upgrade()  # 重复执行不报错
 with h['Session']() as s:
  row=s.get(Experiment,eid)
  assert row.acceptance_no=='ACC-LEGACY'


def test_stale_run_skips_takeover_while_lock_busy_then_succeeds(h,monkeypatch):
 r=_accept(h,'ACC-BUSY',h['cand_a']);eid=r.json()['id']
 assert h['worker'].claim(eid) is True
 with h['engine'].begin() as c:
  c.execute(text("UPDATE experiments SET lease_until=DATETIME('now','-1 hour') WHERE id=:i"),{'i':eid})
 # 模拟旧连接仍持锁
 with h['engine'].connect() as conn:
  assert h['trial_locks'].acquire_serial_lock(conn) is True
  assert h['worker'].reclaim(eid) is False
  assert _get(h,eid)['status']=='running'
  h['trial_locks'].release_serial_lock(conn)
 # 锁释放后下一轮成功接管
 assert h['worker'].reclaim(eid) is True
 assert _get(h,eid)['status']=='queued'
 h['worker'].cycle()
 assert _get(h,eid)['status']=='completed'


# ---------- 规则 5：页面/记录全程同一试验号、最终一份结果 ----------

def test_single_published_result_after_retries(h,monkeypatch):
 r1=_accept(h,'ACC-ONCE',h['cand_a']);eid=r1.json()['id']
 # 完成前的重复受理仍是同一行
 assert _accept(h,'ACC-ONCE',h['cand_a']).json()['id']==eid
 h['worker'].cycle()
 row=_get(h,eid)
 assert row['status']=='completed'
 # 完成后再用同号重试：返回同一已完成试验，不产生新执行、不覆盖结果
 again=_accept(h,'ACC-ONCE',h['cand_a'])
 assert again.status_code==200 and again.json()['id']==eid
 assert again.json()['status']=='completed'
 checksum=row['result']['checksum']
 assert _get(h,eid)['result']['checksum']==checksum
 rows=h['client'].get('/experiments',headers=h['headers']).json()
 assert len([x for x in rows if x['acceptance_no']=='ACC-ONCE'])==1
