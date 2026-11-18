"""Database-level locking and trial-index housekeeping.

MySQL path uses named connection locks (GET_LOCK is bound to the session that
created it): a live worker keeps holding its lock, so a recovering peer cannot
fence the table out from under it. SQLite only exists for local tests and uses
an in-process lock table with the same semantics.
"""
import threading
from sqlalchemy import text
from src.config.database import engine

_TRIAL_INDEX='trial_candidate'
_SERIAL_LOCK='index_trial_serial'
_sqlite_locks={}
_sqlite_lock=threading.Lock()

def is_mysql():
 return engine.dialect.name=='mysql'

def _conn_token(conn):return f'conn-{id(conn)}'

def acquire_serial_lock(conn,timeout_seconds=30):
 if is_mysql():
  if conn.execute(text("SELECT GET_LOCK(:name,:to)"),{'name':_SERIAL_LOCK,'to':timeout_seconds}).scalar()!=1:
   raise RuntimeError('旧执行连接仍持有试验锁，本次不接管')
  return True
 with _sqlite_lock:
  owner=_sqlite_locks.get(_SERIAL_LOCK)
  token=_conn_token(conn)
  if owner is not None and owner!=token:
   return False
  _sqlite_locks[_SERIAL_LOCK]=token
 return True

def release_serial_lock(conn):
 if is_mysql():
  conn.execute(text("SELECT RELEASE_LOCK(:name)"),{'name':_SERIAL_LOCK})
 else:
  with _sqlite_lock:
   if _sqlite_locks.get(_SERIAL_LOCK)==_conn_token(conn):
    _sqlite_locks.pop(_SERIAL_LOCK,None)

def serial_lock_held_elsewhere():
 """True when another live database session still owns the trial lock."""
 if is_mysql():
  with engine.connect() as conn:
   return conn.execute(text("SELECT IS_FREE_LOCK(:name)"),{'name':_SERIAL_LOCK}).scalar()==0
 with _sqlite_lock:return _SERIAL_LOCK in _sqlite_locks

def trial_index_exists(conn):
 if is_mysql():
  return conn.execute(text(
   "SELECT COUNT(*) FROM information_schema.statistics "
   "WHERE table_schema=DATABASE() AND table_name='telemetry' AND index_name=:idx"),
   {'idx':_TRIAL_INDEX}).scalar()>0
 rows=conn.execute(text("PRAGMA index_list(telemetry)")).fetchall()
 return any(_TRIAL_INDEX in str(r) for r in rows)

def drop_trial_index(conn):
 if is_mysql():
  conn.execute(text(f'DROP INDEX {_TRIAL_INDEX} ON telemetry'))
 else:
  conn.execute(text(f'DROP INDEX IF EXISTS {_TRIAL_INDEX}'))

def create_trial_index(conn,columns):
 quoted=','.join('`'+c+'`' for c in columns) if is_mysql() else ','.join('"'+c+'"' for c in columns)
 if is_mysql():
  conn.execute(text(f'CREATE INDEX {_TRIAL_INDEX} ON telemetry ({quoted})'))
 else:
  conn.execute(text(f'CREATE INDEX {_TRIAL_INDEX} ON telemetry ({quoted})'))

def analyze_telemetry(conn):
 if is_mysql():
  conn.execute(text('ANALYZE TABLE telemetry')).all()
 else:
  conn.execute(text('ANALYZE telemetry'))
