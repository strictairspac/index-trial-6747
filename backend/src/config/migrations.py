"""Idempotent lightweight schema upgrades applied at startup.

The canonical schema is created by SQLAlchemy metadata. These statements only
fill gaps on databases created by an earlier release, so existing rows keep
their identity instead of being rebuilt. On a fresh database create_all already
produces the current shape, so every step is guarded.
"""
from sqlalchemy import text
from src.config.database import engine

_EXPERIMENT_COLUMNS={
 'acceptance_no':"VARCHAR(120) NOT NULL DEFAULT ''",
 'worker_id':'VARCHAR(120) NULL',
 'lease_owner':'VARCHAR(120) NULL',
 'lease_until':'DATETIME NULL',
}

def _mysql_columns(conn,table):
 rows=conn.execute(text(
  "SELECT column_name FROM information_schema.columns "
  "WHERE table_schema=DATABASE() AND table_name=:t"),{'t':table})
 return {r[0] for r in rows}

def _sqlite_columns(conn,table):
 return {r[1] for r in conn.execute(text(f'PRAGMA table_info({table})'))}

def _sqlite_table_exists(conn,table):
 return conn.execute(text("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=:t"),{'t':table}).scalar()>0

def upgrade():
 dialect=engine.dialect.name
 with engine.begin() as conn:
  if dialect=='mysql':
   present=_mysql_columns(conn,'experiments')
   if not present:return  # fresh database, create_all already made the current schema
   for column,ddl in _EXPERIMENT_COLUMNS.items():
    if column not in present:
     conn.execute(text(f'ALTER TABLE experiments ADD COLUMN {column} {ddl}'))
   conn.execute(text("UPDATE experiments SET acceptance_no=name WHERE acceptance_no='' OR acceptance_no IS NULL"))
   # legacy releases made the display name unique; acceptance_no is the key now
   dups=conn.execute(text(
    "SELECT COUNT(*) FROM information_schema.statistics "
    "WHERE table_schema=DATABASE() AND table_name='experiments' "
    "AND index_name='ix_experiments_name' AND non_unique=0")).scalar()
   if dups:
    conn.execute(text('ALTER TABLE experiments DROP INDEX ix_experiments_name'))
   missing_unique=conn.execute(text(
    "SELECT COUNT(*) FROM information_schema.statistics "
    "WHERE table_schema=DATABASE() AND table_name='experiments' "
    "AND index_name='ix_experiments_acceptance_no'")).scalar()==0
   if missing_unique:
    conn.execute(text('ALTER TABLE experiments ADD UNIQUE KEY ix_experiments_acceptance_no (acceptance_no)'))
  elif dialect=='sqlite':
   if not _sqlite_table_exists(conn,'experiments'):return
   present=_sqlite_columns(conn,'experiments')
   for column,ddl in _EXPERIMENT_COLUMNS.items():
    if column not in present:
     conn.execute(text(f'ALTER TABLE experiments ADD COLUMN {column} {ddl.split()[0]}'))
   conn.execute(text("UPDATE experiments SET acceptance_no=name WHERE acceptance_no='' OR acceptance_no IS NULL"))
