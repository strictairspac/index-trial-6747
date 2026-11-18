from sqlalchemy import Column,Integer,String,JSON,DateTime,func,ForeignKey
from src.config.database import Base
class Experiment(Base):
 __tablename__='experiments'
 id=Column(Integer,primary_key=True)
 acceptance_no=Column(String(120),unique=True,nullable=False)
 name=Column(String(120),nullable=False)
 workload_id=Column(Integer,ForeignKey('workloads.id'),nullable=False)
 candidate_id=Column(Integer,ForeignKey('candidates.id'),nullable=False)
 status=Column(String(20),default='queued',nullable=False,index=True)
 result=Column(JSON,nullable=True)
 error=Column(String(1000),nullable=True)
 snapshot=Column(JSON,nullable=False)
 worker_id=Column(String(120),nullable=True)
 lease_owner=Column(String(120),nullable=True)
 lease_until=Column(DateTime,nullable=True)
 created_at=Column(DateTime,server_default=func.now())
