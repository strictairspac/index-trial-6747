# Schema v2
SQLAlchemy metadata creates users, audits, workloads, candidates, experiments and telemetry on first startup. Workload/candidate natural names are UNIQUE; experiment snapshots are immutable JSON. Foreign keys prevent orphan references.

v2 changes (applied idempotently at startup by `src/config/migrations.py`):
- `experiments.acceptance_no` is the unique client-facing 受理号/试验号; retries with the same number return the same row and never create a second experiment. `name` is now a plain display label defaulting to the acceptance number and its legacy unique index is dropped.
- `experiments.worker_id` / `lease_owner` / `lease_until` record the running worker and its heartbeat lease, enabling crash recovery and takeover fencing.
- `experiments.status` is indexed for the queue reconciliation sweep.

Index trials affect only the dedicated telemetry table, under a MySQL named connection lock (GET_LOCK). A stale lease is reclaimed only when that lock is free: a live old connection keeps the lock and cannot be fenced. On takeover the worker verifies and drops any leftover `trial_candidate` index before measuring the no-index baseline. The trial index is dropped again after each execution. No production database connection is accepted.
