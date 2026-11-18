# gb-4102 试验受理与崩溃恢复 — 测试报告

日期：2026-11-18
范围：队列断开仍受理、同受理号幂等与冲突拒绝、建索引后中断恢复、旧连接存活时禁止接管、遗留索引清理、同一试验号一份结果。

## 环境说明
本机无 Docker/MySQL/Redis（无 sudo），依赖安装在 `/tmp/pylibs`（不进仓库）。
- 数据库：SQLite 作为 MySQL 8.0 的测试替身，MySQL 专有语句（GET_LOCK/RELEASE_LOCK/IS_FREE_LOCK、information_schema、ANALYZE TABLE、DATE_ADD）集中在 `src/services/trial_locks.py` 与 worker 中按方言分支，控制流与生产一致。
- 队列：`fakeredis` 替身；故障用 `DeadRedis`（sadd/rpush/blpop/lrange 等全部抛 RedisError）注入。
- 真实边界：MySQL 命名锁“连接存活即持锁、连接断开自动释放”的语义由 `trial_locks.py` 的同构进程锁模拟；`GET_LOCK`/`IS_FREE_LOCK` 生产 SQL 未做真机验证，需在 docker compose 环境补一次冒烟。

## 自动化结果
`python3 -m pytest test-artifacts/gb-4102 -v` → **16 passed**（输出见 pytest-output.txt）

| # | 用例 | 验证点 |
|---|---|---|
| 1 | test_accept_while_queue_down_then_auto_run | 队列断开 POST 仍 200 且落库 queued；恢复后 worker 对账自动开跑至 completed |
| 2 | test_health_degraded_when_queue_down | /health 不整体失败，返回 queue=degraded |
| 3 | test_same_acceptance_no_is_idempotent | 同受理号重试返回同一 id，不新增、不重复入队 |
| 4 | test_same_no_different_candidate_rejected | 同号换候选 → 409 |
| 5 | test_same_no_different_workload_rejected | 同号换负载 → 409，且无第二条试验 |
| 6 | test_viewer_cannot_accept | viewer 受理 → 403 |
| 7 | test_crash_after_index_create_is_resumed_with_same_id | running+过期租约+残留索引现场：新 worker 用同一 id 接管、先清索引再测基线、完成后再次清理、只一份结果 |
| 8 | test_baseline_never_runs_with_residual_index | 断言残留索引 drop 发生在首次计时查询之前 |
| 9 | test_live_old_connection_blocks_takeover | 旧连接持锁工作中 reclaim 返回 False、状态保持 running；释放后原执行者公布唯一结果 |
| 10 | test_two_workers_only_one_claims | 并发认领 CAS，仅一个 True |
| 11 | test_reconcile_when_redis_lost_data | Redis 清空后 DB queued 行被补推执行 |
| 12 | test_reconcile_after_pop_without_claim | BLPOP 后、认领前死亡的消息丢失窗口可自动补推 |
| 13 | test_duplicate_queue_messages_run_once | 队列重复消息只执行一次、只一条试验 |
| 14 | test_migration_idempotent_and_backfills_acceptance_no | 迁移幂等、旧行回填受理号 |
| 15 | test_stale_run_skips_takeover_while_lock_busy_then_succeeds | 锁忙跳过、锁释放后成功接管并完成 |
| 16 | test_single_published_result_after_retries | 完成前后同号重试均不新增不覆盖，checksum 保持 |

端到端演练 `e2e_demo.py`（真实 FastAPI TestClient HTTP + 真实 worker 循环线程 + 故障注入）全部断言通过，输出见 e2e-output.txt，覆盖题目指定三场景：受理后断队列、建完索引就退出、旧连接未退出。

另手动验证：
- 全新库启动（create_all + 迁移 + seed）与旧版表结构（name UNIQUE、无新列）升级均成功，升级后含 acceptance_no/worker_id/lease_owner/lease_until 四列且旧行回填受理号。
- 全部后端文件 `py_compile` 通过；改动的三个前端 ES module `node --check` 通过。

## 已知限制
1. MySQL/Redis 真机行为未在本机验证（无 Docker），建议执行 `docker compose up -d --build` 后按 e2e_demo 步骤手工冒烟一次。
2. 租约 60 秒为固定值（`LEASE_SECONDS`）；心跳在每个查询批次间触发，正常 5 次×3 阶段的试验不会超时，极端慢查询下若超过 60 秒无心跳且连接仍持锁，接管会被正确跳过但恢复要等锁释放。
3. SQLite 分支仅为测试存在，生产镜像仍为 MySQL，方言分支不影响部署路径。
