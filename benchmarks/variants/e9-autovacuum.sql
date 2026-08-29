-- E9 — aggressive autovacuum on the kine table.
--
-- kine's compaction DELETEs superseded revisions, and every DELETE leaves a
-- dead tuple that autovacuum must reclaim. On default settings a table only
-- gets vacuumed after 20% of it is dead, which on a high-churn table means
-- scans drag through bloat between passes.
--
-- Applied per-table rather than server-wide, so it is scoped to the table with
-- the churn and reverts with the table.
ALTER TABLE kine SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_vacuum_cost_limit = 2000,
  autovacuum_vacuum_cost_delay = 2
);
