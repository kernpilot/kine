-- E2 — covering index, and drop the provably redundant ones.
--
-- kine creates six indexes (pkg/drivers/pgsql/pgsql.go:50-55) and every INSERT
-- maintains all six. Two of them earn nothing:
--
--   kine_name_index (name)        is a strict prefix of kine_name_id_index
--                                 (name, id), so any plan that can use it can
--                                 use the wider one.
--   kine_id_deleted_index (id, …) leads on id, which is already the BIGSERIAL
--                                 PRIMARY KEY — Postgres builds a unique btree
--                                 for that, and the poll query's `WHERE id > ?
--                                 ORDER BY id ASC` is exactly what it serves.
--
-- kine_name_prev_revision_uindex is NOT touched. It is UNIQUE and enforces
-- correctness, not speed: it is what makes a duplicated create collide instead
-- of silently forking a key's history. Dropping it would trade a benchmark
-- number for data corruption.
--
-- The list index gains INCLUDE columns so the hot list path can take an
-- Index-Only Scan instead of a heap fetch per row. INCLUDE payload columns are
-- stored only in leaf pages and are not part of the key, so they do not widen
-- the search path.

DROP INDEX IF EXISTS kine_name_index;
DROP INDEX IF EXISTS kine_id_deleted_index;

DROP INDEX IF EXISTS kine_list_query_index;
CREATE INDEX kine_list_query_index ON kine (name, id DESC)
  INCLUDE (deleted, created, create_revision, prev_revision, lease);
