-- E1 — emit a revision notification on every insert.
--
-- Pairs with the LISTEN side in the patched kine
-- (pkg/drivers/pgsql/notify.go, branch e1-listen-notify). kine already wakes
-- its own poll loop in-process on every insert, so this changes nothing for a
-- single instance. It exists for the multi-instance case, where the in-process
-- signal cannot reach the other kine and its watchers wait for a 1 s ticker.
--
-- THE PAYLOAD IS THE REVISION ID AND NOTHING ELSE. NOTIFY payloads are capped
-- at 8000 bytes; a Kubernetes object does not fit and must never be sent this
-- way. The listener treats the payload as a hint to poll, not as data.
--
-- Notifications are queued during the transaction and delivered on COMMIT, so
-- a rolled-back insert notifies nobody. They are NOT durable: a listener that
-- is disconnected when the NOTIFY fires never learns about it and cannot
-- replay. This is why kine keeps its periodic poll — see the comment on the
-- external channel in sqllog/sql.go.
--
-- Cost: one trigger invocation per inserted row on the write path. That is the
-- trade this experiment measures.

CREATE OR REPLACE FUNCTION kine_notify_revision() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('kine_revision', NEW.id::text);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS kine_notify_revision_trigger ON kine;
CREATE TRIGGER kine_notify_revision_trigger
  AFTER INSERT ON kine
  FOR EACH ROW EXECUTE FUNCTION kine_notify_revision();
