-- Revert E7, restoring the default durability. Run as its own measured pass so
-- the revert is confirmed to restore baseline behaviour rather than assumed to.
ALTER DATABASE kine RESET synchronous_commit;
