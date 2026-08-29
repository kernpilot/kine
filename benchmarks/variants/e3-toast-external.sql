-- E3 — stop trying to compress payloads that do not compress.
--
-- Postgres stores a bytea wider than ~2 KB out of line in TOAST, and by
-- default (EXTENDED) it first attempts LZ compression. Kubernetes object
-- payloads are protobuf, which is already densely packed, so that attempt
-- usually fails after spending the CPU. SET STORAGE EXTERNAL keeps the
-- out-of-line storage and skips the compression attempt.
--
-- The trade is real and must be measured, not assumed: whatever compression
-- did succeed is now given up, so the table grows. Watch the .sizes.txt for
-- this run against the baseline before treating any CPU win as free.

ALTER TABLE kine ALTER COLUMN value SET STORAGE EXTERNAL;
ALTER TABLE kine ALTER COLUMN old_value SET STORAGE EXTERNAL;
