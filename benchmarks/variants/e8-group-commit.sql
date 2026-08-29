-- E8 — group commit. commit_delay makes a committing backend pause briefly so
-- other concurrent commits can join the same WAL flush, turning N fsyncs into
-- one. It only helps when enough transactions are in flight to batch, which is
-- what commit_siblings gates on.
--
-- This profile is a plausible fit: 100 concurrent writers means the siblings
-- threshold is always met. It is also a plausible LOSS — commit_delay adds
-- latency to every commit, and if the flush was not the bottleneck the delay is
-- pure cost. Which one it is, is the measurement.
--
-- Meaningless on tmpfs, where a "flush" is a memcpy. Disk arm only.
ALTER DATABASE kine SET commit_delay = 1000;
ALTER DATABASE kine SET commit_siblings = 5;
