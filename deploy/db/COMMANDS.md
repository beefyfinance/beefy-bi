```
erc20_transfer_ts
vault_ppfs_ts
oracle_price_ts

-- check compression
select pg_size_pretty(hypertable_size('beefy_raw.'));
SELECT * FROM hypertable_compression_stats('beefy_raw.');

-- flex your compression
SELECT pg_size_pretty(before_compression_total_bytes) as "before compression", pg_size_pretty(after_compression_total_bytes) as "after compression" FROM hypertable_compression_stats('beefy_raw.');

-- find a compression job to run
SELECT * FROM timescaledb_information.jobs;
SELECT * FROM timescaledb_information.job_stats;

-- run a compression job
CALL run_job(1000);
```
