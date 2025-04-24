create TABLE commit_staging (
    sha TEXT,
    committer_id TEXT NOT NULL,
    committer_username TEXT NOT NULL,
    committer_name TEXT NOT NULL,
    committer_email TEXT NOT NULL,
    commit_ts TIMESTAMPTZ NOT NULL,
    pipeline_run_date TEXT NOT NULL,
    PRIMARY KEY (sha, commit_ts)
) PARTITION BY RANGE (commit_ts);

-- Create partitions for each month (e.g., 2025)
CREATE TABLE commit_ts_2024_07 PARTITION OF commit_staging
    FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');

CREATE TABLE commit_ts_2024_08 PARTITION OF commit_staging
    FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');

CREATE TABLE commit_ts_2024_09 PARTITION OF commit_staging
    FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');

CREATE TABLE commit_ts_2024_10 PARTITION OF commit_staging
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

CREATE TABLE commit_ts_2024_11 PARTITION OF commit_staging
    FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');

CREATE TABLE commit_ts_2024_12 PARTITION OF commit_staging
    FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

CREATE TABLE commit_ts_2025_01 PARTITION OF commit_staging
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE commit_ts_2025_02 PARTITION OF commit_staging
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

CREATE TABLE commit_ts_2025_03 PARTITION OF commit_staging
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');

CREATE TABLE commit_ts_2025_04 PARTITION OF commit_staging
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
