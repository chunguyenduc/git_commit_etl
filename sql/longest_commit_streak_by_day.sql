WITH ranked_commit_by_date AS (
  SELECT
    committer_id,
    commit_ts::DATE AS commit_date,
    ROW_NUMBER() OVER (PARTITION BY committer_id ORDER BY commit_ts::DATE) AS rn
  FROM commit_staging
),
commit_streak AS (
  SELECT
    committer_id,
    commit_date,
    rn,
    commit_date - INTERVAL '1 day' * rn AS streak_group
  FROM ranked_commit_by_date
),
commit_streak_length AS (
  SELECT
    committer_id,
    MIN(commit_date) AS streak_start,
    MAX(commit_date) AS streak_end,
    COUNT(*) AS streak_length
  FROM commit_streak
  GROUP BY committer_id, streak_group
),
max_commit_streak AS (
  SELECT
    committer_id,
    MAX(streak_length) AS max_streak_length
  FROM commit_streak_length
  GROUP BY committer_id
)
SELECT
  committer_id,
  max_streak_length
FROM max_commit_streak
ORDER BY max_streak_length DESC
LIMIT 1;