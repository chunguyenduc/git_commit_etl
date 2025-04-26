SELECT
  committer_id,
  COUNT(*) AS commit_count
from commit_staging
GROUP BY
  committer_id
ORDER BY
  commit_count DESC
LIMIT 5;