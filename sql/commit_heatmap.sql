SELECT
  TO_CHAR(commit_ts, 'Dy') AS day_of_week,
  CASE
    WHEN EXTRACT(HOUR FROM commit_ts) BETWEEN 1 AND 3 THEN '01-03'
    WHEN EXTRACT(HOUR FROM commit_ts) BETWEEN 4 AND 6 THEN '04-06'
    WHEN EXTRACT(HOUR FROM commit_ts) BETWEEN 7 AND 9 THEN '07-09'
    WHEN EXTRACT(HOUR FROM commit_ts) BETWEEN 10 AND 12 THEN '10-12'
    WHEN EXTRACT(HOUR FROM commit_ts) BETWEEN 13 AND 15 THEN '13-15'
    WHEN EXTRACT(HOUR FROM commit_ts) BETWEEN 16 AND 18 THEN '16-18'
    WHEN EXTRACT(HOUR FROM commit_ts) BETWEEN 19 AND 21 THEN '19-21'
    WHEN EXTRACT(HOUR FROM commit_ts) >= 22 OR EXTRACT(HOUR FROM commit_ts) = 0 THEN '22-00'
  END AS time_block,
  COUNT(1) AS commit_count
FROM commit_staging
GROUP BY day_of_week, time_block
ORDER BY day_of_week, time_block;