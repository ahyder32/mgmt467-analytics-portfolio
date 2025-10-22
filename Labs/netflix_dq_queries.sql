
-- DQ Query 5.1 (Missingness - Users: Total and % missing)
-- Cell ID: a5287b24
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(region IS NULL) AS missing_region,
  ROUND(COUNTIF(region IS NULL) * 100.0 / COUNT(*), 2) AS pct_missing_region,
  COUNTIF(plan_tier IS NULL) AS missing_plan_tier,
  ROUND(COUNTIF(plan_tier IS NULL) * 100.0 / COUNT(*), 2) AS pct_missing_plan_tier,
  COUNTIF(age_band IS NULL) AS missing_age_band,
  ROUND(COUNTIF(age_band IS NULL) * 100.0 / COUNT(*), 2) AS pct_missing_age_band
FROM
  `${GOOGLE_CLOUD_PROJECT}.netflix.users`;

-- DQ Query 5.1 (Missingness - Users: % plan_tier missing by region)
-- Cell ID: 595a13ba
SELECT
  region,
  COUNT(*) AS total_rows,
  COUNTIF(plan_tier IS NULL) AS missing_plan_tier,
  ROUND(COUNTIF(plan_tier IS NULL) * 100.0 / COUNT(*), 2) AS pct_missing_plan_tier
FROM
  `${GOOGLE_CLOUD_PROJECT}.netflix.users`
GROUP BY
  region
ORDER BY
  pct_missing_plan_tier DESC;

-- DQ Query 5.2 (Duplicates - Watch History: Report duplicate groups)
-- Cell ID: rGGCsOQRXlb1
SELECT user_id, movie_id, device_type, COUNT(*) AS dup_count
FROM `mgmt-467-47888-471119.netflix.watch_history`
GROUP BY user_id, movie_id, device_type
HAVING dup_count > 1
ORDER BY dup_count DESC
LIMIT 20;

-- DQ Query 5.2 (Duplicates - Watch History: Create dedup table)
-- Cell ID: 1g-t2JCgXlb5
CREATE OR REPLACE TABLE `mgmt-467-47888-471119.netflix.watch_history_dedup` AS
SELECT * EXCEPT(rk) FROM (
  SELECT h.*,
         ROW_NUMBER() OVER (
           PARTITION BY user_id, movie_id, device_type
           ORDER BY progress_percentage DESC, watch_duration_minutes DESC
         ) AS rk
  FROM `mgmt-467-47888-471119.netflix.watch_history` h
)
WHERE rk = 1;

-- DQ Query 5.2 (Duplicates - Watch History: Before/After count)
-- Cell ID: noUDlRksoyn4
SELECT 'watch_history_raw' AS table_name, COUNT(*) AS row_count
FROM `mgmt-467-47888-471119.netflix.watch_history`
UNION ALL
SELECT 'watch_history_dedup' AS table_name, COUNT(*) AS row_count
FROM `mgmt-467-47888-471119.netflix.watch_history_dedup`;

-- DQ Query 5.3 (Outliers - Watch History: IQR Bounds and % outliers)
-- Cell ID: p5nCiXS_Xlb5
WITH dist AS (
  SELECT
    APPROX_QUANTILES(watch_duration_minutes, 4)[OFFSET(1)] AS q1,
    APPROX_QUANTILES(watch_duration_minutes, 4)[OFFSET(3)] AS q3
  FROM `mgmt-467-47888-471119.netflix.watch_history_dedup`
),
bounds AS (
  SELECT q1, q3, (q3-q1) AS iqr,
         q1 - 1.5*(q3-q1) AS lo,
         q3 + 1.5*(q3-q1) AS hi
  FROM dist
)
SELECT
  COUNTIF(h.watch_duration_minutes < b.lo OR h.watch_duration_minutes > b.hi) AS outliers,
  COUNT(*) AS total,
  ROUND(100*COUNTIF(h.watch_duration_minutes < b.lo OR h.watch_duration_minutes > b.hi)/COUNT(*),2) AS pct_outliers
FROM `mgmt-467-47888-471119.netflix.watch_history_dedup` h
CROSS JOIN bounds b;

-- DQ Query 5.3 (Outliers - Watch History: Create robust table with capping)
-- Cell ID: SD2Ouu6YXlb5
CREATE OR REPLACE TABLE `mgmt-467-47888-471119.netflix.watch_history_robust` AS
WITH q AS (
  SELECT
    APPROX_QUANTILES(watch_duration_minutes, 100)[OFFSET(1)]  AS p01,
    APPROX_QUANTILES(watch_duration_minutes, 100)[OFFSET(98)] AS p99
  FROM `mgmt-467-47888-471119.netflix.watch_history_dedup`
)
SELECT
  h.*,
  GREATEST(q.p01, LEAST(q.p99, h.watch_duration_minutes)) AS minutes_watched_capped
FROM `mgmt-467-47888-471119.netflix.watch_history_dedup` h, q;

-- DQ Query 5.3 (Outliers - Watch History: Min/Median/Max before vs after capping)
-- Cell ID: bZQ17560pWzj
WITH before AS (
  SELECT
    'before' AS which,
    MIN(watch_duration_minutes) AS min_watched,
    APPROX_QUANTILES(watch_duration_minutes, 2)[OFFSET(1)] AS median_watched,
    MAX(watch_duration_minutes) AS max_watched
  FROM `mgmt-467-47888-471119.netflix.watch_history_dedup`
),
after AS (
  SELECT
    'after' AS which,
    MIN(minutes_watched_capped) AS min_watched,
    APPROX_QUANTILES(minutes_watched_capped, 2)[OFFSET(1)] AS median_watched,
    MAX(minutes_watched_capped) AS max_watched
  FROM `mgmt-467-47888-471119.netflix.watch_history_robust`
)
SELECT * FROM before
UNION ALL
SELECT * FROM after;

-- DQ Query 5.4 (Business Anomaly Flags - Watch History: Binge sessions)
-- Cell ID: cCcstMjjXlb6
SELECT
  COUNTIF(watch_duration_minutes > 8*60) AS sessions_over_8h,
  COUNT(*) AS total,
  ROUND(SAFE_DIVIDE(100*COUNTIF(watch_duration_minutes > 8*60), COUNT(*)),2) AS pct_flag_binge
FROM `mgmt-467-47888-471119.netflix.watch_history_robust`;

-- DQ Query 5.4 (Business Anomaly Flags - Users: Extreme age)
-- Cell ID: tppGlKeDXlb6
SELECT
  COUNTIF(SAFE_CAST(REGEXP_EXTRACT(CAST(age AS STRING), r'\d+') AS INT64) < 10 OR
          SAFE_CAST(REGEXP_EXTRACT(CAST(age AS STRING), r'\d+') AS INT64) > 100) AS extreme_age_rows,
  COUNT(*) AS total,
  ROUND(SAFE_DIVIDE(100*COUNTIF(SAFE_CAST(REGEXP_EXTRACT(CAST(age AS STRING), r'\d+') AS INT64) < 10 OR
                                SAFE_CAST(REGEXP_EXTRACT(CAST(age AS STRING), r'\d+') AS INT64) > 100), COUNT(*)),2) AS pct_flag_age_extreme
FROM `mgmt-467-47888-471119.netflix.users`;

-- DQ Query 5.4 (Business Anomaly Flags - Movies: Duration anomaly)
-- Cell ID: NxBN2oFDXlb6
SELECT
  COUNTIF(duration_minutes < 15 OR duration_minutes > 8*60) AS duration_anomaly_titles,
  COUNT(*) AS total,
  ROUND(SAFE_DIVIDE(100*COUNTIF(duration_minutes < 15 OR duration_minutes > 8*60), COUNT(*)),2) AS pct_flag_duration_anomaly
FROM `mgmt-467-47888-471119.netflix.movies`;

-- DQ Query 5.4 (Business Anomaly Flags - Summary)
-- Cell ID: PfNlsPWPp5DK
SELECT
  'flag_binge' AS flag_name,
  ROUND(SAFE_DIVIDE(100*COUNTIF(watch_duration_minutes > 8*60), COUNT(*)),2) AS pct_of_rows
FROM `mgmt-467-47888-471119.netflix.watch_history_robust`
UNION ALL
SELECT
  'flag_age_extreme' AS flag_name,
  ROUND(SAFE_DIVIDE(100*COUNTIF(SAFE_CAST(REGEXP_EXTRACT(CAST(age AS STRING), r'\d+') AS INT64) < 10 OR
                                SAFE_CAST(REGEXP_EXTRACT(CAST(age AS STRING), r'\d+') AS INT64) > 100), COUNT(*)),2) AS pct_of_rows
FROM `mgmt-467-47888-471119.netflix.users`
UNION ALL
SELECT
  'flag_duration_anomaly' AS flag_name,
  ROUND(SAFE_DIVIDE(100*COUNTIF(duration_minutes < 15 OR duration_minutes > 8*60), COUNT(*)),2) AS pct_of_rows
FROM `mgmt-467-47888-471119.netflix.movies`;
