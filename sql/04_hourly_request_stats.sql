-- Hourly request statistics (UTC hour buckets).
SELECT date_trunc('hour', event_time)   AS hour_start,
       COUNT(*)                         AS requests,
       COUNT(DISTINCT item_id)          AS distinct_items,
       COUNT(DISTINCT user_id)          AS distinct_users,
       ROUND(COUNT(*) / 60.0, 3)        AS avg_requests_per_minute
FROM requests
GROUP BY date_trunc('hour', event_time)
ORDER BY hour_start
