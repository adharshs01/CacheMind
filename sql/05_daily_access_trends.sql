-- Daily totals with day-over-day change.
WITH daily AS (
  SELECT event_date,
         COUNT(*)                AS requests,
         COUNT(DISTINCT user_id) AS distinct_users,
         COUNT(DISTINCT item_id) AS distinct_items
  FROM requests
  GROUP BY event_date
)
SELECT event_date,
       requests,
       distinct_users,
       distinct_items,
       LAG(requests) OVER (ORDER BY event_date)                                AS prev_day_requests,
       ROUND((requests - LAG(requests) OVER (ORDER BY event_date))
             / LAG(requests) OVER (ORDER BY event_date), 6)                    AS pct_change
FROM daily
ORDER BY event_date
