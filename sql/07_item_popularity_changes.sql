-- Items whose popularity rank changed most between the first and second half of the trace.
WITH bounds AS (
  SELECT MIN(event_time_ms) AS lo, MAX(event_time_ms) AS hi FROM requests
),
halves AS (
  SELECT r.item_id,
         CASE WHEN r.event_time_ms < b.lo + (b.hi - b.lo) / 2 THEN 'first' ELSE 'second' END AS half
  FROM requests r CROSS JOIN bounds b          -- bounds has exactly one row
),
counts AS (
  SELECT item_id,
         SUM(CASE WHEN half = 'first'  THEN 1 ELSE 0 END) AS first_half,
         SUM(CASE WHEN half = 'second' THEN 1 ELSE 0 END) AS second_half
  FROM halves
  GROUP BY item_id
),
ranked AS (
  SELECT item_id, first_half, second_half,
         RANK() OVER (ORDER BY first_half DESC)  AS rank_first,
         RANK() OVER (ORDER BY second_half DESC) AS rank_second
  FROM counts
)
SELECT item_id, first_half, second_half, rank_first, rank_second,
       rank_first - rank_second AS rank_gain
FROM ranked
WHERE first_half + second_half >= 5
ORDER BY rank_gain DESC, second_half DESC, item_id
LIMIT 20
