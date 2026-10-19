-- Request volume per region with user and item reach.
SELECT region,
       COUNT(*)                AS requests,
       COUNT(DISTINCT user_id) AS distinct_users,
       COUNT(DISTINCT item_id) AS distinct_items
FROM requests
GROUP BY region
ORDER BY requests DESC, region
