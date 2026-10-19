-- Top requested items with their category and audience size.
SELECT item_id,
       category,
       COUNT(*)                AS requests,
       COUNT(DISTINCT user_id) AS distinct_users,
       SUM(item_size_bytes)    AS bytes_requested
FROM requests
GROUP BY item_id, category
ORDER BY requests DESC, item_id
LIMIT 20
