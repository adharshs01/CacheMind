-- Request volume and share per item category ("unknown" = missing/invalid metadata).
SELECT category,
       COUNT(*)                                          AS requests,
       COUNT(DISTINCT item_id)                           AS distinct_items,
       ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 6)        AS share
FROM requests
GROUP BY category
ORDER BY requests DESC, category
