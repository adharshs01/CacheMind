-- How each user group spreads its requests over categories.
SELECT user_group,
       category,
       COUNT(*)                                                         AS requests,
       ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY user_group), 6) AS share_of_group
FROM requests
GROUP BY user_group, category
ORDER BY user_group, requests DESC, category
