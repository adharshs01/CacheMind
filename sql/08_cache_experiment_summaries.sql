-- Cache experiment summary: mean and spread of hit rate per policy and capacity.
SELECT experiment,
       workload,
       policy,
       capacity,
       COUNT(*)                     AS runs,
       ROUND(AVG(hit_rate), 6)      AS mean_hit_rate,
       ROUND(STDDEV_SAMP(hit_rate), 6) AS std_hit_rate,
       ROUND(MIN(hit_rate), 6)      AS min_hit_rate,
       ROUND(MAX(hit_rate), 6)      AS max_hit_rate,
       ROUND(AVG(wall_time_s), 4)   AS mean_wall_time_s
FROM experiments
GROUP BY experiment, workload, policy, capacity
ORDER BY experiment, workload, capacity, mean_hit_rate DESC
