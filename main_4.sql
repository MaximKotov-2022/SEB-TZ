-- 1.	Вывести ТОП 5 самых конверсионных источника / канала
-- https://console.cloud.google.com/bigquery?sq=481397961595:a4742fef0f7b40b58c34db2239988ec1
SELECT trafficSource.source, trafficSource.medium, COUNT(*) AS visits,
ROUND(SUM(totals.transactions)/COUNT(*)*100,2) AS conversion_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
WHERE date BETWEEN '20170501' AND '20170801'
GROUP BY trafficSource.source, trafficSource.medium
ORDER BY conversion_rate DESC
LIMIT 5


