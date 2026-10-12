-- NepAirWatch — example HiveQL queries (the dashboard's "Hive SQL" tab lists every "-- name:" block).
-- Run from a shell:  docker compose exec hive beeline -u jdbc:hive2://localhost:10000/nepairwatch -f /hive/queries.hql

-- name: Worst 10 city-days of the past year (PM2.5, CAMS)
SELECT city, `date`, ROUND(AVG(pm2_5), 1) AS pm25_24h, MAX(aqi) AS aqi_max
FROM nepairwatch.air_quality
WHERE source NOT LIKE 'demo%'
GROUP BY city, `date`
ORDER BY pm25_24h DESC
LIMIT 10;

-- name: Monthly exceedance of the Nepal 24-h standard (40 µg/m³) per province
SELECT province, SUBSTR(CAST(`date` AS STRING), 1, 7) AS month,
       SUM(CASE WHEN pm25_24h > 40 THEN 1 ELSE 0 END) AS city_days_over_std, COUNT(*) AS city_days
FROM (SELECT province, city, `date`, AVG(pm2_5) AS pm25_24h
      FROM nepairwatch.air_quality WHERE source NOT LIKE 'demo%' GROUP BY province, city, `date`) d
GROUP BY province, SUBSTR(CAST(`date` AS STRING), 1, 7)
ORDER BY month, province;

-- name: Ground stations ranked by mean measured PM2.5 (last 7 days)
SELECT network, station, city, COUNT(*) AS minutes, ROUND(AVG(pm2_5), 1) AS pm25_mean,
       ROUND(PERCENTILE_APPROX(pm2_5, 0.95), 1) AS pm25_p95
FROM nepairwatch.ground
WHERE `date` >= DATE_SUB(CURRENT_DATE, 7) AND pm2_5 IS NOT NULL
GROUP BY network, station, city
ORDER BY pm25_mean DESC;

-- name: Fire hotspots per day within 100 km of a monitored city
SELECT acq_date, nearest_city, COUNT(*) AS hotspots, ROUND(SUM(frp), 0) AS total_frp_mw
FROM nepairwatch.fires
WHERE city_km <= 100
GROUP BY acq_date, nearest_city
ORDER BY acq_date DESC, hotspots DESC
LIMIT 50;

-- name: 25-year aerosol trend from the MapReduce job (AOD by city and year)
SELECT city, yr, aod_mean, hazy_days, very_hazy_days
FROM nepairwatch.aod_yearly_mr
WHERE city IN ('Kathmandu', 'Biratnagar', 'Nepalgunj', 'Pokhara')
ORDER BY city, yr;

-- name: Hazy-season climatology (NASA POWER): mean AOD by calendar month, all cities
SELECT SUBSTR(`date`, 6, 2) AS month, ROUND(AVG(aod_55), 3) AS aod_mean,
       ROUND(AVG(rh2m), 1) AS rh_mean, ROUND(AVG(precip), 2) AS precip_mm_day
FROM nepairwatch.nasa_power
GROUP BY SUBSTR(`date`, 6, 2)
ORDER BY month;

-- name: Raw Flume-ingested hotspots by satellite and confidence
SELECT satellite, confidence, COUNT(*) AS hotspots
FROM nepairwatch.fires_raw
GROUP BY satellite, confidence
ORDER BY hotspots DESC;
