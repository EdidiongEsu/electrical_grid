---------------------- Transformer Demand Map-------------------------
SELECT
  transformer_id,
  area_name,
  latitude,
  longitude,
  AVG(total_load_kw) AS avg_total_load_kw,
  AVG(load_pct) AS avg_load_pct
FROM
  electrical_grid.`03_gold`.gold_area_demand
GROUP BY
  transformer_id,
  area_name,
  latitude,
  longitude
ORDER BY
  avg_total_load_kw DESC


---------------------------- Outage Frequency by Area ------------------
SELECT
  area_name,
  service_band,
  COUNT(*) AS outage_count,
  AVG(duration_minutes) AS avg_duration_minutes,
  SUM(duration_minutes) AS total_downtime_minutes
FROM
  electrical_grid.`03_gold`.gold_outage_log
GROUP BY
  area_name,
  service_band
ORDER BY
  outage_count DESC



----------------------- Average Outage Duration by Area-------------
SELECT
  area_name,
  service_band,
  AVG(duration_minutes) AS avg_duration_minutes,
  MAX(duration_minutes) AS max_duration_minutes,
  COUNT(*) AS outage_count
FROM
  electrical_grid.`03_gold`.gold_outage_log
GROUP BY
  area_name,
  service_band
ORDER BY
  avg_duration_minutes DESC


--------------------- Total Energy Consumed --------------------
SELECT
  SUM(total_kwh) AS total_kwh_consumed
FROM
  gold_billing_by_area



------------------- Energy Consumption by Meter --------------------
SELECT
  area_name,
  SUM(total_kwh) AS total_kwh,
  SUM(total_cost_naira) AS total_cost_naira
FROM
  gold_billing_by_area
GROUP BY
  area_name
ORDER BY
  total_cost_naira DESC