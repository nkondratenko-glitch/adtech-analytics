-- Homework 2: analytical SQL queries for ad campaign performance
-- Database: adtech
-- Shared 30-day window for all questions: 2024-10-01 to 2024-10-30 inclusive
-- Implemented as [start_date, end_date) = ['2024-10-01', '2024-10-31')

SET @start_date = '2024-10-01';
SET @end_date   = '2024-10-31';

-- Q1. Top 5 campaigns with the highest CTR over the 30-day period
SELECT
    c.campaign_id,
    c.campaign_name,
    COUNT(i.impression_id) AS impressions,
    COUNT(cl.click_id) AS clicks,
    ROUND(
        100.0 * COUNT(cl.click_id) / NULLIF(COUNT(i.impression_id), 0),
        2
    ) AS ctr_pct
FROM campaigns c
JOIN impressions i
    ON i.campaign_id = c.campaign_id
   AND i.impression_time >= @start_date
   AND i.impression_time <  @end_date
LEFT JOIN clicks cl
    ON cl.impression_id = i.impression_id
   AND cl.click_time >= @start_date
   AND cl.click_time <  @end_date
GROUP BY c.campaign_id, c.campaign_name
HAVING COUNT(i.impression_id) > 0
ORDER BY ctr_pct DESC, clicks DESC, impressions DESC
LIMIT 5;

-- Q2. Advertisers that spent the most on impressions in the last month
SELECT
    a.advertiser_id,
    a.advertiser_name,
    ROUND(SUM(i.impression_cost), 2) AS total_impression_spend,
    COUNT(i.impression_id) AS impressions,
    COUNT(cl.click_id) AS clicks,
    ROUND(
        100.0 * COUNT(cl.click_id) / NULLIF(COUNT(i.impression_id), 0),
        2
    ) AS ctr_pct
FROM advertisers a
JOIN campaigns c
    ON c.advertiser_id = a.advertiser_id
JOIN impressions i
    ON i.campaign_id = c.campaign_id
   AND i.impression_time >= @start_date
   AND i.impression_time <  @end_date
LEFT JOIN clicks cl
    ON cl.impression_id = i.impression_id
   AND cl.click_time >= @start_date
   AND cl.click_time <  @end_date
GROUP BY a.advertiser_id, a.advertiser_name
ORDER BY total_impression_spend DESC, clicks DESC;

-- Q3. Average CPC and CPM for each campaign
SELECT
    c.campaign_id,
    c.campaign_name,
    COUNT(i.impression_id) AS impressions,
    COUNT(cl.click_id) AS clicks,
    ROUND(COALESCE(SUM(i.impression_cost), 0), 2) AS impression_spend,
    ROUND(COALESCE(SUM(cl.cpc_amount), 0), 2) AS click_spend,
    ROUND(
        COALESCE(SUM(cl.cpc_amount), 0) / NULLIF(COUNT(cl.click_id), 0),
        4
    ) AS avg_cpc,
    ROUND(
        1000.0 * COALESCE(SUM(i.impression_cost), 0) / NULLIF(COUNT(i.impression_id), 0),
        4
    ) AS cpm
FROM campaigns c
LEFT JOIN impressions i
    ON i.campaign_id = c.campaign_id
   AND i.impression_time >= @start_date
   AND i.impression_time <  @end_date
LEFT JOIN clicks cl
    ON cl.impression_id = i.impression_id
   AND cl.click_time >= @start_date
   AND cl.click_time <  @end_date
GROUP BY c.campaign_id, c.campaign_name
ORDER BY c.campaign_id;

-- Q4. Top-performing locations based on total click revenue
SELECT
    l.location_id,
    l.location_name,
    ROUND(SUM(cl.cpc_amount), 2) AS total_click_revenue,
    COUNT(cl.click_id) AS total_clicks
FROM clicks cl
JOIN impressions i
    ON i.impression_id = cl.impression_id
JOIN users u
    ON u.user_id = i.user_id
JOIN locations l
    ON l.location_id = u.location_id
WHERE cl.click_time >= @start_date
  AND cl.click_time <  @end_date
GROUP BY l.location_id, l.location_name
ORDER BY total_click_revenue DESC, total_clicks DESC
LIMIT 10;

-- Q5. Top 10 users who clicked on the most ads
SELECT
    u.user_id,
    COUNT(cl.click_id) AS total_clicks,
    ROUND(SUM(cl.cpc_amount), 2) AS generated_click_revenue
FROM users u
JOIN impressions i
    ON i.user_id = u.user_id
JOIN clicks cl
    ON cl.impression_id = i.impression_id
WHERE cl.click_time >= @start_date
  AND cl.click_time <  @end_date
GROUP BY u.user_id
ORDER BY total_clicks DESC, generated_click_revenue DESC
LIMIT 10;

-- Q6. Campaigns that spent more than 80% of their total budget
SELECT
    c.campaign_id,
    c.campaign_name,
    c.budget,
    c.remaining_budget,
    ROUND(c.budget - c.remaining_budget, 2) AS spent_from_budget_column,
    ROUND(
        100.0 * (c.budget - c.remaining_budget) / NULLIF(c.budget, 0),
        2
    ) AS budget_used_pct,
    ROUND(COALESCE(SUM(i.impression_cost), 0), 2) AS realized_impression_spend,
    ROUND(COALESCE(SUM(cl.cpc_amount), 0), 2) AS realized_click_spend
FROM campaigns c
LEFT JOIN impressions i
    ON i.campaign_id = c.campaign_id
   AND i.impression_time >= @start_date
   AND i.impression_time <  @end_date
LEFT JOIN clicks cl
    ON cl.impression_id = i.impression_id
   AND cl.click_time >= @start_date
   AND cl.click_time <  @end_date
GROUP BY c.campaign_id, c.campaign_name, c.budget, c.remaining_budget
HAVING budget_used_pct > 80
ORDER BY budget_used_pct DESC, remaining_budget ASC;

-- Q7. CTR comparison by device type
-- Requires impressions.device_type to exist.
-- Uncomment and run only after adding the device_type column to Homework 1 schema.
--
-- SELECT
--     i.device_type,
--     COUNT(i.impression_id) AS impressions,
--     COUNT(cl.click_id) AS clicks,
--     ROUND(
--         100.0 * COUNT(cl.click_id) / NULLIF(COUNT(i.impression_id), 0),
--         2
--     ) AS ctr_pct
-- FROM impressions i
-- LEFT JOIN clicks cl
--     ON cl.impression_id = i.impression_id
--    AND cl.click_time >= @start_date
--    AND cl.click_time <  @end_date
-- WHERE i.impression_time >= @start_date
--   AND i.impression_time <  @end_date
-- GROUP BY i.device_type
-- ORDER BY ctr_pct DESC;
