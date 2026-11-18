-- Monthly renewable share per country, month-over-month change (LAG), and rank within each month.
WITH monthly AS (
    SELECT
        country,
        date_trunc('month', date)::date                                AS month,
        SUM(load_mwh)                                                  AS load_mwh,
        SUM(COALESCE(solar_mwh, 0) + COALESCE(wind_mwh, 0))            AS renewable_mwh
    FROM energy_daily
    GROUP BY country, date_trunc('month', date)
),
shares AS (
    SELECT country, month, renewable_mwh / NULLIF(load_mwh, 0) AS share
    FROM monthly
)
SELECT
    country,
    month,
    ROUND(share::numeric, 4)                                                          AS renewable_share,
    ROUND((share - LAG(share) OVER (PARTITION BY country ORDER BY month))::numeric, 4) AS mom_change,
    RANK() OVER (PARTITION BY month ORDER BY share DESC NULLS LAST)                   AS rank_in_month
FROM shares
WHERE month >= (SELECT date_trunc('month', MAX(date)) - INTERVAL '2 months' FROM energy_daily)
ORDER BY month DESC, rank_in_month;
