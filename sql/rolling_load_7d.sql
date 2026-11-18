-- 7-day rolling average daily load per country (window frame over the preceding 6 days).
WITH rolling AS (
    SELECT
        country,
        date,
        load_mwh,
        AVG(load_mwh) OVER (
            PARTITION BY country ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS load_mwh_7d_avg,
        MAX(date) OVER (PARTITION BY country) AS last_date
    FROM energy_daily
    WHERE load_mwh IS NOT NULL
)
SELECT country, date, ROUND(load_mwh::numeric, 0) AS load_mwh, ROUND(load_mwh_7d_avg::numeric, 0) AS load_mwh_7d_avg
FROM rolling
WHERE date > last_date - INTERVAL '14 days'
ORDER BY country, date;
