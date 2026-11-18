-- Top 3 solar generation days per country (ROW_NUMBER over a per-country window).
SELECT country, date, ROUND(solar_mwh::numeric, 0) AS solar_mwh, solar_rank
FROM (
    SELECT
        country,
        date,
        solar_mwh,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY solar_mwh DESC NULLS LAST) AS solar_rank
    FROM energy_daily
    WHERE solar_mwh IS NOT NULL
) ranked
WHERE solar_rank <= 3
ORDER BY country, solar_rank;
