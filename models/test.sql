WITH temperature_data AS (
    SELECT 
        to_date(cast(yyyymmdd as string), 'yyyyMMdd') AS datum,
        tx / 10.0 AS temperatuur
    FROM 
        {{ source('weerdata', 'weerdata_gilze_rijen') }}
),

heatwave_periods AS (
    SELECT 
        datum,
        temperatuur,
        CASE WHEN temperatuur >= 25 THEN 1 ELSE 0 END AS is_warm_day,
        CASE WHEN temperatuur > 30 THEN 1 ELSE 0 END AS is_hot_day
    FROM 
        temperature_data
),

heatwave_groups AS (
    SELECT 
        datum,
        temperatuur,
        SUM(is_warm_day) OVER (ORDER BY datum ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS warm_days_count,
        SUM(is_hot_day) OVER (ORDER BY datum ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS hot_days_count
    FROM 
        heatwave_periods
),

heatwaves AS (
    SELECT 
        datum,
        ROW_NUMBER() OVER (ORDER BY datum) - ROW_NUMBER() OVER (PARTITION BY warm_days_count >= 5 AND hot_days_count >= 3 ORDER BY datum) AS grp
    FROM 
        heatwave_groups
    WHERE 
        warm_days_count >= 5 AND hot_days_count >= 3
)

SELECT 
    MIN(heatwaves.datum) AS startdatum,
    MAX(heatwaves.datum) AS einddatum,
    COUNT(heatwaves.datum) AS aantal_dagen,
    STRING_AGG(CAST(temperatuur AS STRING), ', ') AS temperaturen
FROM 
    heatwaves
JOIN 
    temperature_data ON heatwaves.datum = temperature_data.datum
GROUP BY 
    grp
ORDER BY 
    startdatum;