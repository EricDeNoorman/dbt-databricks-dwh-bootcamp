WITH weerdata_transformed AS (
    SELECT 
        to_date(cast(yyyymmdd as string), 'yyyyMMdd') AS datum,
        tx / 10.0 AS max_temperatuur,
        tn / 10.0 AS min_temperatuur,
        tg / 10.0 AS gemiddelde_temperatuur,
        rh / 10.0 AS neerslag,
        fg / 10.0 AS gemiddelde_windsnelheid
    FROM 
        {{ source('weerdata', 'weerdata_gilze_rijen') }}
)

SELECT 
    datum,
    max_temperatuur,
    min_temperatuur,
    gemiddelde_temperatuur,
    neerslag,
    gemiddelde_windsnelheid
FROM 
    weerdata_transformed