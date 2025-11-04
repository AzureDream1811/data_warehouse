-- TRUNCATE TABLE official; -- nếu muốn xóa dữ liệu cũ trước khi nhập
use staging;
INSERT INTO official
(FullDate, Weekday, `Date`,
 Temperature, UVValue, WindDirection, WindSpeed,
 Humidity, DewPoint, Pressure, Cloud,
 Visibility, CloudCeiling)
SELECT
    -- Chuyển đổi FullDate thành DATETIME, hỗ trợ cả 'YYYY-MM-DD HH:MM:SS'
    COALESCE(
            STR_TO_DATE(NULLIF(FullDate, ''), '%Y-%m-%d %H:%i:%s'),
            STR_TO_DATE(NULLIF(FullDate, ''), '%Y-%m-%d'),
            STR_TO_DATE(NULLIF(FullDate, ''), '%d/%m/%Y %H:%i:%s'),
            STR_TO_DATE(NULLIF(FullDate, ''), '%d/%m/%Y')
    )                                                                                                 AS FullDate_dt,

    NULLIF(Weekday, '')                                                                               AS Weekday,
    NULLIF(`Date`, '')                                                                                AS `Date`,

    CAST(REGEXP_SUBSTR(NULLIF(Temperature, ''), '-?[0-9]+(\\.[0-9]+)?') AS DECIMAL(5, 2))             AS Temperature,
    CAST(REGEXP_SUBSTR(NULLIF(UVValue, ''), '-?[0-9]+(\\.[0-9]+)?') AS DECIMAL(4, 2))                 AS UVValue,

    IF(TRIM(SUBSTRING_INDEX(NULLIF(Wind, ''), ' ', 1)) REGEXP '^[A-Za-z]+',
       TRIM(SUBSTRING_INDEX(NULLIF(Wind, ''), ' ', 1)), REGEXP_SUBSTR(NULLIF(Wind, ''), '[A-Za-z]+')) AS WindDirection,
    CAST(REGEXP_SUBSTR(NULLIF(Wind, ''), '[0-9]+(\\.[0-9]+)?') AS DECIMAL(5, 2))                      AS WindSpeed,

    CAST(REGEXP_SUBSTR(NULLIF(Humidity, ''), '-?[0-9]+(\\.[0-9]+)?') AS DECIMAL(5, 2))                AS Humidity,
    CAST(REGEXP_SUBSTR(NULLIF(DewPoint, ''), '-?[0-9]+(\\.[0-9]+)?') AS DECIMAL(5, 2))                AS DewPoint,
    CAST(REGEXP_SUBSTR(NULLIF(Pressure, ''), '-?[0-9]+(\\.[0-9]+)?') AS DECIMAL(6, 2))                AS Pressure,
    CAST(REGEXP_SUBSTR(NULLIF(Cloud, ''), '-?[0-9]+(\\.[0-9]+)?') AS DECIMAL(5, 2))                   AS Cloud,
    CAST(REGEXP_SUBSTR(NULLIF(Visibility, ''), '-?[0-9]+(\\.[0-9]+)?') AS DECIMAL(5, 2))              AS Visibility,
    CAST(REGEXP_SUBSTR(NULLIF(CloudCeiling, ''), '[0-9]+') AS SIGNED)                                 AS CloudCeiling
FROM temp
WHERE COALESCE(
              STR_TO_DATE(NULLIF(FullDate, ''), '%Y-%m-%d %H:%i:%s'),
              STR_TO_DATE(NULLIF(FullDate, ''), '%Y-%m-%d'),
              STR_TO_DATE(NULLIF(FullDate, ''), '%d/%m/%Y %H:%i:%s'),
              STR_TO_DATE(NULLIF(FullDate, ''), '%d/%m/%Y')
      ) IS NOT NULL;
