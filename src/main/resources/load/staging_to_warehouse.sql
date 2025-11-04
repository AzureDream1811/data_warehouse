-- 1) Làm sạch Fact theo kỳ cần refresh (ví dụ theo ngày)
DELETE
FROM warehouse.FactWeather
WHERE TimeKey IN (SELECT TimeKey
                  FROM warehouse.DimTime
                  WHERE DateOnly BETWEEN CURDATE() - INTERVAL 7 DAY AND CURDATE());

-- DimTime
INSERT INTO warehouse.DimTime
(FullDateTime, DateOnly, Year, Quarter, Month, Day, Weekday, Hour, Minute)
SELECT DISTINCT o.FullDate,
                DATE(o.FullDate),
                YEAR(o.FullDate),
                QUARTER(o.FullDate),
                MONTH(o.FullDate),
                DAY(o.FullDate),
                NULLIF(o.Weekday, ''),
                HOUR(o.FullDate),
                MINUTE(o.FullDate)
FROM staging.official o
WHERE o.FullDate IS NOT NULL
ON DUPLICATE KEY UPDATE
                     -- có thể cập nhật thêm các field derived nếu muốn đồng bộ
                     Weekday  = VALUES(Weekday),
                     DateOnly = VALUES(DateOnly),
                     Year     = VALUES(Year),
                     Quarter  = VALUES(Quarter),
                     Month    = VALUES(Month),
                     Day      = VALUES(Day),
                     Hour     = VALUES(Hour),
                     Minute   = VALUES(Minute);

-- DimWind
INSERT INTO warehouse.DimWind (WindDirection, WindSpeed)
SELECT DISTINCT o.WindDirection,
                o.WindSpeed
FROM staging.official o
WHERE o.WindDirection IS NOT NULL
  AND o.WindSpeed IS NOT NULL
ON DUPLICATE KEY UPDATE WindSpeed = VALUES(WindSpeed);

-- DimUV
INSERT INTO warehouse.DimUV (UVValue)
SELECT DISTINCT o.UVValue
FROM staging.official o
WHERE o.UVValue IS NOT NULL
ON DUPLICATE KEY UPDATE UVValue = VALUES(UVValue);

-- DimCloud
INSERT INTO warehouse.DimCloud (CloudCover, CloudCeiling)
SELECT DISTINCT o.Cloud,
                o.CloudCeiling
FROM staging.official o
WHERE o.Cloud IS NOT NULL
ON DUPLICATE KEY UPDATE CloudCover   = VALUES(CloudCover),
                        CloudCeiling = VALUES(CloudCeiling);


-- 3) Nạp lại FACT cho cửa sổ đã xóa
INSERT INTO warehouse.FactWeather (TimeKey, WindKey, UVKey, CloudKey,
                                   Temperature, Humidity, DewPoint, Pressure, Visibility)
SELECT dt.TimeKey,
       dw.WindKey,
       du.UVKey,
       dc.CloudKey,
       o.Temperature,
       o.Humidity,
       o.DewPoint,
       o.Pressure,
       o.Visibility
FROM staging.official o
         JOIN warehouse.DimTime dt ON dt.FullDateTime = o.FullDate
         LEFT JOIN warehouse.DimWind dw ON dw.WindDirection = o.WindDirection AND dw.WindSpeed = o.WindSpeed
         LEFT JOIN warehouse.DimUV du ON du.UVValue = o.UVValue
         LEFT JOIN warehouse.DimCloud dc ON dc.CloudCover = o.Cloud
    AND ((dc.CloudCeiling IS NULL AND o.CloudCeiling IS NULL) OR dc.CloudCeiling = o.CloudCeiling);
