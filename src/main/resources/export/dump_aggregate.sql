-- Export CSV bằng MySQL (nếu chạy trên server có quyền file)
SELECT * INTO OUTFILE '/D:/DW/Data/aggregate_weather_daily.csv'
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM warehouse.AggregateWeatherDaily;
