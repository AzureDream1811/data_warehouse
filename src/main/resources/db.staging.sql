create table temp(
		Fulldate varchar(20),
        Weekday varchar(20),
        Temperature varchar(20),
        UVValue varchar(20),
        WindDirection varchar(20),
        WindSpeed varchar(20),
        Humidity varchar(20),
        DewPoint varchar(20),
		Pressure varchar(20),
		Cloud varchar(20),
		Visibility varchar(20),
		CloudCeiling varchar(20)
);

create table official (
    Fulldate datetime,
    Weekday varchar(20),
    Temperature decimal(4, 1),
    UVValue decimal(4, 2),
    WindDirection varchar(20),
    WindSpeed decimal(5, 2),
    Humidity decimal(4, 1),
    DewPoint DECIMAL(4, 1),
    Pressure DECIMAL(6, 2),
    Cloud DECIMAL(5, 2),
    Visibility DECIMAL(5, 2),
    CloudCeiling INT
);