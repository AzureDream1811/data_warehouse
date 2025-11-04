package transaction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class Transaction {
    private static final String CSV_FILE = "weather_log.csv";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static class WeatherData {
        private LocalDateTime timestamp;
        private String dayOfWeek;
        private double temperature;
        private double uvIndex;
        private String wind;
        private int humidity;
        private double dewPoint;
        private double pressure;
        private int cloudCover;
        private double visibility;
        private int cloudCeiling;

        public WeatherData(LocalDateTime timestamp, String dayOfWeek, double temperature, double uvIndex, 
                          String wind, int humidity, double dewPoint, double pressure, 
                          int cloudCover, double visibility, int cloudCeiling) {
            this.timestamp = timestamp;
            this.dayOfWeek = dayOfWeek;
            this.temperature = temperature;
            this.uvIndex = uvIndex;
            this.wind = wind;
            this.humidity = humidity;
            this.dewPoint = dewPoint;
            this.pressure = pressure;
            this.cloudCover = cloudCover;
            this.visibility = visibility;
            this.cloudCeiling = cloudCeiling;
        }

        // Getters
        public LocalDateTime getTimestamp() { return timestamp; }
        public String getDayOfWeek() { return dayOfWeek; }
        public double getTemperature() { return temperature; }
        public double getUvIndex() { return uvIndex; }
        public String getWind() { return wind; }
        public int getHumidity() { return humidity; }
        public double getDewPoint() { return dewPoint; }
        public double getPressure() { return pressure; }
        public int getCloudCover() { return cloudCover; }
        public double getVisibility() { return visibility; }
        public int getCloudCeiling() { return cloudCeiling; }

        @Override
        public String toString() {
            return String.format("WeatherData{timestamp=%s, dayOfWeek='%s', temperature=%.1f°C, " +
                            "uvIndex=%.1f, wind='%s', humidity=%d%%, dewPoint=%.1f°C, pressure=%.1f mb, " +
                            "cloudCover=%d%%, visibility=%.1f km, cloudCeiling=%d m}",
                    timestamp, dayOfWeek, temperature, uvIndex, wind, humidity, 
                    dewPoint, pressure, cloudCover, visibility, cloudCeiling);
        }
    }

    public List<WeatherData> readWeatherData() {
        List<WeatherData> weatherDataList = new ArrayList<>();
        
        try (BufferedReader br = new BufferedReader(new FileReader(CSV_FILE))) {
            // Skip header
            String line = br.readLine();
            
            while ((line = br.readLine()) != null) {
                String[] values = line.split("\s*,\s*", -1);
                
                try {
                    LocalDateTime timestamp = LocalDateTime.parse(values[0].trim(), DATE_TIME_FORMATTER);
                    String dayOfWeek = values[1].trim();
                    
                    // Parse temperature (remove °C and convert to double)
                    double temperature = Double.parseDouble(values[2].trim().replace("°C", ""));
                    
                    // Parse UV index (extract the numeric part)
                    double uvIndex = Double.parseDouble(values[3].trim().split("\\s+")[0]);
                    
                    String wind = values[4].trim();
                    
                    // Parse humidity (remove % and convert to int)
                    int humidity = Integer.parseInt(values[5].trim().replace("%", ""));
                    
                    // Parse dew point (remove ° C and convert to double)
                    double dewPoint = Double.parseDouble(values[6].trim().replace("° C", ""));
                    
                    // Parse pressure (extract the numeric part before "mb")
                    double pressure = Double.parseDouble(values[7].trim().split(" ")[1]);
                    
                    // Parse cloud cover (remove % and convert to int)
                    int cloudCover = Integer.parseInt(values[8].trim().replace("%", ""));
                    
                    // Parse visibility (extract the numeric part before "km")
                    double visibility = Double.parseDouble(values[9].trim().split(" ")[0]);
                    
                    // Parse cloud ceiling (extract the numeric part before "m")
                    int cloudCeiling = Integer.parseInt(values[10].trim().split(" ")[0]);
                    
                    WeatherData data = new WeatherData(
                            timestamp, dayOfWeek, temperature, uvIndex, wind, 
                            humidity, dewPoint, pressure, cloudCover, visibility, cloudCeiling
                    );
                    
                    weatherDataList.add(data);
                } catch (Exception e) {
                    System.err.println("Error parsing line: " + line);
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading file: " + CSV_FILE);
            e.printStackTrace();
        }
        
        return weatherDataList;
    }

    public void processAndStoreData() {
        List<WeatherData> weatherDataList = readWeatherData();
        
        // Example: Print all weather data
        System.out.println("Total weather records: " + weatherDataList.size());
        weatherDataList.forEach(System.out::println);
        
        // Here you can add code to store data in database using DBConnection
        // Example:
        // try (Connection conn = DBConnection.getConnection()) {
        //     String sql = "INSERT INTO weather_data (timestamp, temperature, humidity, ...) VALUES (?, ?, ...)";
        //     try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
        //         for (WeatherData data : weatherDataList) {
        //             pstmt.setTimestamp(1, Timestamp.valueOf(data.getTimestamp()));
        //             pstmt.setDouble(2, data.getTemperature());
        //             // Set other parameters...
        //             pstmt.addBatch();
        //         }
        //         pstmt.executeBatch();
        //     }
        // } catch (SQLException e) {
        //     e.printStackTrace();
        // }
    }

    public static void main(String[] args) {
        Transaction transaction = new Transaction();
        transaction.processAndStoreData();
    }
}
