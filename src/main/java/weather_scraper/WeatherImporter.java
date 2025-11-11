package weather_scraper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import utils.DBConnection;

public class WeatherImporter {

  private static final String CSV_PATH = "D:/DW/data/weather_log.csv";
  private static final ZoneId ZONE_ID = ZoneId.of("Asia/Ho_Chi_Minh");
  private static final int HOUR = 1;
  private static final int MIN = 20;

  public static void main(String[] args) {
    // Run once immediately (for testing)
    importCsv();

    // Then schedule to run daily
    scheduleDailyImport();
  }

  private static void importCsv() {
    System.out.println("Stating CSV import from: " + CSV_PATH);

    try {
      Connection conection = DBConnection.getConnection(DBConnection.DatabaseType.MYSQL);
      String sql = "LOAD DATA INFILE ? INTO TABLE temp CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES (FullDate, WeekDay, Day, Temperature, UVValue, Wind, Humidity, DewPoint, Pressure, Cloud, Visibility, CloudCeiling)";

      try {
        PreparedStatement stmt = conection.prepareStatement(sql);
        stmt.setString(1, CSV_PATH);
        stmt.executeUpdate();
        System.out.println("✅ CSV import completed successfully.");
        return;
      } catch (SQLException e) {
        System.err.println("⚠️ LOAD DATA LOCAL INFILE failed: " + e.getMessage());
        System.err.println("👉 Falling back to manual insert...");
      }

      try (Connection connection = DBConnection.getConnection(DBConnection.DatabaseType.MYSQL)) {
        manualInsert(connection);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  private static void manualInsert(Connection connection) throws Exception {
    String insertSQL = "INSERT INTO temp (FullDate, WeekDay, Day, Temperature, UVValue, Wind, Humidity, DewPoint, Pressure, Cloud, Visibility, CloudCeiling) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    try (PreparedStatement statement = connection.prepareStatement(insertSQL);
        BufferedReader br = new BufferedReader(
            new InputStreamReader(new FileInputStream(CSV_PATH), StandardCharsets.UTF_8));) {
      connection.setAutoCommit(false);
      String line = br.readLine(); // Skip header
      int batchSize = 0; // To keep track of batch size
      int count = 0; // To keep track of total inserted records

      while ((line = br.readLine()) != null) {
        String[] cols = line.split(",", -1); // -1 to include trailing empty strings
        if (cols.length != 12)
          continue; // Skip malformed lines

        for (int i = 0; i < 12; i++)
          statement.setString(i + 1, cols[i].trim()); // Set parameters

        statement.addBatch(); // Add to batch
        count++; // Increment total count
        if (++batchSize % 1000 == 0) { // Execute batch every 1000 records
          statement.executeBatch(); // Execute batch
          batchSize = 0; // Reset batch size
        }

      }
      statement.executeBatch(); // Insert remaining records
      connection.commit(); // Commit transaction
      System.out.println("✅ Manual CSV import completed successfully. Total records inserted: " + count);

    }
  }

  private static void scheduleDailyImport() {
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    Runnable task = () -> {
      System.out.println("Scheduled task started at " + java.time.LocalDateTime.now(ZONE_ID));
      importCsv(); // Import CSV
    };

    long delay = millisUntilNextRun(HOUR, MIN, ZONE_ID); // 1:20 AM
    long period = TimeUnit.DAYS.toMillis(1); // 24 hours
    scheduler.scheduleAtFixedRate(task, delay, period, TimeUnit.MILLISECONDS); // Schedule task

    System.out.println("Scheduled daily CSV import at " + String.format("%02d:%02d", HOUR, MIN) + " " + ZONE_ID);
  }

  private static long millisUntilNextRun(int hour, int min, ZoneId zoneId) {
    ZonedDateTime now = ZonedDateTime.now(zoneId); // Current time in specified zone
    ZonedDateTime nextRun = now.withHour(hour).withMinute(min).withSecond(0).withNano(0); // Next run time today
    if (!nextRun.isAfter(now)) // If already past today, schedule for tomorrow
      nextRun = nextRun.plusDays(1);

    return ChronoUnit.MILLIS.between(now, nextRun); // Milliseconds until next run
  }

}
