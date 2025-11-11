package etl;

import java.time.ZoneId;

public class App {
  // config constants (later move to .properties if you like)
  public static final String CSV_PATH = "D:/DW/project/weather_log.csv";
  public static final String SQL_SCRIPT_PATH = "D:/DW/project/src/main/resources/staging/transaction.sql";
  public static final ZoneId ZONE = ZoneId.of("Asia/Ho_Chi_Minh");
  public static final int HOUR = 1, MIN = 53;
  public static final boolean RUN_IMMEDIATELY = false;

  public static void main(String[] args) {
    Runnable etl = () -> {
      System.out.println("\n🚀 ETL started");
      new CsvImporter(CSV_PATH).run(); // Extract/Load -> temp
      new SqlScriptRunner(SQL_SCRIPT_PATH).run(); // Transform/Load -> official
      System.out.println("✅ ETL done");
    };

    if (RUN_IMMEDIATELY)
      etl.run();
    DailyScheduler.scheduleAt(HOUR, MIN, ZONE, etl);
  }
}
