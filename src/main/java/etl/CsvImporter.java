package etl;

import com.opencsv.CSVReader;
import utils.DBConnection;

import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class CsvImporter {
  private final String csvPath;

  public CsvImporter(String csvPath) {
    this.csvPath = csvPath;
  }

  public void run() {
    String sql = """
        INSERT INTO temp
        (FullDate, WeekDay, Day, Temperature, UVValue, Wind, Humidity, DewPoint, Pressure, Cloud, Visibility, CloudCeiling)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """;

    System.out.println("📥 Importing CSV: " + csvPath);

    try (Connection conn = DBConnection.getConnection(DBConnection.DatabaseType.MYSQL);
        PreparedStatement ps = conn.prepareStatement(sql);
        CSVReader reader = new CSVReader(new FileReader(csvPath, StandardCharsets.UTF_8))) {

      conn.setAutoCommit(false);
      reader.skip(1); // header

      String[] row;
      int batch = 0, total = 0;
      while ((row = reader.readNext()) != null) {
        if (row.length != 12)
          continue;
        for (int i = 0; i < 12; i++)
          ps.setString(i + 1, row[i].trim());
        ps.addBatch();
        if (++batch % 1000 == 0) {
          ps.executeBatch();
          batch = 0;
        }
        total++;
      }
      ps.executeBatch();
      conn.commit();
      System.out.println("✅ CSV -> temp rows: " + total);
    } catch (Exception e) {
      System.err.println("❌ CSV import error: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
