package utils;

import java.sql.*;
import java.util.Properties;

public class DBConnection {
  public enum DatabaseType {
    MYSQL(
        // ✅ include allowLoadLocalInfile=true directly in URL
        "jdbc:mysql://localhost:3306/staging"
            + "?useUnicode=true&characterEncoding=utf8"
            + "&allowLoadLocalInfile=true"       // enable client-side file loading
            + "&allowPublicKeyRetrieval=true"
            + "&useSSL=false"
            + "&serverTimezone=Asia/Ho_Chi_Minh",
        "com.mysql.cj.jdbc.Driver"),

    SQLSERVER(
        "jdbc:sqlserver://localhost:1433;databaseName=staging",
        "com.microsoft.sqlserver.jdbc.SQLServerDriver");

    private final String url;
    private final String driverClass;

    DatabaseType(String url, String driverClass) {
      this.url = url;
      this.driverClass = driverClass;
    }

    public String getUrl() {
      return url;
    }

    public String getDriverClass() {
      return driverClass;
    }
  }

  private static final String USER = "root";
  private static final String PASSWORD = "1234";

  // Default to MySQL
  private static DatabaseType defaultDatabase = DatabaseType.MYSQL;

  // Initialize both drivers
  static {
    try {
      Class.forName(DatabaseType.MYSQL.getDriverClass());
      Class.forName(DatabaseType.SQLSERVER.getDriverClass());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("❌ Failed to load database drivers: " + e.getMessage(), e);
    }
  }

  /** Get a connection using the default database type */
  public static Connection getConnection() throws SQLException {
    return getConnection(defaultDatabase);
  }

  /** Get a connection to the specified database type */
  public static Connection getConnection(DatabaseType dbType) throws SQLException {
    Properties info = new Properties();
    info.setProperty("user", USER);
    info.setProperty("password", PASSWORD);

    if (dbType == DatabaseType.SQLSERVER) {
      info.setProperty("encrypt", "false");
      info.setProperty("trustServerCertificate", "true");
    } else {
      info.setProperty("useSSL", "false");
      info.setProperty("allowPublicKeyRetrieval", "true");
      info.setProperty("allowLoadLocalInfile", "true"); // ✅ for MySQL client-side loading
      info.setProperty("autoReconnect", "true");
      info.setProperty("serverTimezone", "Asia/Ho_Chi_Minh");
    }

    return DriverManager.getConnection(dbType.getUrl(), info);
  }

  /** Change default database */
  public static void setDefaultDatabase(DatabaseType dbType) {
    defaultDatabase = dbType;
  }

  public static void main(String[] args) {
    System.out.println("Testing MySQL connection...");
    testConnection(DatabaseType.MYSQL);

    System.out.println("\nTesting SQL Server connection...");
    testConnection(DatabaseType.SQLSERVER);
  }

  private static void testConnection(DatabaseType dbType) {
    try (Connection conn = getConnection(dbType)) {
      DatabaseMetaData meta = conn.getMetaData();
      System.out.println("✅ Connected to " + dbType.name() + "!");
      System.out.println("  Database: " + meta.getDatabaseProductName() + " " + meta.getDatabaseProductVersion());

      // Check if LOCAL INFILE is actually enabled
      try (Statement st = conn.createStatement();
           ResultSet rs = st.executeQuery("SHOW VARIABLES LIKE 'local_infile'")) {
        if (rs.next())
          System.out.println("  local_infile = " + rs.getString(2));
      }

    } catch (Exception e) {
      System.err.println("❌ Connection error to " + dbType.name() + ": " + e.getMessage());
    }
  }
}
