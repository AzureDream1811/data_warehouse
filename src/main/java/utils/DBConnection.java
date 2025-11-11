package utils;

import java.sql.*;
import java.util.Properties;

public class DBConnection {
    public enum DatabaseType {
        MYSQL("jdbc:mysql://localhost:3306/staging", "com.mysql.cj.jdbc.Driver"),
        SQLSERVER("jdbc:sqlserver://localhost:1433;databaseName=staging", "com.microsoft.sqlserver.jdbc.SQLServerDriver");

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

    // Default to MySQL, can be changed via setDefaultDatabase()
    private static DatabaseType defaultDatabase = DatabaseType.MYSQL;

    // Initialize drivers
    static {
        try {
            // Load both drivers at startup
            Class.forName(DatabaseType.MYSQL.getDriverClass());
            Class.forName(DatabaseType.SQLSERVER.getDriverClass());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load database drivers: " + e.getMessage(), e);
        }
    }

    /**
     * Get a connection using the default database type
     */
    public static Connection getConnection() throws SQLException {
        return getConnection(defaultDatabase);
    }

    /**
     * Get a connection to the specified database type
     */
    public static Connection getConnection(DatabaseType dbType) throws SQLException {
        Properties info = new Properties();
        info.setProperty("user", USER);
        info.setProperty("password", PASSWORD);

        if (dbType == DatabaseType.SQLSERVER) {
            // SQL Server specific properties
            info.setProperty("encrypt", "false");
            info.setProperty("trustServerCertificate", "true");
        } else {
            // MySQL specific properties
            info.setProperty("useSSL", "false");
            info.setProperty("allowPublicKeyRetrieval", "true");
            info.setProperty("serverTimezone", "UTC");
            info.setProperty("autoReconnect", "true");
            info.setProperty("useLegacyDatetimeCode", "false");
        }

        return DriverManager.getConnection(dbType.getUrl(), info);
    }

    /**
     * Set the default database type for getConnection() without parameters
     */
    public static void setDefaultDatabase(DatabaseType dbType) {
        defaultDatabase = dbType;
    }

    public static void main(String[] args) {
        // Test MySQL connection
        System.out.println("Testing MySQL connection...");
        testConnection(DatabaseType.MYSQL);

        // Test SQL Server connection
        System.out.println("\nTesting SQL Server connection...");
        testConnection(DatabaseType.SQLSERVER);
    }

    private static void testConnection(DatabaseType dbType) {
        try (Connection conn = getConnection(dbType)) {
            DatabaseMetaData meta = conn.getMetaData();
            System.out.println("✅ Kết nối thành công với " + dbType.name() + "!");
            System.out.println("  Database: " + meta.getDatabaseProductName() + " " + meta.getDatabaseProductVersion());
        } catch (Exception e) {
            System.err.println("❌ Lỗi kết nối đến " + dbType.name() + ": " + e.getMessage());
        }
    }
}

