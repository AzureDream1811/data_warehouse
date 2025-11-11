package etl;

import utils.DBConnection;
import utils.SqlIO;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlScriptRunner {
  private final String scriptPath;

  public SqlScriptRunner(String scriptPath) {
    this.scriptPath = scriptPath;
  }

  public void run() {
    System.out.println("⚙️ Running SQL script: " + scriptPath);

    try (Connection conn = DBConnection.getConnection(DBConnection.DatabaseType.MYSQL)) {
      conn.setAutoCommit(false);

      String script = SqlIO.readUtf8(scriptPath);
      script = SqlIO.stripBlockComments(script);
      String[] stmts = SqlIO.splitBySemicolon(script);

      int executed = 0;
      try (Statement st = conn.createStatement()) {
        for (String s : stmts) {
          String sql = SqlIO.cleanLineComments(s);
          if (sql.isBlank())
            continue;
          try {
            st.execute(sql);
            executed++;
          } catch (SQLException ex) {
            System.err.println("❌ Failed SQL:\n" + sql);
            System.err
                .println("   SQLState=" + ex.getSQLState() + " Code=" + ex.getErrorCode() + " Msg=" + ex.getMessage());
            conn.rollback();
            throw ex;
          }
        }
      }
      conn.commit();
      System.out.println("✅ Script executed. Statements: " + executed);
    } catch (Exception e) {
      System.err.println("❌ Script run error: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
