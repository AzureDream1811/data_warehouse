package utils;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class SqlIO {
  public static String readUtf8(String path) throws IOException {
    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8))) {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        if (sb.length() == 0 && line.startsWith("\uFEFF"))
          line = line.substring(1); // strip BOM
        sb.append(line).append('\n');
      }
      return sb.toString();
    }
  }

  public static String stripBlockComments(String s) {
    return s.replaceAll("/\\*.*?\\*/", " "); // /* ... */
  }

  public static String cleanLineComments(String s) {
    String[] lines = s.split("\\R");
    StringBuilder out = new StringBuilder();
    for (String line : lines) {
      String t = line.trim();
      if (t.startsWith("--"))
        continue;
      out.append(line).append('\n');
    }
    return out.toString().trim();
  }

  public static String[] splitBySemicolon(String script) {
    // simple splitter: good for plain TRUNCATE/INSERT scripts
    return script.split(";");
  }
}
