/*
 * Licensed under the Apache License, Version 2.0.
 */
package io.kloudfetch;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;

/** Arrow/JDBC type coverage and per-operation schema isolation. */
public final class JdbcTypeCoverage {
  private JdbcTypeCoverage() {}

  private static String url() {
    return System.getenv()
        .getOrDefault(
            "KLOUDFETCH_JDBC_URL",
            "jdbc:databricks://proxy:10000/default;"
                + "transportMode=http;ssl=0;AuthMech=3;"
                + "httpPath=/cliservice;UID=token;PWD=test-token;"
                + "EnableTelemetry=0");
  }

  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }

  public static void main(String[] args) throws Exception {
    try (Connection connection = DriverManager.getConnection(url());
        Statement statement = connection.createStatement()) {
      try (ResultSet result =
          statement.executeQuery(
              "SELECT "
                  + "cast(12345678901234567890.123456789 AS decimal(38,9)) AS dec, "
                  + "timestamp'2026-07-24 18:41:42.123456' AS ts, "
                  + "timestamp_ntz'2026-07-24 18:41:42.654321' AS ts_ntz, "
                  + "cast('hello-世界' AS binary) AS bin, "
                  + "array(1, 2, cast(NULL AS int)) AS arr, "
                  + "map('alpha', 1, 'beta', 2) AS mp, "
                  + "named_struct('name', 'nested', 'count', 7) AS st, "
                  + "cast(NULL AS decimal(10,2)) AS null_dec")) {
        check(result.next(), "type query returned no row");
        check(
            new BigDecimal("12345678901234567890.123456789")
                    .compareTo(result.getBigDecimal("dec"))
                == 0,
            "decimal(38,9) mismatch");
        Timestamp timestamp = result.getTimestamp("ts");
        check(
            timestamp.toInstant().equals(
                Instant.parse("2026-07-24T18:41:42.123456Z")),
            "timestamp mismatch: " + timestamp);
        check(result.getTimestamp("ts_ntz") != null, "timestamp_ntz was null");
        check(
            Arrays.equals(
                "hello-世界".getBytes(StandardCharsets.UTF_8),
                result.getBytes("bin")),
            "binary mismatch");
        Array array = result.getArray("arr");
        check(array != null, "array was null");
        Object arrayValue = array.getArray();
        check(arrayValue != null, "array payload was null");
        Object mapValue = result.getObject("mp");
        check(
            mapValue instanceof Map || mapValue.toString().contains("alpha"),
            "map mismatch: " + mapValue);
        Object structValue = result.getObject("st");
        check(
            structValue != null && structValue.toString().contains("nested"),
            "struct mismatch: " + structValue);
        check(result.getBigDecimal("null_dec") == null, "null decimal mismatch");
        check(!result.next(), "type query returned extra rows");
      }

      try (ResultSet empty =
          statement.executeQuery(
              "SELECT cast(1 AS int) AS never, "
                  + "named_struct('x', array(1, 2)) AS nested "
                  + "WHERE false")) {
        ResultSetMetaData metadata = empty.getMetaData();
        check(metadata.getColumnCount() == 2, "empty schema column count");
        check("never".equalsIgnoreCase(metadata.getColumnLabel(1)), "empty schema label");
        check(!empty.next(), "empty result unexpectedly returned a row");
      }

      // Different schemas on one connection ensure metadata is isolated by
      // operation rather than accidentally reused across result sets.
      try (ResultSet first =
          statement.executeQuery("SELECT 7 AS only_integer")) {
        check(first.next() && first.getInt("only_integer") == 7, "first schema");
      }
      try (ResultSet second =
          statement.executeQuery(
              "SELECT 'evolved' AS only_string, array('x', 'y') AS added")) {
        check(second.getMetaData().getColumnCount() == 2, "evolved schema count");
        check(
            second.next() && "evolved".equals(second.getString("only_string")),
            "evolved schema value");
      }
    }
    System.out.println(
        "Databricks JDBC Arrow type coverage OK: nested, temporal, decimal, "
            + "binary, empty, null, schema isolation");
  }
}
