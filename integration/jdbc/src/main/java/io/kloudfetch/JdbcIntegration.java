/*
 * Licensed under the Apache License, Version 2.0.
 */
package io.kloudfetch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.math.BigDecimal;

public final class JdbcIntegration {
  private JdbcIntegration() {}

  public static void main(String[] args) throws Exception {
    String url =
        System.getenv()
            .getOrDefault(
                "KLOUDFETCH_JDBC_URL",
                "jdbc:databricks://proxy:10000/default;"
                    + "transportMode=http;ssl=0;AuthMech=3;"
                    + "httpPath=/cliservice;UID=token;PWD=test-token;"
                    + "EnableTelemetry=0");
    long rows = 0;
    long sum = 0;
    String firstPayload = null;
    String lastPayload = null;
    try (Connection connection = DriverManager.getConnection(url);
        Statement statement = connection.createStatement();
        ResultSet result =
            statement.executeQuery(
                "SELECT id, concat('row-', cast(id AS string)) AS payload "
                    + "FROM range(0, 50000, 1, 4) ORDER BY id")) {
      while (result.next()) {
        long id = result.getLong(1);
        String payload = result.getString(2);
        if (id != rows) {
          throw new AssertionError("out-of-order row at " + rows + ": id=" + id);
        }
        if (rows == 0) {
          firstPayload = payload;
        }
        lastPayload = payload;
        sum += id;
        rows++;
      }
    }
    long expectedSum = 49_999L * 50_000L / 2L;
    if (rows != 50_000
        || sum != expectedSum
        || !"row-0".equals(firstPayload)
        || !"row-49999".equals(lastPayload)) {
      throw new AssertionError(
          "unexpected result rows="
              + rows
              + " sum="
              + sum
              + " first="
              + firstPayload
              + " last="
              + lastPayload);
    }

    try (Connection connection = DriverManager.getConnection(url);
        Statement statement = connection.createStatement();
        ResultSet result =
            statement.executeQuery(
                "SELECT cast(42 AS int) AS i, "
                    + "cast(12.34 AS decimal(10,2)) AS d, "
                    + "cast('2026-07-24' AS date) AS dt, "
                    + "true AS flag, cast(NULL AS string) AS missing")) {
      if (!result.next()
          || result.getInt("i") != 42
          || new BigDecimal("12.34").compareTo(result.getBigDecimal("d")) != 0
          || !"2026-07-24".equals(result.getDate("dt").toString())
          || !result.getBoolean("flag")
          || result.getString("missing") != null
          || result.next()) {
        throw new AssertionError("common Spark types did not round-trip through Arrow");
      }
    }
    System.out.printf(
        "Databricks JDBC Cloud Fetch OK: rows=%d sum=%d first=%s last=%s%n",
        rows, sum, firstPayload, lastPayload);
  }
}
