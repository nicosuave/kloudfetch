/*
 * Licensed under the Apache License, Version 2.0.
 */
package io.kloudfetch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/** Pauses between ExecuteStatement and FetchResults for proxy restart testing. */
public final class JdbcRestart {
  private JdbcRestart() {}

  public static void main(String[] args) throws Exception {
    String url =
        System.getenv()
            .getOrDefault(
                "KLOUDFETCH_JDBC_URL",
                "jdbc:databricks://proxy:10000/default;"
                    + "transportMode=http;ssl=0;AuthMech=3;"
                    + "httpPath=/cliservice;UID=token;PWD=test-token;"
                    + "EnableTelemetry=0");
    long pause =
        Long.parseLong(
            System.getenv().getOrDefault("KLOUDFETCH_RESTART_PAUSE_MILLIS", "15000"));
    long totalRows =
        Long.parseLong(
            System.getenv().getOrDefault("KLOUDFETCH_RESTART_ROWS", "32768"));
    long pauseAfterRows =
        Long.parseLong(
            System.getenv().getOrDefault("KLOUDFETCH_RESTART_AFTER_ROWS", "0"));
    try (Connection connection = DriverManager.getConnection(url);
        Statement statement = connection.createStatement();
        ResultSet result =
            statement.executeQuery(
                "SELECT id, repeat('r', 8192) AS payload "
                    + "FROM range(0, "
                    + totalRows
                    + ", 1, 16) ORDER BY id")) {
      if (pauseAfterRows == 0) {
        System.out.println("KLOUDFETCH_RESTART_READY");
        System.out.flush();
        Thread.sleep(pause);
      }
      long rows = 0;
      while (result.next()) {
        if (result.getLong(1) != rows || result.getString(2).length() != 8192) {
          throw new AssertionError("restart result corrupt at row " + rows);
        }
        rows++;
        if (rows == pauseAfterRows) {
          System.out.println("KLOUDFETCH_RESTART_READY");
          System.out.flush();
          Thread.sleep(pause);
        }
      }
      if (rows != totalRows) {
        throw new AssertionError("restart result short: " + rows);
      }
    }
    System.out.println("Databricks JDBC proxy restart OK");
  }
}
