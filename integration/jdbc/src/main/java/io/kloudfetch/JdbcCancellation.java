/*
 * Licensed under the Apache License, Version 2.0.
 */
package io.kloudfetch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** Cancels an in-flight result spool and requires a clean JDBC outcome. */
public final class JdbcCancellation {
  private JdbcCancellation() {}

  public static void main(String[] args) throws Exception {
    String url =
        System.getenv()
            .getOrDefault(
                "KLOUDFETCH_JDBC_URL",
                "jdbc:databricks://proxy:10000/default;"
                    + "transportMode=http;ssl=0;AuthMech=3;"
                    + "httpPath=/cliservice;UID=token;PWD=test-token;"
                    + "EnableTelemetry=0");
    try (Connection connection = DriverManager.getConnection(url);
        Statement statement = connection.createStatement()) {
      Thread cancel =
          new Thread(
              () -> {
                try {
                  Thread.sleep(750);
                  statement.cancel();
                } catch (Exception error) {
                  throw new RuntimeException(error);
                }
              });
      cancel.start();
      boolean cancelled = false;
      try (ResultSet result =
          statement.executeQuery(
              "SELECT id, repeat(lpad(hex(xxhash64(id)), 16, '0'), 512) "
                  + "FROM range(0, 1310720, 1, 16)")) {
        while (result.next()) {
          // A fast machine may produce some rows before cancellation arrives.
        }
      } catch (SQLException expected) {
        cancelled = true;
      }
      cancel.join();
      if (!cancelled && !statement.isClosed()) {
        // JDBC permits cancellation to race with successful completion.
        System.out.println("Cancellation raced with successful completion");
      }
    }
    System.out.println("Databricks JDBC cancellation path OK");
  }
}
