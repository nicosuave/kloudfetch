/*
 * Licensed under the Apache License, Version 2.0.
 */
package io.kloudfetch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;

/** Bounded end-to-end benchmark using the unmodified Databricks JDBC driver. */
public final class JdbcBenchmark {
  private static final int PAYLOAD_BYTES_PER_ROW = 8 * 1024;

  private JdbcBenchmark() {}

  public static void main(String[] args) throws Exception {
    String url =
        System.getenv()
            .getOrDefault(
                "KLOUDFETCH_JDBC_URL",
                "jdbc:databricks://proxy:10000/default;"
                    + "transportMode=http;ssl=0;AuthMech=3;"
                    + "httpPath=/cliservice;UID=token;PWD=test-token;"
                    + "EnableTelemetry=0");
    long targetBytes =
        Long.parseLong(
            System.getenv().getOrDefault("KLOUDFETCH_BENCHMARK_BYTES", "104857600"));
    int partitions =
        Integer.parseInt(
            System.getenv().getOrDefault("KLOUDFETCH_BENCHMARK_PARTITIONS", "4"));
    long expectedRows =
        (targetBytes + PAYLOAD_BYTES_PER_ROW - 1) / PAYLOAD_BYTES_PER_ROW;
    String sql =
        "SELECT id, repeat(lpad(hex(xxhash64(id)), 16, '0'), 512) AS payload "
            + "FROM range(0, "
            + expectedRows
            + ", 1, "
            + partitions
            + ")";

    long rows = 0;
    long payloadBytes = 0;
    long idSum = 0;
    long started = System.nanoTime();
    try (Connection connection = DriverManager.getConnection(url);
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(sql)) {
      while (result.next()) {
        long id = result.getLong(1);
        String payload = result.getString(2);
        if (id != rows) {
          throw new AssertionError("out-of-order row at " + rows + ": id=" + id);
        }
        if (payload.length() != PAYLOAD_BYTES_PER_ROW) {
          throw new AssertionError(
              "unexpected payload length at " + id + ": " + payload.length());
        }
        rows++;
        payloadBytes += payload.length();
        idSum += id;
      }
    }
    double seconds = (System.nanoTime() - started) / 1_000_000_000.0;
    long expectedSum = (expectedRows - 1) * expectedRows / 2;
    if (rows != expectedRows || idSum != expectedSum) {
      throw new AssertionError(
          "unexpected result rows="
              + rows
              + "/"
              + expectedRows
              + " sum="
              + idSum
              + "/"
              + expectedSum);
    }
    double gib = payloadBytes / (double) (1L << 30);
    System.out.printf(
        Locale.ROOT,
        "KLOUDFETCH_BENCHMARK target_bytes=%d payload_bytes=%d rows=%d "
            + "partitions=%d seconds=%.3f throughput_mib_s=%.2f%n",
        targetBytes,
        payloadBytes,
        rows,
        partitions,
        seconds,
        gib * 1024.0 / seconds);
  }
}
