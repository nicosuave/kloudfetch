/*
 * Licensed under the Apache License, Version 2.0.
 */
package io.kloudfetch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Bounded concurrent-query soak test through the unmodified JDBC driver. */
public final class JdbcConcurrency {
  private JdbcConcurrency() {}

  private static String env(String name, String fallback) {
    return System.getenv().getOrDefault(name, fallback);
  }

  private static String url(int worker) {
    String primary = env(
        "KLOUDFETCH_JDBC_URL",
        "jdbc:databricks://proxy:10000/default;"
            + "transportMode=http;ssl=0;AuthMech=3;"
            + "httpPath=/cliservice;UID=token;PWD=test-token;EnableTelemetry=0");
    return worker % 2 == 0
        ? primary
        : env("KLOUDFETCH_JDBC_URL_2", primary);
  }

  public static void main(String[] args) throws Exception {
    int concurrency = Integer.parseInt(env("KLOUDFETCH_CONCURRENCY", "8"));
    int iterations = Integer.parseInt(env("KLOUDFETCH_ITERATIONS", "10"));
    long rowsPerQuery = Long.parseLong(env("KLOUDFETCH_ROWS_PER_QUERY", "4096"));
    ExecutorService pool = Executors.newFixedThreadPool(concurrency);
    List<Future<Long>> futures = new ArrayList<>();
    long started = System.nanoTime();
    for (int worker = 0; worker < concurrency; worker++) {
      final int workerId = worker;
      futures.add(
          pool.submit(
              (Callable<Long>)
                  () -> {
                    long observed = 0;
                    try (Connection connection =
                            DriverManager.getConnection(url(workerId));
                        Statement statement = connection.createStatement()) {
                      for (int iteration = 0; iteration < iterations; iteration++) {
                        String sql =
                            "SELECT id, repeat(cast("
                                + workerId
                                + " AS string), 1024) AS payload "
                                + "FROM range(0, "
                                + rowsPerQuery
                                + ", 1, 4) ORDER BY id";
                        long expected = 0;
                        try (ResultSet result = statement.executeQuery(sql)) {
                          while (result.next()) {
                            long id = result.getLong(1);
                            if (id != expected || result.getString(2).length() != 1024) {
                              throw new AssertionError(
                                  "worker "
                                      + workerId
                                      + " iteration "
                                      + iteration
                                      + " corrupt at "
                                      + expected);
                            }
                            expected++;
                          }
                        }
                        if (expected != rowsPerQuery) {
                          throw new AssertionError("short result: " + expected);
                        }
                        observed += expected;
                      }
                    }
                    return observed;
                  }));
    }
    long totalRows = 0;
    for (Future<Long> future : futures) {
      totalRows += future.get();
    }
    pool.shutdown();
    double seconds = (System.nanoTime() - started) / 1_000_000_000.0;
    System.out.printf(
        "Databricks JDBC concurrency OK: concurrency=%d queries=%d rows=%d seconds=%.3f%n",
        concurrency, concurrency * iterations, totalRows, seconds);
  }
}
