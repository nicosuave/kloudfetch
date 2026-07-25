#!/usr/bin/env sh
set -eu

exec mvn -q \
  "-Ddatabricks.jdbc.version=${DATABRICKS_JDBC_VERSION:-3.4.2}" \
  "-Dexec.mainClass=${JDBC_TEST_MAIN_CLASS:-io.kloudfetch.JdbcIntegration}" \
  compile exec:java "$@"
