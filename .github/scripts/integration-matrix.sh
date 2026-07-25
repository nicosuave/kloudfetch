#!/usr/bin/env bash
set -euo pipefail

: "${SPARK_VERSION:?SPARK_VERSION is required}"
: "${DATABRICKS_JDBC_VERSIONS:?DATABRICKS_JDBC_VERSIONS is required}"

export COMPOSE_PROJECT_NAME="kloudfetch-ci-${SPARK_VERSION//./-}"
export KLOUDFETCH_S3_PUBLIC_ENDPOINT_URL="http://rustfs:9000"
export KLOUDFETCH_RUSTFS_DATA_DIR="rustfs-data"

cleanup() {
  status=$?
  if (( status != 0 )); then
    docker compose logs --no-color || true
  fi
  docker compose --profile test down --volumes --remove-orphans || true
  exit "$status"
}
trap cleanup EXIT

docker compose up --detach --build --wait rustfs rustfs-init spark proxy

read -r -a jdbc_versions <<< "$DATABRICKS_JDBC_VERSIONS"
failures=()
for jdbc_version in "${jdbc_versions[@]}"; do
  echo "::group::Spark ${SPARK_VERSION} / Databricks JDBC ${jdbc_version}"
  export DATABRICKS_JDBC_VERSION="$jdbc_version"
  if docker compose --profile test build jdbc-test &&
      docker compose --profile test run --rm --no-deps jdbc-test; then
    echo "PASS: Spark ${SPARK_VERSION} / Databricks JDBC ${jdbc_version}"
  else
    failures+=("Spark ${SPARK_VERSION} / Databricks JDBC ${jdbc_version}")
  fi
  echo "::endgroup::"
done

# Exercise the broader Arrow type suite with the newest driver as well as the
# compact ordered-row compatibility check used for every driver.
export DATABRICKS_JDBC_VERSION="${jdbc_versions[${#jdbc_versions[@]} - 1]}"
if ! docker compose --profile test build jdbc-types ||
    ! docker compose --profile test run --rm --no-deps jdbc-types; then
  failures+=("Spark ${SPARK_VERSION} / JDBC ${DATABRICKS_JDBC_VERSION} type suite")
fi

if (( ${#failures[@]} > 0 )); then
  printf 'FAILED: %s\n' "${failures[@]}" >&2
  exit 1
fi
