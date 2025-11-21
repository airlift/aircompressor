#!/bin/bash

set -euo pipefail

DURATION="60m"
TARGET_CLASS="io.airlift.compress.v3.FuzzTests"
COVERAGE_DIR="target/coverage-reports"

FUZZ_TESTS=(
    "fuzzBlockRoundTrip"
    "fuzzBlockDecompress"
    "fuzzAircompressorStreamRoundTrip"
    "fuzzAircompressorDecompress"
    "fuzzHadoopRoundTrip"
    "fuzzHadoopDecompress"
    "fuzzDecompressionBomb"
    "fuzzAircompressorVsApacheCommons"
    "fuzzBlockVsStreamConsistency"
)

./mvnw clean
mkdir -p ${COVERAGE_DIR}

echo "=== Running fuzzing for ${DURATION} ==="

for fuzz_test in "${FUZZ_TESTS[@]}"; do
  echo "Fuzzing: ${fuzz_test}"

  JAZZER_FUZZ=1 ./mvnw test \
    -Djazzer.max_duration=${DURATION} \
    -Dtest="${TARGET_CLASS}#${fuzz_test}"
done

echo "=== Replaying corpus for coverage ==="

for fuzz_test in "${FUZZ_TESTS[@]}"; do
  echo "Replaying: ${fuzz_test}"

  ./mvnw \
    jacoco:prepare-agent \
    -Dtest="${TARGET_CLASS}#${fuzz_test}" \
    -Dmaven.test.failure.ignore=true \
    -Djacoco.skip=false \
    test

  if [ -f "target/jacoco.exec" ]; then
    mv target/jacoco.exec "${COVERAGE_DIR}/jacoco-${fuzz_test}.exec"
  else
    echo "No coverage data found"
  fi
done

echo "=== Merging data and creating report ==="

./mvnw jacoco:merge@merge-results jacoco:report -Djacoco.skip=false

