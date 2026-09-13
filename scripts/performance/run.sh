#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
mkdir -p target/throughput
mvn -o test-compile -DskipTests -Djacoco.skip=true > target/throughput/build.log 2>&1
mvn -o -pl core dependency:build-classpath -Dmdep.includeScope=test \
  -Dmdep.outputFile="$PWD/target/throughput/classpath.txt" >> target/throughput/build.log 2>&1
classpath="core/target/classes:rms/target/classes:claims/target/classes:$(cat target/throughput/classpath.txt)"
javac --release 11 -cp "$classpath" -d target/throughput scripts/performance/ThroughputCheck.java
run_log=$(mktemp target/throughput/run-XXXXXX.log)
echo "Saving run log to $run_log"
java -Xms1g -Xmx6g -XX:ActiveProcessorCount=8 -cp "target/throughput:$classpath" ThroughputCheck "$@" 2>&1 | tee "$run_log"
