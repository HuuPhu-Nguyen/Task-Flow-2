#!/usr/bin/env bash
set -euo pipefail

skip_tests=false
allow_dirty=false
task_count=10000
warmup_task_count=1000
work_units_per_task=64
output_directory="target/baseline"

usage() {
  cat <<'USAGE'
Usage: ./scripts/verify-baseline.sh [options]

Options:
  --skip-tests             Reuse existing Surefire XML reports.
  --allow-dirty            Permit a dirty worktree for harness development only.
  --tasks N                Measured task count (default: 10000).
  --warmup-tasks N         Warm-up task count (default: 1000).
  --work-units N           Deterministic CPU mix iterations per task (default: 64).
  --output DIR             Generated evidence directory (default: target/baseline).
  --help                   Show this help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-tests) skip_tests=true; shift ;;
    --allow-dirty) allow_dirty=true; shift ;;
    --tasks) task_count="$2"; shift 2 ;;
    --warmup-tasks) warmup_task_count="$2"; shift 2 ;;
    --work-units) work_units_per_task="$2"; shift 2 ;;
    --output) output_directory="$2"; shift 2 ;;
    --help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

is_positive_integer() {
  [[ "$1" =~ ^[0-9]+$ ]] && (( "$1" > 0 ))
}

if ! is_positive_integer "$task_count" || (( task_count > 100000 )); then
  echo "--tasks must be in [1, 100000]." >&2
  exit 2
fi
if ! is_positive_integer "$warmup_task_count" || (( warmup_task_count > task_count )); then
  echo "--warmup-tasks must be positive and no larger than --tasks." >&2
  exit 2
fi
if ! is_positive_integer "$work_units_per_task" || (( work_units_per_task > 100000 )); then
  echo "--work-units must be in [1, 100000]." >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
maven_wrapper="$repo_root/mvnw"
if [[ ! -f "$maven_wrapper" ]]; then
  echo "Maven wrapper not found: $maven_wrapper" >&2
  exit 1
fi

cd "$repo_root"
command -v git >/dev/null || { echo "git is required." >&2; exit 1; }
command -v java >/dev/null || { echo "Java 21 or newer is required." >&2; exit 1; }

commit="$(git rev-parse HEAD)"
dirty_lines="$(git status --porcelain --untracked-files=normal)"
if [[ -n "$dirty_lines" && "$allow_dirty" != true ]]; then
  echo "The baseline must run from a clean checkout. Commit/stash changes or use --allow-dirty for harness development only." >&2
  exit 1
fi

if [[ "$output_directory" = /* || "$output_directory" =~ ^[A-Za-z]:[/\\] ]]; then
  output_root="$output_directory"
else
  output_root="$repo_root/$output_directory"
fi
mkdir -p "$output_root"

java_version_text="$(java -version 2>&1)"
java_major="$(printf '%s\n' "$java_version_text" | sed -n 's/.*version "\([0-9][0-9]*\).*/\1/p' | head -n 1)"
if [[ -z "$java_major" || "$java_major" -lt 21 ]]; then
  echo "Java 21 or newer is required; observed: $java_version_text" >&2
  exit 1
fi
maven_version_text="$("$maven_wrapper" -version 2>&1)"

run_maven_logged() {
  local label="$1"
  local log_path="$2"
  local reported_log_path="$3"
  shift 3
  echo
  echo "[$label] $maven_wrapper $*"
  set +e
  "$maven_wrapper" "$@" 2>&1 | tee "$log_path"
  local status=${PIPESTATUS[0]}
  set -e
  if (( status != 0 )); then
    echo "$label failed with exit code $status. See $reported_log_path" >&2
    return "$status"
  fi
}

if [[ "$skip_tests" != true ]]; then
  temporary_test_log="$(mktemp "${TMPDIR:-/tmp}/taskflow-baseline-test.XXXXXX.log")"
  if run_maven_logged "full-test-gate" "$temporary_test_log" "$output_root/maven-test.log" \
      --batch-mode --no-transfer-progress clean test; then
    test_status=0
  else
    test_status=$?
  fi
  mkdir -p "$output_root"
  mv -f "$temporary_test_log" "$output_root/maven-test.log"
  if (( test_status != 0 )); then
    exit "$test_status"
  fi
fi

attribute_value() {
  local line="$1"
  local attribute="$2"
  printf '%s\n' "$line" | sed -n "s/.* ${attribute}=\"\([0-9][0-9]*\)\".*/\1/p"
}

unit_suites=0
unit_tests=0
unit_skipped=0
integration_suites=0
integration_tests=0
integration_skipped=0
total_failures=0
total_errors=0
integration_names=()
report_count=0

while IFS= read -r report; do
  [[ "$(basename "$report")" == "TEST-server.scheduler.BaselineSchedulerExperiment.xml" ]] && continue
  suite_line="$(grep -m 1 '<testsuite ' "$report")"
  tests="$(attribute_value "$suite_line" tests)"
  skipped="$(attribute_value "$suite_line" skipped)"
  failures="$(attribute_value "$suite_line" failures)"
  errors="$(attribute_value "$suite_line" errors)"
  tests=${tests:-0}; skipped=${skipped:-0}; failures=${failures:-0}; errors=${errors:-0}
  report_count=$((report_count + 1))
  total_failures=$((total_failures + failures))
  total_errors=$((total_errors + errors))
  if [[ "$report" == *IntegrationTest.xml || "$report" == *LiveTest.xml ]]; then
    integration_suites=$((integration_suites + 1))
    integration_tests=$((integration_tests + tests))
    integration_skipped=$((integration_skipped + skipped))
    integration_names+=("$(basename "$report" .xml | sed 's/^TEST-//')")
  else
    unit_suites=$((unit_suites + 1))
    unit_tests=$((unit_tests + tests))
    unit_skipped=$((unit_skipped + skipped))
  fi
done < <(find "$repo_root" -path '*/target/surefire-reports/TEST-*.xml' -type f | sort)

if (( report_count == 0 )); then
  echo "No Surefire XML reports found. Run without --skip-tests first." >&2
  exit 1
fi
if (( total_failures != 0 || total_errors != 0 )); then
  echo "Surefire reports contain failures/errors after the test gate." >&2
  exit 1
fi
total_suites=$((unit_suites + integration_suites))
total_tests=$((unit_tests + integration_tests))
total_skipped=$((unit_skipped + integration_skipped))
integration_joined="$(IFS=,; echo "${integration_names[*]-}")"
cat > "$output_root/test-counts.properties" <<EOF
classification=class-name suffix IntegrationTest or LiveTest; baseline experiment excluded
unitComponentSuites=$unit_suites
unitComponentTests=$unit_tests
unitComponentSkipped=$unit_skipped
integrationLiveSuites=$integration_suites
integrationLiveTests=$integration_tests
integrationLiveSkipped=$integration_skipped
totalSuites=$total_suites
totalTests=$total_tests
totalSkipped=$total_skipped
totalFailures=$total_failures
totalErrors=$total_errors
integrationSuiteNames=$integration_joined
EOF

find "$repo_root" -name pom.xml -not -path '*/target/*' -type f \
  | sed "s#^$repo_root/##" | sort > "$output_root/modules.txt"
module_count="$(wc -l < "$output_root/modules.txt" | tr -d ' ')"
scheduler_lines="$(wc -l < "$repo_root/taskflow-core/src/main/java/server/scheduler/TaskScheduler.java" | tr -d ' ')"
protocol_version="$(sed -n 's/.*public static final int CURRENT = \([0-9][0-9]*\).*/\1/p' "$repo_root/taskflow-spi/src/main/java/protocol/ProtocolVersions.java")"
schema_version="$(sed -n 's/.*public static final int CURRENT_SCHEMA_VERSION = \([0-9][0-9]*\).*/\1/p' "$repo_root/taskflow-persistence-sqlite/src/main/java/server/db/DatabaseManager.java")"
if [[ -z "$protocol_version" || -z "$schema_version" ]]; then
  echo "Could not read protocol/schema source constants." >&2
  exit 1
fi

dirty=false
[[ -n "$dirty_lines" ]] && dirty=true
cat > "$output_root/inventory.properties" <<EOF
commit=$commit
dirty=$dirty
moduleCount=$module_count
taskSchedulerLines=$scheduler_lines
protocolVersion=$protocol_version
sqliteSchemaVersion=$schema_version
os=$(uname -s)
architecture=$(uname -m)
logicalProcessors=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo unknown)
EOF
{
  echo "commit: $commit"
  echo "dirty: $dirty"
  echo "uname: $(uname -a)"
  if [[ -r /proc/cpuinfo ]]; then
    echo "cpu: $(grep -m 1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//')"
  fi
  if [[ -r /proc/meminfo ]]; then
    echo "memory: $(grep -m 1 'MemTotal' /proc/meminfo)"
  fi
  echo
  echo "java -version:"
  printf '%s\n' "$java_version_text"
  echo
  echo "maven wrapper -version:"
  printf '%s\n' "$maven_version_text"
} > "$output_root/environment.txt"

windows_output_root="$output_root"
if command -v cygpath >/dev/null 2>&1; then
  windows_output_root="$(cygpath -w "$output_root")"
fi

run_experiment() {
  local workers="$1"
  local metrics_path="$windows_output_root/workers-$workers.properties"
  run_maven_logged "baseline-$workers-worker" "$output_root/workers-$workers.log" \
    "$output_root/workers-$workers.log" \
    --batch-mode --no-transfer-progress -pl taskflow-core -am \
    -Dtest=BaselineSchedulerExperiment \
    -Dsurefire.failIfNoSpecifiedTests=false \
    "-Dtaskflow.baseline.workers=$workers" \
    "-Dtaskflow.baseline.tasks=$task_count" \
    "-Dtaskflow.baseline.warmupTasks=$warmup_task_count" \
    "-Dtaskflow.baseline.workUnits=$work_units_per_task" \
    "-Dtaskflow.baseline.output=$metrics_path" \
    "-DargLine=-Xms64m -Xmx512m" \
    test
  local properties_path="$output_root/workers-$workers.properties"
  [[ -f "$properties_path" ]] || { echo "Metrics file missing: $properties_path" >&2; exit 1; }
  grep -q "^workerCount=$workers$" "$properties_path" || { echo "Worker count mismatch in $properties_path" >&2; exit 1; }
  grep -q "^taskCount=$task_count$" "$properties_path" || { echo "Task count mismatch in $properties_path" >&2; exit 1; }
}

run_experiment 1
run_experiment 4

property_value() {
  local path="$1"
  local key="$2"
  sed -n "s/^${key}=//p" "$path"
}

one_throughput="$(property_value "$output_root/workers-1.properties" throughputTasksPerSecond)"
four_throughput="$(property_value "$output_root/workers-4.properties" throughputTasksPerSecond)"
one_peak="$(property_value "$output_root/workers-1.properties" peakUsedHeapBytes)"
four_peak="$(property_value "$output_root/workers-4.properties" peakUsedHeapBytes)"

cat > "$output_root/summary.md" <<EOF
# TaskFlow Baseline Verification Run

- Commit: $commit
- Dirty worktree: $dirty
- Full Maven tests: $([[ "$skip_tests" == true ]] && echo false || echo true)
- Modules: $module_count
- Unit/component tests: $unit_tests (skipped: $unit_skipped)
- Integration/live tests: $integration_tests (skipped: $integration_skipped)
- TaskScheduler lines: $scheduler_lines
- Protocol version: $protocol_version
- SQLite schema version: $schema_version
- 1-worker throughput: $one_throughput tasks/s
- 4-worker throughput: $four_throughput tasks/s
- $task_count-task peak heap (1 worker): $one_peak bytes
- $task_count-task peak heap (4 workers): $four_peak bytes
EOF

echo
echo "Baseline verification PASS"
echo "Summary: $output_root/summary.md"
