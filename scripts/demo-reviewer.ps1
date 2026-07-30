[CmdletBinding()]
param(
    [switch]$Help
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($Help) {
    Write-Output @"
TaskFlow five-minute reviewer demo

Usage:
  .\scripts\demo-reviewer.ps1

Prerequisites:
  Java 21 or newer and a running Docker Engine.

The command starts disposable RabbitMQ and MinIO Testcontainers, then runs the
real SQLite/scheduler/RabbitMQ reviewer workflow. Full output is retained under
target\tf0804-demo\reviewer-demo.log. Containers are removed automatically.
"@
    exit 0
}

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptRoot "..")).Path
$MavenWrapper = Join-Path $RepoRoot "mvnw.cmd"
$LogDirectory = Join-Path $RepoRoot "target\tf0804-demo"
$LogPath = Join-Path $LogDirectory "reviewer-demo.log"
$MaximumDemoDurationMillis = 300000

$ExpectedTrace = @(
    "TF0804 TRACE 1 STACK_READY coordinator_instance=COORDINATOR_tf0804_demo_1 rabbitmq=UP sqlite_schema=14 minio=UP workers=reviewer-worker-a,reviewer-worker-b",
    "TF0804 TRACE 2 SUBMITTED job_id=job-reviewer-demo task_id=task-job-reviewer-demo-0 accepted=true",
    "TF0804 TRACE 3 ASSIGNED worker_id=reviewer-worker-a attempt_number=1 assignment_id=00000000-0000-0000-0000-000000000801 lease_expires_at_epoch_ms=1767225601000",
    "TF0804 TRACE 4 WORKER_PAUSED worker_id=reviewer-worker-a transport=closed registry_status=DISCONNECTED",
    "TF0804 TRACE 5 LEASE_EXPIRED attempt_number=1 assignment_id=00000000-0000-0000-0000-000000000801 at_epoch_ms=1767225601000 outcome=RETRY_SCHEDULED",
    "TF0804 TRACE 6 REASSIGNED worker_id=reviewer-worker-b attempt_number=2 assignment_id=00000000-0000-0000-0000-000000000802 lease_expires_at_epoch_ms=1767225602000",
    "TF0804 TRACE 7 STALE_REJECTED attempt_number=1 assignment_id=00000000-0000-0000-0000-000000000801 disposition=ACK_DUPLICATE_OR_STALE authoritative_assignment_id=00000000-0000-0000-0000-000000000802",
    "TF0804 TRACE 8 CURRENT_COMMITTED attempt_number=2 assignment_id=00000000-0000-0000-0000-000000000802 disposition=ACK_SUCCESS result=current-result",
    "TF0804 TRACE 9 COMPLETED job_id=job-reviewer-demo authoritative_results=1 final_result=current-result",
    "TF0804 TRACE 10 COORDINATOR_RESTARTED coordinator_instance=COORDINATOR_tf0804_demo_2 recovered_running_jobs=0 persisted_job_status=COMPLETED",
    "TF0804 TRACE 11 PERSISTED_RESULT_RETRIEVED job_id=job-reviewer-demo delivery=JOB_RESULT result=current-result",
    "TF0804 TRACE 12 OBSERVED metrics_assignments=2 metrics_lease_expirations=1 metrics_stale=1 metrics_committed=1 metrics_jobs_completed=1 outbox_published=3 outbox_pending=0 minio_bucket=taskflow-reviewer-demo"
)

if (-not (Test-Path -LiteralPath $MavenWrapper -PathType Leaf)) {
    throw "Maven wrapper was not found at $MavenWrapper."
}
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker was not found. Install Docker Desktop or another compatible Docker Engine."
}

$previousErrorActionPreference = $ErrorActionPreference
try {
    $ErrorActionPreference = "Continue"
    docker info *> $null
    $dockerExitCode = $LASTEXITCODE
} finally {
    $ErrorActionPreference = $previousErrorActionPreference
}
if ($dockerExitCode -ne 0) {
    throw "Docker Engine is not running. Start Docker Desktop and rerun the command."
}

New-Item -ItemType Directory -Path $LogDirectory -Force | Out-Null

Push-Location $RepoRoot
try {
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        # Maven, Testcontainers, and the JVM write informational lines to both
        # streams. Preserve them and use the native exit code as the authority.
        $ErrorActionPreference = "Continue"
        $BuildOutput = @(
            & $MavenWrapper -q `
                -pl taskflow-coordinator `
                -am `
                "-Dtaskflow.reviewer.demo=true" `
                "-Dtest=ReviewerDemoTest" `
                "-Dsurefire.failIfNoSpecifiedTests=false" `
                test 2>&1 |
                ForEach-Object { $_.ToString() }
        )
        $mavenExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    $BuildOutput | Set-Content -LiteralPath $LogPath -Encoding UTF8
    if ($mavenExitCode -ne 0) {
        throw "TF-0804 reviewer demo failed with exit code $mavenExitCode. See $LogPath."
    }

    $Trace = @($BuildOutput | Where-Object { $_ -like "TF0804 TRACE *" })
    if ($Trace.Count -ne $ExpectedTrace.Count) {
        throw "Expected $($ExpectedTrace.Count) trace steps but found $($Trace.Count). See $LogPath."
    }
    for ($index = 0; $index -lt $ExpectedTrace.Count; $index++) {
        if ($Trace[$index] -ne $ExpectedTrace[$index]) {
            throw "Trace step $($index + 1) changed. See $LogPath."
        }
    }

    $ResultLines = @(
        $BuildOutput |
            Where-Object {
                $_ -match "^TF0804 RESULT PASS trace_steps=12 duration_ms=([0-9]+)$"
            }
    )
    if ($ResultLines.Count -ne 1) {
        throw "Expected one TF0804 PASS result line. See $LogPath."
    }
    if ($ResultLines[0] -notmatch "duration_ms=([0-9]+)$") {
        throw "The TF0804 PASS result did not contain a duration. See $LogPath."
    }
    $durationMillis = [long]$Matches[1]
    if ($durationMillis -ge $MaximumDemoDurationMillis) {
        throw "The post-dependency demo took $durationMillis ms; the limit is less than $MaximumDemoDurationMillis ms."
    }

    $Trace | ForEach-Object { Write-Output $_ }
    Write-Output $ResultLines[0]
    Write-Output "TF0804 LOG $LogPath"
} finally {
    Pop-Location
}
