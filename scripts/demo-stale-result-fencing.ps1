[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptRoot "..")).Path
$MavenWrapper = Join-Path $RepoRoot "mvnw.cmd"
$LogDirectory = Join-Path $RepoRoot "target\tf0604-demo"
$LogPath = Join-Path $LogDirectory "stale-result-fencing.log"

$ExpectedTrace = @(
    "TF0604 TRACE 1 SUBMITTED job_id=job-fencing-demo task_id=task-job-fencing-demo-0 accepted=true",
    "TF0604 TRACE 2 ASSIGNED worker_id=executor-a attempt_number=1 assignment_id=00000000-0000-0000-0000-000000000601 lease_expires_at_epoch_ms=1767225601000",
    "TF0604 TRACE 3 LEASE_EXPIRED attempt_number=1 assignment_id=00000000-0000-0000-0000-000000000601 at_epoch_ms=1767225601000 outcome=RETRY_SCHEDULED",
    "TF0604 TRACE 4 REASSIGNED worker_id=executor-a attempt_number=2 assignment_id=00000000-0000-0000-0000-000000000602 lease_expires_at_epoch_ms=1767225602000",
    "TF0604 TRACE 5 STALE_REJECTED attempt_number=1 assignment_id=00000000-0000-0000-0000-000000000601 disposition=ACK_DUPLICATE_OR_STALE authoritative_assignment_id=00000000-0000-0000-0000-000000000602",
    "TF0604 TRACE 6 CURRENT_COMMITTED attempt_number=2 assignment_id=00000000-0000-0000-0000-000000000602 disposition=ACK_SUCCESS result=current-result",
    "TF0604 TRACE 7 COMPLETED job_id=job-fencing-demo authoritative_results=1 stale_results=1 final_result=current-result"
)

New-Item -ItemType Directory -Path $LogDirectory -Force | Out-Null

Push-Location $RepoRoot
try {
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        # Maven and the JVM may write non-fatal warnings to stderr. Capture
        # both streams and decide success from the native exit code.
        $ErrorActionPreference = "Continue"
        $BuildOutput = @(
            & $MavenWrapper -q `
                -pl taskflow-coordinator `
                -am `
                "-Dtest=StaleResultTraceDemoTest" `
                "-Dsurefire.failIfNoSpecifiedTests=false" `
                test 2>&1 |
                ForEach-Object { $_.ToString() }
        )
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    $BuildOutput | Set-Content -LiteralPath $LogPath -Encoding UTF8
    if ($exitCode -ne 0) {
        throw "TF-0604 demo test failed with exit code $exitCode. See $LogPath."
    }

    $Trace = @($BuildOutput | Where-Object { $_ -like "TF0604 TRACE *" })
    if ($Trace.Count -ne $ExpectedTrace.Count) {
        throw "Expected $($ExpectedTrace.Count) trace steps but found $($Trace.Count). See $LogPath."
    }
    for ($index = 0; $index -lt $ExpectedTrace.Count; $index++) {
        if ($Trace[$index] -ne $ExpectedTrace[$index]) {
            throw "Trace step $($index + 1) changed. See $LogPath."
        }
    }

    $Trace | ForEach-Object { Write-Output $_ }
    Write-Output "TF0604 RESULT PASS trace_steps=7 docker_required=false"
} finally {
    Pop-Location
}
