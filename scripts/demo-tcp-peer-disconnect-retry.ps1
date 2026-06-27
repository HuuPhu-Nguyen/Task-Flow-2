[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
$MavenWrapper = Join-Path $RepoRoot "mvnw.cmd"
if (-not (Test-Path $MavenWrapper)) {
    $MavenWrapper = Join-Path $RepoRoot "mvnw"
}

$TestSelector = "TaskSchedulerFailureTest#peerDisconnectReleasesAssignedTaskForImmediateRetryAndIgnoresStaleResult"
$ReportPath = Join-Path $RepoRoot "taskflow-core\target\surefire-reports\server.scheduler.TaskSchedulerFailureTest.txt"

Write-Host "TaskFlow TCP peer-disconnect retry demo"
Write-Host "Scenario: inject a TCP peer disconnect after assignment, retry the same task on another peer, ignore the stale first-peer result, then complete the job from the retry peer."
Write-Host ""

Push-Location $RepoRoot
try {
    & $MavenWrapper -pl taskflow-core -am "-Dtest=$TestSelector" "-Dsurefire.failIfNoSpecifiedTests=false" test
    if ($LASTEXITCODE -ne 0) {
        throw "Demo Maven run failed with exit code $LASTEXITCODE."
    }
} finally {
    Pop-Location
}

Write-Host ""
if (Test-Path $ReportPath) {
    $Summary = Select-String -Path $ReportPath -Pattern "Tests run:" | Select-Object -First 1
    if ($null -ne $Summary) {
        Write-Host "Surefire summary: $($Summary.Line.Trim())"
    }
}

Write-Host "Expected behavior verified:"
Write-Host "- first assignment goes to peer-1"
Write-Host "- tcp_disconnect releases the assigned task"
Write-Host "- retry assignment goes to peer-2 with the same task id"
Write-Host "- stale result from peer-1 is ignored"
Write-Host "- accepted result from peer-2 completes the job"
Write-Host "Demo complete."
