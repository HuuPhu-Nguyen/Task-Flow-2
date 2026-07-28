[CmdletBinding()]
param(
    [switch]$Calibration,
    [switch]$AllowDirty,
    [switch]$Overwrite,
    [int]$TaskCount = 100000,
    [int]$TasksPerJob = 250,
    [int]$WorkerCount = 4,
    [long]$Seed = 55707398,
    [int]$DelayedResultCount = 0,
    [int]$WorkerTerminationCount = 0,
    [int]$BrokerRestartAfter = 0,
    [int]$CoordinatorRestartAfter = 0,
    [long]$LeaseMillis = 2000,
    [long]$DelayedResultMillis = 2500,
    [long]$CompletionTimeoutSeconds = 1800,
    [string]$OutputDirectory = "target/correctness-chaos"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
if (Test-Path variable:PSNativeCommandUseErrorActionPreference) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$RequiredTaskCount = 100000
$RequiredDuplicateBasisPoints = 500
$ExpectedPoisonDeliveryMaximum = 3

if ($TaskCount -lt 3 -or $TaskCount -gt 1000000) {
    throw "TaskCount must be in [3, 1000000]."
}
if ($TasksPerJob -lt 1 -or $TasksPerJob -gt 1000) {
    throw "TasksPerJob must be in [1, 1000]."
}
if ($WorkerCount -lt 1 -or $WorkerCount -gt 32) {
    throw "WorkerCount must be in [1, 32]."
}
if ($LeaseMillis -lt 1) {
    throw "LeaseMillis must be positive."
}
if ($DelayedResultMillis -le $LeaseMillis) {
    throw "DelayedResultMillis must be greater than LeaseMillis."
}
if ($CompletionTimeoutSeconds -lt 1) {
    throw "CompletionTimeoutSeconds must be positive."
}
if (-not $Calibration -and $AllowDirty) {
    throw "AllowDirty is available only with Calibration."
}
if (-not $Calibration -and $TaskCount -lt $RequiredTaskCount) {
    throw "Report-grade execution requires at least $RequiredTaskCount tasks."
}

if ($DelayedResultCount -eq 0) {
    $DelayedResultCount = [Math]::Max(1, [int]($TaskCount / 100))
}
if ($WorkerTerminationCount -eq 0) {
    $WorkerTerminationCount = [Math]::Max(1, [int]($TaskCount / 10000))
}
if ($BrokerRestartAfter -eq 0) {
    $BrokerRestartAfter = [Math]::Max(1, [int]($TaskCount / 3))
}
if ($CoordinatorRestartAfter -eq 0) {
    $CoordinatorRestartAfter = [Math]::Max(
        1,
        [int]($TaskCount * 2 / 3)
    )
}

foreach ($Bound in @(
    [pscustomobject]@{
        Name = "DelayedResultCount"
        Value = $DelayedResultCount
        Minimum = 1
        Maximum = $TaskCount
    },
    [pscustomobject]@{
        Name = "WorkerTerminationCount"
        Value = $WorkerTerminationCount
        Minimum = 1
        Maximum = $TaskCount
    },
    [pscustomobject]@{
        Name = "BrokerRestartAfter"
        Value = $BrokerRestartAfter
        Minimum = 1
        Maximum = $TaskCount - 1
    },
    [pscustomobject]@{
        Name = "CoordinatorRestartAfter"
        Value = $CoordinatorRestartAfter
        Minimum = 1
        Maximum = $TaskCount - 1
    }
)) {
    if ($Bound.Value -lt $Bound.Minimum -or
        $Bound.Value -gt $Bound.Maximum) {
        throw "$($Bound.Name) must be in [$($Bound.Minimum), $($Bound.Maximum)]."
    }
}

$ScriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDirectory "..")).Path
$MavenWrapper = Join-Path $RepoRoot "mvnw.cmd"
if (-not (Test-Path -LiteralPath $MavenWrapper)) {
    throw "Maven wrapper not found: $MavenWrapper"
}

if ([System.IO.Path]::IsPathRooted($OutputDirectory)) {
    $OutputRoot = [System.IO.Path]::GetFullPath($OutputDirectory)
} else {
    $OutputRoot = [System.IO.Path]::GetFullPath(
        (Join-Path $RepoRoot $OutputDirectory)
    )
}

if ($OutputRoot -eq $RepoRoot -or
    $OutputRoot -eq [System.IO.Path]::GetPathRoot($OutputRoot)) {
    throw "OutputDirectory must not be the repository or drive root."
}

function Write-Utf8Lines {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string[]]$Lines
    )
    $Parent = Split-Path -Parent $Path
    if ($Parent) {
        [System.IO.Directory]::CreateDirectory($Parent) | Out-Null
    }
    [System.IO.File]::WriteAllLines(
        $Path,
        $Lines,
        [System.Text.UTF8Encoding]::new($false)
    )
}

function Invoke-NativeText {
    param(
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$Description
    )
    $PreviousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $Lines = @(& $Command @Arguments 2>&1)
        $ExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $PreviousErrorActionPreference
    }
    $Text = ($Lines | ForEach-Object {
        if ($_ -is [System.Management.Automation.ErrorRecord]) {
            [string]$_.Exception.Message
        } else {
            [string]$_
        }
    }) -join [Environment]::NewLine
    if ($ExitCode -ne 0) {
        throw "$Description failed with exit code $ExitCode`: $Text"
    }
    return $Text
}

function Read-Properties {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Description
    )
    if (-not (Test-Path -LiteralPath $Path)) {
        throw "$Description was not created: $Path"
    }
    return ConvertFrom-StringData (
        Get-Content -LiteralPath $Path -Raw -Encoding UTF8
    )
}

function Assert-PropertyEquals {
    param(
        [Parameter(Mandatory = $true)][hashtable]$Properties,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Expected
    )
    if (-not $Properties.ContainsKey($Name)) {
        throw "Audit property is missing: $Name"
    }
    if ([string]$Properties[$Name] -ne $Expected) {
        throw "Audit property $Name expected $Expected but observed $($Properties[$Name])."
    }
}

Push-Location $RepoRoot
try {
    foreach ($RequiredCommand in @("git", "java", "docker")) {
        if (-not (Get-Command $RequiredCommand -ErrorAction SilentlyContinue)) {
            throw "$RequiredCommand is required."
        }
    }

    $Commit = (& git rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "Could not resolve the current Git commit."
    }
    $DirtyLines = @(& git status --porcelain --untracked-files=normal)
    if ($LASTEXITCODE -ne 0) {
        throw "Could not inspect the Git worktree."
    }
    if ($DirtyLines.Count -gt 0 -and -not $AllowDirty) {
        throw "Report-grade chaos requires a clean checkout. Use Calibration with AllowDirty only for harness development."
    }

    $JavaVersion = Invoke-NativeText "java" @("-version") "java -version"
    $JavaMatch = [regex]::Match(
        $JavaVersion,
        'version "(?<major>\d+)'
    )
    if (-not $JavaMatch.Success -or
        [int]$JavaMatch.Groups["major"].Value -lt 21) {
        throw "Java 21 or newer is required; observed: $JavaVersion"
    }
    $MavenVersion = Invoke-NativeText $MavenWrapper @("-version") `
        "Maven wrapper version check"
    $DockerVersion = Invoke-NativeText "docker" @(
        "info",
        "--format",
        "{{.ServerVersion}}"
    ) "Docker engine check"

    $ExpectedFiles = @(
        "audit.properties",
        "configuration.properties",
        "correctness-chaos.db",
        "correctness-chaos.db-shm",
        "correctness-chaos.db-wal",
        "environment.txt",
        "events.jsonl",
        "maven.log",
        "run.properties",
        "summary.md",
        "checksums.sha256"
    )
    $ExistingFiles = @($ExpectedFiles | Where-Object {
        Test-Path -LiteralPath (Join-Path $OutputRoot $_)
    })
    if ($ExistingFiles.Count -gt 0 -and -not $Overwrite) {
        throw "OutputDirectory already contains run evidence ($($ExistingFiles -join ', ')). Choose a fresh directory or pass Overwrite explicitly."
    }
    if ($Overwrite) {
        foreach ($Name in $ExpectedFiles) {
            $Candidate = Join-Path $OutputRoot $Name
            if (Test-Path -LiteralPath $Candidate -PathType Leaf) {
                Remove-Item -LiteralPath $Candidate -Force
            }
        }
    }
    [System.IO.Directory]::CreateDirectory($OutputRoot) | Out-Null

    $CpuName = "unknown"
    $PhysicalMemoryBytes = "unknown"
    try {
        $Cpu = Get-CimInstance Win32_Processor | Select-Object -First 1
        if ($null -ne $Cpu -and $Cpu.Name) {
            $CpuName = $Cpu.Name.Trim()
        }
        $Computer = Get-CimInstance Win32_ComputerSystem
        if ($null -ne $Computer -and $Computer.TotalPhysicalMemory) {
            $PhysicalMemoryBytes = [string]$Computer.TotalPhysicalMemory
        }
    } catch {
        Write-Warning "CIM hardware inventory unavailable: $($_.Exception.Message)"
    }

    $ReportGrade = -not $Calibration
    Write-Utf8Lines (Join-Path $OutputRoot "run.properties") @(
        "commit=$Commit",
        "dirty=$($DirtyLines.Count -gt 0)",
        "reportGrade=$($ReportGrade.ToString().ToLowerInvariant())",
        "taskCount=$TaskCount",
        "tasksPerJob=$TasksPerJob",
        "workerCount=$WorkerCount",
        "seed=$Seed",
        "duplicateBasisPoints=$RequiredDuplicateBasisPoints",
        "delayedResultCount=$DelayedResultCount",
        "workerTerminationCount=$WorkerTerminationCount",
        "brokerRestartAfter=$BrokerRestartAfter",
        "coordinatorRestartAfter=$CoordinatorRestartAfter",
        "leaseMillis=$LeaseMillis",
        "delayedResultMillis=$DelayedResultMillis",
        "completionTimeoutSeconds=$CompletionTimeoutSeconds",
        "outputDirectory=$OutputRoot"
    )
    Write-Utf8Lines (Join-Path $OutputRoot "environment.txt") @(
        "commit: $Commit",
        "dirty: $($DirtyLines.Count -gt 0)",
        "report grade: $ReportGrade",
        "date: $([DateTimeOffset]::Now.ToString('O'))",
        "time zone: $([TimeZoneInfo]::Local.Id)",
        "os: $([System.Environment]::OSVersion.VersionString)",
        "architecture: $([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture)",
        "logical processors: $([System.Environment]::ProcessorCount)",
        "cpu: $CpuName",
        "physical memory bytes: $PhysicalMemoryBytes",
        "docker engine: $DockerVersion",
        "experiment JVM flags: -Xms256m -Xmx2g",
        "",
        "java -version:",
        $JavaVersion,
        "",
        "maven wrapper -version:",
        $MavenVersion
    )

    $MavenLog = Join-Path $OutputRoot "maven.log"
    $Arguments = @(
        "--batch-mode",
        "--no-transfer-progress",
        "-pl", "taskflow-coordinator",
        "-am",
        "-Dtest=CorrectnessChaosExperiment",
        "-Dsurefire.failIfNoSpecifiedTests=false",
        "-Dsurefire.enableOutErrElements=false",
        "-DargLine=-Xms256m -Xmx2g",
        "-Dtaskflow.chaos.reportGrade=$($ReportGrade.ToString().ToLowerInvariant())",
        "-Dtaskflow.chaos.seed=$Seed",
        "-Dtaskflow.chaos.tasks=$TaskCount",
        "-Dtaskflow.chaos.tasksPerJob=$TasksPerJob",
        "-Dtaskflow.chaos.workers=$WorkerCount",
        "-Dtaskflow.chaos.duplicateBasisPoints=$RequiredDuplicateBasisPoints",
        "-Dtaskflow.chaos.delayedResults=$DelayedResultCount",
        "-Dtaskflow.chaos.workerTerminations=$WorkerTerminationCount",
        "-Dtaskflow.chaos.brokerRestartAfter=$BrokerRestartAfter",
        "-Dtaskflow.chaos.coordinatorRestartAfter=$CoordinatorRestartAfter",
        "-Dtaskflow.chaos.leaseMillis=$LeaseMillis",
        "-Dtaskflow.chaos.delayedResultMillis=$DelayedResultMillis",
        "-Dtaskflow.chaos.timeoutSeconds=$CompletionTimeoutSeconds",
        "-Dtaskflow.chaos.output=$OutputRoot",
        "test"
    )

    Write-Host ""
    Write-Host "[correctness-chaos] $MavenWrapper $($Arguments -join ' ')"
    $PreviousLogLevel = $env:TASKFLOW_LOG_LEVEL
    $PreviousTestLogLevel = $env:TASKFLOW_TEST_LOG_LEVEL
    $PreviousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $env:TASKFLOW_LOG_LEVEL = "OFF"
        $env:TASKFLOW_TEST_LOG_LEVEL = "OFF"
        & $MavenWrapper @Arguments 2>&1 |
            ForEach-Object {
                if ($_ -is [System.Management.Automation.ErrorRecord]) {
                    [string]$_.Exception.Message
                } else {
                    [string]$_
                }
            } |
            Tee-Object -FilePath $MavenLog |
            Out-Host
        $ExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $PreviousErrorActionPreference
        if ($null -eq $PreviousLogLevel) {
            Remove-Item Env:TASKFLOW_LOG_LEVEL -ErrorAction SilentlyContinue
        } else {
            $env:TASKFLOW_LOG_LEVEL = $PreviousLogLevel
        }
        if ($null -eq $PreviousTestLogLevel) {
            Remove-Item Env:TASKFLOW_TEST_LOG_LEVEL `
                -ErrorAction SilentlyContinue
        } else {
            $env:TASKFLOW_TEST_LOG_LEVEL = $PreviousTestLogLevel
        }
    }
    if ($ExitCode -ne 0) {
        throw "Correctness chaos failed with exit code $ExitCode. See $MavenLog"
    }

    $Audit = Read-Properties (Join-Path $OutputRoot "audit.properties") `
        "Chaos audit"
    $Configuration = Read-Properties (
        Join-Path $OutputRoot "configuration.properties"
    ) "Chaos configuration"

    Assert-PropertyEquals $Audit "result" "PASS"
    Assert-PropertyEquals $Audit "acceptedTasks" ([string]$TaskCount)
    Assert-PropertyEquals $Audit "completedTasks" ([string]$TaskCount)
    Assert-PropertyEquals $Audit "terminalJobs" (
        [string][Math]::Ceiling($TaskCount / [double]$TasksPerJob)
    )
    Assert-PropertyEquals $Audit "delayedResultsPublished" (
        [string]$DelayedResultCount
    )
    Assert-PropertyEquals $Audit "workerTerminations" (
        [string]$WorkerTerminationCount
    )
    Assert-PropertyEquals $Audit "brokerRestarts" "1"
    Assert-PropertyEquals $Audit "coordinatorRestarts" "1"
    Assert-PropertyEquals $Audit "pendingOutboxAtCompletion" "0"
    Assert-PropertyEquals $Configuration "duplicateBasisPoints" (
        [string]$RequiredDuplicateBasisPoints
    )
    Assert-PropertyEquals $Configuration "reportGrade" (
        $ReportGrade.ToString().ToLowerInvariant()
    )

    if ([long]$Audit.pendingOutboxDuringBrokerOutage -lt 1) {
        throw "The broker outage did not expose a committed pending outbox row."
    }
    if ([long]$Audit.minimumWorkerActiveTasks -lt 0) {
        throw "A negative worker-capacity sample was observed."
    }
    if ([long]$Audit.schedulerDuplicateResults -lt 1) {
        throw "No duplicate task result reached scheduler classification."
    }
    if ([long]$Audit.schedulerStaleResults -lt 1) {
        throw "No beyond-lease result reached stale classification."
    }
    if ([long]$Audit.transportQuarantines -lt 1) {
        throw "The injected poison delivery was not quarantined."
    }
    if ([long]$Audit.poisonDeliveries -gt $ExpectedPoisonDeliveryMaximum) {
        throw "Poison delivery exceeded the configured three-attempt bound."
    }
    if ([long]$Audit.duplicateAssignmentsPublished -ne
        [long]$Audit.duplicateResultsPublished) {
        throw "Assignment/result duplicate counts differ."
    }
    if ($TaskCount % 20 -eq 0) {
        $ExpectedDuplicates = [long]($TaskCount / 20)
        if ([long]$Audit.duplicateAssignmentsPublished -ne
            $ExpectedDuplicates) {
            throw "Expected exactly $ExpectedDuplicates assignment/result duplicates."
        }
    }

    $EventsPath = Join-Path $OutputRoot "events.jsonl"
    if (-not (Test-Path -LiteralPath $EventsPath)) {
        throw "Structured event output was not created: $EventsPath"
    }
    $Events = Get-Content -LiteralPath $EventsPath -Encoding UTF8
    foreach ($RequiredEvent in @(
        "experiment_started",
        "workload_accepted",
        "worker_termination_started",
        "worker_termination_completed",
        "broker_restart_started",
        "broker_restart_completed",
        "coordinator_restart_started",
        "coordinator_restart_completed",
        "delayed_result_published",
        "experiment_completed"
    )) {
        if (-not ($Events | Select-String -SimpleMatch `
            "`"event`":`"$RequiredEvent`"")) {
            throw "Structured output is missing event: $RequiredEvent"
        }
    }

    $SummaryPath = Join-Path $OutputRoot "summary.md"
    Write-Utf8Lines $SummaryPath @(
        "# TF-0706 correctness chaos run",
        "",
        "- Result: ``PASS``",
        "- Commit: ``$Commit``",
        "- Dirty worktree: ``$($DirtyLines.Count -gt 0)``",
        "- Report grade: ``$ReportGrade``",
        "- Seed: ``$Seed``",
        "- Accepted/completed tasks: ``$($Audit.acceptedTasks) / $($Audit.completedTasks)``",
        "- Terminal jobs: ``$($Audit.terminalJobs)``",
        "- Duplicate assignment/result publications: ``$($Audit.duplicateAssignmentsPublished) / $($Audit.duplicateResultsPublished)``",
        "- Scheduler duplicate/stale classifications: ``$($Audit.schedulerDuplicateResults) / $($Audit.schedulerStaleResults)``",
        "- Worker/broker/coordinator restarts: ``$($Audit.workerTerminations) / $($Audit.brokerRestarts) / $($Audit.coordinatorRestarts)``",
        "- Pending outbox during outage/at completion: ``$($Audit.pendingOutboxDuringBrokerOutage) / $($Audit.pendingOutboxAtCompletion)``",
        "- Poison deliveries/quarantines: ``$($Audit.poisonDeliveries) / $($Audit.transportQuarantines)``",
        "- Minimum active-task capacity sample: ``$($Audit.minimumWorkerActiveTasks)``",
        "- Elapsed milliseconds: ``$($Audit.elapsedMillis)``",
        "",
        "Raw files: ``configuration.properties``, ``audit.properties``,",
        "``events.jsonl``, ``correctness-chaos.db``, ``environment.txt``,",
        "``run.properties``, and ``maven.log``."
    )

    $ChecksumNames = @(
        "audit.properties",
        "configuration.properties",
        "correctness-chaos.db",
        "environment.txt",
        "events.jsonl",
        "maven.log",
        "run.properties",
        "summary.md"
    )
    $ChecksumLines = foreach ($Name in $ChecksumNames) {
        $Hash = Get-FileHash -LiteralPath (Join-Path $OutputRoot $Name) `
            -Algorithm SHA256
        "$($Hash.Hash.ToLowerInvariant())  $Name"
    }
    Write-Utf8Lines (Join-Path $OutputRoot "checksums.sha256") $ChecksumLines

    Write-Host ""
    Write-Host "Correctness chaos PASS"
    Write-Host "Summary: $SummaryPath"
} finally {
    Pop-Location
}
