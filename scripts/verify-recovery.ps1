[CmdletBinding()]
param(
    [switch]$Calibration,
    [switch]$AllowDirty,
    [int]$CoordinatorRestartTasks = 1000,
    [int]$SmallPersistedTasks = 10000,
    [int]$LargePersistedTasks = 100000,
    [int]$TasksPerJob = 250,
    [int]$OutboxMessages = 500,
    [int]$OrphanObjects = 1000,
    [long]$WorkerFailureTimeoutMillis = 90000,
    [long]$TaskLeaseMillis = 1000,
    [int]$BatchSize = 100,
    [long]$CompletionTimeoutSeconds = 900,
    [string]$OutputDirectory = "target/recovery/report"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
if (Test-Path variable:PSNativeCommandUseErrorActionPreference) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$ReportCoordinatorRestartTasks = 1000
$ReportSmallPersistedTasks = 10000
$ReportLargePersistedTasks = 100000
$ReportTasksPerJob = 250
$ReportOutboxMessages = 500
$ReportOrphanObjects = 1000
$ReportWorkerFailureTimeoutMillis = 90000
$ReportTaskLeaseMillis = 1000
$ReportBatchSize = 100
$ReportCompletionTimeoutSeconds = 900
$CoordinatorJvmFlags = "-Xms256m -Xmx2g"

function Assert-Range {
    param(
        [Parameter(Mandatory = $true)][long]$Value,
        [Parameter(Mandatory = $true)][long]$Minimum,
        [Parameter(Mandatory = $true)][long]$Maximum,
        [Parameter(Mandatory = $true)][string]$Name
    )
    if ($Value -lt $Minimum -or $Value -gt $Maximum) {
        throw "$Name must be in [$Minimum, $Maximum]."
    }
}

Assert-Range $CoordinatorRestartTasks 1 10000 "CoordinatorRestartTasks"
Assert-Range $SmallPersistedTasks 1 100000 "SmallPersistedTasks"
Assert-Range $LargePersistedTasks $SmallPersistedTasks 200000 `
    "LargePersistedTasks"
Assert-Range $TasksPerJob 1 1000 "TasksPerJob"
Assert-Range $OutboxMessages 1 20000 "OutboxMessages"
Assert-Range $OrphanObjects 2 5000 "OrphanObjects"
Assert-Range $WorkerFailureTimeoutMillis 1 120000 `
    "WorkerFailureTimeoutMillis"
Assert-Range $TaskLeaseMillis 1 120000 "TaskLeaseMillis"
Assert-Range $BatchSize 2 1000 "BatchSize"
Assert-Range $CompletionTimeoutSeconds 10 3600 `
    "CompletionTimeoutSeconds"

if (-not $Calibration -and $AllowDirty) {
    throw "AllowDirty is available only with Calibration."
}
if (-not $Calibration) {
    $Expected = @(
        @($CoordinatorRestartTasks, $ReportCoordinatorRestartTasks,
            "CoordinatorRestartTasks"),
        @($SmallPersistedTasks, $ReportSmallPersistedTasks,
            "SmallPersistedTasks"),
        @($LargePersistedTasks, $ReportLargePersistedTasks,
            "LargePersistedTasks"),
        @($TasksPerJob, $ReportTasksPerJob, "TasksPerJob"),
        @($OutboxMessages, $ReportOutboxMessages, "OutboxMessages"),
        @($OrphanObjects, $ReportOrphanObjects, "OrphanObjects"),
        @($WorkerFailureTimeoutMillis,
            $ReportWorkerFailureTimeoutMillis,
            "WorkerFailureTimeoutMillis"),
        @($TaskLeaseMillis, $ReportTaskLeaseMillis, "TaskLeaseMillis"),
        @($BatchSize, $ReportBatchSize, "BatchSize"),
        @($CompletionTimeoutSeconds,
            $ReportCompletionTimeoutSeconds,
            "CompletionTimeoutSeconds")
    )
    foreach ($Entry in $Expected) {
        if ([long]$Entry[0] -ne [long]$Entry[1]) {
            throw "Report-grade recovery requires $($Entry[2])=$($Entry[1])."
        }
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
if (Test-Path -LiteralPath $OutputRoot) {
    throw "OutputDirectory already exists. Choose a fresh directory: $OutputRoot"
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
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
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
        throw "Recovery property is missing: $Name"
    }
    if ([string]$Properties[$Name] -ne $Expected) {
        throw "Recovery property $Name expected $Expected but observed $($Properties[$Name])."
    }
}

function Assert-PositiveProperty {
    param(
        [Parameter(Mandatory = $true)][hashtable]$Properties,
        [Parameter(Mandatory = $true)][string]$Name
    )
    if (-not $Properties.ContainsKey($Name) -or
        [double]$Properties[$Name] -le 0.0) {
        throw "Recovery property $Name must be positive."
    }
}

function Count-Lines {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Expected recovery artifact is missing: $Path"
    }
    return @(Get-Content -LiteralPath $Path -Encoding UTF8).Count
}

function Convert-ToInvariantDecimal {
    param([Parameter(Mandatory = $true)][double]$Value)
    return $Value.ToString(
        "0.000",
        [Globalization.CultureInfo]::InvariantCulture
    )
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
        throw "Report-grade recovery requires a clean checkout. Use Calibration with AllowDirty only for harness development."
    }

    $JavaVersion = Invoke-NativeText "java" @("-version") "java -version"
    $JavaMatch = [regex]::Match($JavaVersion, 'version "(?<major>\d+)')
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

    [System.IO.Directory]::CreateDirectory($OutputRoot) | Out-Null
    $RunDirectory = Join-Path $OutputRoot "run"
    $MavenLog = Join-Path $OutputRoot "recovery.maven.log"
    $ReportGrade = -not $Calibration
    $StartedAt = [DateTimeOffset]::Now

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

    Write-Utf8Lines (Join-Path $OutputRoot "run.properties") @(
        "commit=$Commit",
        "dirty=$($DirtyLines.Count -gt 0)",
        "reportGrade=$($ReportGrade.ToString().ToLowerInvariant())",
        "coordinatorRestartTasks=$CoordinatorRestartTasks",
        "smallPersistedTasks=$SmallPersistedTasks",
        "largePersistedTasks=$LargePersistedTasks",
        "tasksPerJob=$TasksPerJob",
        "outboxMessages=$OutboxMessages",
        "orphanObjects=$OrphanObjects",
        "workerFailureTimeoutMillis=$WorkerFailureTimeoutMillis",
        "taskLeaseMillis=$TaskLeaseMillis",
        "batchSize=$BatchSize",
        "completionTimeoutSeconds=$CompletionTimeoutSeconds",
        "coordinatorJvmFlags=$CoordinatorJvmFlags",
        "startedAt=$($StartedAt.ToString('O'))",
        "outputDirectory=$OutputRoot"
    )
    Write-Utf8Lines (Join-Path $OutputRoot "environment.txt") @(
        "commit: $Commit",
        "dirty: $($DirtyLines.Count -gt 0)",
        "report grade: $ReportGrade",
        "date: $($StartedAt.ToString('O'))",
        "time zone: $([TimeZoneInfo]::Local.Id)",
        "os: $([System.Environment]::OSVersion.VersionString)",
        "architecture: $([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture)",
        "logical processors: $([System.Environment]::ProcessorCount)",
        "cpu: $CpuName",
        "physical memory bytes: $PhysicalMemoryBytes",
        "docker engine: $DockerVersion",
        "coordinator-harness JVM flags: $CoordinatorJvmFlags",
        "RabbitMQ image: rabbitmq:3.13-management",
        "Toxiproxy image: ghcr.io/shopify/toxiproxy:2.12.0",
        "MinIO image: minio/minio:RELEASE.2025-04-22T22-12-26Z",
        "",
        "java -version:",
        $JavaVersion,
        "",
        "maven wrapper -version:",
        $MavenVersion
    )

    $Arguments = @(
        "--batch-mode",
        "--no-transfer-progress",
        "-pl", "taskflow-coordinator",
        "-am",
        "-Dtest=RecoveryExperiment",
        "-Dsurefire.failIfNoSpecifiedTests=false",
        "-Dsurefire.enableOutErrElements=false",
        "-DTASKFLOW_LOG_LEVEL=OFF",
        "-DTASKFLOW_TEST_LOG_LEVEL=OFF",
        "-DargLine=$CoordinatorJvmFlags",
        "-Dtaskflow.recovery.reportGrade=$($ReportGrade.ToString().ToLowerInvariant())",
        "-Dtaskflow.recovery.outputDirectory=$RunDirectory",
        "-Dtaskflow.recovery.coordinatorRestartTasks=$CoordinatorRestartTasks",
        "-Dtaskflow.recovery.smallPersistedTasks=$SmallPersistedTasks",
        "-Dtaskflow.recovery.largePersistedTasks=$LargePersistedTasks",
        "-Dtaskflow.recovery.tasksPerJob=$TasksPerJob",
        "-Dtaskflow.recovery.outboxMessages=$OutboxMessages",
        "-Dtaskflow.recovery.orphanObjects=$OrphanObjects",
        "-Dtaskflow.recovery.workerFailureTimeoutMillis=$WorkerFailureTimeoutMillis",
        "-Dtaskflow.recovery.taskLeaseMillis=$TaskLeaseMillis",
        "-Dtaskflow.recovery.batchSize=$BatchSize",
        "-Dtaskflow.recovery.completionTimeoutSeconds=$CompletionTimeoutSeconds",
        "test"
    )

    Write-Host "[recovery] $MavenWrapper $($Arguments -join ' ')"
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
        throw "Recovery experiment failed with exit code $ExitCode. See $MavenLog"
    }

    $Metrics = Read-Properties (
        Join-Path $RunDirectory "metrics.properties"
    ) "recovery metrics"
    $Configuration = Read-Properties (
        Join-Path $RunDirectory "configuration.properties"
    ) "recovery configuration"
    $Audit = Read-Properties (
        Join-Path $RunDirectory "audit.properties"
    ) "recovery audit"

    Assert-PropertyEquals $Metrics "result" "PASS"
    Assert-PropertyEquals $Metrics "reportGrade" (
        $ReportGrade.ToString().ToLowerInvariant()
    )
    Assert-PropertyEquals $Configuration "result" "PASS"
    Assert-PropertyEquals $Configuration "coordinatorRestartTaskCount" (
        [string]$CoordinatorRestartTasks
    )
    Assert-PropertyEquals $Configuration "smallPersistedTaskCount" (
        [string]$SmallPersistedTasks
    )
    Assert-PropertyEquals $Configuration "largePersistedTaskCount" (
        [string]$LargePersistedTasks
    )
    Assert-PropertyEquals $Configuration "outboxMessageCount" (
        [string]$OutboxMessages
    )
    Assert-PropertyEquals $Configuration "orphanObjectCount" (
        [string]$OrphanObjects
    )
    Assert-PropertyEquals $Audit "coordinatorRestartRecoveredTasks" (
        [string]$CoordinatorRestartTasks
    )
    Assert-PropertyEquals $Audit "persistedSmallRecoveredTasks" (
        [string]$SmallPersistedTasks
    )
    Assert-PropertyEquals $Audit "persistedLargeRecoveredTasks" (
        [string]$LargePersistedTasks
    )
    Assert-PropertyEquals $Audit "leaseAttemptRows" "2"
    if ([long]$Audit.leaseReassignedAtEpochMillis -lt
        [long]$Audit.leaseExpiredAtEpochMillis) {
        throw "Lease reassignment preceded the durable expiry deadline."
    }
    Assert-PropertyEquals $Audit "outboxRestartRows" ([string]$BatchSize)
    Assert-PropertyEquals $Audit "outboxSteadyRows" ([string]$OutboxMessages)
    Assert-PropertyEquals $Audit "outboxRawDeliveries" (
        [string]($OutboxMessages + $BatchSize)
    )
    Assert-PropertyEquals $Audit "outboxUniqueDeliveries" (
        [string]($OutboxMessages + $BatchSize)
    )
    Assert-PropertyEquals $Audit "outboxSteadyPending" "0"
    Assert-PropertyEquals $Audit "outboxSteadySent" ([string]$OutboxMessages)
    Assert-PropertyEquals $Audit "outboxRestartPending" "0"
    Assert-PropertyEquals $Audit "outboxRestartSent" ([string]$BatchSize)
    Assert-PropertyEquals $Audit "orphanGcRetryRows" "0"
    foreach ($Integrity in @(
        "coordinatorIntegrity",
        "persistedSmallIntegrity",
        "persistedLargeIntegrity",
        "outboxSteadyIntegrity",
        "outboxRestartIntegrity",
        "orphanIntegrity"
    )) {
        Assert-PropertyEquals $Audit $Integrity "ok"
    }
    if ([int]$Audit.orphanGcMaximumExaminedInBatch -gt $BatchSize) {
        throw "Orphan GC exceeded its configured batch size."
    }
    foreach ($Metric in @(
        "workerFailureDetectionMillis",
        "coordinatorRestartRecoveryMillis",
        "persisted10000RecoveryMillis",
        "persisted100000RecoveryMillis",
        "rabbitMqRestartRecoveryMillis",
        "outboxReplayMillis",
        "outboxReplayRowsPerSecond",
        "objectOrphanCleanupMillis",
        "objectOrphanCleanupRowsPerSecond"
    )) {
        Assert-PositiveProperty $Metrics $Metric
    }
    if ([double]$Metrics.leaseExpiryToReassignmentMillis -lt 0.0) {
        throw "Lease expiry-to-reassignment time must not be negative."
    }
    Assert-PropertyEquals $Metrics "outboxReplayRows" (
        [string]$OutboxMessages
    )
    Assert-PropertyEquals $Metrics "objectOrphansDeleted" (
        [string]$OrphanObjects
    )

    $DeliveryLines = Count-Lines (
        Join-Path $RunDirectory "outbox-deliveries.txt"
    )
    if ($DeliveryLines -ne ($OutboxMessages + $BatchSize)) {
        throw "Outbox delivery artifact cardinality is incorrect."
    }
    $ObjectLines = Count-Lines (
        Join-Path $RunDirectory "orphan-object-keys.txt"
    )
    if ($ObjectLines -ne $OrphanObjects) {
        throw "Orphan object artifact cardinality is incorrect."
    }
    $LeaseLines = Count-Lines (
        Join-Path $RunDirectory "lease-assignments.csv"
    )
    if ($LeaseLines -ne 3) {
        throw "Lease assignment artifact must contain two data rows."
    }

    foreach ($Name in @(
        "coordinator-restart.db",
        "persisted-$SmallPersistedTasks.db",
        "persisted-$LargePersistedTasks.db",
        "lease-reassignment.db",
        "outbox-replay.db",
        "rabbitmq-restart.db",
        "orphan-cleanup.db"
    )) {
        $Path = Join-Path $RunDirectory $Name
        if (-not (Test-Path -LiteralPath $Path -PathType Leaf) -or
            (Get-Item -LiteralPath $Path).Length -le 0) {
            throw "Recovery database artifact is missing or empty: $Path"
        }
    }

    $FinishedAt = [DateTimeOffset]::Now
    $ElapsedSeconds = ($FinishedAt - $StartedAt).TotalSeconds
    Write-Utf8Lines (Join-Path $OutputRoot "summary.md") @(
        "# TF-0708 recovery experiment",
        "",
        "- Result: ``PASS``",
        "- Commit: ``$Commit``",
        "- Dirty worktree: ``$($DirtyLines.Count -gt 0)``",
        "- Report grade: ``$ReportGrade``",
        "- Wrapper elapsed seconds: ``$(Convert-ToInvariantDecimal $ElapsedSeconds)``",
        "",
        "| Measurement | Result |",
        "|---|---:|",
        "| Worker failure detection | $($Metrics.workerFailureDetectionMillis) ms |",
        "| Lease expiry to reassignment | $($Metrics.leaseExpiryToReassignmentMillis) ms |",
        "| Coordinator restart ($CoordinatorRestartTasks tasks) | $($Metrics.coordinatorRestartRecoveryMillis) ms |",
        "| Persisted $SmallPersistedTasks-task recovery | $($Metrics.persisted10000RecoveryMillis) ms |",
        "| Persisted $LargePersistedTasks-task recovery | $($Metrics.persisted100000RecoveryMillis) ms |",
        "| RabbitMQ restart and $BatchSize-row replay | $($Metrics.rabbitMqRestartRecoveryMillis) ms |",
        "| Outbox replay | $($Metrics.outboxReplayRowsPerSecond) rows/s ($OutboxMessages rows) |",
        "| Object-orphan cleanup | $($Metrics.objectOrphanCleanupRowsPerSecond) objects/s ($OrphanObjects objects) |"
    )
    Write-Utf8Lines (Join-Path $OutputRoot "finished.properties") @(
        "result=PASS",
        "finishedAt=$($FinishedAt.ToString('O'))",
        "elapsedSeconds=$(Convert-ToInvariantDecimal $ElapsedSeconds)"
    )

    $ChecksumLines = Get-ChildItem -LiteralPath $OutputRoot -File -Recurse |
        Where-Object { $_.Name -ne "checksums.sha256" } |
        Sort-Object FullName |
        ForEach-Object {
            $Relative = $_.FullName.Substring($OutputRoot.Length).TrimStart(
                [char[]]@("\", "/")
            ).Replace("\", "/")
            $Hash = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
            "$Hash  $Relative"
        }
    Write-Utf8Lines (Join-Path $OutputRoot "checksums.sha256") `
        $ChecksumLines

    foreach ($Line in $ChecksumLines) {
        if ($Line -notmatch '^(?<hash>[0-9a-f]{64})  (?<path>.+)$') {
            throw "Invalid checksum manifest line: $Line"
        }
        $File = Join-Path $OutputRoot $Matches.path
        $Observed = (
            Get-FileHash -LiteralPath $File -Algorithm SHA256
        ).Hash.ToLowerInvariant()
        if ($Observed -ne $Matches.hash) {
            throw "Checksum mismatch for $($Matches.path)."
        }
    }
    Write-Host "TF-0708 recovery experiment PASS"
    Write-Host "Evidence: $OutputRoot"
} finally {
    Pop-Location
}
