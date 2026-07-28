[CmdletBinding()]
param(
    [switch]$Calibration,
    [switch]$AllowDirty,
    [int]$TaskCount = 10000,
    [int]$WarmupTaskCount = 1000,
    [int]$TasksPerJob = 250,
    [int]$WorkUnitsPerTask = 300000,
    [int]$PayloadBytes = 128,
    [long]$CompletionTimeoutSeconds = 900,
    [long]$SampleIntervalMillis = 100,
    [string]$OutputDirectory = "target/scaling"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
if (Test-Path variable:PSNativeCommandUseErrorActionPreference) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$WorkerMatrix = @(1, 2, 4, 8)
$ReportTaskMinimum = 10000
$CoordinatorJvmFlags = "-Xms256m -Xmx1g"
$WorkerJvmFlags = "-Xms32m -Xmx128m"

if ($TaskCount -lt 1 -or $TaskCount -gt 100000) {
    throw "TaskCount must be in [1, 100000]."
}
if ($WarmupTaskCount -lt 1 -or $WarmupTaskCount -gt $TaskCount) {
    throw "WarmupTaskCount must be positive and no larger than TaskCount."
}
if ($TasksPerJob -lt 1 -or $TasksPerJob -gt 1000) {
    throw "TasksPerJob must be in [1, 1000]."
}
if ($WorkUnitsPerTask -lt 1 -or $WorkUnitsPerTask -gt 1000000) {
    throw "WorkUnitsPerTask must be in [1, 1000000]."
}
if ($PayloadBytes -lt 16 -or $PayloadBytes -gt 32768) {
    throw "PayloadBytes must be in [16, 32768]."
}
if ($CompletionTimeoutSeconds -lt 1 -or
    $CompletionTimeoutSeconds -gt 3600) {
    throw "CompletionTimeoutSeconds must be in [1, 3600]."
}
if ($SampleIntervalMillis -lt 25 -or $SampleIntervalMillis -gt 1000) {
    throw "SampleIntervalMillis must be in [25, 1000]."
}
if (-not $Calibration -and $AllowDirty) {
    throw "AllowDirty is available only with Calibration."
}
if (-not $Calibration -and $TaskCount -lt $ReportTaskMinimum) {
    throw "Report-grade execution requires at least $ReportTaskMinimum tasks."
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
        throw "Scaling property is missing: $Name"
    }
    if ([string]$Properties[$Name] -ne $Expected) {
        throw "Scaling property $Name expected $Expected but observed $($Properties[$Name])."
    }
}

function Assert-PositiveProperty {
    param(
        [Parameter(Mandatory = $true)][hashtable]$Properties,
        [Parameter(Mandatory = $true)][string]$Name
    )
    if (-not $Properties.ContainsKey($Name) -or
        [double]$Properties[$Name] -le 0.0) {
        throw "Scaling property $Name must be positive."
    }
}

function Assert-CsvRows {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][long]$ExpectedRows
    )
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Expected CSV was not created: $Path"
    }
    $Rows = @(Get-Content -LiteralPath $Path -Encoding UTF8).Count - 1
    if ($Rows -ne $ExpectedRows) {
        throw "$Path expected $ExpectedRows data rows but observed $Rows."
    }
}

function Convert-ToInvariantDecimal {
    param([Parameter(Mandatory = $true)][double]$Value)
    return $Value.ToString("0.000", [Globalization.CultureInfo]::InvariantCulture)
}

function Invoke-ScalingPoint {
    param(
        [Parameter(Mandatory = $true)][int]$Workers,
        [Parameter(Mandatory = $true)][bool]$ReportGrade
    )
    $PointDirectory = Join-Path $OutputRoot "workers-$Workers"
    $MavenLog = Join-Path $OutputRoot "workers-$Workers.maven.log"
    if (Test-Path -LiteralPath $PointDirectory) {
        throw "Scaling point output already exists: $PointDirectory"
    }
    if (Test-Path -LiteralPath $MavenLog) {
        throw "Scaling point Maven log already exists: $MavenLog"
    }

    $Arguments = @(
        "--batch-mode",
        "--no-transfer-progress",
        "-pl", "taskflow-coordinator",
        "-am",
        "-Dtest=ScalingExperiment",
        "-Dsurefire.failIfNoSpecifiedTests=false",
        "-Dsurefire.enableOutErrElements=false",
        "-DargLine=$CoordinatorJvmFlags",
        "-Dtaskflow.scaling.reportGrade=$($ReportGrade.ToString().ToLowerInvariant())",
        "-Dtaskflow.scaling.workers=$Workers",
        "-Dtaskflow.scaling.tasks=$TaskCount",
        "-Dtaskflow.scaling.warmupTasks=$WarmupTaskCount",
        "-Dtaskflow.scaling.tasksPerJob=$TasksPerJob",
        "-Dtaskflow.scaling.workUnits=$WorkUnitsPerTask",
        "-Dtaskflow.scaling.payloadBytes=$PayloadBytes",
        "-Dtaskflow.scaling.timeoutSeconds=$CompletionTimeoutSeconds",
        "-Dtaskflow.scaling.sampleIntervalMillis=$SampleIntervalMillis",
        "-Dtaskflow.scaling.output=$PointDirectory",
        "test"
    )

    Write-Host ""
    Write-Host "[scaling-$Workers-workers] $MavenWrapper $($Arguments -join ' ')"
    $PreviousLogLevel = $env:TASKFLOW_LOG_LEVEL
    $PreviousTestLogLevel = $env:TASKFLOW_TEST_LOG_LEVEL
    $PreviousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $env:TASKFLOW_LOG_LEVEL = "WARN"
        $env:TASKFLOW_TEST_LOG_LEVEL = "WARN"
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
        throw "Scaling point $Workers failed with exit code $ExitCode. See $MavenLog"
    }

    $Metrics = Read-Properties (
        Join-Path $PointDirectory "metrics.properties"
    ) "$Workers-worker metrics"
    $Configuration = Read-Properties (
        Join-Path $PointDirectory "configuration.properties"
    ) "$Workers-worker configuration"

    Assert-PropertyEquals $Metrics "result" "PASS"
    Assert-PropertyEquals $Metrics "reportGrade" (
        $ReportGrade.ToString().ToLowerInvariant()
    )
    Assert-PropertyEquals $Metrics "workerCount" ([string]$Workers)
    Assert-PropertyEquals $Metrics "taskCount" ([string]$TaskCount)
    Assert-PropertyEquals $Metrics "warmupTaskCount" (
        [string]$WarmupTaskCount
    )
    Assert-PropertyEquals $Metrics "tasksPerJob" ([string]$TasksPerJob)
    Assert-PropertyEquals $Metrics "workUnitsPerTask" (
        [string]$WorkUnitsPerTask
    )
    Assert-PropertyEquals $Metrics "payloadBytes" ([string]$PayloadBytes)
    Assert-PropertyEquals $Metrics "completedTasks" ([string]$TaskCount)
    Assert-PropertyEquals $Metrics "terminalJobs" (
        [string][Math]::Ceiling($TaskCount / [double]$TasksPerJob)
    )
    Assert-PropertyEquals $Metrics "authoritativeAttemptMismatches" "0"
    Assert-PropertyEquals $Metrics "pendingOutboxAtCompletion" "0"
    Assert-PropertyEquals $Metrics "rabbitMqQueueDepthAtCompletion" "0"
    Assert-PropertyEquals $Configuration "workerCount" ([string]$Workers)
    Assert-PropertyEquals $Configuration "reportGrade" (
        $ReportGrade.ToString().ToLowerInvariant()
    )
    Assert-PositiveProperty $Metrics "throughputTasksPerSecond"
    Assert-PositiveProperty $Metrics "taskCompletionDurationNanos"
    Assert-PositiveProperty $Metrics "coordinatorProcessCpuNanos"
    Assert-PositiveProperty $Metrics "coordinatorPeakHeapBytes"
    Assert-PositiveProperty $Metrics "sqliteWriteCount"
    Assert-PositiveProperty $Metrics "resourceSampleCount"

    if ([long]$Metrics.workerExecutionCount -lt $TaskCount) {
        throw "Worker execution count is below authoritative task count."
    }
    if ([long]$Metrics.workerDuplicateExecutions -ne
        ([long]$Metrics.workerExecutionCount - $TaskCount)) {
        throw "Worker duplicate-execution accounting is inconsistent."
    }

    Assert-CsvRows (
        Join-Path $PointDirectory "task-latencies.csv"
    ) $TaskCount
    Assert-CsvRows (
        Join-Path $PointDirectory "sqlite-writes.csv"
    ) ([long]$Metrics.sqliteWriteCount)
    Assert-CsvRows (
        Join-Path $PointDirectory "resource-samples.csv"
    ) ([long]$Metrics.resourceSampleCount)
    Assert-CsvRows (
        Join-Path $PointDirectory "worker-metrics.csv"
    ) $Workers

    for ($Index = 0; $Index -lt $Workers; $Index++) {
        $WorkerDirectory = Join-Path $PointDirectory "scaling-worker-$Index"
        foreach ($Name in @(
            "metrics.properties",
            "ready.signal",
            "stop.signal",
            "worker.log"
        )) {
            if (-not (Test-Path -LiteralPath (
                Join-Path $WorkerDirectory $Name
            ) -PathType Leaf)) {
                throw "Worker evidence is missing: $WorkerDirectory\$Name"
            }
        }
        if (Test-Path -LiteralPath (
            Join-Path $WorkerDirectory "failure.signal"
        )) {
            throw "Worker failure signal exists: scaling-worker-$Index"
        }
    }
    return $Metrics
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
        throw "Report-grade scaling requires a clean checkout. Use Calibration with AllowDirty only for harness development."
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

    $ExistingEvidence = @(
        @(
            "run.properties",
            "environment.txt",
            "matrix.csv",
            "summary.md",
            "checksums.sha256"
        ) | Where-Object {
            Test-Path -LiteralPath (Join-Path $OutputRoot $_)
        }
    )
    foreach ($Workers in $WorkerMatrix) {
        if (Test-Path -LiteralPath (
            Join-Path $OutputRoot "workers-$Workers"
        )) {
            $ExistingEvidence += "workers-$Workers"
        }
        if (Test-Path -LiteralPath (
            Join-Path $OutputRoot "workers-$Workers.maven.log"
        )) {
            $ExistingEvidence += "workers-$Workers.maven.log"
        }
    }
    if ($ExistingEvidence.Count -gt 0) {
        throw "OutputDirectory already contains scaling evidence ($($ExistingEvidence -join ', ')). Choose a fresh directory."
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
    $StartedAt = [DateTimeOffset]::Now
    Write-Utf8Lines (Join-Path $OutputRoot "run.properties") @(
        "commit=$Commit",
        "dirty=$($DirtyLines.Count -gt 0)",
        "reportGrade=$($ReportGrade.ToString().ToLowerInvariant())",
        "workerMatrix=$($WorkerMatrix -join ',')",
        "taskCount=$TaskCount",
        "warmupTaskCount=$WarmupTaskCount",
        "tasksPerJob=$TasksPerJob",
        "workUnitsPerTask=$WorkUnitsPerTask",
        "payloadBytes=$PayloadBytes",
        "completionTimeoutSeconds=$CompletionTimeoutSeconds",
        "sampleIntervalMillis=$SampleIntervalMillis",
        "coordinatorJvmFlags=$CoordinatorJvmFlags",
        "workerJvmFlags=$WorkerJvmFlags",
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
        "worker JVM flags: $WorkerJvmFlags",
        "RabbitMQ image: rabbitmq:3.13-management",
        "",
        "java -version:",
        $JavaVersion,
        "",
        "maven wrapper -version:",
        $MavenVersion
    )

    $MetricsByWorker = @{}
    foreach ($Workers in $WorkerMatrix) {
        $MetricsByWorker[$Workers] = Invoke-ScalingPoint `
            -Workers $Workers `
            -ReportGrade $ReportGrade
    }

    $OneWorkerThroughput = [double](
        $MetricsByWorker[1].throughputTasksPerSecond
    )
    $MatrixLines = @(
        "workers,throughput_tasks_per_second,parallel_efficiency_percent,assignment_p50_ms,assignment_p95_ms,assignment_p99_ms,end_to_end_p50_ms,end_to_end_p95_ms,end_to_end_p99_ms,coordinator_cpu_core_percent,coordinator_cpu_host_percent,coordinator_peak_heap_bytes,worker_utilization_percent,worker_duplicate_executions,rabbitmq_depth_p50,rabbitmq_depth_p95,rabbitmq_depth_p99,rabbitmq_depth_max,sqlite_write_p50_ms,sqlite_write_p95_ms,sqlite_write_p99_ms,completion_duration_nanos"
    )
    $SummaryRows = @()
    foreach ($Workers in $WorkerMatrix) {
        $Metrics = $MetricsByWorker[$Workers]
        $Efficiency = [double]$Metrics.throughputTasksPerSecond `
            / ($OneWorkerThroughput * $Workers) * 100.0
        $EfficiencyText = Convert-ToInvariantDecimal $Efficiency
        $MatrixLines += (
            @(
                $Workers,
                $Metrics.throughputTasksPerSecond,
                $EfficiencyText,
                $Metrics.assignmentLatencyP50Millis,
                $Metrics.assignmentLatencyP95Millis,
                $Metrics.assignmentLatencyP99Millis,
                $Metrics.endToEndTaskLatencyP50Millis,
                $Metrics.endToEndTaskLatencyP95Millis,
                $Metrics.endToEndTaskLatencyP99Millis,
                $Metrics.coordinatorProcessCpuCorePercent,
                $Metrics.coordinatorProcessCpuHostPercent,
                $Metrics.coordinatorPeakHeapBytes,
                $Metrics.workerUtilizationPercent,
                $Metrics.workerDuplicateExecutions,
                $Metrics.rabbitMqQueueDepthP50,
                $Metrics.rabbitMqQueueDepthP95,
                $Metrics.rabbitMqQueueDepthP99,
                $Metrics.rabbitMqQueueDepthMax,
                $Metrics.sqliteWriteLatencyP50Millis,
                $Metrics.sqliteWriteLatencyP95Millis,
                $Metrics.sqliteWriteLatencyP99Millis,
                $Metrics.taskCompletionDurationNanos
            ) -join ","
        )
        $SummaryRows += "| $Workers | $($Metrics.throughputTasksPerSecond) | $EfficiencyText | $($Metrics.assignmentLatencyP50Millis) / $($Metrics.assignmentLatencyP95Millis) / $($Metrics.assignmentLatencyP99Millis) | $($Metrics.endToEndTaskLatencyP50Millis) / $($Metrics.endToEndTaskLatencyP95Millis) / $($Metrics.endToEndTaskLatencyP99Millis) | $($Metrics.coordinatorProcessCpuCorePercent) | $($Metrics.coordinatorPeakHeapBytes) | $($Metrics.workerUtilizationPercent) |"
    }
    Write-Utf8Lines (Join-Path $OutputRoot "matrix.csv") $MatrixLines

    $FinishedAt = [DateTimeOffset]::Now
    $ElapsedSeconds = ($FinishedAt - $StartedAt).TotalSeconds
    $SummaryLines = @(
        "# TF-0707 scaling matrix",
        "",
        "- Result: ``PASS``",
        "- Commit: ``$Commit``",
        "- Dirty worktree: ``$($DirtyLines.Count -gt 0)``",
        "- Report grade: ``$ReportGrade``",
        "- Matrix: ``$($WorkerMatrix -join ', ')`` workers",
        "- Workload per point: ``$TaskCount`` measured tasks after ``$WarmupTaskCount`` warm-up tasks",
        "- Task shape: ``$WorkUnitsPerTask`` mix iterations, ``$PayloadBytes``-byte payload",
        "- Wrapper elapsed seconds: ``$(Convert-ToInvariantDecimal $ElapsedSeconds)``",
        "",
        "| Workers | Throughput tasks/s | Efficiency % | Assignment p50/p95/p99 ms | End-to-end p50/p95/p99 ms | Coordinator CPU core % | Peak heap bytes | Worker utilization % |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|"
    )
    $SummaryLines += $SummaryRows
    $SummaryLines += @(
        "",
        "Raw metrics, CSV samples, SQLite databases, worker-process evidence,",
        "and Maven logs are under the corresponding ``workers-N`` paths."
    )
    Write-Utf8Lines (Join-Path $OutputRoot "summary.md") $SummaryLines

    $ChecksumFiles = Get-ChildItem -LiteralPath $OutputRoot -Recurse -File |
        Where-Object { $_.Name -ne "checksums.sha256" } |
        Sort-Object FullName
    $ChecksumLines = foreach ($File in $ChecksumFiles) {
        $Relative = $File.FullName.Substring($OutputRoot.Length).TrimStart("\", "/").Replace("\", "/")
        $Hash = Get-FileHash -LiteralPath $File.FullName -Algorithm SHA256
        "$($Hash.Hash.ToLowerInvariant())  $Relative"
    }
    Write-Utf8Lines (
        Join-Path $OutputRoot "checksums.sha256"
    ) $ChecksumLines

    Write-Host ""
    Write-Host "Scaling matrix PASS"
    Write-Host "Summary: $(Join-Path $OutputRoot 'summary.md')"
} finally {
    Pop-Location
}
