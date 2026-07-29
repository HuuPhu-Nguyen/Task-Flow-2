[CmdletBinding()]
param(
    [switch]$Calibration,
    [switch]$AllowDirty,
    [int]$Waves = 5,
    [int]$SubmissionsPerWave = 200,
    [int]$MailboxCapacity = 1,
    [int]$ActiveJobLimit = 32,
    [int]$MaxPendingOutboxRows = 16,
    [long]$TaskLeaseMillis = 5000,
    [long]$CompletionTimeoutSeconds = 300,
    [long]$HeapPlateauSpanBytes = 16777216,
    [long]$HeapCeilingBytes = 134217728,
    [string]$OutputDirectory = "target/overload/report"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
if (Test-Path variable:PSNativeCommandUseErrorActionPreference) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$ReportWaves = 5
$ReportSubmissionsPerWave = 200
$ReportMailboxCapacity = 1
$ReportActiveJobLimit = 32
$ReportMaxPendingOutboxRows = 16
$ReportTaskLeaseMillis = 5000
$ReportCompletionTimeoutSeconds = 300
$ReportHeapPlateauSpanBytes = 16777216
$ReportHeapCeilingBytes = 134217728
$CoordinatorJvmFlags = "-Xms256m -Xmx256m -XX:+UseSerialGC"

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

Assert-Range $Waves 3 20 "Waves"
Assert-Range $SubmissionsPerWave 40 100000 "SubmissionsPerWave"
Assert-Range $MailboxCapacity 1 10000 "MailboxCapacity"
Assert-Range $ActiveJobLimit 2 10000 "ActiveJobLimit"
Assert-Range $MaxPendingOutboxRows 2 100000 "MaxPendingOutboxRows"
Assert-Range $TaskLeaseMillis 100 120000 "TaskLeaseMillis"
Assert-Range $CompletionTimeoutSeconds 10 3600 "CompletionTimeoutSeconds"
Assert-Range $HeapPlateauSpanBytes 1 1073741824 "HeapPlateauSpanBytes"
Assert-Range $HeapCeilingBytes $HeapPlateauSpanBytes 2147483648 `
    "HeapCeilingBytes"

if (-not $Calibration -and $AllowDirty) {
    throw "AllowDirty is available only with Calibration."
}
if (-not $Calibration) {
    $Expected = @(
        @($Waves, $ReportWaves, "Waves"),
        @($SubmissionsPerWave, $ReportSubmissionsPerWave,
            "SubmissionsPerWave"),
        @($MailboxCapacity, $ReportMailboxCapacity, "MailboxCapacity"),
        @($ActiveJobLimit, $ReportActiveJobLimit, "ActiveJobLimit"),
        @($MaxPendingOutboxRows, $ReportMaxPendingOutboxRows,
            "MaxPendingOutboxRows"),
        @($TaskLeaseMillis, $ReportTaskLeaseMillis, "TaskLeaseMillis"),
        @($CompletionTimeoutSeconds, $ReportCompletionTimeoutSeconds,
            "CompletionTimeoutSeconds"),
        @($HeapPlateauSpanBytes, $ReportHeapPlateauSpanBytes,
            "HeapPlateauSpanBytes"),
        @($HeapCeilingBytes, $ReportHeapCeilingBytes, "HeapCeilingBytes")
    )
    foreach ($Entry in $Expected) {
        if ([long]$Entry[0] -ne [long]$Entry[1]) {
            throw "Report-grade overload requires $($Entry[2])=$($Entry[1])."
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

function Require-Property {
    param(
        [Parameter(Mandatory = $true)][hashtable]$Properties,
        [Parameter(Mandatory = $true)][string]$Name
    )
    if (-not $Properties.ContainsKey($Name)) {
        throw "Overload property is missing: $Name"
    }
    return [string]$Properties[$Name]
}

function Assert-PropertyEquals {
    param(
        [Parameter(Mandatory = $true)][hashtable]$Properties,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Expected
    )
    $Observed = Require-Property $Properties $Name
    if ($Observed -ne $Expected) {
        throw "Overload property $Name expected $Expected but observed $Observed."
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
        throw "Report-grade overload requires a clean checkout. Use Calibration with AllowDirty only for harness development."
    }
    & docker info --format "{{.ServerVersion}}" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker engine is unavailable."
    }

    [System.IO.Directory]::CreateDirectory($OutputRoot) | Out-Null
    $RunDirectory = Join-Path $OutputRoot "run"
    $MavenLog = Join-Path $OutputRoot "overload.maven.log"
    $ReportGrade = -not $Calibration
    $StartedAt = [DateTimeOffset]::Now
    $MavenArgs = @(
        "--batch-mode",
        "--no-transfer-progress",
        "-pl",
        "taskflow-coordinator",
        "-am",
        "-Dtest=OverloadExperiment",
        "-Dsurefire.failIfNoSpecifiedTests=false",
        "-Dtaskflow.overload.outputDirectory=$RunDirectory",
        "-Dtaskflow.overload.reportGrade=$($ReportGrade.ToString().ToLowerInvariant())",
        "-Dtaskflow.overload.waves=$Waves",
        "-Dtaskflow.overload.submissionsPerWave=$SubmissionsPerWave",
        "-Dtaskflow.overload.mailboxCapacity=$MailboxCapacity",
        "-Dtaskflow.overload.activeJobLimit=$ActiveJobLimit",
        "-Dtaskflow.overload.maxPendingOutboxRows=$MaxPendingOutboxRows",
        "-Dtaskflow.overload.taskLeaseMillis=$TaskLeaseMillis",
        "-Dtaskflow.overload.completionTimeoutSeconds=$CompletionTimeoutSeconds",
        "-Dtaskflow.overload.heapPlateauSpanBytes=$HeapPlateauSpanBytes",
        "-Dtaskflow.overload.heapCeilingBytes=$HeapCeilingBytes",
        "-DargLine=$CoordinatorJvmFlags",
        "test"
    )

    $PreviousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $MavenWrapper @MavenArgs 2>&1 |
            Tee-Object -FilePath $MavenLog
        $MavenExit = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $PreviousErrorActionPreference
    }
    if ($MavenExit -ne 0) {
        throw "Overload Maven run failed with exit code $MavenExit."
    }

    $Configuration = Read-Properties `
        (Join-Path $RunDirectory "configuration.properties") `
        "Overload configuration"
    $Metrics = Read-Properties `
        (Join-Path $RunDirectory "metrics.properties") `
        "Overload metrics"
    $Audit = Read-Properties `
        (Join-Path $RunDirectory "audit.properties") `
        "Overload audit"
    $TotalFlood = [long]$Waves * [long]$SubmissionsPerWave
    $ExpectedSubmitted = $TotalFlood + 4L

    Assert-PropertyEquals $Configuration "waves" ([string]$Waves)
    Assert-PropertyEquals $Configuration "submissionsPerWave" `
        ([string]$SubmissionsPerWave)
    Assert-PropertyEquals $Configuration "totalFloodSubmissions" `
        ([string]$TotalFlood)
    Assert-PropertyEquals $Configuration "mailboxCapacity" `
        ([string]$MailboxCapacity)
    Assert-PropertyEquals $Configuration "maxPendingOutboxRows" `
        ([string]$MaxPendingOutboxRows)
    Assert-PropertyEquals $Metrics "submittedJobs" ([string]$ExpectedSubmitted)
    Assert-PropertyEquals $Metrics "acceptedJobs" "4"
    Assert-PropertyEquals $Metrics "typedRejections" ([string]$TotalFlood)
    Assert-PropertyEquals $Metrics "jobsCompleted" "4"
    Assert-PropertyEquals $Metrics "taskResultsCommitted" "4"
    Assert-PropertyEquals $Metrics "mailboxSubmissionCapacity" `
        ([string]$MailboxCapacity)
    Assert-PropertyEquals $Metrics "mailboxSubmissionHighWater" `
        ([string]$MailboxCapacity)
    Assert-PropertyEquals $Metrics "mailboxResultCapacity" "1"
    Assert-PropertyEquals $Metrics "mailboxResultHighWater" "1"
    Assert-PropertyEquals $Metrics "outboxAdmissionThreshold" `
        ([string]$MaxPendingOutboxRows)
    Assert-PropertyEquals $Metrics "outboxPendingHighWater" `
        ([string]($MaxPendingOutboxRows + 1))
    Assert-PropertyEquals $Metrics "restartCount" "0"
    Assert-PropertyEquals $Metrics "freshJobAcceptedAfterRecovery" "true"
    if ([long](Require-Property $Metrics "leaseExpirations") -lt 1L) {
        throw "At least one lease expiration must be processed."
    }
    if ([long](Require-Property $Metrics "brokerSubmissionQueueHighWater") `
        -lt 1L) {
        throw "Broker submission queue high-water must be positive."
    }
    if ([long](Require-Property $Metrics "heapPlateauSpanBytes") `
        -gt $HeapPlateauSpanBytes) {
        throw "Retained heap did not satisfy the plateau span."
    }
    if ([long](Require-Property $Metrics "heapPlateauMaximumBytes") `
        -ge $HeapCeilingBytes) {
        throw "Retained heap exceeded the experiment ceiling."
    }

    Assert-PropertyEquals $Audit "databaseSchemaVersion" "14"
    Assert-PropertyEquals $Audit "databaseIntegrity" "ok"
    Assert-PropertyEquals $Audit "durableJobs" "4"
    Assert-PropertyEquals $Audit "durableCompletedJobs" "4"
    Assert-PropertyEquals $Audit "durableTasks" "4"
    Assert-PropertyEquals $Audit "durableCompletedTasks" "4"
    Assert-PropertyEquals $Audit "pendingOutboxRows" "0"
    Assert-PropertyEquals $Audit "brokerSubmissionQueueFinal" "0"
    Assert-PropertyEquals $Audit "brokerResultQueueFinal" "0"
    if ([long](Require-Property $Audit "maximumAttemptNumber") -lt 2L) {
        throw "Durable attempt audit did not observe lease reassignment."
    }

    $HeapLineCount = @(Get-Content -LiteralPath `
        (Join-Path $RunDirectory "heap-samples.csv") -Encoding UTF8).Count
    if ($HeapLineCount -ne $Waves + 1) {
        throw "Heap sample CSV expected $($Waves + 1) lines; observed $HeapLineCount."
    }
    $ResponseLineCount = @(Get-Content -LiteralPath `
        (Join-Path $RunDirectory "responses.csv") -Encoding UTF8).Count
    if ($ResponseLineCount -ne $TotalFlood + 5L) {
        throw "Response CSV expected $($TotalFlood + 5L) lines; observed $ResponseLineCount."
    }

    $CompletedAt = [DateTimeOffset]::Now
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
        # Environment metadata remains explicitly unknown when CIM is absent.
    }
    $PreviousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $JavaVersion = @(& java -version 2>&1 | Select-Object -First 1)
    } finally {
        $ErrorActionPreference = $PreviousErrorActionPreference
    }
    $DockerVersion = (& docker info --format "{{.ServerVersion}}").Trim()
    $EnvironmentLines = @(
        "commit=$Commit",
        "reportGrade=$($ReportGrade.ToString().ToLowerInvariant())",
        "startedAt=$($StartedAt.ToString('o'))",
        "completedAt=$($CompletedAt.ToString('o'))",
        "durationSeconds=$([Math]::Round(($CompletedAt - $StartedAt).TotalSeconds, 3))",
        "os=$([System.Environment]::OSVersion.VersionString)",
        "cpu=$CpuName",
        "processorCount=$([System.Environment]::ProcessorCount)",
        "physicalMemoryBytes=$PhysicalMemoryBytes",
        "java=$([string]$JavaVersion[0])",
        "dockerServer=$DockerVersion",
        "jvmFlags=$CoordinatorJvmFlags",
        "rabbitMqImage=rabbitmq:3.13-management"
    )
    [System.IO.File]::WriteAllLines(
        (Join-Path $OutputRoot "environment.properties"),
        $EnvironmentLines,
        [System.Text.UTF8Encoding]::new($false)
    )

    $ManifestPath = Join-Path $OutputRoot "checksums.sha256"
    $ManifestLines = @()
    $Files = Get-ChildItem -LiteralPath $OutputRoot -Recurse -File |
        Where-Object { $_.FullName -ne $ManifestPath } |
        Sort-Object FullName
    foreach ($File in $Files) {
        $Relative = $File.FullName.Substring($OutputRoot.Length)
        $Relative = $Relative.TrimStart(
            [System.IO.Path]::DirectorySeparatorChar,
            [System.IO.Path]::AltDirectorySeparatorChar
        ).Replace("\", "/")
        $Hash = (Get-FileHash -LiteralPath $File.FullName -Algorithm SHA256).Hash
        $ManifestLines += "$($Hash.ToLowerInvariant())  $Relative"
    }
    [System.IO.File]::WriteAllLines(
        $ManifestPath,
        $ManifestLines,
        [System.Text.UTF8Encoding]::new($false)
    )
    if ($ManifestLines.Count -lt 7) {
        throw "Checksum manifest is unexpectedly incomplete."
    }
    Write-Host "Overload verification PASS"
    Write-Host "Output: $OutputRoot"
    Write-Host "Commit: $Commit"
    Write-Host "Checksums: $($ManifestLines.Count)"
} finally {
    Pop-Location
}
