[CmdletBinding()]
param(
    [switch]$SkipTests,
    [switch]$AllowDirty,
    [int]$TaskCount = 10000,
    [int]$WarmupTaskCount = 1000,
    [int]$WorkUnitsPerTask = 64,
    [string]$OutputDirectory = "target/baseline"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
if (Test-Path variable:PSNativeCommandUseErrorActionPreference) {
    $PSNativeCommandUseErrorActionPreference = $false
}

if ($TaskCount -lt 1 -or $TaskCount -gt 100000) {
    throw "TaskCount must be in [1, 100000]."
}
if ($WarmupTaskCount -lt 1 -or $WarmupTaskCount -gt $TaskCount) {
    throw "WarmupTaskCount must be positive and no larger than TaskCount."
}
if ($WorkUnitsPerTask -lt 1 -or $WorkUnitsPerTask -gt 100000) {
    throw "WorkUnitsPerTask must be in [1, 100000]."
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
    $OutputRoot = [System.IO.Path]::GetFullPath((Join-Path $RepoRoot $OutputDirectory))
}

function Write-Utf8Lines {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][AllowEmptyString()][string[]]$Lines
    )
    $Parent = Split-Path -Parent $Path
    if ($Parent) {
        [System.IO.Directory]::CreateDirectory($Parent) | Out-Null
    }
    [System.IO.File]::WriteAllLines($Path, $Lines, [System.Text.UTF8Encoding]::new($false))
}

function Invoke-MavenLogged {
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$LogPath,
        [string]$ReportedLogPath = ""
    )
    Write-Host ""
    Write-Host "[$Label] $MavenWrapper $($Arguments -join ' ')"
    $PreviousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $MavenWrapper @Arguments 2>&1 |
            ForEach-Object {
                if ($_ -is [System.Management.Automation.ErrorRecord]) {
                    [string]$_.Exception.Message
                } else {
                    [string]$_
                }
            } |
            Tee-Object -FilePath $LogPath |
            Out-Host
        $ExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $PreviousErrorActionPreference
    }
    if ($ExitCode -ne 0) {
        if ([string]::IsNullOrWhiteSpace($ReportedLogPath)) {
            $ReportedLogPath = $LogPath
        }
        throw "$Label failed with exit code $ExitCode. See $ReportedLogPath"
    }
}

function Get-SurefireCounts {
    $Reports = Get-ChildItem -Path $RepoRoot -Recurse -Filter "TEST-*.xml" |
        Where-Object {
            $_.FullName -like "*\target\surefire-reports\*" -and
            $_.Name -ne "TEST-server.scheduler.BaselineSchedulerExperiment.xml"
        }
    if (-not $Reports) {
        throw "No Surefire XML reports found. Run without -SkipTests first."
    }

    $Suites = foreach ($Report in $Reports) {
        [xml]$Xml = Get-Content -LiteralPath $Report.FullName -Raw -Encoding UTF8
        $Suite = $Xml.testsuite
        [pscustomobject]@{
            Name = [string]$Suite.name
            Tests = [int]$Suite.tests
            Failures = [int]$Suite.failures
            Errors = [int]$Suite.errors
            Skipped = [int]$Suite.skipped
        }
    }

    function Measure-SuiteField {
        param([object[]]$Items, [string]$Field)
        $Measurement = $Items | Measure-Object -Property $Field -Sum
        if ($null -eq $Measurement.Sum) {
            return 0
        }
        return [int]$Measurement.Sum
    }

    $Integration = @($Suites | Where-Object { $_.Name -match "(IntegrationTest|LiveTest)$" })
    $Unit = @($Suites | Where-Object { $_.Name -notmatch "(IntegrationTest|LiveTest)$" })
    $All = @($Suites)

    return [pscustomobject]@{
        UnitSuites = $Unit.Count
        UnitTests = Measure-SuiteField $Unit "Tests"
        UnitSkipped = Measure-SuiteField $Unit "Skipped"
        IntegrationSuites = $Integration.Count
        IntegrationTests = Measure-SuiteField $Integration "Tests"
        IntegrationSkipped = Measure-SuiteField $Integration "Skipped"
        TotalSuites = $All.Count
        TotalTests = Measure-SuiteField $All "Tests"
        TotalSkipped = Measure-SuiteField $All "Skipped"
        TotalFailures = Measure-SuiteField $All "Failures"
        TotalErrors = Measure-SuiteField $All "Errors"
        IntegrationNames = @($Integration | Sort-Object Name | ForEach-Object { $_.Name })
    }
}

function Get-SourceConstant {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Pattern,
        [Parameter(Mandatory = $true)][string]$Label
    )
    $Text = Get-Content -LiteralPath $Path -Raw -Encoding UTF8
    $Match = [regex]::Match($Text, $Pattern)
    if (-not $Match.Success) {
        throw "Could not find $Label in $Path"
    }
    return $Match.Groups[1].Value
}

function Read-Properties {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        throw "Expected metrics file was not created: $Path"
    }
    return ConvertFrom-StringData (Get-Content -LiteralPath $Path -Raw -Encoding UTF8)
}

function Invoke-BaselineExperiment {
    param([Parameter(Mandatory = $true)][ValidateSet(1, 4)][int]$Workers)
    $MetricsPath = Join-Path $OutputRoot "workers-$Workers.properties"
    $LogPath = Join-Path $OutputRoot "workers-$Workers.log"
    $Arguments = @(
        "--batch-mode",
        "--no-transfer-progress",
        "-pl", "taskflow-core",
        "-am",
        "-Dtest=BaselineSchedulerExperiment",
        "-Dsurefire.failIfNoSpecifiedTests=false",
        "-Dtaskflow.baseline.workers=$Workers",
        "-Dtaskflow.baseline.tasks=$TaskCount",
        "-Dtaskflow.baseline.warmupTasks=$WarmupTaskCount",
        "-Dtaskflow.baseline.workUnits=$WorkUnitsPerTask",
        "-Dtaskflow.baseline.output=$MetricsPath",
        "-DargLine=-Xms64m -Xmx512m",
        "test"
    )
    Invoke-MavenLogged "baseline-$Workers-worker" $Arguments $LogPath
    $Metrics = Read-Properties $MetricsPath
    if ([int]$Metrics.workerCount -ne $Workers -or [int]$Metrics.taskCount -ne $TaskCount) {
        throw "Metrics file does not match the requested $Workers-worker/$TaskCount-task run."
    }
    return $Metrics
}

Push-Location $RepoRoot
try {
    if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
        throw "git is required."
    }
    if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
        throw "Java 21 or newer is required."
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
        throw "The baseline must run from a clean checkout. Commit/stash changes or use -AllowDirty for harness development only."
    }

    [System.IO.Directory]::CreateDirectory($OutputRoot) | Out-Null

    $PreviousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $JavaVersionLines = @(& java -version 2>&1)
        $JavaVersionExitCode = $LASTEXITCODE
        $MavenVersionLines = @(& $MavenWrapper -version 2>&1)
        $MavenVersionExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $PreviousErrorActionPreference
    }
    $JavaVersionText = ($JavaVersionLines | ForEach-Object {
        if ($_ -is [System.Management.Automation.ErrorRecord]) {
            [string]$_.Exception.Message
        } else {
            [string]$_
        }
    }) -join [Environment]::NewLine
    if ($JavaVersionExitCode -ne 0) {
        throw "java -version failed."
    }
    $JavaMatch = [regex]::Match($JavaVersionText, 'version "(?<major>\d+)')
    if (-not $JavaMatch.Success -or [int]$JavaMatch.Groups["major"].Value -lt 21) {
        throw "Java 21 or newer is required; observed: $JavaVersionText"
    }
    $MavenVersionText = ($MavenVersionLines | ForEach-Object {
        if ($_ -is [System.Management.Automation.ErrorRecord]) {
            [string]$_.Exception.Message
        } else {
            [string]$_
        }
    }) -join [Environment]::NewLine
    if ($MavenVersionExitCode -ne 0) {
        throw "Maven wrapper version check failed."
    }

    if (-not $SkipTests) {
        $TemporaryTestLog = [System.IO.Path]::GetTempFileName()
        try {
            Invoke-MavenLogged "full-test-gate" @(
                "--batch-mode",
                "--no-transfer-progress",
                "clean",
                "test"
            ) $TemporaryTestLog (Join-Path $OutputRoot "maven-test.log")
        } finally {
            [System.IO.Directory]::CreateDirectory($OutputRoot) | Out-Null
            if (Test-Path -LiteralPath $TemporaryTestLog) {
                Move-Item -LiteralPath $TemporaryTestLog `
                    -Destination (Join-Path $OutputRoot "maven-test.log") `
                    -Force
            }
        }
    }

    $Counts = Get-SurefireCounts
    if ($Counts.TotalFailures -ne 0 -or $Counts.TotalErrors -ne 0) {
        throw "Surefire reports contain failures/errors after the test gate."
    }
    Write-Utf8Lines (Join-Path $OutputRoot "test-counts.properties") @(
        "classification=class-name suffix IntegrationTest or LiveTest; baseline experiment excluded",
        "unitComponentSuites=$($Counts.UnitSuites)",
        "unitComponentTests=$($Counts.UnitTests)",
        "unitComponentSkipped=$($Counts.UnitSkipped)",
        "integrationLiveSuites=$($Counts.IntegrationSuites)",
        "integrationLiveTests=$($Counts.IntegrationTests)",
        "integrationLiveSkipped=$($Counts.IntegrationSkipped)",
        "totalSuites=$($Counts.TotalSuites)",
        "totalTests=$($Counts.TotalTests)",
        "totalSkipped=$($Counts.TotalSkipped)",
        "totalFailures=$($Counts.TotalFailures)",
        "totalErrors=$($Counts.TotalErrors)",
        "integrationSuiteNames=$($Counts.IntegrationNames -join ',')"
    )

    $PomFiles = @(Get-ChildItem -Path $RepoRoot -Recurse -Filter "pom.xml" |
        Where-Object { $_.FullName -notlike "*\target\*" })
    $Modules = foreach ($Pom in $PomFiles) {
        [xml]$Xml = Get-Content -LiteralPath $Pom.FullName -Raw -Encoding UTF8
        $Relative = $Pom.FullName.Substring($RepoRoot.Length).TrimStart("\", "/").Replace("\", "/")
        "$($Xml.project.artifactId)`t$Relative"
    }
    $Modules = @($Modules | Sort-Object)
    Write-Utf8Lines (Join-Path $OutputRoot "modules.txt") $Modules

    $SchedulerPath = Join-Path $RepoRoot "taskflow-core\src\main\java\server\scheduler\TaskScheduler.java"
    $SchedulerLines = (Get-Content -LiteralPath $SchedulerPath -Encoding UTF8).Count
    $ProtocolVersion = Get-SourceConstant `
        (Join-Path $RepoRoot "taskflow-spi\src\main\java\protocol\ProtocolVersions.java") `
        'public\s+static\s+final\s+int\s+CURRENT\s*=\s*(\d+)' `
        "protocol version"
    $SchemaVersion = Get-SourceConstant `
        (Join-Path $RepoRoot "taskflow-persistence-sqlite\src\main\java\server\db\DatabaseManager.java") `
        'public\s+static\s+final\s+int\s+CURRENT_SCHEMA_VERSION\s*=\s*(\d+)' `
        "SQLite schema version"

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

    Write-Utf8Lines (Join-Path $OutputRoot "inventory.properties") @(
        "commit=$Commit",
        "dirty=$($DirtyLines.Count -gt 0)",
        "moduleCount=$($Modules.Count)",
        "taskSchedulerLines=$SchedulerLines",
        "protocolVersion=$ProtocolVersion",
        "sqliteSchemaVersion=$SchemaVersion",
        "os=$([System.Environment]::OSVersion.VersionString)",
        "architecture=$([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture)",
        "logicalProcessors=$([System.Environment]::ProcessorCount)",
        "cpu=$CpuName",
        "physicalMemoryBytes=$PhysicalMemoryBytes"
    )
    Write-Utf8Lines (Join-Path $OutputRoot "environment.txt") @(
        "commit: $Commit",
        "dirty: $($DirtyLines.Count -gt 0)",
        "os: $([System.Environment]::OSVersion.VersionString)",
        "architecture: $([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture)",
        "logical processors: $([System.Environment]::ProcessorCount)",
        "cpu: $CpuName",
        "physical memory bytes: $PhysicalMemoryBytes",
        "",
        "java -version:",
        $JavaVersionText,
        "",
        "maven wrapper -version:",
        $MavenVersionText
    )

    $OneWorker = Invoke-BaselineExperiment 1
    $FourWorkers = Invoke-BaselineExperiment 4

    $SummaryPath = Join-Path $OutputRoot "summary.md"
    Write-Utf8Lines $SummaryPath @(
        "# TaskFlow Baseline Verification Run",
        "",
        "- Commit: ``$Commit``",
        "- Dirty worktree: ``$($DirtyLines.Count -gt 0)``",
        "- Full Maven tests: ``$(-not $SkipTests)``",
        "- Modules: ``$($Modules.Count)``",
        "- Unit/component tests: ``$($Counts.UnitTests)`` (skipped: ``$($Counts.UnitSkipped)``)",
        "- Integration/live tests: ``$($Counts.IntegrationTests)`` (skipped: ``$($Counts.IntegrationSkipped)``)",
        "- TaskScheduler lines: ``$SchedulerLines``",
        "- Protocol version: ``$ProtocolVersion``",
        "- SQLite schema version: ``$SchemaVersion``",
        "- 1-worker throughput: ``$($OneWorker.throughputTasksPerSecond) tasks/s``",
        "- 4-worker throughput: ``$($FourWorkers.throughputTasksPerSecond) tasks/s``",
        "- $TaskCount-task peak heap (1 worker): ``$($OneWorker.peakUsedHeapBytes) bytes``",
        "- $TaskCount-task peak heap (4 workers): ``$($FourWorkers.peakUsedHeapBytes) bytes``"
    )

    Write-Host ""
    Write-Host "Baseline verification PASS"
    Write-Host "Summary: $SummaryPath"
} finally {
    Pop-Location
}
