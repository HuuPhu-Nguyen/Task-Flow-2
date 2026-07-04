[CmdletBinding()]
param(
    [switch]$SkipDocker,
    [switch]$KeepRabbitMq,
    [switch]$NoLaunchGui,
    [switch]$Help
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($Help) {
    Write-Host @"
TaskFlow RabbitMQ JavaFX desktop smoke helper

Usage:
  .\scripts\smoke-rabbitmq-gui.ps1

Options:
  -SkipDocker   Use an already-running RabbitMQ broker on localhost:5672
  -KeepRabbitMq Leave the Docker RabbitMQ service running after the smoke
  -NoLaunchGui  Prepare broker/coordinator/input only; print GUI launch env

Outputs:
  target\gui-rabbitmq-smoke\input
  target\gui-rabbitmq-smoke\output
  target\gui-rabbitmq-smoke\logs
  target\gui-rabbitmq-smoke\evidence.md
"@
    exit 0
}

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptRoot "..")).Path
$SmokeRoot = Join-Path $RepoRoot "target\gui-rabbitmq-smoke"
$InputDir = Join-Path $SmokeRoot "input"
$OutputDir = Join-Path $SmokeRoot "output"
$LogDir = Join-Path $SmokeRoot "logs"
$EvidencePath = Join-Path $SmokeRoot "evidence.md"
$RunId = "gui-smoke-" + (Get-Date -Format "yyyyMMddHHmmss")
$RabbitMqExchange = "taskflow.$RunId.exchange"
$RabbitMqQueuePrefix = "taskflow.$RunId"
$StartedProcesses = New-Object System.Collections.Generic.List[System.Diagnostics.Process]
$StartedRabbitMq = $false

function ConvertTo-SingleQuotedLiteral {
    param([Parameter(Mandatory = $true)][string]$Value)
    return "'" + ($Value -replace "'", "''") + "'"
}

function Invoke-DockerCompose {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    if (Get-Command docker -ErrorAction SilentlyContinue) {
        & docker compose version *> $null
        if ($LASTEXITCODE -eq 0) {
            & docker compose @Arguments
            if ($LASTEXITCODE -ne 0) {
                throw "docker compose $($Arguments -join ' ') failed."
            }
            return
        }
    }

    if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
        & docker-compose @Arguments
        if ($LASTEXITCODE -ne 0) {
            throw "docker-compose $($Arguments -join ' ') failed."
        }
        return
    }

    throw "Docker Compose was not found. Install Docker Desktop, or start RabbitMQ yourself and rerun with -SkipDocker."
}

function Wait-TcpPort {
    param(
        [Parameter(Mandatory = $true)][string]$Address,
        [Parameter(Mandatory = $true)][int]$Port,
        [int]$TimeoutSeconds = 90
    )

    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        $client = [System.Net.Sockets.TcpClient]::new()
        try {
            $connect = $client.ConnectAsync($Address, $Port)
            if ($connect.Wait(1000) -and $client.Connected) {
                return
            }
        } catch {
            Start-Sleep -Milliseconds 500
        } finally {
            $client.Close()
        }
        Start-Sleep -Milliseconds 500
    }

    throw "Timed out waiting for ${Address}:$Port."
}

function Wait-RabbitMqReady {
    param(
        [Parameter(Mandatory = $true)][bool]$UseDockerHealthCheck,
        [int]$TimeoutSeconds = 120
    )

    if (-not $UseDockerHealthCheck) {
        Wait-TcpPort -Address "localhost" -Port 5672 -TimeoutSeconds $TimeoutSeconds
        return
    }

    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        $health = & docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" taskflow-rabbitmq 2>$null
        if ($LASTEXITCODE -eq 0 -and $health -eq "healthy") {
            return
        }
        Start-Sleep -Seconds 2
    }

    throw "Timed out waiting for RabbitMQ Docker container to become healthy."
}

function Invoke-NativeCommand {
    param(
        [Parameter(Mandatory = $true)][scriptblock]$Command,
        [Parameter(Mandatory = $true)][string]$FailureMessage
    )

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        & $Command
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    if ($exitCode -ne 0) {
        throw "$FailureMessage exited with code $exitCode."
    }
}

function Write-SmokeInput {
    New-Item -ItemType Directory -Force -Path $InputDir, $OutputDir, $LogDir | Out-Null
    Remove-Item -Path (Join-Path $InputDir "*") -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -Path (Join-Path $OutputDir "*") -Recurse -Force -ErrorAction SilentlyContinue

    Set-Content -Path (Join-Path $InputDir "sample.txt") -Encoding UTF8 -Value @(
        "TaskFlow RabbitMQ GUI smoke test",
        "alpha beta beta",
        "distributed JavaFX peer"
    )
}

function Start-TaskFlowProcess {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Module,
        [Parameter(Mandatory = $true)][string]$Goal,
        [string]$PeerId,
        [switch]$VisibleHelper
    )

    $stdout = Join-Path $LogDir "$Name.out.log"
    $stderr = Join-Path $LogDir "$Name.err.log"
    Remove-Item -Force -ErrorAction SilentlyContinue $stdout, $stderr

    $lines = @(
        "`$ErrorActionPreference = 'Stop'",
        "Set-Location -LiteralPath $(ConvertTo-SingleQuotedLiteral $RepoRoot)",
        "`$env:TASKFLOW_TRANSPORT = 'rabbitmq'",
        "`$env:TASKFLOW_RABBITMQ_HOST = 'localhost'",
        "`$env:TASKFLOW_RABBITMQ_PORT = '5672'",
        "`$env:TASKFLOW_RABBITMQ_USERNAME = 'guest'",
        "`$env:TASKFLOW_RABBITMQ_PASSWORD = 'guest'",
        "`$env:TASKFLOW_RABBITMQ_EXCHANGE = $(ConvertTo-SingleQuotedLiteral $RabbitMqExchange)",
        "`$env:TASKFLOW_RABBITMQ_QUEUE_PREFIX = $(ConvertTo-SingleQuotedLiteral $RabbitMqQueuePrefix)",
        "`$env:TASKFLOW_RABBITMQ_PREFETCH = '3'"
    )

    if ($PeerId) {
        $lines += "`$env:TASKFLOW_PEER_ID = $(ConvertTo-SingleQuotedLiteral $PeerId)"
    }

    $lines += "& .\mvnw.cmd --batch-mode --no-transfer-progress -pl $Module $Goal"
    $command = $lines -join [Environment]::NewLine
    $startInfo = @{
        FilePath = "powershell.exe"
        ArgumentList = @("-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $command)
        RedirectStandardOutput = $stdout
        RedirectStandardError = $stderr
        PassThru = $true
    }
    if (-not $VisibleHelper) {
        $startInfo.WindowStyle = "Hidden"
    }

    $process = Start-Process @startInfo
    $StartedProcesses.Add($process)
    Write-Host "Started $Name (PID $($process.Id)); logs: $stdout"
    return $process
}

function Wait-ProcessLogPattern {
    param(
        [Parameter(Mandatory = $true)][System.Diagnostics.Process]$Process,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Pattern,
        [int]$TimeoutSeconds = 60
    )

    $outPath = Join-Path $LogDir "$Name.out.log"
    $errPath = Join-Path $LogDir "$Name.err.log"
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)

    while ([DateTime]::UtcNow -lt $deadline) {
        $Process.Refresh()
        if ($Process.HasExited) {
            Write-Host "---- $Name stdout ----"
            if (Test-Path $outPath) { Get-Content $outPath -Tail 80 }
            Write-Host "---- $Name stderr ----"
            if (Test-Path $errPath) { Get-Content $errPath -Tail 80 }
            throw "$Name exited before readiness was confirmed."
        }

        if (Test-Path $outPath) {
            $match = Select-String -Path $outPath -Pattern $Pattern -SimpleMatch -ErrorAction SilentlyContinue |
                Select-Object -First 1
            if ($match) {
                return
            }
        }

        Start-Sleep -Seconds 1
    }

    throw "Timed out waiting for $Name readiness log: $Pattern"
}

function Stop-ProcessTree {
    param([Parameter(Mandatory = $true)][int]$ProcessId)

    $children = Get-CimInstance Win32_Process -Filter "ParentProcessId = $ProcessId" -ErrorAction SilentlyContinue
    foreach ($child in $children) {
        Stop-ProcessTree -ProcessId $child.ProcessId
    }

    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
}

function Write-Evidence {
    param([Parameter(Mandatory = $true)][string]$Status)

    $resultFile = Join-Path $OutputDir "text-analysis-results.csv"
    $guiLog = Join-Path $LogDir "gui.out.log"
    $coordinatorLog = Join-Path $LogDir "coordinator.out.log"
    $resultExists = Test-Path -LiteralPath $resultFile
    $guiSubmitted = (Test-Path $guiLog) -and [bool](Select-String -Path $guiLog -Pattern "event=gui_job_submitted" -SimpleMatch -ErrorAction SilentlyContinue)
    $guiSaved = (Test-Path $guiLog) -and [bool](Select-String -Path $guiLog -Pattern "event=gui_results_saved" -SimpleMatch -ErrorAction SilentlyContinue)
    $brokerFailureLogged = (Test-Path $guiLog) -and [bool](Select-String -Path $guiLog -Pattern "event=gui_rabbitmq_heartbeat_failed" -SimpleMatch -ErrorAction SilentlyContinue)
    $jobCompleted = (Test-Path $coordinatorLog) -and [bool](Select-String -Path $coordinatorLog -Pattern "event=job_completed" -SimpleMatch -ErrorAction SilentlyContinue)

    Set-Content -Path $EvidencePath -Encoding UTF8 -Value @"
# RabbitMQ GUI Desktop Smoke Evidence

- Run ID: $RunId
- Status: $Status
- Timestamp: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss zzz")
- RabbitMQ exchange: $RabbitMqExchange
- RabbitMQ queue prefix: $RabbitMqQueuePrefix
- Input directory: $InputDir
- Output directory: $OutputDir
- Result file exists: $resultExists
- GUI submitted job log found: $guiSubmitted
- GUI saved result log found: $guiSaved
- Coordinator completed job log found: $jobCompleted
- GUI broker-failure heartbeat log found: $brokerFailureLogged
- Logs directory: $LogDir

Expected result file: $resultFile
"@
    Write-Host "Evidence written to $EvidencePath"
}

try {
    New-Item -ItemType Directory -Force -Path $SmokeRoot, $LogDir | Out-Null
    Write-SmokeInput

    Write-Host "Building coordinator and GUI modules with tests skipped..."
    Push-Location $RepoRoot
    try {
        Invoke-NativeCommand `
            -Command { & .\mvnw.cmd --batch-mode --no-transfer-progress -pl taskflow-coordinator,taskflow-gui -am -DskipTests install } `
            -FailureMessage "Maven install"
    } finally {
        Pop-Location
    }

    if (-not $SkipDocker) {
        Write-Host "Starting RabbitMQ with Docker Compose..."
        Invoke-DockerCompose -Arguments @("up", "-d", "rabbitmq")
        $StartedRabbitMq = $true
    } else {
        Write-Host "Skipping Docker startup; expecting RabbitMQ on localhost:5672."
    }

    Wait-RabbitMqReady -UseDockerHealthCheck (-not $SkipDocker) -TimeoutSeconds 120
    Write-Host "RabbitMQ is ready on localhost:5672."

    $coordinator = Start-TaskFlowProcess -Name "coordinator" -Module "taskflow-coordinator" -Goal "exec:java"
    Wait-ProcessLogPattern `
        -Process $coordinator `
        -Name "coordinator" `
        -Pattern "event=coordinator_started transport=rabbitmq" `
        -TimeoutSeconds 60

    if (-not $NoLaunchGui) {
        $null = Start-TaskFlowProcess -Name "gui" -Module "taskflow-gui" -Goal "javafx:run" -PeerId "gui-smoke-peer"
    } else {
        Write-Host ""
        Write-Host "Launch the GUI in another PowerShell window with:"
        Write-Host "`$env:TASKFLOW_TRANSPORT = 'rabbitmq'"
        Write-Host "`$env:TASKFLOW_RABBITMQ_HOST = 'localhost'"
        Write-Host "`$env:TASKFLOW_RABBITMQ_PORT = '5672'"
        Write-Host "`$env:TASKFLOW_RABBITMQ_USERNAME = 'guest'"
        Write-Host "`$env:TASKFLOW_RABBITMQ_PASSWORD = 'guest'"
        Write-Host "`$env:TASKFLOW_RABBITMQ_EXCHANGE = '$RabbitMqExchange'"
        Write-Host "`$env:TASKFLOW_RABBITMQ_QUEUE_PREFIX = '$RabbitMqQueuePrefix'"
        Write-Host "`$env:TASKFLOW_PEER_ID = 'gui-smoke-peer'"
        Write-Host ".\mvnw.cmd --batch-mode --no-transfer-progress -pl taskflow-gui javafx:run"
    }

    Write-Host ""
    Write-Host "Manual GUI steps:"
    Write-Host "1. In the JavaFX window, connect to broker host localhost and port 5672."
    Write-Host "2. Select Text Analysis / csv."
    Write-Host "3. Upload $InputDir\sample.txt."
    Write-Host "4. Start the job."
    Write-Host "5. Save results to $OutputDir when the download window appears."
    Write-Host ""
    Read-Host "Press Enter after the result has been saved, or after the smoke has failed"

    if (-not $SkipDocker) {
        Write-Host "Stopping RabbitMQ to exercise broker-failure handling. Watch for the GUI to remain open and log heartbeat failure."
        Invoke-DockerCompose -Arguments @("stop", "rabbitmq")
        Start-Sleep -Seconds 35
        Read-Host "Press Enter after observing the GUI during broker stop"
    } else {
        Write-Host "SkipDocker is set. Stop your external broker manually if you want to exercise broker-failure handling."
        Read-Host "Press Enter after broker-failure observation, or press Enter now to skip it"
    }

    Write-Evidence -Status "operator-completed"
    Write-Host "Smoke helper complete. Review $EvidencePath before recording gate evidence."
} finally {
    foreach ($process in $StartedProcesses) {
        try {
            $process.Refresh()
            if (-not $process.HasExited) {
                Stop-ProcessTree -ProcessId $process.Id
            }
        } catch {
            Write-Warning "Could not stop process $($process.Id): $($_.Exception.Message)"
        }
    }

    if ($StartedRabbitMq -and -not $KeepRabbitMq) {
        Write-Host "Stopping RabbitMQ Docker Compose service..."
        try {
            Invoke-DockerCompose -Arguments @("down")
        } catch {
            Write-Warning $_.Exception.Message
        }
    } elseif ($StartedRabbitMq) {
        Write-Host "RabbitMQ left running because -KeepRabbitMq was set."
    }
}
