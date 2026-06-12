[CmdletBinding()]
param(
    [int]$PeerCount = 3,
    [int]$FileCount = 12,
    [ValidateSet("png", "jpg", "bmp", "gif")]
    [string]$TargetFormat = "png",
    [switch]$SkipDocker,
    [switch]$KeepRabbitMq,
    [switch]$KeepRunning,
    [switch]$Help
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($Help) {
    Write-Host @"
TaskFlow RabbitMQ demo

Usage:
  .\scripts\demo-rabbitmq.ps1

Options:
  -PeerCount <n>       Number of RabbitMQ worker peers to start. Default: 3
  -FileCount <n>       Number of generated image tasks to submit. Default: 12
  -TargetFormat <fmt>  Image output format: png, jpg, bmp, or gif. Default: png
  -SkipDocker          Use an already-running RabbitMQ broker on localhost:5672
  -KeepRabbitMq        Leave the Docker RabbitMQ container running after the demo
  -KeepRunning         Leave coordinator and peer Java processes running after submit

Outputs:
  target\demo-input    Generated sample images
  target\demo-results  Converted result files copied from the submitting peer output
  target\demo-logs     Coordinator, peer, and submitter logs
"@
    exit 0
}

if ($PeerCount -lt 1) {
    throw "PeerCount must be at least 1."
}

if ($FileCount -lt 1) {
    throw "FileCount must be at least 1."
}

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptRoot "..")).Path
$LogDir = Join-Path $RepoRoot "target\demo-logs"
$InputDir = Join-Path $RepoRoot "target\demo-input"
$ResultRoot = Join-Path $RepoRoot "target\rabbitmq-results"
$DemoResultsDir = Join-Path $RepoRoot "target\demo-results"
$DemoRunId = "demo-" + (Get-Date -Format "yyyyMMddHHmmss")
$RabbitMqExchange = "taskflow.$DemoRunId.exchange"
$RabbitMqQueuePrefix = "taskflow.$DemoRunId"
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

function Invoke-NativeCommandWithLog {
    param(
        [Parameter(Mandatory = $true)][scriptblock]$Command,
        [Parameter(Mandatory = $true)][string]$LogPath,
        [Parameter(Mandatory = $true)][string]$FailureMessage
    )

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        # Native tools such as Maven can write JVM warnings to stderr even when
        # exiting successfully. Capture those lines as log text and rely on the
        # native exit code for failure detection.
        $ErrorActionPreference = "Continue"
        & $Command 2>&1 |
            ForEach-Object { $_.ToString() } |
            Tee-Object -FilePath $LogPath
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    if ($exitCode -ne 0) {
        throw "$FailureMessage exited with code $exitCode. See $LogPath."
    }
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

function New-DemoImages {
    New-Item -ItemType Directory -Force -Path $InputDir | Out-Null
    Get-ChildItem -Path $InputDir -Filter "*.png" -ErrorAction SilentlyContinue | Remove-Item -Force

    Add-Type -AssemblyName System.Drawing

    for ($i = 1; $i -le $FileCount; $i++) {
        $bitmap = [System.Drawing.Bitmap]::new(64, 64)
        try {
            for ($x = 0; $x -lt 64; $x++) {
                for ($y = 0; $y -lt 64; $y++) {
                    $r = (($x * 4) + ($i * 31)) % 256
                    $g = (($y * 4) + ($i * 47)) % 256
                    $b = (($x + $y) * 2 + ($i * 19)) % 256
                    $bitmap.SetPixel($x, $y, [System.Drawing.Color]::FromArgb($r, $g, $b))
                }
            }
            $outputPath = Join-Path $InputDir ("sample-{0}.png" -f $i)
            $bitmap.Save($outputPath, [System.Drawing.Imaging.ImageFormat]::Png)
        } finally {
            $bitmap.Dispose()
        }
    }
}

function Stop-ProcessTree {
    param([Parameter(Mandatory = $true)][int]$ProcessId)

    $children = Get-CimInstance Win32_Process -Filter "ParentProcessId = $ProcessId" -ErrorAction SilentlyContinue
    foreach ($child in $children) {
        Stop-ProcessTree -ProcessId $child.ProcessId
    }

    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
}

function Start-TaskFlowProcess {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Module,
        [string]$PeerId,
        [string]$ExecArgs
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

    $mavenCommand = "& .\mvnw.cmd -pl $Module exec:java"
    if ($ExecArgs) {
        $mavenCommand += " -Dexec.args=$(ConvertTo-SingleQuotedLiteral $ExecArgs)"
    }
    $lines += $mavenCommand

    $command = $lines -join [Environment]::NewLine
    $process = Start-Process `
        -FilePath "powershell.exe" `
        -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $command) `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -WindowStyle Hidden `
        -PassThru

    $StartedProcesses.Add($process)
    Write-Host "Started $Name (PID $($process.Id)); logs: $stdout"
    return $process
}

function Assert-ProcessStillRunning {
    param(
        [Parameter(Mandatory = $true)][System.Diagnostics.Process]$Process,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $Process.Refresh()
    if ($Process.HasExited) {
        $errPath = Join-Path $LogDir "$Name.err.log"
        $outPath = Join-Path $LogDir "$Name.out.log"
        Write-Host "---- $Name stdout ----"
        if (Test-Path $outPath) { Get-Content $outPath -Tail 40 }
        Write-Host "---- $Name stderr ----"
        if (Test-Path $errPath) { Get-Content $errPath -Tail 40 }
        throw "$Name exited before the demo could run."
    }
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

    Write-Host "---- $Name stdout ----"
    if (Test-Path $outPath) { Get-Content $outPath -Tail 80 }
    Write-Host "---- $Name stderr ----"
    if (Test-Path $errPath) { Get-Content $errPath -Tail 80 }
    throw "Timed out waiting for $Name readiness log: $Pattern"
}

function Copy-LatestResults {
    $resultDirs = Get-ChildItem -Path $ResultRoot -Directory -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending

    if (-not $resultDirs -or $resultDirs.Count -eq 0) {
        Write-Warning "No RabbitMQ result directory was found under $ResultRoot."
        return
    }

    Remove-Item -Path $DemoResultsDir -Recurse -Force -ErrorAction SilentlyContinue
    New-Item -ItemType Directory -Force -Path $DemoResultsDir | Out-Null
    Copy-Item -Path (Join-Path $resultDirs[0].FullName "*") -Destination $DemoResultsDir -Force
    Write-Host "Copied latest results from $($resultDirs[0].FullName) to $DemoResultsDir"
}

function Install-TaskFlowModules {
    $buildLog = Join-Path $LogDir "build.out.log"
    Remove-Item -Force -ErrorAction SilentlyContinue $buildLog

    Write-Host "Building TaskFlow modules with tests skipped..."
    Push-Location $RepoRoot
    try {
        Invoke-NativeCommandWithLog `
            -Command { & .\mvnw.cmd -DskipTests install } `
            -LogPath $buildLog `
            -FailureMessage "Maven install"
    } finally {
        Pop-Location
    }
}

try {
    New-Item -ItemType Directory -Force -Path $LogDir, $ResultRoot | Out-Null
    Install-TaskFlowModules

    if (-not $SkipDocker) {
        Write-Host "Starting RabbitMQ with Docker Compose..."
        Invoke-DockerCompose -Arguments @("up", "-d", "rabbitmq")
        $StartedRabbitMq = $true
    } else {
        Write-Host "Skipping Docker startup; expecting RabbitMQ on localhost:5672."
    }

    Wait-RabbitMqReady -UseDockerHealthCheck (-not $SkipDocker) -TimeoutSeconds 120
    Write-Host "RabbitMQ is ready on localhost:5672."

    New-DemoImages
    Write-Host "Generated sample inputs in $InputDir"

    $coordinator = Start-TaskFlowProcess -Name "coordinator" -Module "taskflow-coordinator"
    Wait-ProcessLogPattern `
        -Process $coordinator `
        -Name "coordinator" `
        -Pattern "event=coordinator_started transport=rabbitmq" `
        -TimeoutSeconds 60

    $peerProcesses = @()
    for ($i = 1; $i -le $PeerCount; $i++) {
        $peerName = "peer-$i"
        $peerProcesses += [PSCustomObject]@{
            Name = $peerName
            Process = Start-TaskFlowProcess -Name $peerName -Module "taskflow-peer" -PeerId ("demo-peer-{0}" -f $i)
        }
    }

    foreach ($peerProcess in $peerProcesses) {
        Wait-ProcessLogPattern `
            -Process $peerProcess.Process `
            -Name $peerProcess.Name `
            -Pattern "event=peer_consuming_assignments transport=rabbitmq" `
            -TimeoutSeconds 60
    }

    Start-Sleep -Seconds 5
    Assert-ProcessStillRunning -Process $coordinator -Name "coordinator"

    $inputFiles = Get-ChildItem -Path $InputDir -Filter "*.png" | Sort-Object FullName
    $relativeInputs = $inputFiles | ForEach-Object { Join-Path "target\demo-input" $_.Name }
    $execArgs = "submit image $TargetFormat " + ($relativeInputs -join " ")
    $submitLog = Join-Path $LogDir "submitter.out.log"
    Remove-Item -Force -ErrorAction SilentlyContinue $submitLog

    Write-Host "Submitting image conversion job with $($inputFiles.Count) files..."
    Push-Location $RepoRoot
    try {
        $env:TASKFLOW_TRANSPORT = "rabbitmq"
        $env:TASKFLOW_RABBITMQ_HOST = "localhost"
        $env:TASKFLOW_RABBITMQ_PORT = "5672"
        $env:TASKFLOW_RABBITMQ_USERNAME = "guest"
        $env:TASKFLOW_RABBITMQ_PASSWORD = "guest"
        $env:TASKFLOW_RABBITMQ_EXCHANGE = $RabbitMqExchange
        $env:TASKFLOW_RABBITMQ_QUEUE_PREFIX = $RabbitMqQueuePrefix
        $env:TASKFLOW_PEER_ID = "demo-submit"

        Invoke-NativeCommandWithLog `
            -Command { & .\mvnw.cmd -pl taskflow-peer exec:java "-Dexec.args=$execArgs" } `
            -LogPath $submitLog `
            -FailureMessage "RabbitMQ submitter"
    } finally {
        Pop-Location
    }

    Copy-LatestResults

    Write-Host ""
    Write-Host "Demo complete."
    Write-Host "Results: $DemoResultsDir"
    Write-Host "Logs:    $LogDir"
    Write-Host "RabbitMQ management UI: http://localhost:15672 (guest / guest)"
} finally {
    if (-not $KeepRunning) {
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
    } else {
        Write-Host "Coordinator and peers left running because -KeepRunning was set."
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
