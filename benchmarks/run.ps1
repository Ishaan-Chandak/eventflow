param(
    [ValidateRange(1, 2147483647)][int]$EventCount = 1000,
    [ValidateRange(0, 2147483647)][int]$WarmupCount = 100,
    [ValidateRange(1, 1024)][int]$PublisherConcurrency = 10,
    [ValidateRange(1, 128)][int]$WorkerCount = 1,
    [ValidateRange(96, 1048000)][int]$PayloadBytes = 1024,
    [ValidateRange(0, 4900)][int]$ReceiverLatencyMs = 0,
    [ValidateRange(1, 86400)][int]$DrainTimeoutSeconds = 120,
    [ValidateRange(1, 4096)][int]$ReceiverMaxConcurrency = 128,
    [string]$Label = "benchmark",
    [string]$ResultsRoot = "$PSScriptRoot\results",
    [string]$DockerPath = "docker",
    [switch]$KeepStack
)

$ErrorActionPreference = "Stop"
$runId = [Guid]::NewGuid().ToString("N")
$project = "eventflow-bench-$runId"
$resultDirectory = Join-Path (Resolve-Path -LiteralPath (New-Item -ItemType Directory -Force -Path $ResultsRoot)).Path $runId
New-Item -ItemType Directory -Force -Path $resultDirectory | Out-Null
$composeFile = Join-Path $PSScriptRoot "docker-compose.yml"
$env:BENCHMARK_RESULTS_DIR = $resultDirectory
$env:RECEIVER_MAX_CONCURRENCY = $ReceiverMaxConcurrency.ToString()

$exitCode = 1
try {
    & $DockerPath compose -p $project -f $composeFile build
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose build failed" }
    & $DockerPath compose -p $project -f $composeFile up -d --scale "benchmark-worker=$WorkerCount" postgres redis benchmark-receiver benchmark-api benchmark-worker
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose startup failed" }
    & (Join-Path $PSScriptRoot "capture-machine.ps1") -OutputPath (Join-Path $resultDirectory "machine.json") -DockerPath $DockerPath -Project $project -ComposeFile $composeFile
    & $DockerPath compose -p $project -f $composeFile run --rm --no-deps benchmark-runner run `
        --run-id $runId --label $Label --event-count $EventCount --warmup-count $WarmupCount `
        --publisher-concurrency $PublisherConcurrency --worker-count $WorkerCount `
        --payload-bytes $PayloadBytes --receiver-latency-ms $ReceiverLatencyMs `
        --drain-timeout-seconds $DrainTimeoutSeconds --output-dir /results
    $exitCode = $LASTEXITCODE
}
finally {
    if (-not $KeepStack) {
        & $DockerPath compose -p $project -f $composeFile down --volumes --remove-orphans
    }
    Write-Host "Benchmark artifacts: $resultDirectory"
}

exit $exitCode
