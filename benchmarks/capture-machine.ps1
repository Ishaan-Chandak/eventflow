param(
    [Parameter(Mandatory = $true)][string]$OutputPath,
    [string]$DockerPath = "docker",
    [string]$Project,
    [string]$ComposeFile
)

$ErrorActionPreference = "Stop"
$outputDirectory = Split-Path -Parent $OutputPath
New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null

$operatingSystem = Get-CimInstance Win32_OperatingSystem
$processors = @(Get-CimInstance Win32_Processor)
$dockerVersion = & $DockerPath version --format '{{json .}}' | ConvertFrom-Json
$dockerInfo = & $DockerPath info --format '{{json .}}' | ConvertFrom-Json
$composeVersion = & $DockerPath compose version --short
$images = @()
if ($Project -and $ComposeFile) {
    $images = @(& $DockerPath compose -p $Project -f $ComposeFile images --format json | ForEach-Object { $_ | ConvertFrom-Json })
}

$metadata = [ordered]@{
    capturedAt = [DateTimeOffset]::UtcNow.ToString("o")
    host = [ordered]@{
        os = $operatingSystem.Caption
        osVersion = $operatingSystem.Version
        architecture = $env:PROCESSOR_ARCHITECTURE
        logicalProcessors = ($processors | Measure-Object -Property NumberOfLogicalProcessors -Sum).Sum
        physicalCores = ($processors | Measure-Object -Property NumberOfCores -Sum).Sum
        memoryBytes = [int64]$operatingSystem.TotalVisibleMemorySize * 1024
    }
    docker = [ordered]@{
        clientVersion = $dockerVersion.Client.Version
        serverVersion = $dockerVersion.Server.Version
        operatingSystem = $dockerInfo.OperatingSystem
        architecture = $dockerInfo.Architecture
        cpus = $dockerInfo.NCPU
        memoryBytes = $dockerInfo.MemTotal
        composeVersion = $composeVersion
        images = $images
    }
}

$metadata | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 -Path $OutputPath
