param(
    [string]$Dll = (Join-Path $PSScriptRoot 'MinecraftProxy_msvc.dll'),
    [string]$Injector = (Join-Path $PSScriptRoot 'reflective_injector.exe'),
    [string]$WindowTitle = 'KKCraft'
)

# Two ports are in play:
#   25565 -- what the second client connects to.  Owned by reflective_injector
#            from process start, so B can connect while Minecraft is still
#            booting.  The injector bridges it to the in-game proxy.
#   25566 -- the in-game proxy itself, loopback only.

$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $Injector)) {
    throw "Reflective injector not found: $Injector"
}
if (-not (Test-Path -LiteralPath $Dll)) {
    throw "DLL not found: $Dll"
}

$busy = Get-NetTCPConnection -LocalPort 25565 -State Listen `
    -ErrorAction SilentlyContinue | Select-Object -First 1
if ($busy) {
    throw "25565 is already owned by PID $($busy.OwningProcess) - another injector is probably still running."
}

Write-Host "DLL      : $Dll"
Write-Host "Injector : $Injector (reflective mapping, will request UAC)"
Write-Host "Watching : visible Java window whose title contains `"$WindowTitle`"" -ForegroundColor Cyan
Write-Host "Connect B to 127.0.0.1:25565 as soon as the injector is listening - it will wait there." -ForegroundColor Cyan

& $Injector $Dll $WindowTitle
if ($LASTEXITCODE -ne 0) {
    throw "Reflective injector failed with exit code $LASTEXITCODE."
}

Write-Host 'Injection complete: 25565 (relay) -> 127.0.0.1:25566 (in-game proxy)' `
    -ForegroundColor Green
