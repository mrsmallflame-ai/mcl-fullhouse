<#
.SYNOPSIS
    Spawn mcl-fullhouse terminals: sharded fillers and/or a seat watchdog.
    Compatible with Windows PowerShell 5.1 and pwsh 7+. ASCII-only on purpose.

.EXAMPLE
    .\launch_shards.ps1 -Movie "kung fu soccer"                          # 3 dry-run shards
    .\launch_shards.ps1 -Movie "kung fu soccer" -Shards 9 -Live
    .\launch_shards.ps1 -Cinema "airside" -Movie "kung fu soccer" -Poll 20 -Live
#>
[CmdletBinding()]
param(
    [string]$Movie = "",
    [string]$Cinema = "",
    [string]$Date = "",
    [int]$Shards = 3,
    [ValidateRange(1, 24)]
    [int]$Houses = 3,
    [ValidateRange(1, 32)]
    [int]$Workers = 8,
    [switch]$Live,
    [switch]$Drain,
    [double]$Poll = 0,
    [string]$Repo = "C:\Users\mrsma\deepseek harness\mcl-fullhouse"
)

$py = Join-Path $Repo "fill_all.py"
if (-not (Test-Path $py)) { throw "fill_all.py not found at $py" }

$mode = "DRY-RUN"
if ($Live) { $mode = "LIVE" }
if ($Poll -gt 0) { $mode = "WATCH ($([math]::Round($Poll))s poll)" }

for ($i = 1; $i -le $Shards; $i++) {
    $inner = "`$env:PYTHONIOENCODING='utf-8'; Set-Location '$Repo'; python fill_all.py"
    if ($Live)        { $inner += ' --live' }
    if ($Movie)       { $inner += " --movie '$Movie'" }
    if ($Cinema)      { $inner += " --cinema '$Cinema'" }
    if ($Date)        { $inner += " --date '$Date'" }
    if ($Drain)       { $inner += ' --drain' }
    if ($Poll -gt 0)  { $inner += " --poll $Poll" }
    if ($Poll -le 0)  { $inner += " --shard $i/$Shards --houses $Houses" }
    $inner += " --workers $Workers --max-rounds 40 --refresh 5"

    Start-Process powershell -ArgumentList '-NoExit', '-Command', $inner `
        -WorkingDirectory $Repo `
        -WindowStyle Normal
    Start-Sleep -Milliseconds 400   # stagger discovery bursts a touch
}

Write-Host ("Launched {0} window(s) ({1}, workers={2})" -f $Shards, $mode, $Workers)
Write-Host "Close any window (or Ctrl+C inside) to stop it; re-run anytime."
