<#
.SYNOPSIS
    Spawn N shard terminals for mcl-fullhouse, each with a disjoint slice.
    Compatible with Windows PowerShell 5.1 and pwsh 7+. ASCII-only on purpose.

.EXAMPLE
    .\launch_shards.ps1 -Movie "kung fu soccer"                    # 3 dry-run windows
    .\launch_shards.ps1 -Movie "kung fu soccer" -Shards 9 -Live -Houses 3 -Workers 12
#>
[CmdletBinding()]
param(
    [string]$Movie = "",
    [int]$Shards = 3,
    [ValidateRange(1, 24)]
    [int]$Houses = 3,
    [ValidateRange(1, 32)]
    [int]$Workers = 8,
    [switch]$Live,
    [string]$Repo = "C:\Users\mrsma\deepseek harness\mcl-fullhouse"
)

$py = Join-Path $Repo "fill_all.py"
if (-not (Test-Path $py)) { throw "fill_all.py not found at $py" }

$mode = "DRY-RUN"
if ($Live) { $mode = "LIVE" }

for ($i = 1; $i -le $Shards; $i++) {
    $inner = "`$env:PYTHONIOENCODING='utf-8'; Set-Location '$Repo'; python fill_all.py"
    if ($Live)  { $inner += ' --live' }
    if ($Movie) { $inner += " --movie '$Movie'" }
    $inner += " --shard $i/$Shards --houses $Houses --workers $Workers --max-rounds 40 --refresh 5"

    Start-Process powershell -ArgumentList '-NoExit', '-Command', $inner `
        -WorkingDirectory $Repo `
        -WindowStyle Normal
    Start-Sleep -Milliseconds 400   # stagger discovery bursts a touch
}

Write-Host ("Launched {0} shard windows ({1}, houses={2}, workers={3})" -f $Shards, $mode, $Houses, $Workers)
Write-Host "Close any window (or Ctrl+C inside) to stop that shard; re-run anytime."
