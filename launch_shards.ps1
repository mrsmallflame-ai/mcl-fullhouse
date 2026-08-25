<#
.SYNOPSIS
    Spawn N shard terminals for mcl-fullhouse, each with a disjoint slice.

.DESCRIPTION
    Each spawned window runs fill_all.py on its own --shard slice of the movie
    queue. Defaults to DRY-RUN; pass -Live to actually claim seats (adds the
    usual 5-second abort window in every window).

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

$repoLeaf = Split-Path $Repo -Leaf
$py = Join-Path $Repo "fill_all.py"

for ($i = 1; $i -le $Shards; $i++) {
    $inner = "`$env:PYTHONIOENCODING='utf-8'; Set-Location '$Repo'; python fill_all.py"
    if ($Live)        { $inner += ' --live' }
    if ($Movie)       { $inner += " --movie '$Movie'" }
    $inner += " --shard $i/$Shards --houses $Houses --workers $Workers --max-rounds 40 --refresh 5"

    Start-Process powershell -ArgumentList '-NoExit', '-Command', $inner `
        -WorkingDirectory $Repo `
        -WindowStyle Normal
    Start-Sleep -Milliseconds 400   # stagger discovery bursts a touch
}

Write-Host "🚀 launched $Shards shard windows ($([bool]$Live ? 'LIVE' : 'DRY-RUN'), houses=$Houses, workers=$Workers)"
Write-Host "   close any window (or Ctrl+C inside) to stop that shard; re-run anytime."
