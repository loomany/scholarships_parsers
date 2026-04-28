# Останавливает все процессы python, запущенные с sources/scholarships_com/parser.py
$ErrorActionPreference = "SilentlyContinue"
$killed = @()
Get-CimInstance Win32_Process | ForEach-Object {
  $c = $_.CommandLine
  if (-not $c) { return }
  if ($c -match "scholarships_com[\\/]parser\.py" -or ($c -match "scholarships_com" -and $c -match "parser\.py")) {
    $killed += $_.ProcessId
    Stop-Process -Id $_.ProcessId -Force
  }
}
if ($killed.Count -gt 0) {
  Write-Host "Stopped PIDs: $($killed -join ', ')" -ForegroundColor Green
} else {
  Write-Host "No scholarships.com parser.py processes found." -ForegroundColor Yellow
}
