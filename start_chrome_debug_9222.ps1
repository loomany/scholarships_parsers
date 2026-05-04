# Start Chrome with remote debugging on port 9222 + separate TEMP user-data-dir.
# ASCII only in Host strings — fixes PowerShell 5 parsing on CP1252 / bad quotes.
$ErrorActionPreference = "Stop"

$candidates = @(
    (Join-Path $env:ProgramFiles "Google\Chrome\Application\chrome.exe"),
    (Join-Path ${env:ProgramFiles(x86)} "Google\Chrome\Application\chrome.exe"),
    (Join-Path $env:LOCALAPPDATA "Google\Chrome\Application\chrome.exe")
)
$chromeExe = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $chromeExe) {
    Write-Host "ERROR: chrome.exe not found in standard paths." -ForegroundColor Red
    exit 1
}

$debugProfile = Join-Path $env:TEMP "unigo-chrome-cdp-profile"
[System.IO.Directory]::CreateDirectory($debugProfile) | Out-Null

Write-Host "Chrome CDP profile (separate from your daily Chrome):" -ForegroundColor Cyan
Write-Host "  $debugProfile"
Write-Host "Launch: --remote-debugging-port=9222" -ForegroundColor Cyan

$chromeArgs = @(
    "--remote-debugging-port=9222",
    "--remote-allow-origins=*",
    "--user-data-dir=$debugProfile"
)
Start-Process -FilePath $chromeExe -ArgumentList $chromeArgs -WindowStyle Normal

Start-Sleep -Seconds 3
try {
    $r = Invoke-WebRequest -Uri "http://127.0.0.1:9222/json/version" -UseBasicParsing -TimeoutSec 8
    Write-Host ""
    Write-Host "OK: 127.0.0.1:9222 is up. Open unigo.com in THIS new Chrome window." -ForegroundColor Green
    Write-Host ""
    Write-Host $r.Content
    exit 0
} catch {
    Write-Host ""
    Write-Host "FAIL: port 9222 did not respond. Close all Chrome, run again." -ForegroundColor Yellow
    Write-Host "Maybe antivirus or corp policy blocked it."
    Write-Host $_.Exception.Message
    exit 1
}
