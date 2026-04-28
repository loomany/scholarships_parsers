# Запуск run_all с видимым прогрессом в консоли (небуферизованный stdout/stderr).
# Браузерные парсеры: окно Chromium остаётся видимым во время работы (headful задаётся в run_all.py).
$ErrorActionPreference = "Stop"
$env:PYTHONUNBUFFERED = "1"
$env:PYTHONIOENCODING = "utf-8"
Set-Location $PSScriptRoot
if (-not (Test-Path ".\.venv\Scripts\python.exe")) {
    Write-Host "Нет .venv — создайте venv и установите зависимости (requirements.txt)." -ForegroundColor Red
    exit 1
}
& ".\.venv\Scripts\python.exe" -u ".\run_all.py" @args
