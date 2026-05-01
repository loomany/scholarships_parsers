# Scholarships.com: пачки "50+50" до конца сайта (и цикл, пока не остановите).
# - ~50: discovery-обход (SCHOLARSHIPS_COM_DISCOVERY_BATCH_PAGES)
# - 50: успешных upsert за один прогон (TARGET_NEW_ITEMS), дальше run_all завершает источник
# - уже известные в БД пропускаются (SKIP_EXISTING + new_only, без FORCE_REFRESH)
# - браузер видимый (SCHOLARSHIPS_COM_HEADLESS=0, RUN_ALL_ALLOW_HEADLESS=0)
#
# Учётные данные: SCHOLARSHIPS_COM_EMAIL / PASSWORD в .env
# Один круг:  .\run_scholarships_com_batch50.ps1 -Once
# Пока Ctrl+C: .\run_scholarships_com_batch50.ps1
param(
    [switch] $Once
)
Set-Location $PSScriptRoot
$ErrorActionPreference = "Stop"
$env:PYTHONUNBUFFERED = "1"
$env:PYTHONIOENCODING = "utf-8"

# Видимый Chromium; не дать .env перекрыть headless=1
$env:RUN_ALL_ALLOW_HEADLESS = "0"
$env:SCHOLARSHIPS_COM_HEADLESS = "0"
$env:SCHOLARSHIPS_COM_KEEP_BROWSER_OPEN = "0"

$env:PARSER_SOURCES = "scholarships_com"
$env:SCHOLARSHIPS_COM_ENABLED = "1"

# Пропуск уже в базе
$env:SKIP_EXISTING_ON_LIST = "1"
$env:DISCOVERY_MODE = "new_only"
$env:USE_TITLE_FALLBACK_KNOWN = "0"
$env:SCHOLARSHIPS_COM_FORCE_REFRESH = "0"

# "50+50" за один запуск run_all
$env:TARGET_NEW_ITEMS = "50"
$env:SCHOLARSHIPS_COM_DISCOVERY_BATCH_PAGES = "50"
# Деталки не обрубаем пачкой — upsert-лимит задаёт TARGET; при необходимости поставьте, например, 500
$env:SCHOLARSHIPS_COM_MAX_DETAIL_SCANS_PER_RUN = "0"

$env:SCHOLARSHIPS_COM_RUN_MODE = "full"
$env:SCHOLARSHIPS_COM_ONLY_INTERNATIONAL = "0"
$env:SCHOLARSHIPS_COM_MAX_RECORDS_DEBUG = "0"
$env:NO_NEW_PAGES_STOP = "0"
$env:MAX_LIST_PAGES = "10000"
# Верхние рамки сайта; приоритет — пачка discovery=50 + TARGET upsert=50
$env:SCHOLARSHIPS_COM_MAX_DISCOVERY_PAGES = "500000"
$env:SCHOLARSHIPS_COM_MAX_LISTING_PAGES = "10000"
$env:SCHOLARSHIPS_COM_MAX_DETAIL_PAGES = "200000"
$env:SOURCE_TIMEOUT_SECONDS = "43200"
$env:SCHOLARSHIPS_COM_DISCOVERY_CHECKPOINT = "1"
$env:SCHOLARSHIPS_COM_DETAIL_CHECKPOINT = "1"

$py = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) { throw "Нет venv: $py" }

do {
    Write-Host "[scholarships_com batch50] starting run_all.py" -ForegroundColor Cyan
    & $py -u .\run_all.py
    $code = $LASTEXITCODE
    Write-Host "[scholarships_com batch50] exit=$code" -ForegroundColor $(if ($code -eq 0) { "Green" } else { "Yellow" })
    if ($Once) { break }
    Write-Host "[scholarships_com batch50] next batch in 3s (Ctrl+C to stop)" -ForegroundColor DarkGray
    Start-Sleep -Seconds 3
} while ($true)

exit $code
