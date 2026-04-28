# Цикл 500/500 с резюмом из чекпоинтов (без очистки прогресса)
# 1) collect: обойти 500 discovery/listing страниц
# 2) process: обработать 500 detail URL
# 3) повторять бесконечно

Set-Location $PSScriptRoot
$ErrorActionPreference = "Continue"
$env:PYTHONIOENCODING = "utf-8"

# Общие настройки Scholarships.com
$env:SCHOLARSHIPS_COM_ENABLED = "1"
$env:SCHOLARSHIPS_COM_ONLY_INTERNATIONAL = "0"
$env:SCHOLARSHIPS_COM_EMAIL = "loomany.self@gmail.com"
$env:SCHOLARSHIPS_COM_PASSWORD = "Kiska@7777"
$env:SCHOLARSHIPS_COM_HEADLESS = "0"   # чтобы видеть браузер
$env:SCHOLARSHIPS_COM_KEEP_BROWSER_OPEN = "0"
$env:SCHOLARSHIPS_COM_DISCOVERY_CHECKPOINT = "1"
$env:SCHOLARSHIPS_COM_DETAIL_CHECKPOINT = "1"
$env:SCHOLARSHIPS_COM_SKIP_DISCOVERY = "0"
$env:SCHOLARSHIPS_COM_FORCE_REFRESH = "0"
$env:SCHOLARSHIPS_COM_MAX_DISCOVERY_PAGES = "5000"
$env:SCHOLARSHIPS_COM_MAX_LISTING_PAGES = "5000"
$env:SCHOLARSHIPS_COM_MAX_DETAIL_PAGES = "20000"
$env:TARGET_NEW_ITEMS = "100000"

# Пути, чтобы продолжать с уже накопленной очереди (если есть)
$intlRoutes = Join-Path $PSScriptRoot ".scholarships_com_international_routes.json"
$fullRoutes = Join-Path $PSScriptRoot ".scholarships_com_routes.json"
if (Test-Path $intlRoutes) {
  $env:SCHOLARSHIPS_COM_ROUTE_RULES_PATH = $intlRoutes
  Write-Host "Routes source: $intlRoutes" -ForegroundColor Cyan
} elseif (Test-Path $fullRoutes) {
  $env:SCHOLARSHIPS_COM_ROUTE_RULES_PATH = $fullRoutes
  Write-Host "Routes source: $fullRoutes" -ForegroundColor Cyan
}

# Отдельные full checkpoint/store
$env:SCHOLARSHIPS_COM_DETAIL_CHECKPOINT_PATH = ".scholarships_com_full_detail_checkpoint.json"
$env:SCHOLARSHIPS_COM_PREFILTER_STORE_PATH = ".scholarships_com_full_prefilter_store.json"

while ($true) {
  # PHASE A: collect 500 страниц
  while ($true) {
    $env:SCHOLARSHIPS_COM_RUN_MODE = "collect"
    $env:SCHOLARSHIPS_COM_DISCOVERY_BATCH_PAGES = "500"
    $env:SCHOLARSHIPS_COM_MAX_DETAIL_SCANS_PER_RUN = "0"
    Write-Host "[scholarships_com] phase collect(500): starting" -ForegroundColor Yellow
    & ".\.venv\Scripts\python.exe" "sources/scholarships_com/parser.py"
    $code = $LASTEXITCODE
    Write-Host "[scholarships_com] phase collect(500): exit=$code" -ForegroundColor Yellow
    if ($code -eq 0) { break }
    Write-Host "[scholarships_com] phase collect(500): restart in 5s" -ForegroundColor DarkYellow
    Start-Sleep -Seconds 5
  }

  # PHASE B: process 500 detail URL
  while ($true) {
    $env:SCHOLARSHIPS_COM_RUN_MODE = "process"
    $env:SCHOLARSHIPS_COM_DISCOVERY_BATCH_PAGES = "0"
    $env:SCHOLARSHIPS_COM_MAX_DETAIL_SCANS_PER_RUN = "500"
    Write-Host "[scholarships_com] phase process(500 urls): starting" -ForegroundColor Green
    & ".\.venv\Scripts\python.exe" "sources/scholarships_com/parser.py"
    $code = $LASTEXITCODE
    Write-Host "[scholarships_com] phase process(500 urls): exit=$code" -ForegroundColor Green
    if ($code -eq 0) { break }
    Write-Host "[scholarships_com] phase process(500 urls): restart in 5s" -ForegroundColor DarkGreen
    Start-Sleep -Seconds 5
  }
}
