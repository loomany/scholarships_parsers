# Сценарий:
# 1) Сначала process по уже собранным URL (без лимита detail scans за запуск).
# 2) Потом бесконечный цикл 50/50:
#    collect(50 страниц) -> process(50 detail URL) -> repeat.

Set-Location $PSScriptRoot
$ErrorActionPreference = "Continue"
$env:PYTHONIOENCODING = "utf-8"

# Общие настройки Scholarships.com
$env:SCHOLARSHIPS_COM_ENABLED = "1"
$env:SCHOLARSHIPS_COM_ONLY_INTERNATIONAL = "0"
$env:SCHOLARSHIPS_COM_EMAIL = "loomany.self@gmail.com"
$env:SCHOLARSHIPS_COM_PASSWORD = "Kiska@7777"
$env:SCHOLARSHIPS_COM_HEADLESS = "0"   # видно браузер
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

# Шаг 1: сначала вычитать уже найденные URL (process без лимита).
while ($true) {
  $env:SCHOLARSHIPS_COM_RUN_MODE = "process"
  $env:SCHOLARSHIPS_COM_DISCOVERY_BATCH_PAGES = "0"
  $env:SCHOLARSHIPS_COM_MAX_DETAIL_SCANS_PER_RUN = "0"
  Write-Host "[scholarships_com] bootstrap process(all found urls): starting" -ForegroundColor Magenta
  & ".\.venv\Scripts\python.exe" "sources/scholarships_com/parser.py"
  $code = $LASTEXITCODE
  Write-Host "[scholarships_com] bootstrap process(all found urls): exit=$code" -ForegroundColor Magenta
  if ($code -eq 0) { break }
  Write-Host "[scholarships_com] bootstrap process: restart in 5s" -ForegroundColor DarkMagenta
  Start-Sleep -Seconds 5
}

# Шаг 2: постоянный цикл 50/50.
while ($true) {
  # PHASE A: collect 50 страниц
  while ($true) {
    $env:SCHOLARSHIPS_COM_RUN_MODE = "collect"
    $env:SCHOLARSHIPS_COM_DISCOVERY_BATCH_PAGES = "50"
    $env:SCHOLARSHIPS_COM_MAX_LISTING_PAGES = "50"
    $env:SCHOLARSHIPS_COM_MAX_DETAIL_SCANS_PER_RUN = "0"
    Write-Host "[scholarships_com] phase collect(50): starting" -ForegroundColor Yellow
    & ".\.venv\Scripts\python.exe" "sources/scholarships_com/parser.py"
    $code = $LASTEXITCODE
    Write-Host "[scholarships_com] phase collect(50): exit=$code" -ForegroundColor Yellow
    if ($code -eq 0) { break }
    Write-Host "[scholarships_com] phase collect(50): restart in 5s" -ForegroundColor DarkYellow
    Start-Sleep -Seconds 5
  }

  # PHASE B: process 50 detail URL
  while ($true) {
    $env:SCHOLARSHIPS_COM_RUN_MODE = "process"
    $env:SCHOLARSHIPS_COM_DISCOVERY_BATCH_PAGES = "0"
    $env:SCHOLARSHIPS_COM_MAX_LISTING_PAGES = "5000"
    $env:SCHOLARSHIPS_COM_MAX_DETAIL_SCANS_PER_RUN = "50"
    Write-Host "[scholarships_com] phase process(50 urls): starting" -ForegroundColor Green
    & ".\.venv\Scripts\python.exe" "sources/scholarships_com/parser.py"
    $code = $LASTEXITCODE
    Write-Host "[scholarships_com] phase process(50 urls): exit=$code" -ForegroundColor Green
    if ($code -eq 0) { break }
    Write-Host "[scholarships_com] phase process(50 urls): restart in 5s" -ForegroundColor DarkGreen
    Start-Sleep -Seconds 5
  }
}
