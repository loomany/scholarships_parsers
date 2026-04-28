# FULL SWEEP 500000 + RESUME
# - Большой боевой проход (collect/process без мелких лимитов)
# - При любом сбое автоматически перезапускает фазу
# - Продолжает с чекпоинтов (discovery/detail), прогресс не теряется

Set-Location $PSScriptRoot
$ErrorActionPreference = "Continue"
$env:PYTHONIOENCODING = "utf-8"

$env:SCHOLARSHIPS_COM_ENABLED = "1"
$env:SCHOLARSHIPS_COM_ONLY_INTERNATIONAL = "0"
$env:SCHOLARSHIPS_COM_EMAIL = "loomany.self@gmail.com"
$env:SCHOLARSHIPS_COM_PASSWORD = "Kiska@7777"
$env:SCHOLARSHIPS_COM_HEADLESS = "0"
$env:SCHOLARSHIPS_COM_KEEP_BROWSER_OPEN = "0"

$env:SCHOLARSHIPS_COM_DISCOVERY_CHECKPOINT = "1"
$env:SCHOLARSHIPS_COM_DETAIL_CHECKPOINT = "1"
$env:SCHOLARSHIPS_COM_SKIP_DISCOVERY = "0"
$env:SCHOLARSHIPS_COM_FORCE_REFRESH = "0"
$env:TARGET_NEW_ITEMS = "1000000"

# Пользовательский запрос: 500000 (вместо 5000).
$env:SCHOLARSHIPS_COM_MAX_DISCOVERY_PAGES = "500000"
$env:SCHOLARSHIPS_COM_MAX_LISTING_PAGES = "500000"
$env:SCHOLARSHIPS_COM_MAX_DETAIL_PAGES = "500000"

# process без искусственного лимита за один запуск.
$env:SCHOLARSHIPS_COM_MAX_DETAIL_SCANS_PER_RUN = "0"
$env:SCHOLARSHIPS_COM_DISCOVERY_BATCH_PAGES = "0"

# Используем накопленный routes файл, если есть.
$intlRoutes = Join-Path $PSScriptRoot ".scholarships_com_international_routes.json"
$fullRoutes = Join-Path $PSScriptRoot ".scholarships_com_routes.json"
if (Test-Path $intlRoutes) {
  $env:SCHOLARSHIPS_COM_ROUTE_RULES_PATH = $intlRoutes
  Write-Host "Routes source: $intlRoutes" -ForegroundColor Cyan
} elseif (Test-Path $fullRoutes) {
  $env:SCHOLARSHIPS_COM_ROUTE_RULES_PATH = $fullRoutes
  Write-Host "Routes source: $fullRoutes" -ForegroundColor Cyan
}

# Чекпоинты full sweep.
$env:SCHOLARSHIPS_COM_DETAIL_CHECKPOINT_PATH = ".scholarships_com_full_detail_checkpoint.json"
$env:SCHOLARSHIPS_COM_PREFILTER_STORE_PATH = ".scholarships_com_full_prefilter_store.json"

while ($true) {
  while ($true) {
    $env:SCHOLARSHIPS_COM_RUN_MODE = "collect"
    Write-Host "[scholarships_com] FULL collect(500000): starting" -ForegroundColor Yellow
    & ".\.venv\Scripts\python.exe" "sources/scholarships_com/parser.py"
    $code = $LASTEXITCODE
    Write-Host "[scholarships_com] FULL collect(500000): exit=$code" -ForegroundColor Yellow
    if ($code -eq 0) { break }
    Write-Host "[scholarships_com] FULL collect: restart in 5s (resume from checkpoint)" -ForegroundColor DarkYellow
    Start-Sleep -Seconds 5
  }

  while ($true) {
    $env:SCHOLARSHIPS_COM_RUN_MODE = "process"
    Write-Host "[scholarships_com] FULL process(all pending): starting" -ForegroundColor Green
    & ".\.venv\Scripts\python.exe" "sources/scholarships_com/parser.py"
    $code = $LASTEXITCODE
    Write-Host "[scholarships_com] FULL process(all pending): exit=$code" -ForegroundColor Green
    if ($code -eq 0) { break }
    Write-Host "[scholarships_com] FULL process: restart in 5s (resume from checkpoint)" -ForegroundColor DarkGreen
    Start-Sleep -Seconds 5
  }
}
