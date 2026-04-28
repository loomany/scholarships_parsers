# Один прогон: process по уже собранным URL + все гранты, подходящие по общим правилам (как Bold).
# Не international-only. Перед запуском при необходимости: .\stop_scholarships_com_parsers.ps1
Set-Location $PSScriptRoot
$ErrorActionPreference = "Stop"
$env:PYTHONIOENCODING = "utf-8"

$env:SCHOLARSHIPS_COM_ENABLED = "1"
$env:SCHOLARSHIPS_COM_ONLY_INTERNATIONAL = "0"
$env:SCHOLARSHIPS_COM_RUN_MODE = "process"

# Очередь: большой merge-файл, иначе стандартный routes
$rq = Join-Path $PSScriptRoot ".scholarships_com_international_routes.json"
$rq2 = Join-Path $PSScriptRoot ".scholarships_com_routes.json"
if (Test-Path $rq) {
  $env:SCHOLARSHIPS_COM_ROUTE_RULES_PATH = $rq
  Write-Host "Routes: $rq"
} elseif (Test-Path $rq2) {
  $env:SCHOLARSHIPS_COM_ROUTE_RULES_PATH = $rq2
  Write-Host "Routes: $rq2"
} else {
  Write-Warning "No routes json in folder; process mode may fail. Run collect/full first."
}

$env:SCHOLARSHIPS_COM_DETAIL_CHECKPOINT_PATH = ".scholarships_com_full_detail_checkpoint.json"
$env:SCHOLARSHIPS_COM_PREFILTER_STORE_PATH = ".scholarships_com_full_prefilter_store.json"
$env:TARGET_NEW_ITEMS = "2000"
$env:SCHOLARSHIPS_COM_MAX_DETAIL_PAGES = "20000"
$env:SCHOLARSHIPS_COM_MAX_DETAIL_SCANS_PER_RUN = "0"
$env:SCHOLARSHIPS_COM_DISCOVERY_CHECKPOINT = "1"
$env:SCHOLARSHIPS_COM_DETAIL_CHECKPOINT = "1"
# Браузер: 1 = headless; 0 = видимое окно
$env:SCHOLARSHIPS_COM_HEADLESS = "1"

& ".\.venv\Scripts\python.exe" "sources/scholarships_com/parser.py"
exit $LASTEXITCODE
