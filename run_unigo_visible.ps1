Set-Location $PSScriptRoot
$ErrorActionPreference = "Stop"

function Import-LocalEnv {
    param([string] $Path)
    if (-not (Test-Path $Path)) { return }
    Write-Host "[unigo] loading local env: $Path" -ForegroundColor DarkGray
    Get-Content $Path | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#")) { return }
        $idx = $line.IndexOf("=")
        if ($idx -le 0) { return }
        $name = $line.Substring(0, $idx).Trim()
        $value = $line.Substring($idx + 1).Trim()
        if (
            ($value.StartsWith('"') -and $value.EndsWith('"')) -or
            ($value.StartsWith("'") -and $value.EndsWith("'"))
        ) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        [Environment]::SetEnvironmentVariable($name, $value, "Process")
    }
}

Import-LocalEnv (Join-Path $PSScriptRoot ".env")

# -----------------------------------------------------------------------------
# Альтернатива Cloudflare (рекомендуется): свой Chrome + CDP.
# 1) Из scholarships_parsers: .\start_chrome_debug_9222.ps1  (ОТДЕЛЬНОЕ окно + профиль в %TEMP%\unigo-chrome-cdp-profile)
#    Так порт 9222 поднимается даже если основной Chrome уже был открыт.
# 3) В этом Chrome вручную открой https://www.unigo.com/scholarships и дождись загрузки сайта.
# 4) В .env.unigo.local добавь строку:
#    UNIGO_CDP_URL=http://127.0.0.1:9222
#    (профиль ниже можно оставить; при CDP он не используется)
# -----------------------------------------------------------------------------

$env:PARSER_SOURCES = "unigo"
$env:UNIGO_ENABLED = "1"
$env:UNIGO_HEADLESS = "0"
# Cloudflare: системный Chrome + постоянный профиль (cookies остаются в .unigo_browser_profile/)
$env:UNIGO_USE_PERSISTENT_PROFILE = "1"
$env:UNIGO_PLAYWRIGHT_CHANNEL = "chrome"
# После загрузки первой вкладки — ручное прохождение CF, затем Enter в этом окне терминала
$env:UNIGO_REQUIRE_MANUAL_START = "1"
$env:SKIP_EXISTING_ON_LIST = "1"
$env:DISCOVERY_MODE = "new_only"
# Полный прогон: пока есть ссылки из листингов и гранты не кончились ожидать upsert только по фильтрам.
if (-not $env:TARGET_NEW_ITEMS) { $env:TARGET_NEW_ITEMS = "0" }
if (-not $env:UNIGO_MAX_DISCOVERED) { $env:UNIGO_MAX_DISCOVERED = "0" }
if (-not $env:UNIGO_BY_MAJOR_MAX_CATEGORIES) { $env:UNIGO_BY_MAJOR_MAX_CATEGORIES = "0" }

Import-LocalEnv (Join-Path $PSScriptRoot ".env.unigo.local")

$cdp = [Environment]::GetEnvironmentVariable("UNIGO_CDP_URL", "Process").Trim()
if ($cdp) {
    $checkUrl = "http://127.0.0.1:9222/json/version"
    if ($cdp -match "127\.0\.0\.1:\d+|localhost:\d+") {
        if ($cdp -match ":(\d+)") {
            $p = [int]$Matches[1]
            $checkUrl = "http://127.0.0.1:${p}/json/version"
        }
    }
    try {
        Invoke-WebRequest -Uri $checkUrl -UseBasicParsing -TimeoutSec 4 | Out-Null
    } catch {
        Write-Host ""
        Write-Host "[unigo] UNIGO_CDP_URL set but CDP probe failed: $checkUrl" -ForegroundColor Red
        Write-Host "        Run first: .\start_chrome_debug_9222.ps1  (fixes port 9222)" -ForegroundColor Yellow
        Write-Host ""
        exit 1
    }
}

$env:PYTHONUNBUFFERED = "1"
$env:PYTHONIOENCODING = "utf-8"

$py = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) {
    $py = "python"
}

Write-Host '[unigo] Cloudflare: UNIGO_CDP_URL=http://127.0.0.1:9222 + Chrome --remote-debugging-port=9222' -ForegroundColor DarkYellow
& $py -u -m sources.unigo
exit $LASTEXITCODE
