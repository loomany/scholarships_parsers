Set-Location $PSScriptRoot
$ErrorActionPreference = "Stop"

function Import-LocalEnv {
    param([string] $Path)
    if (-not (Test-Path $Path)) { return }
    Write-Host "[mastersportal] loading local env: $Path" -ForegroundColor DarkGray
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

# Load shared secrets first, then source-specific local overrides.
Import-LocalEnv (Join-Path $PSScriptRoot ".env")

$env:PARSER_SOURCES = "mastersportal"
$env:MASTERSPORTAL_ENABLED = "1"
$env:MASTERSPORTAL_HEADLESS = "0"
$env:MASTERSPORTAL_DETAIL_FETCH = "0"
$env:MASTERSPORTAL_KEEP_BROWSER_OPEN = "1"
$env:MASTERSPORTAL_TIMEOUT_MS = "120000"
$env:MASTERSPORTAL_AUTH_WAIT_SECONDS = "900"
$env:MASTERSPORTAL_LISTING_DELAY_MS = "15000"
$env:MASTERSPORTAL_USE_PERSISTENT_PROFILE = "1"
$env:MASTERSPORTAL_BROWSER_CHANNEL = "chrome"
$env:MASTERSPORTAL_DIRECT_PAGE_GOTO = "0"
$env:SCHOLARSHIP_AI_FINAL_ENABLED = "0"
$env:TARGET_NEW_ITEMS = "0"
$env:MAX_LIST_PAGES = "10000"
$env:NO_NEW_PAGES_STOP = "0"
$env:SKIP_EXISTING_ON_LIST = "1"
$env:USE_TITLE_FALLBACK_KNOWN = "0"

Import-LocalEnv (Join-Path $PSScriptRoot ".env.mastersportal.local")

$env:PYTHONUNBUFFERED = "1"
$env:PYTHONIOENCODING = "utf-8"

# Keep the browser profile between runs so Cloudflare/login sees the same browser.
Remove-Item (Join-Path $PSScriptRoot "mastersportal_session.json") -ErrorAction SilentlyContinue

$py = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) {
    $py = "python"
}

& $py -u -m sources.mastersportal.parser
exit $LASTEXITCODE
