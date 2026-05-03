Set-Location $PSScriptRoot
$ErrorActionPreference = "Stop"

function Import-LocalEnv {
    param([string] $Path)
    if (-not (Test-Path $Path)) { return }
    Write-Host "[wemakescholars] loading env: $Path" -ForegroundColor DarkGray
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
Import-LocalEnv (Join-Path $PSScriptRoot ".env.wemakescholars.local")

$env:PYTHONUNBUFFERED = "1"
$env:PYTHONIOENCODING = "utf-8"
$env:PARSER_SOURCES = "wemakescholars"
$env:WEMAKE_SCHOLARS_ENABLED = "1"

$py = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) {
    $py = "python"
}

& $py -u -m sources.wemakescholars.parser
exit $LASTEXITCODE
