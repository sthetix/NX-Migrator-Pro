#Requires -Version 5.1
<#
.SYNOPSIS
    Build a distributable zip for NX Migrator Pro.

.DESCRIPTION
    Creates a zip archive containing core/, gui/, tool/, install.bat, and main.py.

.PARAMETER OutputZip
    Path for the output zip file. Defaults to dist/NXMigratorPro-X.X.X.zip,
    where the version comes from the latest git tag.

.EXAMPLE
    .\package.ps1

.EXAMPLE
    .\package.ps1 -OutputZip "C:\releases\NXMigratorPro-1.0.5.zip"
#>
param(
    [string]$OutputZip
)

$ErrorActionPreference = "Stop"

$Root = $PSScriptRoot

function Get-LatestGitTagVersion {
    $tag = git -C $Root tag --sort=-v:refname 2>$null | Select-Object -First 1
    if (-not $tag) {
        throw "No git tags found. Create a tag (e.g. git tag 1.0.5) before packaging."
    }

    return ($tag.Trim() -replace '^v', '')
}

if (-not $OutputZip) {
    $version = Get-LatestGitTagVersion
    $OutputZip = Join-Path $Root "dist\NXMigratorPro-$version.zip"
}
$Include = @("core", "gui", "tool", "install.bat", "main.py")

foreach ($item in $Include) {
    $path = Join-Path $Root $item
    if (-not (Test-Path $path)) {
        throw "Required path not found: $path"
    }
}

$outputDir = Split-Path -Parent $OutputZip
if ($outputDir -and -not (Test-Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
}

if (Test-Path $OutputZip) {
    Remove-Item $OutputZip -Force
}

Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem

$zip = [System.IO.Compression.ZipFile]::Open(
    $OutputZip,
    [System.IO.Compression.ZipArchiveMode]::Create
)

try {
    foreach ($item in $Include) {
        $fullPath = Join-Path $Root $item

        if (Test-Path $fullPath -PathType Container) {
            Get-ChildItem -Path $fullPath -Recurse -File |
                Where-Object {
                    $_.FullName -notmatch "\\__pycache__\\" -and
                    $_.Extension -ne ".pyc"
                } |
                ForEach-Object {
                    $entryName = $_.FullName.Substring($Root.Length + 1).Replace("\", "/")
                    [void][System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
                        $zip,
                        $_.FullName,
                        $entryName
                    )
                }
        }
        else {
            [void][System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
                $zip,
                $fullPath,
                $item
            )
        }
    }
}
finally {
    $zip.Dispose()
}

Write-Host "Created $OutputZip"
