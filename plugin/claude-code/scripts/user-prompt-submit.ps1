#!/usr/bin/env pwsh
# Engram — Windows-native UserPromptSubmit hook for Claude Code
#
# Optional fallback for enterprise Windows environments where Git Bash/MSYS2
# fork emulation is slowed or blocked by Defender/EDR. Keep this script small
# and dependency-free; it must never block prompt submission.

# Ensure UTF-8 output so JSON payloads with non-ASCII characters are not
# mangled when Claude Code reads this hook's stdout. Without this, Windows
# defaults to the system codepage (e.g. CP1252/CP850) which corrupts
# multi-byte characters in the systemMessage JSON (issue #421).
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::InputEncoding  = [System.Text.Encoding]::UTF8

$ErrorActionPreference = 'SilentlyContinue'

function Write-EmptyHookResponse {
  Write-Output '{}'
}

function Write-ToolSearchMessage {
  $message = "Engram Terminal Memory tools for settled root user turns:`nselect:mcp__engram__mem_current_project,mcp__engram__mem_search,mcp__engram__mem_get_observation,mcp__engram__mem_checkpoint,mcp__engram__mem_checkpoint_status`n`nUse the engram-memory skill for the saved, needs_review, or skipped(no_durable_knowledge) disposition after all causal work settles."
  [PSCustomObject]@{ systemMessage = $message } | ConvertTo-Json -Compress
}

function Resolve-EngramProject {
  param(
    [string]$EngramUrl,
    [string]$Cwd
  )
  if ([string]::IsNullOrWhiteSpace($Cwd)) { return $null }
  try {
    $encodedCwd = [System.Uri]::EscapeDataString($Cwd)
    $resolution = Invoke-RestMethod -Method Get -Uri "$EngramUrl/project/current?cwd=$encodedCwd" -TimeoutSec 1
    $projectProperty = @($resolution.PSObject.Properties | Where-Object { $_.Name -ceq 'project' })
    $sourceProperty = @($resolution.PSObject.Properties | Where-Object { $_.Name -ceq 'project_source' })
    $strengthProperty = @($resolution.PSObject.Properties | Where-Object { $_.Name -ceq 'project_strength' })
    $implicitWriteProperty = @($resolution.PSObject.Properties | Where-Object { $_.Name -ceq 'implicit_write_allowed' })
    if ($projectProperty.Count -ne 1 -or $sourceProperty.Count -ne 1 -or $strengthProperty.Count -ne 1 -or $implicitWriteProperty.Count -ne 1 -or $projectProperty[0].Value -isnot [string] -or $sourceProperty[0].Value -isnot [string] -or $strengthProperty[0].Value -isnot [string] -or $implicitWriteProperty[0].Value -isnot [bool]) {
      return $null
    }
    $project = $projectProperty[0].Value
    $strength = $strengthProperty[0].Value
    $writeAllowed = $implicitWriteProperty[0].Value
    $validStrengths = @('strong', 'explicit')
    if ([string]::IsNullOrWhiteSpace($project) -or $validStrengths -cnotcontains $strength -or ($strength -cne 'explicit' -and -not $writeAllowed) -or $null -ne $resolution.PSObject.Properties['error_hint']) {
      return $null
    }
    return $project
  } catch {
    return $null
  }
}

function Invoke-EngramPromptCaptureRequest {
  param(
    [string]$EngramUrl,
    [string]$SessionId,
    [string]$Project,
    [string]$Prompt
  )
  # Fail-silent and bounded. Core persists only with explicit capture consent;
  # the adapter cannot infer or grant it.
  if ([string]::IsNullOrWhiteSpace($Prompt) -or [string]::IsNullOrWhiteSpace($SessionId) -or [string]::IsNullOrWhiteSpace($Project)) { return }
  try {
    $body = [PSCustomObject]@{
      session_id = $SessionId
      project    = $Project
      content    = $Prompt
    } | ConvertTo-Json -Compress
    $null = Invoke-RestMethod -Method Post -Uri "$EngramUrl/prompts" `
      -ContentType 'application/json' -Body $body -TimeoutSec 1
  } catch { }
}

try {
  $engramPort = if ($env:ENGRAM_PORT) { $env:ENGRAM_PORT } else { '7437' }
  $engramUrl  = "http://127.0.0.1:$engramPort"

  $inputJson = [Console]::In.ReadToEnd()
  $payload = $inputJson | ConvertFrom-Json
  $sessionID = [string]($payload.session_id)
  $cwd       = [string]($payload.cwd)
  $prompt    = [string]($payload.prompt)

  if ([string]::IsNullOrWhiteSpace($sessionID)) {
    $sessionID = "windows-$PID"
  }

  # Request capture only after canonical server resolution; do not infer a
  # project in the hook when the server is unavailable, invalid, or ambiguous.
  $project = Resolve-EngramProject -EngramUrl $engramUrl -Cwd $cwd
  Invoke-EngramPromptCaptureRequest -EngramUrl $engramUrl -SessionId $sessionID -Project $project -Prompt $prompt

  $safeSessionID = $sessionID -replace '[^a-zA-Z0-9_-]', '_'
  $stateFile = Join-Path ([IO.Path]::GetTempPath()) "engram-claude-$safeSessionID-tools-loaded"

  if (-not (Test-Path -LiteralPath $stateFile)) {
    New-Item -ItemType File -Path $stateFile -Force | Out-Null
    Write-ToolSearchMessage
    exit 0
  }

  Write-EmptyHookResponse
  exit 0
} catch {
  Write-EmptyHookResponse
  exit 0
}
