$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

function Write-CheckpointIntegrationFailure {
    param([string]$Message)

    [ordered]@{
        systemMessage = "Engram checkpoint verifier integration failure: $Message"
    } | ConvertTo-Json -Compress
}

function Add-WindowsArgumentBackslashes {
    param(
        [System.Text.StringBuilder]$Builder,
        [int]$Count
    )

    for ($index = 0; $index -lt $Count; $index++) {
        [void]$Builder.Append([char]92)
    }
}

function ConvertTo-WindowsProcessArgument {
    param([string]$Value)

    if ($Value.Length -eq 0) {
        return '""'
    }
    if ($Value -notmatch '[\s"]') {
        return $Value
    }

    $builder = New-Object System.Text.StringBuilder
    [void]$builder.Append([char]34)
    $backslashes = 0
    foreach ($character in $Value.ToCharArray()) {
        if ($character -eq [char]92) {
            $backslashes++
            continue
        }
        if ($character -eq [char]34) {
            Add-WindowsArgumentBackslashes -Builder $builder -Count (($backslashes * 2) + 1)
            [void]$builder.Append([char]34)
            $backslashes = 0
            continue
        }
        Add-WindowsArgumentBackslashes -Builder $builder -Count $backslashes
        $backslashes = 0
        [void]$builder.Append($character)
    }
    Add-WindowsArgumentBackslashes -Builder $builder -Count ($backslashes * 2)
    [void]$builder.Append([char]34)
    return $builder.ToString()
}

try {
    $rawInput = [Console]::In.ReadToEnd()
    $payload = $rawInput | ConvertFrom-Json -ErrorAction Stop
    if (-not ($payload.session_id -is [string]) -or
        -not ($payload.turn_id -is [string]) -or
        -not ($payload.stop_hook_active -is [bool]) -or
        [string]::IsNullOrWhiteSpace($payload.session_id) -or
        [string]::IsNullOrWhiteSpace($payload.turn_id)) {
        Write-CheckpointIntegrationFailure 'Stop input is missing a string session_id, string turn_id, or boolean stop_hook_active.'
        exit 0
    }

    $arguments = @(
        'checkpoint',
        'status',
        '--host=codex',
        "--session-id=$($payload.session_id)",
        "--root-turn-id=$($payload.turn_id)",
        '--json'
    )
    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = 'engram'
    $startInfo.Arguments = (($arguments | ForEach-Object { ConvertTo-WindowsProcessArgument $_ }) -join ' ')
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $startInfo
    if (-not $process.Start()) {
        throw 'engram did not start'
    }
    $stdoutTask = $process.StandardOutput.ReadToEndAsync()
    $stderrTask = $process.StandardError.ReadToEndAsync()
    if (-not $process.WaitForExit(2000)) {
        $process.Kill()
        $process.WaitForExit()
        Write-CheckpointIntegrationFailure 'checkpoint status timed out after 2 seconds.'
        exit 0
    }
    $process.WaitForExit()
    $stdout = $stdoutTask.Result.Trim()
    $stderr = $stderrTask.Result.Trim()
    $statusCode = $process.ExitCode

    if ($statusCode -eq 0 -and [string]::IsNullOrWhiteSpace($stderr)) {
        $status = $stdout | ConvertFrom-Json -ErrorAction Stop
        $disposition = [string]$status.checkpoint.disposition
        if ($status.checkpoint.identity.host -ceq 'codex' -and
            $status.checkpoint.identity.session_id -ceq $payload.session_id -and
            $status.checkpoint.identity.root_turn_id -ceq $payload.turn_id -and
            @('saved', 'skipped', 'needs_review') -ccontains $disposition) {
            Write-Output '{}'
            exit 0
        }
    }

    if ($statusCode -ne 0 -and [string]::IsNullOrWhiteSpace($stdout)) {
        try {
            $statusError = $stderr | ConvertFrom-Json -ErrorAction Stop
            if ($statusError.code -ceq 'checkpoint_not_found') {
                if ($payload.stop_hook_active) {
                    Write-CheckpointIntegrationFailure 'checkpoint is still missing after the single recovery continuation.'
                    exit 0
                }
                $identity = [ordered]@{
                    host = 'codex'
                    session_id = $payload.session_id
                    root_turn_id = $payload.turn_id
                } | ConvertTo-Json -Compress
                $reason = "Finalize the missing Engram checkpoint for the original root user turn $identity using the Engram memory skill. Preserve this identity unchanged; do not checkpoint this continuation."
                [ordered]@{ decision = 'block'; reason = $reason } | ConvertTo-Json -Compress
                exit 0
            }
        } catch {
        }
    }

    Write-CheckpointIntegrationFailure 'checkpoint status did not return the expected terminal result.'
} catch {
    Write-CheckpointIntegrationFailure 'the verifier could not execute checkpoint status.'
}

exit 0
