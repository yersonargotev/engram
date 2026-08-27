$ErrorActionPreference = 'Stop'

& engram checkpoint verify-stop --host=codex
exit $LASTEXITCODE
