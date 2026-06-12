# Shared helper: Docker Compose on PowerShell writes progress to stderr.
# With $ErrorActionPreference = "Stop", that becomes a false fatal error.
#
# Always pass docker args as a string array so flags like -T and -d are not
# parsed as PowerShell parameters:
#   Invoke-Compose -Command @('exec', '-T', 'postgres', 'psql', ...)

function Invoke-Compose {
    [CmdletBinding()]
    param(
        [switch]$AllowFailure,
        [Parameter(Mandatory)]
        [string[]]$Command
    )

    $prevErrorAction = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & docker compose --env-file .env @Command
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $prevErrorAction

    if (-not $AllowFailure -and $exitCode -ne 0) {
        throw "docker compose failed (exit ${exitCode}): docker compose $($Command -join ' ')"
    }
    return $exitCode
}
