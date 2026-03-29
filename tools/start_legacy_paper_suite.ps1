param(
    [switch]$Once
)

$ErrorActionPreference = 'Stop'
Set-Location 'F:\Trading'

$baseEnv = @{
    TRADING_BROKER_MODE = 'PAPER'
    TRADING_SYMBOL = '^NSEI'
    TRADING_INTERVAL = '5m'
    TRADING_PERIOD = '5d'
    TRADING_CAPITAL = '100000'
    TRADING_RISK_PCT = '1.0'
    TRADING_RR_RATIO = '2.0'
    MAX_TRADES_PER_DAY = '3'
    MAX_DAILY_LOSS = '2500'
    LIVE_TRADING_ENABLED = 'false'
}

$strategies = @(
    'Breakout'
    'Demand Supply (Retest)'
    'Indicator'
    'One Trade/Day'
    'MTF 5m'
    'BTST'
    'AMD + FVG + Supply/Demand'
)

$daemonArgs = 'py -3 -m src.operational_daemon'
if ($Once) {
    $daemonArgs += ' --once'
}

foreach ($strategy in $strategies) {
    $envAssignments = foreach ($item in $baseEnv.GetEnumerator()) {
        '$env:{0}=''{1}''' -f $item.Key, $item.Value
    }
    $envAssignments += '$env:TRADING_STRATEGY=''{0}''' -f $strategy
    $command = @(
        'Set-Location ''F:\Trading'''
        $envAssignments
        $daemonArgs
    ) -join '; '

    Start-Process powershell -ArgumentList '-NoExit', '-Command', $command
}
