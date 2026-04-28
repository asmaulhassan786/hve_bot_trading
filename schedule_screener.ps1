# Creates a Windows Scheduled Task to run finviz_screener.py
# every weekday at 3:50 PM Eastern Time.
# Run once as Administrator: powershell -ExecutionPolicy Bypass -File schedule_screener.ps1

$scriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$pythonExe  = (Get-Command python).Source   # change to full path if needed
$scriptPath = Join-Path $scriptDir "finviz_screener.py"
$logPath    = Join-Path $scriptDir "screener_log.txt"
$taskName   = "FinvizScreener_3_50pm"

$action = New-ScheduledTaskAction `
    -Execute $pythonExe `
    -Argument "`"$scriptPath`" >> `"$logPath`" 2>&1"

# 3:50 PM — adjust timezone offset if your PC clock is not ET
$trigger = New-ScheduledTaskTrigger `
    -Weekly `
    -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday `
    -At "15:50"

$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
    -StartWhenAvailable

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -RunLevel Highest `
    -Force

Write-Host "Scheduled task '$taskName' created. Output logged to $logPath"
