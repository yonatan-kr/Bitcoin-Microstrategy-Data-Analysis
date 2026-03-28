# schedule_refresh.ps1
# Registers a Windows Task Scheduler job to run refresh.py
# Mon-Fri at 6:15 PM EST (after MSTR market close)
# Run this script once as Administrator in PowerShell

$PythonPath = "C:\Users\16479\.virtualenvs\dbt-env\Scripts\python.exe"
$ScriptPath = "C:\Users\16479\Documents\GitHub\Bitcoin-Microstrategy-Data-Analysis\refresh.py"
$WorkingDir = "C:\Users\16479\Documents\GitHub\Bitcoin-Microstrategy-Data-Analysis"
$LogPath    = "C:\Users\16479\Documents\GitHub\Bitcoin-Microstrategy-Data-Analysis\logs\refresh.log"

# Create logs directory if it doesn't exist
New-Item -ItemType Directory -Force -Path "$WorkingDir\logs" | Out-Null

# Use cmd to redirect stdout + stderr to log file
$Action  = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c `"$PythonPath $ScriptPath >> $LogPath 2>&1`"" `
    -WorkingDirectory $WorkingDir

$Trigger = New-ScheduledTaskTrigger `
    -Weekly `
    -DaysOfWeek Monday, Tuesday, Wednesday, Thursday, Friday `
    -At "6:15PM"

$Settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1) `
    -RunOnlyIfNetworkAvailable `
    -StartWhenAvailable

Register-ScheduledTask `
    -TaskName   "BTC-MSTR Pipeline Refresh" `
    -Action     $Action `
    -Trigger    $Trigger `
    -Settings   $Settings `
    -Description "Incremental BTC + MSTR data refresh and dbt rebuild" `
    -Force

Write-Host ""
Write-Host "Task registered successfully." -ForegroundColor Green
Write-Host "Runs Mon-Fri at 6:15 PM. To verify:"
Write-Host '  Get-ScheduledTask -TaskName "BTC-MSTR Pipeline Refresh"'
Write-Host ""
Write-Host "To run manually right now:"
Write-Host '  Start-ScheduledTask -TaskName "BTC-MSTR Pipeline Refresh"'
