# Starts the demo backends from examples/lb.conf, then the load balancer.
#   scripts\demo.ps1 [-Build build]
# Ctrl-C stops the balancer; the backends are stopped on exit.
param([string]$Build = "build")
$ErrorActionPreference = "Stop"
$procs = @()
try {
  foreach ($i in 1..3) {
    $procs += Start-Process -FilePath "$Build\demo_backend.exe" -ArgumentList "--port", "900$i", "--id", "web$i" -NoNewWindow -PassThru
  }
  foreach ($i in 1..2) {
    $procs += Start-Process -FilePath "$Build\demo_backend.exe" -ArgumentList "--port", "910$i", "--id", "echo$i", "--mode", "tcp" -NoNewWindow -PassThru
  }
  Start-Sleep -Milliseconds 500
  Write-Host "try: curl.exe localhost:8080/echo   curl.exe -H 'X-Canary: 1' localhost:8080/   curl.exe localhost:9901/stats"
  & "$Build\hybridlb.exe" --config examples\lb.conf
} finally {
  $procs | ForEach-Object { Stop-Process -Id $_.Id -ErrorAction SilentlyContinue }
}
