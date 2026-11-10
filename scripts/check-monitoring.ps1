# Step 5 checks: series per web pod, CFS counters, ready replicas, postgres-exporter, kubectl top.
$ErrorActionPreference = "Continue"
$prom = "http://localhost:9090/api/v1/query"
function Q($label, $query) {
    $r = Invoke-RestMethod -Uri $prom -Method Get -Body @{ query = $query } -ErrorAction Stop
    $n = @($r.data.result).Count
    Write-Host ("{0,-52} series: {1}" -f $label, $n)
    foreach ($s in $r.data.result | Select-Object -First 6) {
        Write-Host ("    " + (($s.metric.PSObject.Properties | Where-Object { $_.Name -in 'pod','container','deployment','state','job' } | ForEach-Object { "$($_.Name)=$($_.Value)" }) -join " ") + "  value=" + $s.value[1])
    }
}
Write-Host "--- targets ---"
$t = Invoke-RestMethod -Uri "http://localhost:9090/api/v1/targets" -ErrorAction Stop
$t.data.activeTargets | Group-Object { $_.labels.job } | ForEach-Object { Write-Host ("{0,-40} {1} targets, up: {2}" -f $_.Name, $_.Count, @($_.Group | Where-Object health -eq 'up').Count) }
Write-Host "--- series ---"
Q "http_requests_total per web pod" 'http_requests_total{job="web"}'
Q "CFS periods (container web)" 'container_cpu_cfs_periods_total{container="web"}'
Q "CFS throttled periods (container web)" 'container_cpu_cfs_throttled_periods_total{container="web"}'
Q "kube_deployment_status_replicas_ready" 'kube_deployment_status_replicas_ready{deployment="web"}'
Q "pg_stat_activity_count (postgres-exporter)" 'pg_stat_activity_count{datname="appointments_db"}'
Write-Host "--- kubectl top pods ---"
kubectl top pods
