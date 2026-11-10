# Monitoring + metrics-server. Called from deploy.ps1 (can also be run alone, cluster must exist).
$ErrorActionPreference = "Continue"
Set-Location (Split-Path -Parent $PSScriptRoot)
function Run($cmd) { Write-Host ">> $cmd" -ForegroundColor Cyan; Invoke-Expression $cmd; if ($LASTEXITCODE -ne 0) { throw "failed: $cmd" } }

Run "helm repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update"
Run "helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/ --force-update"
Run "helm repo update"

# kube-prometheus-stack: Prometheus (5 s), kube-state-metrics, kubelet/cAdvisor; everything on the role=infra node
Run "helm upgrade --install kps prometheus-community/kube-prometheus-stack -n monitoring --create-namespace -f k8s/monitoring/kps-values.yaml --wait --timeout 10m"

# metrics-server for kubectl top and HPA; kind needs --kubelet-insecure-tls
Run "helm upgrade --install metrics-server metrics-server/metrics-server -n kube-system --set 'args[0]=--kubelet-insecure-tls' --wait --timeout 5m"

# scrape targets: web pods and postgres-exporter (CRDs exist after the chart install)
Run "kubectl apply -f k8s/monitoring/postgres-exporter.yaml"
Run "kubectl apply -f k8s/monitoring/web-servicemonitor.yaml"
Run "kubectl apply -f k8s/monitoring/worker-servicemonitor.yaml"
Run "kubectl rollout status deployment/postgres-exporter --timeout=120s"
Write-Host "Prometheus: http://localhost:9090" -ForegroundColor Green
