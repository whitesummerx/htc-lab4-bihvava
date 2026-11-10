# Full deployment of lab 4 into a local kind cluster. Run from the project root:
#   .\scripts\deploy.ps1
$ErrorActionPreference = "Continue"
$Cluster = "lab4"
$Image   = "appointments-web:lab4"
$WorkerImage = "appointments-worker:lab4"
Set-Location (Split-Path -Parent $PSScriptRoot)

function Run($cmd) { Write-Host ">> $cmd" -ForegroundColor Cyan; Invoke-Expression $cmd; if ($LASTEXITCODE -ne 0) { throw "failed: $cmd" } }

# 0. password from .env (never stored in the image, ConfigMap or repository)
if (-not (Test-Path .env)) { throw ".env not found (see .env.example)" }
$line = Get-Content .env | Where-Object { $_ -match '^\s*POSTGRES_PASSWORD\s*=' } | Select-Object -First 1
if (-not $line) { throw "POSTGRES_PASSWORD is not set in .env" }
$Password = ($line -split '=', 2)[1].Trim().Trim('"').Trim("'")

# 1. cluster
$exists = (kind get clusters 2>$null) -contains $Cluster
if (-not $exists) { Run "kind create cluster --name $Cluster --config k8s/kind-config.yaml" }
Run "kubectl config use-context kind-$Cluster"

# 2. label the control-plane node for infrastructure pods (DB, monitoring)
Run "kubectl label node $Cluster-control-plane role=infra --overwrite"

# 3. build the service image and load it into the cluster nodes
Run "docker build -t $Image ."
Run "kind load docker-image $Image --name $Cluster"
Run "docker build -f Dockerfile.worker -t $WorkerImage ."
Run "kind load docker-image $WorkerImage --name $Cluster"

# 4. secret and schema ConfigMap
kubectl create secret generic postgres-secret --from-literal=POSTGRES_PASSWORD="$Password" --dry-run=client -o yaml | kubectl apply -f -
if ($LASTEXITCODE -ne 0) { throw "secret failed" }
kubectl create configmap db-init --from-file=init.sql=db/init.sql --dry-run=client -o yaml | kubectl apply -f -
if ($LASTEXITCODE -ne 0) { throw "db-init failed" }

# 5. database (StatefulSet) and migration Job (Jobs are immutable: recreate)
Run "kubectl apply -f k8s/postgres.yaml"
Run "kubectl rollout status statefulset/db --timeout=180s"
kubectl delete job db-migration --ignore-not-found | Out-Null
Run "kubectl apply -f k8s/migration-job.yaml"
Run "kubectl wait --for=condition=complete job/db-migration --timeout=180s"

# 6. web service (the replica count is NOT in the manifest, so re-applying keeps it)
Run "kubectl apply -f k8s/web.yaml"
Run "kubectl rollout restart deployment/web"
Run "kubectl rollout status deployment/web --timeout=180s"

# 6b. workers (the replica count is NOT in the manifest; default 1, scripts use kubectl scale)
Run "kubectl apply -f k8s/worker.yaml"
Run "kubectl rollout restart deployment/worker"
Run "kubectl rollout status deployment/worker --timeout=180s"

# 7. monitoring (Prometheus, kube-state-metrics, cAdvisor, postgres-exporter, metrics-server)
& "$PSScriptRoot\monitoring.ps1"

Write-Host "`nDone. Service: http://localhost:8080  (check: curl.exe -i http://localhost:8080/livez)" -ForegroundColor Green
