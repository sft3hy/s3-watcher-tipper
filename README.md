# s3-watcher

Watches an S3 bucket for changes hourly and prints new/modified/deleted file contents to stdout.

## Project Structure

```
s3-watcher/
├── app/
│   ├── watcher.py          # Application code
│   ├── requirements.txt
│   └── Dockerfile
├── helm/
│   └── s3-watcher/
│       ├── Chart.yaml
│       ├── values.yaml
│       └── templates/
│           ├── _helpers.tpl
│           ├── deployment.yaml
│           ├── secret.yaml
│           └── serviceaccount.yaml
└── argocd-application.yaml
```

## 1. Build & Push the Docker Image

```bash
docker build -t your-registry/s3-watcher:1.0.0 ./app
docker push your-registry/s3-watcher:1.0.0
```

## 2. Configure Values

Edit `helm/s3-watcher/values.yaml` and set:
- `image.repository` — your registry path
- `aws.region`
- `s3.bucketName`

**Do not commit credentials to git.** Use one of these approaches instead:

### Option A — Pre-create the secret and reference it
```bash
kubectl create namespace s3-watcher
kubectl create secret generic s3-watcher-creds \
  --from-literal=aws-access-key-id=AKIA... \
  --from-literal=aws-secret-access-key=... \
  -n s3-watcher
```
Then set `aws.existingSecret: s3-watcher-creds` in values.yaml.

### Option B — Sealed Secrets / External Secrets Operator
Use your cluster's secret management solution and reference the resulting secret via `aws.existingSecret`.

### Option C — Helm set at deploy time (local testing only)
```bash
helm install s3-watcher ./helm/s3-watcher \
  --set aws.accessKeyId=AKIA... \
  --set aws.secretAccessKey=... \
  --set s3.bucketName=my-bucket \
  -n s3-watcher --create-namespace
```

## 3. Deploy via ArgoCD

1. Push this repo to your git remote.
2. Edit `argocd-application.yaml` → set `spec.source.repoURL` to your repo.
3. Apply the ArgoCD Application:
   ```bash
   kubectl apply -f argocd-application.yaml
   ```
4. ArgoCD will sync and deploy the Helm chart automatically.

## 4. View Logs

```bash
kubectl logs -f deployment/s3-watcher-s3-watcher -n s3-watcher
```
