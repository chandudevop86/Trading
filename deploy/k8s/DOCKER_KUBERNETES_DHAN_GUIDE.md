# Docker + Kubernetes + Dhan API (Future) Guide

This guide deploys the Streamlit app with Docker and Kubernetes, routes traffic from `chandudevopai.shop`, and keeps a future-ready Dhan API tab in the app.

## 1) Local Docker run

```bash
cd /opt/intratrade
vim deploy/docker/Dockerfile
vim deploy/docker/docker-compose.yml

# Build image
docker build -f deploy/docker/Dockerfile -t intratrade:latest .

# Run container
docker compose -f deploy/docker/docker-compose.yml up -d

# Verify
docker ps
docker logs intratrade_app --tail 100
```

Open `http://<server-ip>:8501`.

## 2) Kubernetes deployment (single node or cluster)

```bash
cd /opt/intratrade

# Create namespace + config
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/configmap.yaml

# Create secrets from template
cp deploy/k8s/secret.example.yaml deploy/k8s/secret.yaml
vim deploy/k8s/secret.yaml
kubectl apply -f deploy/k8s/secret.yaml

# Apply workload
kubectl apply -f deploy/k8s/deployment.yaml
kubectl apply -f deploy/k8s/service.yaml
kubectl apply -f deploy/k8s/ingress.yaml
kubectl apply -f deploy/k8s/hpa.yaml

# Verify
kubectl get all -n intratrade
kubectl get ingress -n intratrade
```

## 3) Push image to registry (recommended)

```bash
# Example with Docker Hub
docker tag intratrade:latest <dockerhub-user>/intratrade:latest
docker push <dockerhub-user>/intratrade:latest

# Then update deploy/k8s/deployment.yaml image:
# image: <dockerhub-user>/intratrade:latest
kubectl apply -f deploy/k8s/deployment.yaml
```

## 4) Domain routing

Set DNS A record:
- Host: `chandudevopai.shop`
- Value: your load balancer/public IP

If using NGINX ingress controller, ensure ports 80/443 are open.

For TLS (HTTPS):
- Install cert-manager in cluster.
- Add TLS secret and issuer in ingress manifest.

## 5) App feature included for future Dhan API

The app now includes strategy tab:
- `Dhan API (Future)`

Inside that tab:
- Auth panel for `client_id` and access token
- Market feed contract example
- Order router payload example
- Webhook event schema example
- Downloadable `dhan_order_template.csv`

Current status:
- Dhan tab is scaffold-only (safe for future extension)
- Existing bots continue to run normally

## 6) Useful operations

```bash
# Restart app deployment
kubectl rollout restart deployment intratrade-app -n intratrade

# Follow logs
kubectl logs -f deployment/intratrade-app -n intratrade

# Scale manually
kubectl scale deployment intratrade-app --replicas=2 -n intratrade

# Remove everything
kubectl delete ns intratrade
```
