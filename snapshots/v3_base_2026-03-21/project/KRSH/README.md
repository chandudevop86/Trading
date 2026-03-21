# KRSH Generic Trading Platform

KRSH is a new standalone integration and deployment project built from the existing trading application code and data.

Goals:
- keep the original `F:\Trading` project unchanged
- reuse the existing trading code and data inside `KRSH\app`
- provide generic deployment assets for Docker, Kubernetes, Terraform, Jenkins, shell, and Ansible
- keep production usage safe by default: paper trading is the default operating model and live broker routing is optional

## Project Layout

- `app/`
  - copied trading application source, data, Streamlit config, and requirements from the original project
- `docker/`
  - generic container build and compose files
- `k8s/`
  - generic Kubernetes manifests
- `terraform/`
  - generic Terraform stack for Kubernetes deployment resources
- `ansible/`
  - server bootstrap and compose deployment playbook
- `jenkins/`
  - CI/CD pipeline for build and deploy flow
- `scripts/`
  - helper scripts for local and server automation

## Runtime Model

KRSH keeps the same trading code as the original project. The business logic is unchanged.

The output remains:
- market analysis
- trade signals
- execution-ready rows
- paper-trade logs
- optional live broker routing if explicitly configured

## Safe Default

- default mode should be `PAPER`
- live broker credentials must be injected through secrets or environment variables
- Kubernetes secret manifests are not committed with real values
- Terraform and Jenkins are templated for generic reuse

## Local Run

```powershell
cd app
py -3 -m pip install -r requirements.txt
py -3 -m streamlit run src/Trading.py --server.address 0.0.0.0 --server.port 8501
```

## Docker Run

```powershell
docker build -f docker/Dockerfile -t krsh-trading:latest .
docker compose -f docker/docker-compose.yml up -d
```

## Kubernetes Apply

```powershell
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.example.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/ingress.yaml
```

## Terraform

```powershell
cd terraform
terraform init
terraform plan -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars
```

## Jenkins

Use `jenkins/Jenkinsfile` as the baseline pipeline. It supports:
- dependency installation
- optional tests
- Docker image build
- image push
- Kubernetes rollout using `kubectl`

## Ansible

Use `ansible/playbook.yml` to prepare a Linux host with Docker and deploy the compose stack.

## Notes

This project is genericized for future integration work. You can adapt naming, registry, ingress host, namespace, and secrets without touching the reused trading source.
