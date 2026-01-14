
```bash
# Switch to the context
kubectl config use-context arn:aws:eks:us-east-1:837578041534:cluster/prefect-flows-dev
kubectl create namespace prefect-3
```

### Configuring worker

#### Prefect Cloud UI
Create an API Key
Create a Workpool, Kubernetes. Named K8S
Use the namespace prefect-3 (step 2)
Finished JOB TTL 300 Seconds

#### API Key

API Created as PREFECT_K8S_WORKER in the Prefect Cloud
```bash
kubectl create secret generic prefect-api-key --from-literal=key=<PREFECT_API_KEY> -n prefect-3
```

#### Helm Install

```bash
helm repo add prefect https://prefecthq.github.io/prefect-helm
helm install prefect-worker prefect/prefect-worker --namespace prefect-3 -f values.yaml
```

#### Upgrading Helm Chart

```bash
kubectl config use-context arn:aws:eks:us-east-1:837578041534:cluster/prefect-flows-dev
helm repo update
helm upgrade prefect-worker prefect/prefect-worker --namespace prefect-3 -f values.yaml
```