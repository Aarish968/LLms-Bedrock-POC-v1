Working notes on the kubernetes deployment of prefect

set the context to the prefect-flows-dev

```bash
kubectl config use-context arn:aws:eks:us-east-1:837578041534:cluster/prefect-flows-dev
```

namespaces:
- prefect (v1 and legacy)
  - No resources
- prefect-2 (v2)
    - Has a worker, 2024
- prefect2  (?) 
  - Has an agent and worker, 2023
  - Crashing for a year!

#### Getting the deployed values
Helm needs a "Release Name"

```bash
kubectl get pods -n prefect2 --show-labels
```

Find the label app.kubernetes.io/instance:<release_name>

Use the release name to get the values
```bash
helm get values prefect2-agent -n prefect2
helm get values prefect2-worker -n prefect2

```