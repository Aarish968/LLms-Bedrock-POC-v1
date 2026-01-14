# tagging_flows

Tagging Flows

### Prefect Deployment

Since we are building wheels of internal packages ensure you have the latest versions of these repositories

```bash
uv lock --upgrade-package common-prefect-next --upgrade-package common-serial-resolution && uv sync --all-extras
```

### Deploy Dev
```bash
uv lock --upgrade-package common-prefect-next --upgrade-package common-serial-resolution && uv sync --all-extras&& uv run prefect --no-prompt deploy --prefect-file ./prefect.dev.yaml --all 
```

### Deploy Prod
```bash
uv lock --upgrade-package common-prefect-next --upgrade-package common-serial-resolution && uv sync --all-extras && uv run prefect --no-prompt deploy --prefect-file ./prefect.yaml --all 
```
