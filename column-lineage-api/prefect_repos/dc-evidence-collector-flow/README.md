**create_evidence_collector** is a flow triggered from /api/v2/workflows/uploads/collector endpoint.
This flow uploads collector evidence file

# About


Refresh packages
```bash
uv sync --all-extras
```

Upgrade packages
```bash
uv lock --upgrade
```

### Prefect Deployment

#### Dev

```bash
uv lock --upgrade-package common-prefect-next && uv sync --all-extras
uv run prefect --no-prompt deploy --prefect-file ./prefect.dev.yaml --all 
```

#### Prod

```bash
uv lock --upgrade-package common-prefect-next && uv sync --all-extras
uv run prefect --no-prompt deploy --prefect-file ./prefect.yaml --all 
```


### Logging
To ensure logs from `dc-evidence-collector-flow` appear, include PREFECT_LOGGING_EXTRA_LOGGERS: dc_evidence_collector_flow in the deployment
If using common_prefect_next, snowflake logging is already included in the base image

