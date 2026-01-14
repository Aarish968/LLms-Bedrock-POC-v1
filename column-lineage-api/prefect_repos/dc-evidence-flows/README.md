**create_evidence_customer** is a flow triggered from /api/v2/workflows/lookups/tag-history endpoint.
This flow generates tag history report

# About


Refresh packages
```bash
uv sync --all-extras
```

Upgrade packages
```bash
uv lock --upgrade-package common-prefect-next --upgrade-package common-serial-resolution && uv sync --all-extras
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
To ensure logs from `dc-evidence-customer-flow` appear, include PREFECT_LOGGING_EXTRA_LOGGERS: evidence_flows in the deployment
If using common_prefect_next, snowflake logging is already included in the base image

