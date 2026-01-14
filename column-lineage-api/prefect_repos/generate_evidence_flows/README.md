**evidence-zone-aggregates** is scheduled flow which runs every one hour.
Creates DC_EVIDENCE_ZONES table with customer and collector data.


Refresh packages
```bash
uv sync --all-extras --upgrade-package common-prefect-next
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
To ensure logs from `generate_evidence_flows` appear, include PREFECT_LOGGING_EXTRA_LOGGERS: generate_evidence_flows in the deployment
If using common_prefect_next, snowflake logging is already included in the base image

