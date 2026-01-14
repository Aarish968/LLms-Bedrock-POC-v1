**daily-table-snapshots** is a flow that cleans up old snapshots based on retention policy



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

#### Prod

```bash
uv lock --upgrade-package common-prefect-next && uv sync --all-extras
uv run prefect --no-prompt deploy --prefect-file ./prefect.yaml --all 
```


### Logging

To ensure logs from `table_snapshots` appear, include PREFECT_LOGGING_EXTRA_LOGGERS: table_snapshots in the deployment
If using common_prefect_next, snowflake logging is already included in the base image
