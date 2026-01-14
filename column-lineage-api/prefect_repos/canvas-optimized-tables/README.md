Refresh packages
```bash
uv sync --all-extras
```

Upgrade packages
```bash
uv lock --upgrade-package common_prefect_next
```
### Prefect Deployment

#### Dev

```bash
uv lock --upgrade-package common_prefect_next && uv sync --all-extras 
```

```bash
uv run prefect --no-prompt deploy --prefect-file ./prefect.dev.yaml --all
```

#### Prod

```bash
uv lock --upgrade-package common_prefect_next && uv sync --all-extras
```

```bash
uv run prefect --no-prompt deploy --prefect-file ./prefect.yaml --all 
```


### Logging
To ensure logs from canvas_optimized_tables appear, include PREFECT_LOGGING_EXTRA_LOGGERS: canvas_optimized_tables in the deployment
If using common_prefect_next, snowflake logging is already included in the base image