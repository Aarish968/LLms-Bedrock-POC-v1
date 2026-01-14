**daily_dc_metrics** is a scheduled flow which run daily.
Creates tag_view, related tables in snowflake db and fetch daily metrics.


Refresh packages
```bash
uv sync --all-packages
```

Upgrade packages
```bash
uv lock --upgrade
```

Bump version
```bash
uv run --with hatch hatch version minor
```


### Prefect Deployment

```bash
uv run prefect --no-prompt deploy -n daily_dc_metrics
```


### Logging
To ensure logs from daily_dc_metrics appear, include PREFECT_LOGGING_EXTRA_LOGGERS: daily_dc_metrics in the deployment
If using common_prefect_next, snowflake logging is already included in the base image

