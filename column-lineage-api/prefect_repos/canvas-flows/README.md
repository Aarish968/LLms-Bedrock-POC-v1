This flow is mostly a wrapper
around [common-canvas-next](https://us-east-1.console.aws.amazon.com/codesuite/codecommit/repositories/common-canvas-next/browse?region=us-east-1).
Business logic and other feature of creating a canvas are
managed in the common-canvas-next repository.

Upgrade packages

```bash
uv lock --upgrade
```

### Prefect Deployment

Since we are building wheels of internal packages ensure you have the latest versions of these repositories

```bash
uv lock --upgrade-package common-canvas-next --upgrade-package common-prefect-next && uv sync --all-extras
```

#### Dev

```bash
uv lock --upgrade-package common-canvas-next \
 --upgrade-package common-prefect-next \
 && uv sync --all-extras
uv run prefect --no-prompt deploy --prefect-file ./prefect.dev.yaml --all 
```

#### Prod

```bash
uv lock --upgrade-package common-canvas-next \
 --upgrade-package common-prefect-next \
 && uv sync --all-extras
uv run prefect --no-prompt deploy --prefect-file ./prefect.yaml --all 
```

### Logging

To ensure logs from canvas-flows appear, include PREFECT_LOGGING_EXTRA_LOGGERS: common_canvas_next, canvas_flows in the
deployment
If using common_prefect_next, snowflake logging is already included in the base image