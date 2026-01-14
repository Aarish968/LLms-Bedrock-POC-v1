# thoughtspot-flows

### Prefect Deployment


Since we are building wheels of internal packages ensure you have the latest versions of these repositories

```bash
uv lock --upgrade-package dc-canvas-service --upgrade-package common-prefect-next && uv sync --all-extras
```

### Versioning

```bash
uv run version minor
```

```bash
uv lock --upgrade-package dc-canvas-service --upgrade-package common-prefect-next \
  && uv sync --all-extras \
  && uv run prefect --no-prompt deploy --all
```

#### Dev

```bash
uv lock --upgrade-package dc-canvas-service --upgrade-package common-prefect-next \
  && uv sync --all-extras \
  && uv run prefect --no-prompt deploy --prefect-file ./prefect.dev.yaml --all
```

```bash
rm -rf ./dist
```

#### Prod

```bash
uv lock --upgrade-package dc-canvas-service --upgrade-package common-prefect-next \
  && uv sync --all-extras \
  && uv run prefect --no-prompt deploy --prefect-file ./prefect.yaml --all
```


```bash
rm -rf ./dist
```