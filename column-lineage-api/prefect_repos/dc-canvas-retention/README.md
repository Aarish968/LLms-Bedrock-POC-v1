# dc_canvas_retention

A prefect flow for DC Canvas retention (deactivate and delete old canvases).

### Prefect Deployment

Since we are building wheels of internal packages ensure you have the latest versions of these repositories

```bash
uv lock --upgrade-package common-prefect-next --upgrade-package common-canvas-next && uv sync --all-extras
```

### Deploy Dev
```bash
uv lock --upgrade-package common-prefect-next --upgrade-package common-canvas-next && uv sync --all-extras
uv run prefect --no-prompt deploy --prefect-file ./prefect.dev.yaml --all 
```

```bash
rm -rf ./dist
```

### Deploy Prod
```bash
uv lock --upgrade-package common-prefect-next --upgrade-package common-canvas-next && uv sync --all-extras
uv run prefect --no-prompt deploy --prefect-file ./prefect.yaml --all 
```

```bash
rm -rf ./dist
```