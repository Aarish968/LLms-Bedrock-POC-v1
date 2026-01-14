# {{cookiecutter.project_slug}}

{{cookiecutter.project_short_description}}

### Prefect Deployment

Since we are building wheels of internal packages ensure you have the latest versions of these repositories

```bash
uv lock --upgrade-package common-prefect-next{%- if cookiecutter.writes_excel -%}[excel]{%- endif %} && uv sync --all-extras
```

### Deploy Dev
```bash
uv lock --upgrade-package common-prefect-next{%- if cookiecutter.writes_excel -%}[excel]{%- endif %} && uv sync --all-extras
uv run prefect --no-prompt deploy --prefect-file ./prefect.dev.yaml --all 
```

### Deploy Prod
```bash
uv lock --upgrade-package common-prefect-next{%- if cookiecutter.writes_excel -%}[excel]{%- endif %} && uv sync --all-extras
uv run prefect --no-prompt deploy --prefect-file ./prefect.yaml --all 
```
