# Dependency Management

This project uses modern Python dependency management with `pyproject.toml` instead of legacy `requirements.txt` files.

## Package Manager Options

### Option 1: UV (Recommended)
UV is a fast Python package manager that works with `pyproject.toml`:

```bash
# Install UV
pip install uv

# Install dependencies
uv sync

# Install with dev dependencies
uv sync --dev

# Run commands in the virtual environment
uv run uvicorn api.main:app --reload
uv run pytest
uv run ruff check
```

### Option 2: Pip (Standard)
Standard pip also works with `pyproject.toml`:

```bash
# Install in editable mode
pip install -e .

# Install with dev dependencies
pip install -e .[dev]

# Run normally
python -m uvicorn api.main:app --reload
```

## Dependency Categories

### Production Dependencies
Defined in `pyproject.toml` under `[project.dependencies]`:
- FastAPI, Uvicorn
- Database connectors (Snowflake)
- AWS services (Boto3, Cognito)
- Data processing (Pandas, SQLGlot)

### Development Dependencies
Defined in `pyproject.toml` under `[project.optional-dependencies.dev]`:
- Testing (Pytest)
- Code quality (Ruff, MyPy)
- Documentation (MkDocs)
- Pre-commit hooks

## Adding New Dependencies

### Production Dependency
```bash
# Add to pyproject.toml [project.dependencies]
"new-package>=1.0.0"

# Then sync
uv sync
```

### Development Dependency
```bash
# Add to pyproject.toml [project.optional-dependencies.dev]
"new-dev-package>=1.0.0"

# Then sync with dev
uv sync --dev
```

## Migration from requirements.txt

✅ **Completed**: This project has been migrated from `requirements.txt` to `pyproject.toml`

- ❌ `requirements.txt` - Removed
- ❌ `requirements-dev.txt` - Removed  
- ✅ `pyproject.toml` - Contains all dependencies
- ✅ `uv.lock` - Lock file for reproducible builds

## Benefits of pyproject.toml

1. **Single file** for all project metadata
2. **Standard format** (PEP 518/621)
3. **Better dependency resolution**
4. **Editable installs** with `-e .`
5. **Tool configuration** in same file
6. **Modern ecosystem** compatibility