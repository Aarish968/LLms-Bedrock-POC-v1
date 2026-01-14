# DC P1 HTEC ETL Flow

This flow is deployed on CAE and is built locally. This required configuring Docker Desktop to not use `containerd`.

## Update Requirements

Export Flow Requirements:
```bash
uv export --no-hashes --no-dev --no-annotate > ./src/flow_requirements.txt && git add ./src/flow_requirements.txt
```

Export Build Requirements:
```bash
uv export --no-hashes --no-annotate > ./src/build_requirements.txt && git add ./src/build_requirements.txt
```

## Prefect Deployment

### Legacy Prefect (0.15.13)

Build and Register Flow:
```bash
PYTHONPATH=./src uv run prefect register -p ./src/dc_p1_htec_etl --project "ElasticSearch Canvas Load"
```

### Prefect 3.0 (New)

This project now supports Prefect 3.0. Follow these steps to configure and deploy securely:

1. Configure Prefect API key in AWS Parameter Store (NEVER commit actual API keys):
   ```bash
   # Store API key in parameter store (replace with actual key)
   aws ssm put-parameter --name "/cam/dev/prefect/api_key" --value "your-api-key" --type SecureString
   ```

2. Set up your local profile configuration:
   ```bash
   # Create profiles directory if it doesn't exist
   mkdir -p %USERPROFILE%\.prefect     # Windows
   mkdir -p ~/.prefect                 # Linux/macOS
   
   # Copy the template (NEVER commit credentials to git)
   copy prefect-profiles.toml %USERPROFILE%\.prefect\profiles.toml    # Windows
   cp prefect-profiles.toml ~/.prefect/profiles.toml                    # Linux/macOS
   ```

3. Set environment variables for credentials (do NOT hard-code in files):
   ```bash
   # Windows
   set PREFECT_API_KEY=your-api-key
   set PREFECT_ACCOUNT_ID=your-account-id
   set PREFECT_WORKSPACE_ID=your-workspace-id
   
   # Linux/macOS
   export PREFECT_API_KEY=your-api-key
   export PREFECT_ACCOUNT_ID=your-account-id
   export PREFECT_WORKSPACE_ID=your-workspace-id
   ```
   
   You can alternatively update the profiles.toml file directly with your account and workspace IDs,
   but ALWAYS keep the API key as an environment variable.

4. Deploy using Prefect 3.0 CLI:
   ```bash
   prefect deploy
   ```

## Troubleshooting

If you encounter an "AuthorizationError: Forbidden" error, ensure that:
1. You have created an API key in Prefect Cloud 3.0
2. The API key is correctly stored in AWS Parameter Store
3. Your profiles.toml file has correct account and workspace IDs