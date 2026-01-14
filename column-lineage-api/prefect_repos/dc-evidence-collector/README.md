This flow is deployed on AWS and is built on Codebuild. Working with internal CodeCommit repos requires a few extra steps

This uses the common-prefect base image

Export Flow Requirements (Excluding the Codecommit repositories)
```bash
uv export --no-hashes --no-dev --no-annotate > ./src/flow_requirements.txt && git add ./src/flow_requirements.txt
```
Export Build Requirements (If using CodeBuild)

```bash
uv export --no-hashes --no-annotate > ./src/build_requirements.txt && git add ./src/build_requirements.txt
```

## dc-evidence-collector

The Collector File is a file generated from an internal system. Users download this file, which helps to determine scope.

The format of this file uses Serial Number as the primary identifier. However, we need to resolve this to Serial Number in order to store them in the database.

This flow leverages Serial Resolution to resolve the Serial Number to the InstanceId. This is nearly 1:1 with the Serial Resolution flow with the exception of the Excel output.

Run Codebuild
```bash
aws codebuild start-build --project-name dc-evidence-collector
```