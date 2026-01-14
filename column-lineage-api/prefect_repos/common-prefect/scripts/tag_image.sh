#!/usr/bin/env bash

set -eoux pipefail

VERSION=$(grep '^__version' ./src/common_tasks/__version__.py | cut -d '"' -f 2 | tr -d '"' | tr -d '[:space:]')
echo "Version: $VERSION"
docker tag bases/common-prefect:latest 837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/common-prefect:latest
docker tag bases/common-prefect:latest 837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/common-prefect:$VERSION
docker push 837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/common-prefect:latest
docker push 837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/common-prefect:$VERSION

