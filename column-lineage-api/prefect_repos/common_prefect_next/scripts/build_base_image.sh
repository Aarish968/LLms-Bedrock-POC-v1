# This script is used to build the base image for the project.
# Ensure we are in the project root directory

set -eou pipefail

if [[ ! -e "base.Dockerfile" ]]; then
  echo "base.Dockerfile not found. Ensure you are in the project root directory"
  cd ..
fi

REPONAME=837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/common-prefect-next
PROJECT_VERSION=$(grep '^version' pyproject.toml | cut -d '"' -f 2)
GIT_SHA=$(git rev-parse --short HEAD)
PY_VERSION=python-3.12

docker build -f base.Dockerfile -t bases/common-prefect-next .
docker tag bases/common-prefect-next "$REPONAME":latest
docker tag "$REPONAME" "$REPONAME":"$PROJECT_VERSION"
docker tag "$REPONAME" "$REPONAME":"$GIT_SHA"
docker tag "$REPONAME" "$REPONAME":"$PY_VERSION"

docker push "$REPONAME":latest
docker push "$REPONAME":"$PROJECT_VERSION"
docker push "$REPONAME":"$GIT_SHA"
docker push "$REPONAME":"$PY_VERSION"
