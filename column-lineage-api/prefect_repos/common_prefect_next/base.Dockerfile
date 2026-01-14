FROM 837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/python:3.12-slim AS base-builder

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=private \
    --mount=target=/var/cache/apt,type=cache,sharing=private \
    rm -f /etc/apt/apt.conf.d/docker-clean \
    && apt-get update && apt-get --no-install-recommends install -y git gpg build-essential

COPY . /tmp/builder

RUN mkdir /tmp/wheels \
    && python -m pip wheel ./tmp/builder --wheel-dir /tmp/wheels /tmp/builder

FROM 837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/python:3.12-slim AS base

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=private \
    --mount=target=/var/cache/apt,type=cache,sharing=private \
    rm -f /etc/apt/apt.conf.d/docker-clean \
    && apt-get update && apt-get --no-install-recommends install -y git tar gzip curl ca-certificates

COPY --from=base-builder /tmp/wheels /tmp/wheels

RUN python -m pip install --root-user-action ignore --no-cache-dir /tmp/wheels/* \
    && rm -rf /tmp/wheels

# Create prefect home
RUN mkdir -p /opt/.prefect

COPY prefect-logging.yml /opt/.prefect/logging.yml
ENV PREFECT_HOME=/opt/.prefect
ENV PREFECT_LOGGING_SETTINGS_PATH=/opt/.prefect/logging.yml

WORKDIR /opt

