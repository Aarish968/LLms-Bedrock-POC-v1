ARG PYTHON_VERSION=3.10
FROM python:${PYTHON_VERSION}-slim-bookworm

ARG PREFECT_VERSION=0.15.13
ARG PREFECT_EXTRAS=all_orchestratrion_extras
ARG REPO_URL=https://github.com/PrefectHQ/prefect.git

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

RUN --mount=target=/var/lib/apt/lists,type=cache \
    --mount=target=/var/cache/apt,type=cache \
    rm -r /etc/apt/apt.conf.d/docker-clean \
    && apt-get update \
    && apt-get install -y \
    gpg \
    git \
    tini \
    build-essential

# Prefect specific installation
# The following is a workaround for the issue described in https://github.com/PrefectHQ/prefect/pull/7397

WORKDIR /tmp
RUN git clone --depth 1 --branch ${PREFECT_VERSION} ${REPO_URL} prefect \
    && cd prefect \
    && sed -i 's/kubernetes >= 9.0.0a1.0/kubernetes >= 9.0.0a1/' setup.py \
    && pip install --no-cache-dir --no-use-pep517 .[${PREFECT_EXTRAS}]

WORKDIR /
COPY --chmod=755 entrypoint.sh /usr/local/bin/entrypoint.sh



# CommonPrefect
COPY pyproject.toml ./src /tmp/common_tasks_build/
RUN rm -f /wheels \
    && python -m pip install /tmp/common_tasks_build/ \
    && mkdir -p /wheels \
    && python -m pip wheel --no-deps --wheel-dir=/wheels /tmp/common_tasks_build/ \
    && rm -rf /tmp/common_tasks_build



ENTRYPOINT ["tini", "-g", "--", "entrypoint.sh"]
