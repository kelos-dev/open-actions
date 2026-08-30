ARG OPEN_ACTIONS_RUNNER_IMAGE=ghcr.io/kelos-dev/open-actions-runner:latest
FROM ${OPEN_ACTIONS_RUNNER_IMAGE}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends rsync \
    && rm -rf /var/lib/apt/lists/*

USER 65532:65532
