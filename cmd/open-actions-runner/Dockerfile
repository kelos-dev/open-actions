FROM golang:1.27.4-trixie AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/open-actions-runner ./cmd/open-actions-runner

FROM node:24.19.0-trixie@sha256:e3883d5e4d0b1998cc27f355bd9202cbaf86edc14d07b54b715def0ed4c71093 AS node24

FROM docker:29.7.2-cli@sha256:000bb62ff495f986c9f5578eb67cc2cb98b91138eda81d7762d5371eb8a497fe AS docker-cli

FROM node:20.20.2-trixie@sha256:26dbaa01b7b1e98d004d4b5bdc9502dedd0c8051dc7ae25dfc9fdfa5ee5c5065 AS node20

FROM ubuntu:26.04@sha256:6a06111c3b0a8600feadb270aec771dc239bf407b25625f93f44ab00e11f4d3c

ARG GH_VERSION=2.98.0
ARG KUBECTL_VERSION=1.35.8

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates curl git jq make netcat-openbsd openssl passwd tar tini zstd \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 65532 open-actions \
    && useradd --uid 65532 --gid 65532 --create-home open-actions
RUN set -eu; \
    architecture="$(dpkg --print-architecture)"; \
    case "${architecture}" in \
        amd64) \
            gh_sha256="3b8ac6b30336802fc1a858d7c084e11cdf24ac1a761ca90b68022d7d729208de"; \
            kubectl_sha256="874d5e72dbb819f43cff16bcd1e4f8bac5b7f2361fe1e55049b0a6c676fb0cbf" \
            ;; \
        arm64) \
            gh_sha256="cf689084f3a3618f7eae4a2420d335d74626d65f5e594b9828d125d69f800d86"; \
            kubectl_sha256="cc749967b62f4422260bc9c0aa7a7c55f45175ae38cb8d95767b5d2b7e04c1fd" \
            ;; \
        *) \
            echo "unsupported architecture: ${architecture}" >&2; \
            exit 1 \
            ;; \
    esac; \
    curl -fsSLo /tmp/gh.tar.gz "https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_${architecture}.tar.gz"; \
    echo "${gh_sha256}  /tmp/gh.tar.gz" | sha256sum --check; \
    tar -xzf /tmp/gh.tar.gz -C /tmp; \
    install -m 0755 "/tmp/gh_${GH_VERSION}_linux_${architecture}/bin/gh" /usr/local/bin/gh; \
    curl -fsSLo /tmp/kubectl "https://dl.k8s.io/release/v${KUBECTL_VERSION}/bin/linux/${architecture}/kubectl"; \
    echo "${kubectl_sha256}  /tmp/kubectl" | sha256sum --check; \
    install -m 0755 /tmp/kubectl /usr/local/bin/kubectl; \
    rm -rf /tmp/gh.tar.gz "/tmp/gh_${GH_VERSION}_linux_${architecture}" /tmp/kubectl
COPY --from=node20 /usr/local/ /usr/local/
COPY --from=node20 /opt/ /opt/
COPY --from=build /usr/local/go /usr/local/go
COPY --from=build /out/open-actions-runner /open-actions-runner
COPY --from=node24 /usr/local/bin/node /usr/local/bin/node24
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker

ENV PATH=/usr/local/go/bin:${PATH}

USER 65532:65532
WORKDIR /workspace
ENTRYPOINT ["/usr/bin/tini", "--", "/open-actions-runner"]
