FROM golang:1.25-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/open-actions-runner ./cmd/open-actions-runner

FROM node:24.19.0-bookworm@sha256:934240a162082fd8b8a2f90cd5114446443f1eba1c5378f6687167ca405e6584 AS node24

FROM node:20.20.2-bookworm@sha256:8f693eaa7e0a8e71560c9a82b55fd54c2ae920a2ba5d2cde28bac7d1c01c9ba5

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git make \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 65532 open-actions \
    && useradd --uid 65532 --gid 65532 --create-home open-actions
COPY --from=build /usr/local/go /usr/local/go
COPY --from=build /out/open-actions-runner /open-actions-runner
COPY --from=node24 /usr/local/bin/node /usr/local/bin/node24

ENV PATH=/usr/local/go/bin:${PATH}

USER 65532:65532
WORKDIR /workspace
ENTRYPOINT ["/open-actions-runner"]
