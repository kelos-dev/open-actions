FROM golang:1.25-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/open-actions-runner ./cmd/open-actions-runner

FROM node:20-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git make \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 65532 open-actions \
    && useradd --uid 65532 --gid 65532 --create-home open-actions
COPY --from=build /usr/local/go /usr/local/go
COPY --from=build /out/open-actions-runner /open-actions-runner

ENV PATH=/usr/local/go/bin:${PATH}

USER 65532:65532
WORKDIR /workspace
ENTRYPOINT ["/open-actions-runner"]
