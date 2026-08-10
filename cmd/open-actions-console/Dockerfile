FROM golang:1.25-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/open-actions-console ./cmd/open-actions-console

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/open-actions-console /open-actions-console

ENTRYPOINT ["/open-actions-console"]
