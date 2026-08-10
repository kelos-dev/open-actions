FROM golang:1.25-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/open-actions-controller ./cmd/open-actions-controller

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/open-actions-controller /open-actions-controller

ENTRYPOINT ["/open-actions-controller"]
