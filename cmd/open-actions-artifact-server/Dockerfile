FROM golang:1.27.4-trixie AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/open-actions-artifact-server ./cmd/open-actions-artifact-server

FROM gcr.io/distroless/static-debian13:nonroot

COPY --from=build /out/open-actions-artifact-server /open-actions-artifact-server

ENTRYPOINT ["/open-actions-artifact-server"]
