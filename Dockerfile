# syntax=docker/dockerfile:1
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api ./api
COPY internal ./internal
COPY cmd ./cmd
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/station-runtime ./cmd/station-runtime

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/station-runtime /station-runtime
ENV STATION_RUNTIME_ADDR=0.0.0.0:5052 STATION_RUNTIME_PROBE_ADDR=0.0.0.0:5053
USER 65532:65532
ENTRYPOINT ["/station-runtime"]
