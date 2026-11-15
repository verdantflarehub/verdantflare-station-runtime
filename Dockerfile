# syntax=docker/dockerfile:1
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api ./api
COPY internal ./internal
COPY cmd ./cmd
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/station-runtime ./cmd/station-runtime && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/station-gpu-agent ./cmd/station-gpu-agent && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/station-cri-status-proxy ./cmd/station-cri-status-proxy

FROM gcr.io/distroless/base-debian12:nonroot
COPY --from=build /out/station-runtime /station-runtime
COPY --from=build /out/station-gpu-agent /station-gpu-agent
COPY --from=build /out/station-cri-status-proxy /station-cri-status-proxy
ENV STATION_RUNTIME_ADDR=0.0.0.0:5052 STATION_RUNTIME_PROBE_ADDR=0.0.0.0:5053
USER 65532:65532
ENTRYPOINT ["/station-runtime"]
