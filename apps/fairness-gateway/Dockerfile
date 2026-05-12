# syntax=docker/dockerfile:1.6
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
    go build -trimpath -ldflags="-s -w" -o /out/fairness-gateway ./apps/fairness-gateway

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/fairness-gateway /usr/local/bin/fairness-gateway
USER nonroot
ENTRYPOINT ["/usr/local/bin/fairness-gateway"]
