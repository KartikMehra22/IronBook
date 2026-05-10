# syntax=docker/dockerfile:1.6
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
    go build -trimpath -ldflags="-s -w" -o /out/admission-webhook ./apps/admission-webhook

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/admission-webhook /usr/local/bin/admission-webhook
USER nonroot
ENTRYPOINT ["/usr/local/bin/admission-webhook"]
